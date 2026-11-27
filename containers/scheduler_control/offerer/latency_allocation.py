"""Value allocation with soft latency targets and bounded MIP solving."""
import heapq
import logging
import math
import time

log = logging.getLogger('offerer')


def allocate_jobs(candidates, pending, performance, *, url_budget, domain_slots,
                  per_domain_cap, max_active_domains, horizon_sec=60, probe_urls=8,
                  overload_penalty=.2, method='mip', solve_seconds=1):
    """Observed capacity is a target, never a hard bound on the URL allocation.

    max sum(v*x + exploration_bonus*y) - lambda*(s1+4*s2+16*s3)
    p+x <= target+s1+s2+s3; 0<=s1,s2<=target; s3>=0.
    Hard bounds: available frontier, batch/global URL budget, and domain slots.
    """
    if method not in ('mip', 'greedy'):
        raise ValueError('Job allocation must be mip or greedy')
    unique = {c.domain_id:c for c in reversed(list(candidates))
              if math.isfinite(c.allocation_value) and c.allocation_value>0}
    rows = sorted(unique.values(), key=lambda c:(-c.allocation_value,c.domain_id))
    rows = [c for c in rows if min(per_domain_cap,c.available_urls if c.available_urls is not None else per_domain_cap)>0]
    if not rows or url_budget<=0 or domain_slots<=0:
        return {},dict(method='none',elapsed_ms=0,targets={})
    scale = max(c.allocation_value for c in rows)
    caps=[];targets=[];bonuses=[]
    for c in rows:
        caps.append(min(per_domain_cap,c.available_urls if c.available_urls is not None else per_domain_cap))
        stats=performance.get(c.domain_id,{})
        latency=stats.get('service_latency_sec',0);parallel=stats.get('observed_parallelism',0);n=stats.get('samples',0)
        valid=all(isinstance(v,(int,float)) and math.isfinite(v) and v>0 for v in (latency,parallel,n))
        target=max(probe_urls,horizon_sec*parallel/latency) if valid and n>=3 else probe_urls
        targets.append(min(float(target),1000000.0))
        # Value-weighted exploration rewards learning, without a site-name rule.
        bonuses.append((c.allocation_value/scale)*probe_urls*.2/math.sqrt(max(0,n)+1) if valid else (c.allocation_value/scale)*probe_urls*.2)
    active={key for key,value in pending.items() if value>0}
    free_new=max(0,max_active_domains-len(active))
    def penalty(excess,target):
        excess=max(0,excess)
        return overload_penalty*(min(excess,target)+4*min(max(0,excess-target),target)+16*max(0,excess-2*target))
    def objective(plan):
        result=0
        for i,c in enumerate(rows):
            x=plan.get(c.domain_id,0);old=pending.get(c.domain_id,0)-targets[i]
            result+=(c.allocation_value/scale)*x+(bonuses[i] if x else 0)-penalty(old+x,targets[i])+penalty(old,targets[i])
        return result
    # Marginal-value heuristic uses exactly the same soft-target objective.
    plan={};heap=[]
    def marginal(i,count):
        c=rows[i];old=pending.get(c.domain_id,0)+count-targets[i]
        return c.allocation_value/scale-penalty(old+1,targets[i])+penalty(old,targets[i])+(bonuses[i] if count==0 else 0)
    for i in range(len(rows)):heapq.heappush(heap,(-marginal(i,0),i))
    added_new=0;remaining=url_budget
    while heap and remaining>0:
        neg,i=heapq.heappop(heap);c=rows[i];count=plan.get(c.domain_id,0)
        if neg>=0:break
        if count==0:
            if len(plan)>=domain_slots or (c.domain_id not in active and added_new>=free_new):continue
            added_new+=int(c.domain_id not in active)
        plan[c.domain_id]=count+1;remaining-=1
        if count+1<caps[i]:heapq.heappush(heap,(-marginal(i,count+1),i))
    baseline=plan;started=time.monotonic();used='greedy';status=None
    if method=='mip':
        try:
            import numpy as np
            from scipy.optimize import Bounds,LinearConstraint,milp
            from scipy.sparse import lil_matrix
            n=len(rows);matrix=lil_matrix((3+3*n,5*n));lower=np.full(3+3*n,-np.inf);upper=np.zeros(3+3*n)
            matrix[0,:n]=1;upper[0]=url_budget;matrix[1,n:2*n]=1;upper[1]=domain_slots
            for i,c in enumerate(rows):
                matrix[2,n+i]=int(c.domain_id not in active)
                matrix[3+3*i,i]=1;matrix[3+3*i,n+i]=-caps[i]
                matrix[4+3*i,n+i]=1;matrix[4+3*i,i]=-1
                matrix[5+3*i,i]=1
                for band in range(3):matrix[5+3*i,(2+band)*n+i]=-1
                upper[5+3*i]=targets[i]-pending.get(c.domain_id,0)
            upper[2]=free_new
            costs=[-c.allocation_value/scale for c in rows]+[-b for b in bonuses]+[overload_penalty]*n+[4*overload_penalty]*n+[16*overload_penalty]*n
            result=milp(c=costs,integrality=[1]*(2*n)+[0]*(3*n),
                        bounds=Bounds(np.zeros(5*n),caps+[1]*n+targets+targets+[np.inf]*n),
                        constraints=LinearConstraint(matrix.tocsc(),lower,upper),
                        options={'time_limit':solve_seconds,'mip_rel_gap':.01})
            status=result.status
            if result.x is not None:
                amounts=[int(round(x)) for x in result.x[:n]]
                proposed={c.domain_id:x for c,x in zip(rows,amounts) if x>0}
                if (all(0<=x<=cap for x,cap in zip(amounts,caps)) and sum(amounts)<=url_budget
                    and len(proposed)<=domain_slots and sum(key not in active for key in proposed)<=free_new
                    and objective(proposed)>=objective(baseline)-1e-7):
                    plan=proposed;used='mip'
            if used!='mip':log.warning('offer.job_mip_fallback',extra={'event':'offer.job_mip_fallback','solver_status':status})
        except ImportError:
            log.warning('offer.job_mip_fallback',extra={'event':'offer.job_mip_fallback','reason':'solver_unavailable'})
    return plan,dict(method=used,solver_status=status,elapsed_ms=round((time.monotonic()-started)*1000,3),
                     objective=objective(plan),targets={str(c.domain_id):round(t,2) for c,t in zip(rows,targets)},
                     allocated_urls=sum(plan.values()))
