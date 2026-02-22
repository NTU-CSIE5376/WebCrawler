from __future__ import annotations

from datetime import datetime, date
from typing import Dict, Any

from sqlalchemy import func, select, text, Integer
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from libs.db import SummaryDaily, DomainStatsDaily, DomainState


def get_summary_daily(session: Session, event_date: date) -> SummaryDaily:
    """
    Ensure summary_daily(event_date) exists, return ORM row.
    """
    row = session.get(SummaryDaily, event_date)
    if row is None:
        row = SummaryDaily(event_date=event_date)
        session.add(row)
        session.flush()
    return row

def get_domain_stats_daily(
    session: Session,
    domain_id: int,
    event_date: date,
) -> DomainStatsDaily:
    """
    Ensure domain_stats_daily(domain_id, event_date) exists.
    """
    row = session.get(DomainStatsDaily, {"domain_id": domain_id, "event_date": event_date})
    if row is None:
        domain_row = session.get(DomainState, domain_id)
        if domain_row is None:
            # unknown domain
            return None

        row = DomainStatsDaily(domain_id=domain_id, event_date=event_date, shard_id=domain_row.shard_id)
        session.add(row)
        session.flush()
    return row

def add_scalar_fields(target, delta: Dict[str, int]):
    for field, value in delta.items():
        if field == "fail_reasons":
            continue
        if hasattr(target, field):
            setattr(target, field, (getattr(target, field) or 0) + int(value))


def add_fail_reasons(session: Session, model, filters: Dict[str, Any], increments: Dict[str, int]):
    if not increments:
        return

    table = model.__table__
    expr = model.fail_reasons

    for reason, count in increments.items():
        expr = func.jsonb_set(
            expr,
            text(f"'{{{reason}}}'"),
            func.to_jsonb(
                func.coalesce(
                    model.fail_reasons[reason].astext.cast(Integer),
                    0,
                ) + int(count)
            ),
            True,
        )

    conds = [getattr(model, k) == v for k, v in filters.items()]
    session.execute(
        table.update()
        .where(*conds)
        .values(fail_reasons=expr)
    )


def apply_stats_delta(session: Session, delta: dict):
    """
    Apply ONE stats delta dict.
    Does NOT commit; caller controls transaction.
    """
    now = delta.get("generated_at")
    if now:
        day = datetime.fromisoformat(now).date()
    else:
        day = date.today()

    counters = delta.get("counters") or {}
    domains = delta.get("domains") or {}

    # ---- global summary ----
    summary = get_summary_daily(session, day)
    add_scalar_fields(summary, counters)

    if counters.get("fail_reasons"):
        add_fail_reasons(
            session,
            SummaryDaily,
            {"event_date": day},
            counters["fail_reasons"],
        )

    # ---- per-domain daily stats ----
    for domain_id_raw, stats in domains.items():
        try:
            domain_id = int(domain_id_raw)
        except Exception:
            continue

        row = get_domain_stats_daily(
            session,
            domain_id,
            day
        )
        if not row:
            print(f"[stats] ERROR domain_id {domain_id} not exists", flush=True)
            continue

        add_scalar_fields(row, stats)

        if stats.get("fail_reasons"):
            add_fail_reasons(
                session,
                DomainStatsDaily,
                {"domain_id": domain_id, "event_date": day},
                stats["fail_reasons"],
            )

