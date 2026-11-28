"""Standard-Python URL policy implementation, without Arrow dependencies."""
from .trap_rules import URLPolicy, selected_rules, match_rules
from .url_normalize import strip_named_params, strip_params


def evaluate(urls, policy=None):
    """Return decision dictionaries for an iterable of str/None URLs.

    Results have the same fields, rule order and decisions as the columnar
    backend. The function preserves input order and performs no I/O.
    """
    policy = URLPolicy() if policy is None else policy
    names = selected_rules(policy)
    decisions = []
    for url in urls:
        normalized, shadow, hits = url, None, set()
        if policy.enabled and url:
            if policy.normalize:
                normalized = strip_named_params(url,policy.normalize_params)
            if policy.shadow_normalize:
                full = strip_params(url,generic_session_tokens=True)
                shadow = full if full != normalized else None
            if policy.drop:
                hits = set(match_rules(normalized)).intersection(names)
        decisions.append({
            'url':url, 'normalized_url':normalized, 'shadow_url':shadow,
            **{name:name in hits for name in names}, 'dropped':bool(hits),
        })
    return decisions
