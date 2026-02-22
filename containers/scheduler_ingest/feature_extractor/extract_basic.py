from datetime import datetime, timezone

def extract_basic(rec: dict):
    if rec.get("status") != "ok":
        return None

    return {
        "url": rec.get("url"),
        "shard_id": int(rec.get("shard_id")),
        "domain_id": int(rec.get("domain_id")),
        "fetched_at": datetime.fromisoformat(rec["fetched_at"]) if rec.get("fetched_at") else datetime.now(timezone.utc),
        "content_length": len(rec.get("content", "")),
        "content_hash": rec.get("content_hash"),
        "num_links": len(rec.get("outlinks", [])),
    }

