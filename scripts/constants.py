"""Shared constants for scripts."""

NUM_SHARDS = 256

CRAWLERDB = dict(host="172.16.191.1", port=5432, user="crawler", password="crawler", dbname="crawlerdb")
METRICDB = dict(host="172.16.191.1", port=5433, user="metric", password="metric", dbname="metricdb")

# url_state_current.source values
SOURCE_NATURAL = 0
SOURCE_GOLDEN = 1
