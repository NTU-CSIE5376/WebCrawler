from __future__ import annotations

import unittest

from scripts import migrate_add_golden_parent_patrol as migration


class GoldenParentPatrolMigrationSqlTest(unittest.TestCase):
    def test_table_name_constant(self):
        self.assertEqual(
            migration.TABLE_NAME, "golden_parent_patrol_state"
        )

    def test_create_table_sql_uses_if_not_exists(self):
        # Idempotency: rerunning the migration must not error.
        self.assertIn("CREATE TABLE IF NOT EXISTS", migration.CREATE_TABLE_SQL)

    def test_create_table_sql_has_primary_key(self):
        self.assertIn("parent_key", migration.CREATE_TABLE_SQL)
        self.assertIn("PRIMARY KEY", migration.CREATE_TABLE_SQL)

    def test_create_table_sql_has_required_lifecycle_columns(self):
        sql = migration.CREATE_TABLE_SQL
        for col in (
            "fetch_url",
            "aliases",
            "parent_domain",
            "shard_id",
            "source_type",
            "status",
            "cadence_bucket",
            "next_patrol_at",
            "last_patrol_at",
            "last_observed_fetch_at",
            "lifetime_golden_child_count",
            "last_seen_new_url_count",
            "consecutive_no_new_url",
            "last_eval_batch_id",
            "consecutive_miss_batches",
            "consecutive_fetch_fail",
            "updated_at",
            "metadata_json",
        ):
            self.assertIn(col, sql, f"missing column: {col}")

    def test_create_table_sql_defaults_status_and_cadence_to_trial(self):
        # Default state for a freshly-enrolled parent: trial / trial. Cron
        # is responsible for promotion / demotion thereafter.
        sql = migration.CREATE_TABLE_SQL
        self.assertIn(
            "status                       TEXT        NOT NULL DEFAULT 'trial'",
            sql,
        )
        self.assertIn(
            "cadence_bucket               TEXT        NOT NULL DEFAULT 'trial'",
            sql,
        )

    def test_due_index_is_partial_excluding_retired(self):
        sql = migration.create_due_index_sql()
        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_golden_parent_patrol_state_due",
            sql,
        )
        self.assertIn("(next_patrol_at)", sql)
        self.assertIn("WHERE status <> 'retired'", sql)

    def test_due_index_targets_patrol_state_table(self):
        sql = migration.create_due_index_sql()
        self.assertIn("ON golden_parent_patrol_state", sql)
        self.assertNotIn("url_state_current", sql)

    def test_domain_index_unconditional(self):
        sql = migration.create_domain_index_sql()
        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_golden_parent_patrol_state_domain",
            sql,
        )
        self.assertIn("(parent_domain)", sql)
        # Domain index is for ad-hoc analysis; no partial WHERE clause.
        self.assertNotIn(" WHERE ", sql)


if __name__ == "__main__":
    unittest.main()
