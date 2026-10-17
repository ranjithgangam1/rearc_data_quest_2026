-- Unity Catalog access controls: read-only analyst access to the Gold layer only.
--
-- Principle of least privilege:
--   * Analysts can read the three Gold tables and nothing else. Bronze (raw) and Silver (partially modeled) tables
--     are never granted, so analysts cannot see unvetted data or the Volume with the raw files.
--   * Unity Catalog has no DENY. Access is "none" unless explicitly granted, so omitting grants is the control.
--   * Analysts get no CREATE / MODIFY privileges on anything. The pipeline owner (a user or service principal)
--     owns and writes the tables.
--
-- Run as the owner of the catalog/schema (or a metastore admin).
-- Replace `analysts` with your real group. Create the group first (account console -> User management -> Groups).
-- NOTE: single-user environments such as Free Edition may not allow creating groups. In that case, validate the
-- statements by running them against a user or group you can create, and by reading SHOW GRANTS output.

-- 1) Let the group reach the catalog and schema (traversal only; this does not expose any table by itself).
GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  workspace.rearc_quest TO `analysts`;

-- 2) Read-only access to the Gold tables (materialized views are granted as tables).
GRANT SELECT ON TABLE workspace.rearc_quest.gold_population_stats                  TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_best_year_by_series               TO `analysts`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_prs30006032_q01_with_population   TO `analysts`;

-- 3) Verify what the group can do.
SHOW GRANTS `analysts` ON SCHEMA  workspace.rearc_quest;
SHOW GRANTS `analysts` ON TABLE   workspace.rearc_quest.gold_best_year_by_series;

-- Expected: USE SCHEMA on the schema; SELECT only on the three gold_* tables.
-- Not expected: any grant on bronze_* / silver_* tables or on the raw Volume.

-- 4) Negative test (run as a member of `analysts`, or as the analyst user): this should fail with a permission error.
-- SELECT * FROM workspace.rearc_quest.bronze_bls_pr_data LIMIT 1;

-- ---------------------------------------------------------------------------------------------
-- Notes
-- * Querying materialized views requires a serverless or Pro SQL warehouse; give the group CAN USE on the warehouse
--   (SQL Warehouses -> Permissions).
-- * To use the Genie space, analysts also need access to the space itself (Share -> add the group with CAN RUN).
-- * Stronger isolation for a real deployment: publish Gold to its own schema (for example workspace.rearc_quest_gold)
--   and grant on that schema, so analysts never have USE SCHEMA on the schema that holds Bronze and Silver.
--   This project keeps one schema to keep the pipeline simple.
-- * Grants on the Volume are separate (READ VOLUME / WRITE VOLUME). Analysts get none.
-- ---------------------------------------------------------------------------------------------
