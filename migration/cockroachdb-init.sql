-- Provisioning for a CockroachDB-backed DAS database. Run once, then migrate:
--
--   cockroach sql --host=<node> -f migration/cockroachdb-init.sql
--   INIT_FILE_PATH=init.sql \
--   DATABASE_URL='postgres://root@<node>:26257/das?sslmode=disable&options=-c%20default_int_size%3D4' \
--     migration up
--
-- The `options=-c default_int_size=4` in the migrator's URL is REQUIRED. Postgres
-- `integer` is 32-bit but CockroachDB's `INT` defaults to 64-bit, and the DAS entities
-- decode tokens.decimals, asset.royalty_amount, asset_creators.share and four more as
-- i32 - sqlx rejects INT8 for them ("mismatched types ... INT4 ... INT8") the moment a
-- row exists. The setting only matters while the schema is created, so the ingester and
-- das_api don't need it.
--
-- It has to be on the connection: tested on v26.2.6, `ALTER DATABASE das SET
-- default_int_size = 4` (and `ALTER ROLE ALL IN DATABASE ...`) is stored but NOT applied
-- when DDL is parsed, and a `SET` earlier in the same batch doesn't apply either.

CREATE DATABASE IF NOT EXISTS das;

-- sqlx uses the extended query protocol, which CockroachDB treats as an implicit
-- transaction for DDL. Lets migrations run schema changes through it.
ALTER DATABASE das SET autocommit_before_ddl = true;
