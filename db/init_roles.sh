#!/bin/sh

# This runs during first-time Postgres initialization and applies env-backed role passwords.
POSTGRES_USER="${POSTGRES_USER:?POSTGRES_USER must be set}"
POSTGRES_DB="${POSTGRES_DB:?POSTGRES_DB must be set}"
ETL_WRITER_PASSWORD="${ETL_WRITER_PASSWORD:?ETL_WRITER_PASSWORD must be set}"
BI_READER_PASSWORD="${BI_READER_PASSWORD:?BI_READER_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=etl_writer_password="$ETL_WRITER_PASSWORD" \
  --set=bi_reader_password="$BI_READER_PASSWORD" <<'SQL'
SELECT 'CREATE ROLE etl_writer WITH LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_writer');
\gexec

ALTER ROLE etl_writer WITH PASSWORD :'etl_writer_password';

SELECT 'CREATE ROLE bi_reader WITH LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_reader');
\gexec

ALTER ROLE bi_reader WITH PASSWORD :'bi_reader_password';

GRANT USAGE, CREATE ON SCHEMA curated TO etl_writer;
GRANT USAGE, CREATE ON SCHEMA audit   TO etl_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA curated TO etl_writer;
GRANT SELECT, INSERT, UPDATE         ON ALL TABLES IN SCHEMA audit   TO etl_writer;

GRANT USAGE  ON SCHEMA curated TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO bi_reader;
GRANT USAGE  ON SCHEMA audit   TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA audit   TO bi_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT ALL    ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit   GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit   GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_writer;
SQL
