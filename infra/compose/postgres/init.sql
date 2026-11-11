-- Runs once, when the Postgres volume is first created.
-- Polaris's JDBC scripts expect this schema to exist already.
CREATE SCHEMA IF NOT EXISTS polaris_schema;
