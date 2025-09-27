-- Adjust storage credentials/locations for your environment

-- 1) Create catalog & schema
CREATE CATALOG IF NOT EXISTS retail;
USE CATALOG retail;
CREATE SCHEMA IF NOT EXISTS core;

-- (Optional) Create external locations & volumes here if needed.
