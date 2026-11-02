-- =====================================================
-- SETUP NOTEBOOK
-- Purpose: Initialize catalog, schemas, and governance
--          objects BEFORE the DLT pipeline runs.
-- Runs as: Job Task (standalone notebook, not inside DLT)
-- =====================================================

-- COMMAND ----------
-- Create Catalog (idempotent - safe to re-run)

-- CREATE CATALOG IF NOT EXISTS ${catalog};

-- USE CATALOG ${catalog};

-- COMMAND ----------
-- Create Schemas for every layer

-- CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
-- COMMENT 'Raw streaming ingestion layer - untouched source data + audit columns';

-- CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
-- COMMENT 'Cleaned, validated, deduplicated, SCD-managed data';

-- CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
-- COMMENT 'Business-ready facts, dimensions, and KPIs';

-- CREATE SCHEMA IF NOT EXISTS ${catalog}.ops
-- COMMENT 'Pipeline observability: run metrics, data quality trends and other ops';

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.bronze');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.silver');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.gold');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.ops');

SHOW SCHEMAS IN IDENTIFIER(:catalog);

SELECT
    :catalog AS catalog_initialized,
    :env AS environment,
    current_timestamp() AS setup_run_at;

-- COMMAND ----------
-- Monitoring table: tracks health of every pipeline run
-- Your `data_quality` task will INSERT into this after each run

-- CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_metrics (
--     run_id               STRING,
--     pipeline_name        STRING,
--     layer                STRING,
--     table_name           STRING,
--     rows_processed       BIGINT,
--     rows_quarantined     BIGINT,
--     quarantine_pct       DOUBLE,
--     run_timestamp        TIMESTAMP,
--     status               STRING,
--     env                  STRING
-- )
-- USING DELTA
-- COMMENT 'Row counts, quarantine %, and status per table, per run - used for trend monitoring and alerting';

-- COMMAND ----------
-- (Optional but recommended) Governance: restrict access by layer
-- Uncomment and adjust group names to match your workspace's account groups

-- Analysts/BI tools: Gold layer ONLY
-- GRANT USAGE ON CATALOG ${catalog_name} TO `analysts`;
-- GRANT USAGE ON SCHEMA gold TO `analysts`;
-- GRANT SELECT ON SCHEMA gold TO `analysts`;

-- Data engineers: full access to bronze/silver/quarantine
-- GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA silver TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA quarantine TO `data_engineers`;

-- COMMAND ----------
-- Verification

-- SHOW SCHEMAS IN ${catalog};

-- SELECT 
--   '${catalog}' AS catalog_initialized,
--   '${env}' AS environment,
--   current_timestamp() AS setup_run_at; :param_1