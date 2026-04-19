-- ============================================================
-- Drug Risk Intelligence Pipeline - Phase 1: Environment Setup
-- Snowflake DDL with RBAC Best Practices
-- ============================================================

-- Use role with account admin privileges
USE ROLE SECURITYADMIN;

-- --------------------------------------------------------
-- 1. CREATE WAREHOUSE
-- --------------------------------------------------------
CREATE OR REPLACE WAREHOUSE DRUG_RISK_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_SIZE = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for Drug Risk Intelligence Pipeline';

-- Grant usage on warehouse to roles
GRANT USAGE ON WAREHOUSE DRUG_RISK_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE DRUG_RISK_WH TO ROLE DBT_ROLE;

-- --------------------------------------------------------
-- 2. CREATE DATABASE
-- --------------------------------------------------------
CREATE OR REPLACE DATABASE DRUG_INTEL_DB
    COMMENT = 'Drug Intelligence Database - FAERS Data';

-- --------------------------------------------------------
-- 3. CREATE SCHEMAS
-- --------------------------------------------------------
-- RAW Schema - Read-only for dbt, managed by data engineering
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.RAW
    COMMENT = 'Raw FAERS data from JSON ingestion';

-- STAGING Schema - Transformed data for dbt processing
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.STAGING
    COMMENT = 'Staging layer for dbt transformations';

-- ANALYTICS Schema - Final analytical tables
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.ANALYTICS
    COMMENT = 'Analytics layer with final analytical tables';

-- --------------------------------------------------------
-- 4. CREATE ROLE-BASED ACCESS CONTROL (RBAC)
-- --------------------------------------------------------
-- Create dedicated roles
CREATE OR REPLACE ROLE DATA_ENG_ROLE
    COMMENT = 'Role for Data Engineers - full access to RAW';

CREATE OR REPLACE ROLE DBT_ROLE
    COMMENT = 'Role for dbt - staging/analytics write, raw read';

-- Grant schema permissions to DATA_ENG_ROLE (full access)
GRANT ALL ON SCHEMA DRUG_INTEL_DB.RAW TO ROLE DATA_ENG_ROLE;
GRANT ALL ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DATA_ENG_ROLE;
GRANT ALL ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DATA_ENG_ROLE;

-- Grant schema permissions to DBT_ROLE (RBAC: read RAW, write STAGING/ANALYTICS)
GRANT USAGE ON DATABASE DRUG_INTEL_DB TO ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE DRUG_RISK_WH TO ROLE DBT_ROLE;

-- RAW schema - read only (no ownership, no future grants)
GRANT USAGE ON SCHEMA DRUG_INTEL_DB.RAW TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DRUG_INTEL_DB.RAW TO ROLE DBT_ROLE;

-- STAGING schema - read/write
GRANT USAGE ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DBT_ROLE;
GRANT CREATE VIEW ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DBT_ROLE;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DBT_ROLE;
GRANT SELECT ON SCHEMA DRUG_INTEL_DB.STAGING TO ROLE DBT_ROLE;

-- ANALYTICS schema - read/write
GRANT USAGE ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DBT_ROLE;
GRANT CREATE VIEW ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DBT_ROLE;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DBT_ROLE;
GRANT SELECT ON SCHEMA DRUG_INTEL_DB.ANALYTICS TO ROLE DBT_ROLE;

-- --------------------------------------------------------
-- 5. CREATE RAW VARIANT TABLE FOR FAERS JSON DATA
-- --------------------------------------------------------
USE ROLE DATA_ENG_ROLE;
USE WAREHOUSE DRUG_RISK_WH;
USE DATABASE DRUG_INTEL_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE FAERS_RAW_DATA (
    FILE_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    FILE_NAME VARCHAR(255) NOT NULL,
    INGESTION_Timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_JSON VARIANT NOT NULL,
    FILE_STATUS VARCHAR(20) DEFAULT 'PENDING'
)
    COMMENT = 'Raw FAERS data stored as JSON variant'
    STAGE_FILE_FORMAT = (TYPE = 'JSON')
    DATA_RETENTION_CLONE = 7;

-- --------------------------------------------------------
-- 6. CREATE STAGE FOR FILE INGESTION
-- --------------------------------------------------------
CREATE OR REPLACE STAGE FAERS_RAW_STAGE
    URL = 's3://your-bucket/faers-json-files/'  -- Replace with actual S3 bucket
    STORAGE_INTEGRATION = S3_INTEGRATION  -- Replace with actual storage integration
    CREDENTIALS = (AWS_KEY_ID = 'AWS_KEY' AWS_SECRET_KEY = 'AWS_SECRET')  -- Use STORAGE_INTEGRATION instead in production
    FILE_FORMAT = (TYPE = 'JSON')
    COMMENT = 'Stage for FAERS JSON file ingestion';

-- Alternative: Internal stage for local file upload
CREATE OR REPLACE STAGE FAERS_INTERNAL_STAGE
    FILE_FORMAT = (TYPE = 'JSON')
    COMMENT = 'Internal stage for local JSON file upload';

-- Grant read access to DBT_ROLE for staging files
GRANT READ ON STAGE DRUG_INTEL_DB.RAW.FAERS_INTERNAL_STAGE TO ROLE DBT_ROLE;

-- --------------------------------------------------------
-- 7. CREATE FILE FORMAT
-- --------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FAERS_JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE_FEATURES = FALSE
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON file format for FAERS data';

-- ============================================================
-- SUMMARY: RBAC PERMISSIONS MATRIX
-- ============================================================
-- | Resource       | DATA_ENG_ROLE | DBT_ROLE |
-- |-----------------|---------------|----------|
-- | RAW Schema      | ALL           | SELECT  |  -- dbt reads raw for transformation
-- | STAGING Schema  | ALL           | ALL     |  -- dbt writes staging
-- | ANALYTICS Schema| ALL           | ALL     |  -- dbt writes analytics
-- | Warehouse      | USAGE         | USAGE   |
-- ============================================================