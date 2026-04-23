-- ============================================================
-- Drug Risk Intelligence Pipeline - Phase 1: Environment Setup
-- Snowflake DDL with RBAC Best Practices
-- ============================================================

-- Use role with account admin privileges
USE ROLE ACCOUNTADMIN;

-- --------------------------------------------------------
-- 1. CREATE WAREHOUSE
-- --------------------------------------------------------
CREATE OR REPLACE WAREHOUSE DRUG_RISK_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- --------------------------------------------------------
-- 2. CREATE DATABASE
-- --------------------------------------------------------
CREATE OR REPLACE DATABASE DRUG_INTEL_DB;

-- --------------------------------------------------------
-- 3. SCHEMAS AND TABLES
-- --------------------------------------------------------
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.RAW;
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.STAGING;
CREATE OR REPLACE SCHEMA DRUG_INTEL_DB.ANALYTICS;

USE DATABASE DRUG_INTEL_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE FAERS_RAW_DATA (
    FILE_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    FILE_NAME VARCHAR(255) NOT NULL,
    INGESTION_Timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_JSON VARIANT NOT NULL,
    FILE_STATUS VARCHAR(20) DEFAULT 'PENDING'
)
    DATA_RETENTION_TIME_IN_DAYS = 1;


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