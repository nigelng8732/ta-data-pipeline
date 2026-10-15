-- To connect Snowflake with our data from the S3 bucket, we would have to set up the storage integration. 
-- The steps taken are from the official Snowflake documentation guide listed here.
-- https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration 
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION ta_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::915357406572:role/ta-snowflake-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ta-data-pipeline-project/ta/snapshots/')

-- Retrieve the ARN for the IAM role, we use the DESCRIBE function
DESC INTEGRATION ta_s3_integration;