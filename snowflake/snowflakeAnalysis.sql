-- 1 Organization -> X Accounts ; (X > 0)
-- Snowflake account:  https://ivwojpw-oo06449.snowflakecomputing.com

-- Account parameters/state:

    -- (S.1)
    -- SHOW PARAMETERS
    --   [ LIKE '<pattern>' ]
    --   [ { IN | FOR } {
    --         { SESSION | ACCOUNT }
    --       | { USER | WAREHOUSE | DATABASE | SCHEMA | TASK } <name>
    --       | TABLE [ <table_or_view_name> ]
    --     } ]
    show parameters in account ivwojpw.oo06449; -- "bash env" in Snowflake

    -- (A.1)
    -- Alter account parameters syntax: (on same account)
    -- ALTER ACCOUNT <name> SET { [ accountParams ] [ objectParams ] [ sessionParams ] }
    show parameters like 'WEEK_START' in account;

-- Altering state: of {ACCOUNT | WAREHOUSE | DATABASE | SCHEMA | TASK | USER | SESSION }

    -- (A)
    -- ALTER {ACCOUNT | WAREHOUSE | DATABASE | SCHEMA | TASK | USER | SESSION } ... ;

    alter account set WEEK_START=1;
    -- (A.1.1)
    -- Alter account parameters syntax: (on different account)
    -- ALTER ACCOUNT <name> ...
    -- alter account oo06449 rename to dios; (Error)

-- Account functions/operations:

-- (S.2)
-- SHOW FUNCTIONS [ LIKE '<pattern>' ]
--                [ IN
--                     {
--                       ACCOUNT                       |
--                       CLASS <class_name>            |
--                       DATABASE <database_name>      |
--                       MODEL <model_name>
--                          [ VERSION <version_name> ] |
--                       SCHEMA <database_name>.<schema_name>
--                     }
--                ]
show functions in account; -- "bash man" in Snowflake

-- Important state identifiers:
show functions like '%current%' in account;
-- (Q.1)
select current_organization_name();
select current_account_name(); -- ??? Where is it in "man" ???


-- Ignoring keywords till later:
    -- "tags"
    -- "connection"

-- Regions: do not limit user access to Snowflake;
-- They only dictate the geographic location where data is stored and compute resources are provisioned and the prices of such resources.

-- (S.3)
-- SHOW REGIONS [ LIKE '<pattern>' ]
show regions;


-- Warehouses:
    -- (C.0)
    -- CREATE WAREHOUSE IF NOT EXISTS <name>
    --        [ objectProperties ]
    --        [ TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

    -- where 'objectProperties' are:
    --   WAREHOUSE_TYPE = { STANDARD | 'SNOWPARK-OPTIMIZED' }
    --   WAREHOUSE_SIZE = { XSMALL | SMALL | MEDIUM | LARGE | XLARGE | XXLARGE | XXXLARGE | X4LARGE | X5LARGE | X6LARGE }
    --   MAX_CLUSTER_COUNT = <num>
    --   MIN_CLUSTER_COUNT = <num>
    --   SCALING_POLICY = { STANDARD | ECONOMY }
    --   AUTO_SUSPEND = { <num> | NULL }
    --   AUTO_RESUME = { TRUE | FALSE }
    --   INITIALLY_SUSPENDED = { TRUE | FALSE }
    --   RESOURCE_MONITOR = <monitor_name>
    --   COMMENT = '<string_literal>'
    --   ENABLE_QUERY_ACCELERATION = { TRUE | FALSE }
    --   QUERY_ACCELERATION_MAX_SCALE_FACTOR = <num>

    --   MAX_CONCURRENCY_LEVEL = <num>
    --   STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = <num>
    --   STATEMENT_TIMEOUT_IN_SECONDS = <num>

create warehouse if not exists calculator
    WAREHOUSE_TYPE =  STANDARD
    WAREHOUSE_SIZE = XSMALL
    MAX_CLUSTER_COUNT = 5
    MIN_CLUSTER_COUNT = 1
    AUTO_SUSPEND = 180
    AUTO_RESUME = FALSE
    INITIALLY_SUSPENDED = TRUE;

    -- (U.0)
    -- USE WAREHOUSE <name> ;
use warehouse calculator;

    -- (A.0)
    -- ALTER WAREHOUSE IF EXISTS <name> { SUSPEND | RESUME | SET | RENAME TO | ABORT ALL QUERIES } ... ;
alter warehouse if exists calculator resume;

    -- (D.0)
    -- DROP WAREHOUSE IF EXISTS <name> ;
drop warehouse if exists calculator;

    -- (S.0)
    -- SHOW warehouses [LIKE '<pattern>'];
show warehouses;





-- 1 Account -> X Users ; (X > 0)

-- Users:
-- (C.1)
-- CREATE USER IF NOT EXISTS <name>
--   [ objectProperties ]
--   [ objectParams ]
--   [ sessionParams ]

-- where objectProperties define the user-state as ::=
--   PASSWORD = '<string>'
--   LOGIN_NAME = <string>
--   DISPLAY_NAME = <string>
--   FIRST_NAME = <string>
--   MIDDLE_NAME = <string>
--   LAST_NAME = <string>
--   EMAIL = <string>
--   MUST_CHANGE_PASSWORD = TRUE | FALSE
--   DISABLED = TRUE | FALSE
--   DAYS_TO_EXPIRY = <integer>
--   MINS_TO_UNLOCK = <integer>
--   DEFAULT_WAREHOUSE = <string>
--   DEFAULT_NAMESPACE = <string>
--   DEFAULT_ROLE = <string>
--   DEFAULT_SECONDARY_ROLES = ( 'ALL' )
--   ... et cetera
create user if not exists name_already_taken
    password= 'tooshort'
    login_name= 'taken'
    display_name= 'nuser'
    must_change_password = false;

-- (A.2)
-- ALTER USER IF EXISTS <name> { SET | UNSET | RENAME TO | ... } ...
alter user if exists name_already_taken rename to lemon;

-- (S.4.1)
show users;

-- (S.4.2)
describe user migs;

-- (D.1)
-- DROP USER IF EXISTS <name>
-- drop user if exists lemon;



-- 1 User -> X Roles ; (X >= 0)
-- Roles:

-- Roles own Snowflake securabel-objects(database, schema, table, stage, ...)
-- Snowflake securable-objects may be altered only by privileged roles.
-- Roles are also securable-objects.

-- (C.6.1?)
-- CREATE ROLE IF NOT EXISTS <name>
--   [ COMMENT = '<string_literal>' ]
--   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

-- (C.6.2?)
-- CREATE DATABASE ROLE IF NOT EXISTS <name>
--   [ COMMENT = '<string_literal>' ]

create role if not exists worker
    comment = 'there is only one';

-- (U.3)
USE ROLE WORKER;

grant role worker to role SYSADMIN;

grant role worker to user lemon;

alter user if exists lemon set
    default_warehouse = calculator
    default_role = worker;





-- 1 Role -> X Privileges (X >= 0)
-- Privileges: Each role can be granted multiple access privileges

-- (G.0)
-- GRANT {

    -- { globalPrivileges | ALL [ PRIVILEGES ] } ON ACCOUNT

    -- | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON
    -- { USER | RESOURCE MONITOR | WAREHOUSE | COMPUTE POOL |
    -- DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP | EXTERNAL VOLUME } <object_name>

    -- NOTE: The above two are ACCOUNT-ROLE privileges only.


    -- | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON
    -- { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }

    -- | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }

    -- | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON
    -- { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }

    -- | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE
    -- <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
--       }
--   TO [ DATABASE ] ROLE <role_name> [ WITH GRANT OPTION ]

grant usage on warehouse calculator to role worker;
grant select on all tables to worker;


-- (R.0)
    -- REVOKE [ GRANT OPTION FOR ]
    --     {
    --        { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
    --      | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON
    --      { RESOURCE MONITOR | WAREHOUSE | COMPUTE POOL | DATABASE |
    --      INTEGRATION | FAILOVER GROUP | REPLICATION GROUP | EXTERNAL VOLUME } <object_name>

    --      | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON
    --      { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }

    --      | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }

    --      | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON
    --      { <object_type> <object_name> | ALL <object_type_plural> IN SCHEMA <schema_name> }

    --      | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE
    --      <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
    --     }
    --   FROM [ ROLE ] <role_name> [ RESTRICT | CASCADE ]



-- Databases:

-- (C.0)
    -- CREATE [ TRANSIENT ] DATABASE IF NOT EXISTS <name>
    --     [ CLONE <source_db>
    --           [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
    --     [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
    --     [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
    --     [ EXTERNAL_VOLUME = <external_volume_name> ]
    --     [ CATALOG = <catalog_integration_name> ]
    --     [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
    --     [ COMMENT = '<string_literal>' ]
    --     [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]


create database if not exists taskdb;

use database taskdb;
create database role if not exists worker;
grant all on database taskdb to database role worker;
grant database role worker to role worker;

-- (A.??)
-- ALTER DATABASE IF EXISTS <name> {
--         RENAME TO <new_db_name> |
--         {SET | UNSET} { TAG <tagname> = <tag_value> | dbParams [ <paramName> = <paramValue> ] }
--         }
-- dbParams:=
--     [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
--     [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
--     [ EXTERNAL_VOLUME = <external_volume_name> ]
--     [ CATALOG = <catalog_integration_name> ]
--     [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
--     [ LOG_LEVEL = '<log_level>' ]
--     [ TRACE_LEVEL = '<trace_level>' ]
--     [ COMMENT = '<string_literal>' ]
alter database if exists taskdb set comment = '???';
-- alter database if exists taskdb rename to cribdb; -- Access control error from worker-role

-- (D.4)
-- drop database if exists <dbname> [ CASCADE | RESTRICT ]

-- (S.8)
-- SHOW DATABASES [ HISTORY ] [ LIKE '<pattern>' ]
--                                      [ STARTS WITH '<name_string>' ]
--                                      [ LIMIT <rows> [ FROM '<name_string>' ] ]
show databases history;

-- (U.4)
use database taskdb;

-- Schema: 1 Database -> X Shcemas (X >= 1)

-- (C.9)
-- CREATE [ TRANSIENT ] SCHEMA IF NOT EXISTS <name>
--   [ CLONE <source_schema>
--         [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
--   [ WITH MANAGED ACCESS ]
--   [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
--   [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
--   [ EXTERNAL_VOLUME = <external_volume_name> ]
--   [ CATALOG = <catalog_integration_name> ]
--   [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
--   [ COMMENT = '<string_literal>' ]
--   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

create schema if not exists company
    with managed access
    comment = 'create company if not exists';

-- (A.9)
-- ALTER SCHEMA IF EXISTS <name> {
--     RENAME TO <newname> |
--     { SET | UNSET } {TAG <tagname> = <tagval> | schemaParams } |
--     SWAP WITH <target_schema> |
--     { ENABLE | DISABLE } MANAGED ACCESS |
-- }
alter schema if exists company rename to yin;

-- (U.5)
-- use schema <name> ;
use schema yin;

-- (S.10)
-- SHOW SCHEMAS
--   [ HISTORY ]
--   [ LIKE '<pattern>' ]
--   [ IN { ACCOUNT | APPLICATION <app_name> | DATABASE [ <db_name> ] } ]
--   [ STARTS WITH '<name_string>' ]
--   [ LIMIT <rows> [ FROM '<name_string>' ] ]
show schemas in database taskdb;


-- Integrations: (Incomplete)
-- (C.2)
-- CREATE <integration_type> INTEGRATION IF NOT EXISTS <object_name>
--   [ <integration_type_params> ]
--   [ COMMENT = '<string_literal>' ]

-- API INTEGRATION:

-- (C.2.1.1) For AWS
-- CREATE API INTEGRATION IF NOT EXISTS <integration_name>
--     API_PROVIDER = { aws_api_gateway | aws_private_api_gateway |
--                      aws_gov_api_gateway | aws_gov_private_api_gateway }
--     API_AWS_ROLE_ARN = '<iam_role>'
--     [ API_KEY = '<api_key>' ]
--     API_ALLOWED_PREFIXES = ('<...>')
--     [ API_BLOCKED_PREFIXES = ('<...>') ]
--     ENABLED = { TRUE | FALSE }
--     [ COMMENT = '<string_literal>' ]
--     ;

-- (C.2.1.2) for Azure
-- CREATE API INTEGRATION IF NOT EXISTS <integration_name>
--     API_PROVIDER = azure_api_management
--     AZURE_TENANT_ID = '<tenant_id>'
--     AZURE_AD_APPLICATION_ID = '<azure_application_id>'
--     [ API_KEY = '<api_key>' ]
--     API_ALLOWED_PREFIXES = ( '<...>' )
--     [ API_BLOCKED_PREFIXES = ( '<...>' ) ]
--     ENABLED = { TRUE | FALSE }
--     [ COMMENT = '<string_literal>' ]
--     ;

-- (C.2.1.3) for GCP
-- CREATE API INTEGRATION IF NOT EXISTS <integration_name>
--     API_PROVIDER = google_api_gateway
--     GOOGLE_AUDIENCE = '<google_audience_claim>'
--     API_ALLOWED_PREFIXES = ( '<...>' )
--     [ API_BLOCKED_PREFIXES = ( '<...>' ) ]
--     ENABLED = { TRUE | FALSE }
--     [ COMMENT = '<string_literal>' ]
--     ;


-- ...


show network rules;

-- Loading Data:

-- File encoding/format:
-- (C.3)
-- CREATE [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
--   TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ]
--   [ COMMENT = '<string_literal>' ]

-- where, 'formatTypeOptions' are:
-- IF TYPE = CSV
--      COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
--      RECORD_DELIMITER = '<character>' | NONE
--      FIELD_DELIMITER = '<character>' | NONE
--      FILE_EXTENSION = '<string>'
--      PARSE_HEADER = TRUE | FALSE
--      SKIP_HEADER = <integer>
--      SKIP_BLANK_LINES = TRUE | FALSE
--      DATE_FORMAT = '<string>' | AUTO
--      TIME_FORMAT = '<string>' | AUTO
--      TIMESTAMP_FORMAT = '<string>' | AUTO
--      BINARY_FORMAT = HEX | BASE64 | UTF8
--      ESCAPE = '<character>' | NONE
--      ESCAPE_UNENCLOSED_FIELD = '<character>' | NONE
--      TRIM_SPACE = TRUE | FALSE
--      FIELD_OPTIONALLY_ENCLOSED_BY = '<character>' | NONE
--      NULL_IF = ( '<string>' [ , '<string>' ... ] )
--      ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE | FALSE
--      REPLACE_INVALID_CHARACTERS = TRUE | FALSE
--      EMPTY_FIELD_AS_NULL = TRUE | FALSE
--      SKIP_BYTE_ORDER_MARK = TRUE | FALSE
--      ENCODING = '<string>' | UTF8

create file format if not exists fmt1
    TYPE = CSV
        COMPRESSION = GZIP

        RECORD_DELIMITER = '\n'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'

        -- Left-aligns fields and truncates excess fields and "NULL"ifies missing fields
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE

        PARSE_HEADER = TRUE
        TRIM_SPACE = TRUE -- Ignores white-spaces in fields ; eg. 1,   2   ,3  ,   5
        NULL_IF = 'NA'
        EMPTY_FIELD_AS_NULL = TRUE;


-- If TYPE = AVRO
     -- COMPRESSION = AUTO | GZIP | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
     -- TRIM_SPACE = TRUE | FALSE
     -- REPLACE_INVALID_CHARACTERS = TRUE | FALSE
     -- NULL_IF = ( '<string>' [ , '<string>' ... ] )

-- If TYPE = ORC
     -- TRIM_SPACE = TRUE | FALSE
     -- REPLACE_INVALID_CHARACTERS = TRUE | FALSE
     -- NULL_IF = ( '<string>' [ , '<string>' ... ] )

-- If TYPE = PARQUET
     -- COMPRESSION = AUTO | LZO | SNAPPY | NONE
     -- SNAPPY_COMPRESSION = TRUE | FALSE (Deprecated: Use COMPRESSION = SNAPPY instead)
     -- BINARY_AS_TEXT = TRUE | FALSE
     -- USE_LOGICAL_TYPE = TRUE | FALSE
     -- TRIM_SPACE = TRUE | FALSE
     -- REPLACE_INVALID_CHARACTERS = TRUE | FALSE
     -- NULL_IF = ( '<string>' [ , '<string>' ... ] )


-- (D,2)
-- DROP FILE FORMAT [ IF EXISTS ] <name>


-- Stages: (Named/User(~)/Table(%))
-- Data is loaded and unloaded to/from Tables through Stages.
-- It stores encoded files in different formats and compressions
-- cannot be queried or interpreted by SQL as Tables.

-- NOTE: Named stages are a database-specific 'object'. (Database or Schema?)

    -- Internal stage:
    -- CREATE [ TEMP ] STAGE IF NOT EXISTS <internal_stage_name>
    --     internalStageParams
    --     directoryTableParams
    --   [ COMMENT = '<string_literal>' ]
    --   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

        -- internalStageParams ::=
        -- [ ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL' | TYPE = 'SNOWFLAKE_SSE') ]

    -- External stage
    -- CREATE [ TEMP ] STAGE IF NOT EXISTS <external_stage_name>
    --     externalStageParams
    --     directoryTableParams
    --   [ COMMENT = '<string_literal>' ]
    --   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

        -- externalStageParams::= ... (varies for Amazon S3, Microsoft Azure and GCP)

    -- directoryTableParams (for internal stages) ::=
    --     [ DIRECTORY = ( ENABLE = { TRUE | FALSE }
    --                   [ REFRESH_ON_CREATE =  { TRUE | FALSE } ] ) ]

        -- directoryTableParams (for S3, Azure and GCP) ::=
            -- [ AUTO_REFRESH = { TRUE | FALSE } ] ) ]


-- (A.3.1)
-- ALTER STAGE IF EXISTS <name> RENAME TO <name> ;

-- (A.3.2)
-- ALTER STAGE IF EXISTS <name> SET
  -- [ FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' ) ]
  -- [ COPY_OPTIONS = ( copyArgs ) ]
  -- [ COMMENT = '<string_literal>' ]

  -- where copyArgs ::=
  --    ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | 'SKIP_FILE_<num>%' | ABORT_STATEMENT }
  --    SIZE_LIMIT = <num>
  --    PURGE = TRUE | FALSE
  --    RETURN_FAILED_ONLY = TRUE | FALSE
  --    MATCH_BY_COLUMN_NAME = CASE_SENSITIVE | CASE_INSENSITIVE | NONE
  --    ENFORCE_LENGTH = TRUE | FALSE
  --    TRUNCATECOLUMNS = TRUE | FALSE
  --    FORCE = TRUE | FALSE

  -- For various external stage S3, Azure, GCP:
  -- [ externalStageParams ]


-- (D.3)
-- drop stage if exists <name>;


-- (S.5)
-- SHOW STAGES [ LIKE '<pattern>' ]
            -- [ IN
            --      {
            --        ACCOUNT                  |
            --        DATABASE <database_name> |
            --        SCHEMA <schema_name>
            --      }]



