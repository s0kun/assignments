-- as accountadmin:

-- Q.1 Create roles as per the mentioned hierarchy.

create role if not exists admin;
create role if not exists developer;
create role if not exists pii;

grant role pii to role accountadmin;
grant role admin to role accountadmin;
grant role developer to role admin;


-- Q.2 Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries
show warehouses;
create warehouse if not exists assignment_wh
    WAREHOUSE_TYPE = STANDARD
    WAREHOUSE_SIZE = MEDIUM
    MAX_CLUSTER_COUNT = 3
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = ECONOMY
    AUTO_SUSPEND = 180
    AUTO_RESUME = FALSE
    INITIALLY_SUSPENDED = TRUE ;



-- Account-admin statements:
grant operate on warehouse assignment_wh to role admin with grant option;
grant usage on warehouse assignment_wh to role admin with grant option;
grant create database on account to role admin;
show grants to role admin;

-- Q.3 Switch to the admin role
use role admin; -- as admin ...

grant operate on warehouse assignment_wh to role developer;
grant usage on warehouse assignment_wh to role developer;

grant operate on warehouse assignment_wh to role pii;
grant usage on warehouse assignment_wh to role pii;


use warehouse assignment_wh;
alter warehouse assignment_wh resume;

-- Q.4 Create a database assignment_b
create database if not exists assignment_db;
use database assignment_db;

-- Q.5 Create a schema my_schema
create schema if not exists my_schema;
use schema my_schema;

-- Loading CSV file from Internal Stage:
-- Schema: id int, name varchar(50), phone varchar(20), email varchar(100), hiredate timestamp, age int, salary int

-- Q.6 Create a table using any sample cv. You can get 1 by googling for sample csv's.
create table if not exists emp (
    id int not null primary key,
    name varchar(50),
    phone varchar(20),
    email varchar(100),
    hiredate timestamp,
    age int,
    salary int,
    elt_by varchar(50) default concat(current_user(), ' as ', current_role()),
    elt_ts TIMESTAMP default current_timestamp(),
    file_name varchar(100)
);

-- Q.7 Also, create a variant version of this dataset
create table if not exists emp_variant (
    id variant,
    name variant,
    phone variant,
    email variant,
    hiredate variant,
    age variant,
    salary variant,
    elt_by varchar(50) default concat(current_user(), ' as ', current_role()),
    elt_ts TIMESTAMP default current_timestamp(),
    file_name variant
);


create file format if not exists basecsv
    TYPE = CSV
        COMPRESSION = AUTO

        RECORD_DELIMITER = '\n'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'

        -- Left-aligns fields and truncates excess fields and "NULL"ifies missing fields
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE

        PARSE_HEADER = FALSE
        SKIP_HEADER = 1

        TRIM_SPACE = TRUE -- Ignores white-spaces before/after fields ; eg. 1,   2   ,3 3  ,   5 -> 1,2,3 3,5
        NULL_IF = 'NA'
        EMPTY_FIELD_AS_NULL = TRUE;


-- Q.8 Internal stage
create stage if not exists localcsv;

alter stage localcsv set
  FILE_FORMAT = ( FORMAT_NAME = basecsv )
  COPY_OPTIONS = (
     ON_ERROR = ABORT_STATEMENT
     SIZE_LIMIT = null
     PURGE = FALSE
     TRUNCATECOLUMNS = FALSE
     FORCE = FALSE
  );

-- Q.8 External stage

use role accountadmin;

create storage integration aws
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::176495710345:role/myrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mysnowflakeassignment/');

-- alter sotrage integration if exists aws set
--     STORAGE_AWS_ROLE_ARN = 'the_real_deal';

grant usage on integration aws to role admin;




use role admin;

create stage if not exists awscsv
    URL = 's3://mysnowflakeassignment/empData.csv'
    STORAGE_INTEGRATION = aws
    FILE_FORMAT = basecsv;

describe stage awscsv;

-- Q.9 Load data into the tables using copy into statements.
-- In one table load from the internal stage and in another from the external

-- On local 'snowsql' process:
-- put file:///Users/sounak/Documents/assignments/snowflake/empData.csv @localcsv;

list @localcsv;
describe stage localcsv;

copy into emp ($1,$2,$3,$4,$5,$6,$7,$10) from
    (select $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME from @localcsv/empData.csv.gz);


list @awscsv;
describe stage awscsv;

copy into emp_variant ($1,$2,$3,$4,$5,$6,$7,$10) from
    (select
        $1::variant, $2::variant, $3::variant, $4::variant, $5::variant, $6::variant, $7::variant, METADATA$FILENAME::variant
    from
        @awscsv);

select * from emp_variant limit 100;


-- Q.10 Upload any parquet file to the stage location and infer the schema of the file.
create stage if not exists localparquet;

create file format if not exists baseparq
    TYPE = PArQUET
        COMPRESSION = AUTO
        BINARY_AS_TEXT = FALSE
        USE_LOGICAL_TYPE = FALSE
        TRIM_SPACE = TRUE
        REPLACE_INVALID_CHARACTERS = FALSE
        NULL_IF = ( 'NA' );

alter stage if exists localparquet set
    file_format = baseparq;


-- Copying the 'emp' table as PARQUET to the stage:
copy into @localparquet from
    (select id,name,phone,email,hiredate,age,salary from emp)
    -- validation_mode = return_rows -- Used to validation/verification before performing unloading
    file_format = baseparq
    overwrite = true
    header = true; -- To include column names in the file

list @localparquet;

select * from TABLE(INFER_SCHEMA(LOCATION=>'@localparquet/data_0_0_0.snappy.parquet', FILE_FORMAT=>'baseparq'));

-- Q.11 Run a select query on the staged parquet file without loading it to a snowflake table
alter warehouse assignment_wh resume;
select
    T.$1
from
    @localparquet/data_0_0_0.snappy.parquet (file_format => baseparq) as T
limit 10;

-- Q.12 Add masking to the PII columns such as phone number, etc. Show as '**masked**' to a user with the developer role.
-- If the role is PIl the value of these columns should be visible.

-- Configuring developer privileges:
use role admin;

grant usage on database assignment_db to role developer;
grant usage on schema my_schema to role developer;
grant select on table emp to role developer;

grant usage on database assignment_db to role pii;
grant usage on schema my_schema to role pii;
grant select on table emp to role pii;


create masking policy if not exists dev_str_mask as
    (v string) returns string -> (
    case true
        when current_role() = 'DEVELOPER' then '**MASKED**'
        when true then v
    end);


create masking policy if not exists dev_int_mask as
    (v int) returns int -> (
    case true
        when current_role() = 'DEVELOPER' then null
        when true then v
    end);


alter table if exists EMP modify column phone set masking policy dev_str_mask;
alter table if exists EMP modify column email set masking policy dev_str_mask;

alter table if exists EMP modify column salary set masking policy dev_int_mask;


use role developer;
select * from emp limit 10;
show grants to role developer;

use role pii;
select * from emp limit 10;
show grants to role pii;