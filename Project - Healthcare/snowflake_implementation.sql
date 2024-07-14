-- accountadmin
use role accountadmin;

show warehouses;
create warehouse if not exists hospitalCalc
    WAREHOUSE_TYPE = STANDARD
    WAREHOUSE_SIZE = SMALL
    MAX_CLUSTER_COUNT = 2
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = ECONOMY
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE ;

create role hospitalWorker;

grant operate on warehouse hospitalCalc to role hospitalWorker with grant option;
grant usage on warehouse hospitalCalc to role hospitalWorker with grant option;
show grants to role hospitalworker;

create database if not exists hospitaldb;
grant usage,create schema on database hospitaldb to role hospitalworker;

grant role hospitalworker to user migs;

-- hospitalworker
use role hospitalworker;
use database hospitaldb;

create schema if not exists hospital with managed access;
use schema hospital;


create file format if not exists fmt
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

create stage if not exists load;
alter stage load set
  FILE_FORMAT = ( FORMAT_NAME = fmt )
  COPY_OPTIONS = (
     ON_ERROR = ABORT_STATEMENT
     SIZE_LIMIT = null
     PURGE = FALSE
     TRUNCATECOLUMNS = FALSE
     FORCE = FALSE
  );

-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Keep/Keep.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Contain/Contain.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Pharmacy/Pharmacy.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Disease/Disease.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Person/Person.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Prescription/Prescription.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/InsuranceCompany/InsuranceCompany.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Patient/Patient.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/InsurancePlan/InsurancePlan.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Address/Address.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Medicine/Medicine.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Treatment/Treatment.csv' @load;
-- PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Claim/Claim.csv' @load;


list @load;

-- Source Table 1:
CREATE TABLE if not exists Address(
   addressID INTEGER  PRIMARY KEY
  ,address1  VARCHAR(200)
  ,city      VARCHAR(100)
  ,state     VARCHAR(20)
  ,zip       INTEGER
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null
);


copy into address ($1,$2,$3,$4,$5,$7) from
    (select $1,$2,$3,$4,$5,METADATA$START_SCAN_TIME from @load/Address.csv.gz);

-- Warehouse turned green at this point

select * from address limit 10;


-- Source Table 2
CREATE TABLE if not exists Disease(
   diseaseID   INTEGER  NOT NULL PRIMARY KEY
  ,diseaseName VARCHAR(100) NOT NULL
  ,description VARCHAR(1000) NOT NULL
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null
);

copy into disease ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/Disease.csv.gz);

select * from disease limit 10;

CREATE TABLE if not exists Pharmacy(
   pharmacyID   INTEGER  NOT NULL PRIMARY KEY
  ,pharmacyName VARCHAR(33) NOT NULL
  ,phone        BIGINT  NOT NULL
  ,addressID    INTEGER  NOT NULL
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,FOREIGN KEY (addressID) REFERENCES address (addressID) enforced
);

copy into pharmacy ($1,$2,$3,$4,$6) from
    (select $1,$2,$3,$4,METADATA$START_SCAN_TIME from @load/Pharmacy.csv.gz);

select * from pharmacy limit 10;

-- Source Table 3
CREATE TABLE if not exists Medicine(
   medicineID         INTEGER  PRIMARY KEY
  ,companyName        VARCHAR(101)
  ,productName        VARCHAR(174)
  ,description        VARCHAR(161)
  ,substanceName      VARCHAR(255)
  ,productType        INTEGER
  ,taxCriteria        VARCHAR(3)
  ,hospitalExclusive  VARCHAR(1)
  ,governmentDiscount VARCHAR(1)
  ,taxImunity         VARCHAR(1)
  ,maxPrice           NUMERIC(9,2)
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null
);

copy into medicine ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$13) from
    (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,METADATA$START_SCAN_TIME from @load/Medicine.csv.gz);

select * from medicine limit 10;


CREATE TABLE if not exists InsuranceCompany(
   companyID   INTEGER  PRIMARY KEY
  ,companyName VARCHAR(100)
  ,addressID   INTEGER
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (addressID) references address (addressID)
);

-- Invalid UTF8 detected in string 'Star Health & Allied Insurance Co. Ltd.0xA0'
-- File 'InsuranceCompany.csv.gz', line 6, character 6 ; Row 5, column "INSURANCECOMPANY"["COMPANYNAME":2]
alter file format if exists fmt set ENCODING = 'ISO88591';

-- Encoding: ISO88591
copy into insurancecompany ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/InsuranceCompany.csv.gz);

select * from insurancecompany limit 10;


CREATE TABLE if not exists InsurancePlan(
   uin       VARCHAR(25) PRIMARY KEY
  ,planName  VARCHAR(100)
  ,companyID INTEGER
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (companyID) references insurancecompany (companyID)
);

-- Encoding: ISO88591
copy into insuranceplan ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/InsurancePlan.csv);

select * from insuranceplan limit 10;


CREATE TABLE if not exists Claim(
   claimID BIGINT  NOT NULL PRIMARY KEY
  ,balance BIGINT  NOT NULL
  ,uin     VARCHAR(25) NOT NULL
  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (uin) references insuranceplan (uin)
);

-- Encoding: ISO88591
copy into claim ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/Claim.csv);

select * from claim limit 10;


CREATE TABLE if not exists Person(
   personID    INTEGER  PRIMARY KEY
  ,personName  VARCHAR(22)
  ,phoneNumber BIGINT
  ,gender      VARCHAR(6)
  ,addressID   INTEGER

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (addressID) references address (addressID)
);

alter file format fmt set encoding='UTF8';

-- ENCODING: UTF8
copy into person ($1,$2,$3,$4,$5,$7) from
    (select $1,$2,$3,$4,$5,METADATA$START_SCAN_TIME from @load/Person.csv.gz);

select * from person limit 10;


CREATE TABLE if not exists Patient(
   patientID INTEGER  PRIMARY KEY
  ,ssn       INTEGER
  ,dob       DATE

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (patientID) references person (personId)
);

-- ENCODING: UTF8
copy into patient ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/Patient.csv.gz);

select * from patient limit 10;


CREATE TABLE if not exists Keep(
   pharmacyID INTEGER
  ,medicineID INTEGER
  ,quantity   INTEGER
  ,discount   INTEGER
  ,PRIMARY KEY(pharmacyID,medicineID)

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (pharmacyID) references pharmacy (pharmacyid)
  ,foreign key (medicineid) references medicine (medicineid)
);

copy into keep ($1,$2,$3,$4,$6) from
    (select $1,$2,$3,$4,METADATA$START_SCAN_TIME from @load/Keep.csv.gz);

select * from keep limit 10;


CREATE TABLE if not exists Treatment(
   treatmentID INTEGER  PRIMARY KEY
  ,date        DATE
  ,patientID   INTEGER not null
  ,diseaseID   INTEGER not null
  ,claimID     BIGINT

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (patientid) references patient (patientid)
  ,foreign key (diseaseid) references disease (diseaseid)
  ,foreign key (claimid) references claim (claimid)
);

copy into treatment ($1,$2,$3,$4,$5,$7) from
    (select $1,$2,$3,$4,$5,METADATA$START_SCAN_TIME from @load/Treatment.csv.gz);

select * from treatment limit 10;


CREATE TABLE if not exists Prescription(
   prescriptionID BIGINT  PRIMARY KEY
  ,pharmacyID     INTEGER
  ,treatmentID    INTEGER

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  -- Constraints are not enforced in Snowflake at all ??!! How are data insertions validated then ?
  ,foreign key (pharmacyID) references pharmacy (pharmacyid) enforced not deferrable initially immediate enable validate rely
  ,foreign key (treatmentid) references treatment (treatmentid) enforced not deferrable initially immediate enable validate rely
);


copy into prescription ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/Prescription.csv.gz);


select * from prescription limit 10;


CREATE TABLE if not exists Contain(
   prescriptionID BIGINT  NOT NULL
  ,medicineID     INTEGER  NOT NULL
  ,quantity       INTEGER  NOT NULL
  ,PRIMARY KEY(prescriptionID,medicineID)

  ,_by string default concat(current_user(), ' as ', current_role())
  ,_at timestamp not null

  ,foreign key (medicineid) references medicine (medicineid) enforced not deferrable initially immediate enable validate rely
  ,foreign key (prescriptionid) references prescription (prescriptionid) enforced not deferrable initially immediate enable validate rely
);

copy into contain ($1,$2,$3,$5) from
    (select $1,$2,$3,METADATA$START_SCAN_TIME from @load/Contain.csv.gz);

select * from contain limit 10;

------------------------------------------------ DATA LOADED ------------------------------------------------


-- I.1

-- Jimmy, from the healthcare department, has requested a report that shows
-- how the number of treatments each age category of patients has gone through in the year 2022.
-- The age category is as follows,
    -- Children (00-14 years),
    -- Youth (15-24 years),
    -- Adults (25-64 years),
    -- Seniors (65 years and over).
-- Assist Jimmy in generating the report.

-- Answer:
select
    age_group,
    count(treatmentid) as total_treatments
from
    (select
        treatmentid,
        (case true
            when age < 15 then 'Children'
            when age < 25 then 'Youth'
            when age < 64 then 'Adult'
            when true then 'Senior'
        end) as age_group,
    from
        (select
            T.treatmentid,
            datediff('year',P.dob,T.date) as age
        from
            treatment as T inner join patient as P on T.patientid = P.patientid
        where year(T.date) = '2022') as X) as Y
group by
    age_group;



-- I.2

-- Jimmy, from the healthcare department, wants to know which disease is infecting people of which gender more often.
-- Assist Jimmy with this purpose by generating a report that shows
-- for each disease the male-to-female ratio. Sort the data in a way that is helpful for Jimmy.

-- Identifying the keys for male and female genders
select distinct gender from person;
    -- female
    -- male

-- Answer:
select
    T.diseaseid,
    D.DISEASENAME,
    sum(iff(gender = 'male',1,0)) as male,
    sum(iff(gender = 'female',1,0)) as female,
    count(treatmentid) as total,
    male/female as MF_ratio
from
    -- (select distinct -- multiple Treatment IDs for same (patient,disease) within 7-day interval are identical
    --     patientid, diseaseid, round(datediff(day,'2002-09-12',date)/7,0) as bucket
    -- from treatment) as T
    treatment as T
    inner join person as P on P.personid = T.patientid
    inner join disease as D on D.diseaseid = T.diseaseid
group by
    T.diseaseid, D.diseasename
order by
    total;



-- I.3

-- Jacob, from insurance management, has noticed that insurance claims are not made for all the treatments.
-- He also wants to figure out if the gender of the patient has any impact on the insurance claim.
-- Assist Jacob in this situation by generating a report that finds
-- for each gender the number of treatments, number of claims, and treatment-to-claim ratio.
-- Also notice if there is a significant difference between the treatment-to-claim ratio of male and female patients.

-- Answer:
select
    P.gender,
    count(claimid) as claims,
    count(treatmentid) as treatments,
    treatments/claims as ratio
from
    treatment as T inner join person as P on P.personid = T.patientid
group by
    P.gender;


-- I.4

-- The Healthcare department wants a report about the inventory of pharmacies.
-- Generate a report on their behalf that shows how many units of medicine each pharmacy has in their inventory,
-- the total maximum retail price of those medicines, and the total price of all the medicines after discount.
-- Note: discount field in keep signifies the percentage of discount on the maximum price.

-- Answer:
select
    P.pharmacyname,
    sum(K.quantity) as units,
    sum(M.maxprice) as totalMRP,
    round(sum(M.maxprice*K.discount/100),2) as totalPrice
from
    keep as K inner join medicine as M on M.medicineid = K.medicineid
    inner join pharmacy as P on P.pharmacyid = K.pharmacyid
group by
    P.pharmacyname
order by
    totalPrice desc;

-- I.5

-- The healthcare department suspects that some pharmacies prescribe more medicines than others in a single prescription,
-- for them, generate a report that finds
-- for each pharmacy the maximum, minimum and average number of medicines prescribed in their prescriptions.

desc table prescription;

-- Answer:
select
    Ph.pharmacyname,

    max(C.tot) as most_Num_Med,
    max(C.totQ) as most_Q_Med,

    min(C.tot) as least_Num_Med,
    min(C.totQ) as least_Q_Med,

    avg(C.tot) as avg_Num_Med,
    avg(C.totQ) as avg_Q_Med
from
    (pharmacy as Ph inner join prescription as Pr on Ph.pharmacyid = Pr.pharmacyid)
    inner join (select prescriptionid, count(medicineid) as tot, sum(quantity) as totQ from contain group by prescriptionid) as C
    on C.PRESCRIPTIONID = Pr.prescriptionid
group by
    Ph.pharmacyname;


-- II.1

-- A company needs to set up 3 new pharmacies.
-- They have come up with an idea that the pharmacy can be set up in cities
-- where the pharmacy-to-prescription ratio is the lowest and the number of prescriptions should exceed 100.
-- Assist the company to identify those cities where the pharmacy can be set up.

desc table pharmacy;
desc table address;
desc table prescription;


-- Answer:
with
X as (select
        A.city,
        count(distinct T.pharmacyid) as pharmacies,
        sum(T.total) as prescriptions,
        (count(distinct T.pharmacyid)/sum(T.total)) as ratio
    from
        (select
            pharmacyid,
            count(distinct prescriptionid) as total,
        from
            prescription
        group by
            pharmacyid) as T inner join pharmacy as P on P.pharmacyid = T.pharmacyid
        inner join address as A on A.addressid = P.addressid
    group by
        A.city
    having
        prescriptions > 100)
select
    city,
    pharmacies,
    prescriptions
from
    X
where
    ratio = (select min(ratio) from X);



-- II.2

-- The State of Alabama (AL) is trying to manage its healthcare resources more efficiently.
-- For each city in their state, they need to identify the disease for which the maximum number of patients have gone for treatment.
-- Assist the state for this purpose. ** Note: The state of Alabama is represented as AL in Address Table. **

desc table treatment;
desc table disease;
desc table person;
desc table address;

-- Answer:
with
X as (select
    T.diseaseid,
    count(distinct T.patientid) as affected
from
    (select
        *
    from
        address
    where
        state = 'AL') as A inner join person as P on P.addressid = A.addressid
    inner join treatment as T on P.personid = T.patientid
group by
    T.diseaseid)
select
    D.diseasename,
    A.affected
from
    (select * from X where affected = (select max(affected) from X)) as A
    inner join disease as D on D.diseaseid = A.diseaseid;



-- II.3

-- The healthcare department needs a report about insurance plans.
-- The report is required to include the insurance plan, which was claimed the most and least for each disease.
-- Assist to create such a report.

desc table insuranceplan;
desc table claim;
desc table disease;
desc table treatment;

-- Answer:
select
    D.diseaseid,

    listagg(distinct iff(ctr=MX or ctr is null,planname,null),', ') as mostClaimed,
    ifnull(min(MX),0) as MXclaims,

    listagg(distinct iff(ctr=MN or ctr is null,planname,null),', ') as leastClaimed,
    ifnull(max(MN),0) as MNclaims
from
    (select
        T.diseaseid,
        I.planname,
        count(distinct T.treatmentid) as ctr,

        -- Is window applied on grouped tables individually ? What is asssumed context/state/scope of partitioning ?
        -- Why are these not always "valid group-by expressions"?
        max(ctr) over(partition by T.diseaseid) as MX,
        min(ctr) over(partition by T.diseaseid) as MN
    from
        treatment as T inner join claim as C on T.claimid = C.claimid
        inner join insuranceplan as I on I.uin = C.uin
    group by
        T.diseaseid, I.planname) as X right outer join
    disease as D on X.diseaseid = D.diseaseid
where
    ctr = MX or ctr = MN
group by
    D.diseaseid;



-- II.4

-- The Healthcare department wants to know which disease is most likely to infect multiple people in the same household.
-- For each disease find the number of households that has more than one patient with the same disease.
-- Note: 2 people are considered to be in the same household if they have the same address.

desc table address;

-- Answer:
select distinct
    D.diseaseid,
    D.diseasename,
    sum(iff(cases > 1,1,0)) as households
from
    (select
        T.diseaseid,
        P.addressid,
        count(distinct T.patientid) as cases
    from
        treatment T inner join person as P on P.personid = T.patientid
    group by
        T.diseaseid, P.addressid) as X
    right outer join disease as D on D.diseaseid = X.diseaseid
group by
    D.diseaseid,D.diseasename
order by
    households;



-- II.5

-- An Insurance company wants a state wise report of the
-- treatments to claim ratio between 1st April 2021 (01-04-2021) and 31st March 2022 (31-03-2022) (days both included).
-- Assist them to create such a report.

desc table treatment;

-- Answer:
select
    A.state,
    count(distinct treatmentid) as treatments,
    count(distinct claimid) as claims,
    treatments/claims as ratio
from
    (select
        *
    from
        treatment
    where
        datediff(day,'2021-04-01',date) >= 0 and datediff(day, date, '2022-03-31') >= 0) as T
    inner join person as P on T.patientid = P.personid
    right outer join address as A on P.addressid = A.addressid
group by
    A.state;


-- III.1

-- Some complaints have been lodged by patients that they have been prescribed hospital-exclusive medicine
-- that they can’t find elsewhere and facing problems due to that.

-- Joshua, from the pharmacy management, wants to get a report of which pharmacies have prescribed
-- hospital-exclusive medicines the most in the years 2021 and 2022.

-- Assist Joshua to generate the report so that the pharmacies who
-- prescribe hospital-exclusive medicine more often are advised to avoid such practice if possible.

-- Meta-analysis:
desc table prescription; -- for (pharmacyid <-1- prescriptionid) mapping
desc table contain; -- for (medicineid -N-> prescriptionid) mapping
desc table medicine; -- for (hospitalexclusive -N-> medicineid) mapping
desc table treatment; -- for (prescriptionid <-N- date) mapping
select distinct hospitalexclusive from medicine; -- 'N', 'S', null

-- Answer:
with
X (pharmacyid, medsCount, medsQ) as (select
    T3.pharmacyid,
    sum(T2.medsCount),
    sum(T2.medsQ)
from
        (select distinct
            treatmentid
        from
            treatment
        where
            datediff(day,'2021-01-01',date) >= 0 and datediff(day, date, '2022-12-31') >= 0) as T1
    inner join prescription as T3 on T3.treatmentid = T1.treatmentid
    inner join
        (select
            C.prescriptionid,
            sum(C.quantity) as medsQ,
            count(distinct C.medicineid) as medsCount
        from
            contain as C inner join medicine as M on C.medicineid = M.medicineid
        where
            M.HOSPITALEXCLUSIVE != 'N'
        group by
            C.prescriptionid) as T2
    on T3.prescriptionid = T2.prescriptionid
group by
    T3.pharmacyid)
select
    pharmacyname as name,
    -- medsCount as exclusive_meds,
    medsQ as total_quantity
from
    (select * from X where medsQ = (select max(medsQ) from X)) as T4
    inner join pharmacy as P on P.pharmacyid = T4.pharmacyid;

-- Doubt: medsCount or medsQ ?


-- III.2

-- Insurance companies want to assess the performance of their insurance plans.
-- Generate a report that shows:
-- each insurance plan, the company that issues the plan, and the number of treatments the plan was claimed for.

-- Meta-Analysis
desc table insuranceplan;

select uin, count(companyid) as ctr from insuranceplan group by uin having ctr > 1;
select * from insuranceplan where uin = 'SHAHLGP22134V012122';
-- select * from insurancecompany where companyid=1118 or companyid=9461;

select * from insuranceplan where uin = 'TATHLGP21004V032122';
-- Snowflake validates ONLY 'NOT NULL' constraints on tables.
-- Hence, all other constraints are simply "comments".

-- Answer:
select distinct
    I1.companyid,
    I1.companyname,
    I.planname,
    ifnull(T2.ctr,0) as total
from
    (select
        C.uin,
        sum(T1.ctr) as ctr
    from
        (select claimid, count(treatmentid) as ctr from treatment group by claimid) as T1
        inner join claim as C on C.claimid = T1.claimid
    group by
        C.uin) as T2 right outer join
    insuranceplan as I on T2.uin = I.uin left outer join
    insurancecompany as I1 on I1.companyid = I.companyid
order by
    total;


-- III.3

-- Insurance companies want to assess the performance of their insurance plans.
-- Generate a report that shows each insurance company's name with their most and least claimed insurance plans.

desc table claim;

-- Answer:
select
    I2.companyname,
    listagg(distinct iff(X.claims = MX,X.planname,null), ', ') as most_claimed,
    min(ifnull(X.MX,0)) as most_claims,
    listagg(distinct iff(X.claims = MN,X.planname,null), ', ') as least_claimed,
    max(ifnull(X.MN,0)) as least_claims
from
    (select
        I1.companyid,
        I1.planname,
        ifnull(T1.claims,0) as claims,
        max(ifnull(T1.claims,0)) over(partition by I1.companyid) as MX,
        min(ifnull(T1.claims,0)) over(partition by I1.companyid) as MN
    from
        (select
            uin,
            count(distinct claimid) as claims
        from
            claim
        group by
            uin) as T1 right outer join insuranceplan as I1
        on I1.uin = T1.uin) as X right outer join
        insurancecompany as I2 on I2.companyid = X.companyid
group by
    I2.companyid,I2.companyname;



-- III.4

-- The healthcare department wants a state-wise health report to assess
-- which state requires more attention in the healthcare sector. Generate a report for them
-- that shows the state name, number of registered people in the state,
-- number of registered patients in the state, and the people-to-patient ratio.
-- Sort the data by people-to-patient ratio.

-- Meta-analysis:

-- person -1-> addressid
-- addressid -1-> state
-- patient -1-> person

-- Answer:
select
    A.state,
    count(Pe.personid) as registered_people,
    count(Pt.patientid) as registered_patients,
    count(Pe.personid)/count(Pt.patientid) as ratio
from
    person as Pe right outer join address as A on Pe.addressid = A.addressid
    left outer join patient as Pt on Pt.patientid = Pe.personid
group by
    A.state
order by
    ratio asc;


-- III.5

-- Jhonny, from the finance department of Arizona(AZ), has requested a report that lists
-- the total quantity of medicine each pharmacy in his state has prescribed
-- that falls under Tax criteria I for treatments that took place in 2021.
-- Assist Jhonny in generating the report.

-- Meta-Analysis:
desc table medicine;
select distinct taxcriteria from medicine;

-- Answer:
with
X (pharmacyid, medsQ) as (select
    T3.pharmacyid,
    sum(T2.medsQ)
from
        (select distinct
            treatmentid
        from
            treatment
        where
            datediff(day,'2021-01-01',date) >= 0 and datediff(day, date, '2021-12-31') >= 0) as T1
    inner join
    (prescription as T3 inner join
        (select
            C.prescriptionid,
            sum(C.quantity) as medsQ
        from
            contain as C inner join medicine as M on C.medicineid = M.medicineid
        where
            M.taxcriteria = 'I'
        group by
            C.prescriptionid) as T2 on T3.prescriptionid = T2.prescriptionid)
    on T3.treatmentid = T1.treatmentid
group by
    T3.pharmacyid)
select
    P.pharmacyname as name,
    X.medsQ as total_quantity
from
    X inner join (
    select
        *
    from
        pharmacy as Ph inner join address as A on A.addressid = Ph.addressid
    where
        A.state = 'AZ') as P on P.pharmacyid = X.pharmacyid;




-- IV.1

-- "HealthDirect” pharmacy finds it difficult to deal with
-- the product type of medicine being displayed in numerical form,
-- they want the product type in words. Also, they want to filter the medicines based on tax criteria.

create table if not exists medType (
    typeid int not null primary key,
    typename varchar(100)
);

insert into medType (typeid,typename) values
    (1, 'Generic'),
    (2, 'Patent'),
    (3, 'Reference'),
    (4, 'Similar'),
    (5, 'New'),
    (6, 'Specific'),
    (7, 'Biological'),
    (8, 'Dinamized') ;

-- Display only the medicines of product categories 1, 2, and 3 for medicines that come under tax category I
-- and medicines of product categories 4, 5, and 6 for medicines that come under tax category II for "HealthDirect"

-- Meta-analysis / Constraints :
-- pharmacyname -x-> pharmacyid ( x in N ; x >= 0 )
-- pharmacyid -x-> medicineid ( x in N ; x >= 0 )
-- medicineid -1-> taxcriteria
-- medicineid -1-> producttype
-- typeid <-1-> producttype
-- typeid -1-> typename

desc table keep;

select
    P.pharmacyid,
    K.medicineid,
    M1.productname,
    M1.description,
    M2.typename,
    M1.taxcriteria
from
    (select * from pharmacy where pharmacyname='HealthDirect') as P left outer join
    keep as K on K.pharmacyid = P.pharmacyid inner join
    (
        (select medicineid, productname, description, producttype, taxcriteria
        from medicine
        where (producttype in (1,2,3) and taxcriteria = 'I') or (producttype in (4,5,6) and taxcriteria = 'II')) as M1
        left outer join medtype as M2 on M2.typeid = M1.producttype
    )
    on M1.medicineid = K.medicineid;

-- Doubt: No producttypes 4,5,6 ?


-- IV.2

-- 'Ally Scripts' pharmacy company wants to find out the quantity of medicine prescribed in each of its prescriptions.
-- Write a query that finds the sum of the quantity of all the medicines in a prescription and

-- if the total quantity of medicine is less than 20 tag it as “low quantity”.
-- If the quantity of medicine is from 20 to 49 (both numbers including) tag it as “medium quantity“ and
-- if the quantity is more than equal to 50 then tag it as “high quantity”.

-- Show the prescription Id, the Total Quantity of all the medicines in that prescription, and
-- the Quantity tag for all the prescriptions issued by 'Ally Scripts'.

create table if not exists quantBound (
    GLB int not null, -- GLB : Greatest Lower Bound
    category varchar(50)
);

insert into quantBound (glb, category) values
    (0, 'low quantity'),
    (20,'medium quantity'),
    (50, 'high quantity');

-- Meta-analysis:
desc table pharmacy; -- pharmacyname -x-> pharmacyid ( x in N ; x >=0 )
desc table prescription; -- pharmacyid -x-> prescriptionid ( x in N ; x >=0 )
desc table contain; -- prescriptionid -x-> quantity ( x in N ; x >=0 )
desc table quantBound; -- quantity -1-> category

-- Answer:
with
cats (glb,lub,cat) as (select
        glb,
        (lead(glb) over(order by glb asc)) - 1,
        category
    from
        quantbound)
select
    Pr.prescriptionid,
    Q.medsQ as quantity,
    (select max(cat) from cats where Q.medsQ >= glb and (lub is null or Q.medsQ <= lub)) as category
from
    (select * from pharmacy where pharmacyname = 'Ally Scripts') as Ph left outer join
    prescription as Pr on Pr.pharmacyid = Ph.pharmacyid left outer join
    (select prescriptionid, sum(quantity) as medsQ from contain group by prescriptionid) as Q
    on Q.prescriptionid = Pr.prescriptionid;


-- IV.3

-- In the Inventory of a pharmacy 'Spot Rx' the quantity of medicine is considered
-- ‘HIGH QUANTITY’ when the quantity exceeds 7500 and
-- ‘LOW QUANTITY’ when the quantity falls short of 1000.

with
catsQuant (glb,lub,cat) as(
    (select null, 1000, 'low quantity') union
    (select 7500, null, 'high quantity')
)
select * from catsQuant;

-- The discount is considered
-- “HIGH” if the discount rate on a product is 30% or higher, and
-- the discount is considered “NONE” when the discount rate on a product is 0%.

with
catsDisc (glb,lub,cat) as(
    (select 30, 100, 'HIGH') union
    (select 0, 0, 'NONE')
)
select * from catsDisc;

-- 'Spot Rx' needs to find all the Low quantity products with high discounts and
-- all the high-quantity products with no discount so they can adjust the discount rate according to the demand.

-- Write a query for the pharmacy listing all the necessary details relevant to the given requirement.
-- Hint: Inventory is reflected in the Keep table.


-- Meta-analysis:
desc table pharmacy; -- pharmacyname -x-> pharmacyid (x in N ; x>=0 )

desc table keep;
-- pharmacyid -x-> medicineid (x in N ; x>=0)
-- medicineid -1/0-> discount
-- medicineid -1/0-> quantity

desc table medicine;
-- medicineid -1/0-> productname
-- medicineid -1/0-> description

-- Answer:
with
catsQuant (glb,lub,cat) as(
    (select null, 999, 'low quantity') union
    (select 7501, null, 'high quantity')
),
catsDisc (glb,lub,cat) as(
    (select 30, 100, 'HIGH') union
    (select 0, 0, 'NONE')
)
select
    M.medicineid,
    M.productname,
    M.description,
    (select max(cat) from catsQuant where (K.quantity >= glb or glb is null) and (K.quantity <= lub or lub is null)) as quant,
    (select min(cat) from catsDisc where (K.discount >= glb or glb is null) and (K.discount <= lub or lub is null)) as discount
from
    (select * from pharmacy where pharmacyname = 'Spot Rx') as Ph left outer join
    (select * from keep where (quantity < 1000 and discount >= 30) or (quantity > 7500 and discount = 0)) as K
    on Ph.pharmacyid = K.pharmacyid left outer join medicine as M on M.medicineid = K.medicineid
order by
    K.quantity asc, K.discount asc;


-- IV.4

-- Mack, From 'HealthDirect' Pharmacy, wants to get a list of all the affordable and costly, hospital-exclusive medicines in the database.
-- Where
-- affordable medicines are the medicines that have a maximum price of less than 50% of the avg maximum price of
-- all the medicines in the database,
-- and
-- costly medicines are the medicines that have a maximum price of more than
-- double the avg maximum price of all the medicines in the database.


-- Mack wants clear text next to each medicine name to be displayed that identifies the medicine as affordable or costly.
-- The medicines that do not fall under either of the two categories need not be displayed.

-- Meta-analysis:

-- Doubt: Does Mack, from 'HealthDirect' only care about medicine in 'HealthDirect' keep?
desc table pharmacy; -- pharmacyname -x-> pharmacyid (x in N ; x >= 0)
desc table keep; -- pharmacyid -x-> medicineid (x in N ; x>=0)

desc table medicine;
-- medicineid -1/0-> maxprice
-- medicineid -1/0-> hospitalexclusive


-- Doubt: Average maximum price of all medicine weighted by quantity in all keeps?
-- with
-- avgprice as (select
--         avg(maxprice)
--     from
--         medicine
-- )
-- select * from avgprice;


-- 'affordable' and 'costly' categories:
with
avgprice as (select
        avg(maxprice)
    from
        medicine
),
catCost (glb,lub,cat) as (
    (select null,0.5*(select * from avgprice), 'affordable') union
    (select 2*(select * from avgprice), null, 'costly')
)
select * from catCost;


-- Answer:
with
avgprice as (select
        avg(maxprice)
    from
        medicine
),
catCost (glb,lub,cat) as (
    (select null,0.5*(select * from avgprice), 'affordable') union
    (select 2*(select * from avgprice), null, 'costly')
)
select
    M.medicineid,
    M.productname,
    M.description,
    M.cost
from
    (select pharmacyid from pharmacy where pharmacyname = 'HealthDirect') as Ph left outer join
    keep as K on K.pharmacyid = Ph.pharmacyid inner join
    (select
        medicineid,
        productname,
        description,
        (select max(cat) from catCost where (maxprice > glb or glb is null) and (maxprice < lub or lub is null)) as cost,
    from
        medicine
    where
        hospitalexclusive = 'S' and
        (maxprice > 2*(select * from avgprice) or 2*maxprice < (select * from avgprice))) as M
    on K.medicineid = M.medicineid;



-- IV.5

-- The healthcare department wants to categorize the patients into the following category.

-- YoungMale: Born on or after 1st Jan  2005  and gender male.
-- YoungFemale: Born on or after 1st Jan  2005  and gender female.
-- AdultMale: Born before 1st Jan 2005 but on or after 1st Jan 1985 and gender male.
-- AdultFemale: Born before 1st Jan 2005 but on or after 1st Jan 1985 and gender female.
-- MidAgeMale: Born before 1st Jan 1985 but on or after 1st Jan 1970 and gender male.
-- MidAgeFemale: Born before 1st Jan 1985 but on or after 1st Jan 1970 and gender female.
-- ElderMale: Born before 1st Jan 1970, and gender male.
-- ElderFemale: Born before 1st Jan 1970, and gender female.

-- Write a SQL query to list all the patient name, gender, dob, and their category.

-- Meta-analysis:
desc table patient; -- patientid -1/0-> dob


-- Age categories:
create table if not exists catAge (glb date primary key,cat varchar(50));
insert into catAge values
    (cast('2005-01-01' as date), 'Young'),
    (cast('1985-01-01' as date), 'Adult'),
    (cast('1970-01-01' as date), 'MidAge'),
    -- 'glb' cannot be set to null ; Interestingly(in Snowflake SQL) 'lub' can be set to null ;
    (cast('1000-01-01' as date), 'Elder');
select * from catAge;

-- ***NOTE***: (Summary: null > not null , in Snowflake)
    CREATE or replace FUNCTION null_property(val variant)
      RETURNS BOOLEAN
      AS
      $$
        (select x from ((select null as x) union (select val)) as T order by x desc limit 1) is null
      $$
    ;
    -- The ignorance of SQL in assuming 'null' to be undefined, yet also assuming for all X : null_property(X) ;
    -- One cannot define the undefined, for that is a contradiciton of itself.
    -- SQL 'null' is described by the characteristic properties that it satisfies and it is these
    -- properties that give it a definition. There is none that is undefined, for if it is, it is defined by itself.


desc table person;
-- personid -1/0-> personname
-- personid -1/0-> gender

select distinct gender from person;
-- male
-- female


-- Answer:
with
cats (glb,lub,cat) as (select
        glb,
        (lead(glb) over(order by glb asc)),
        cat
    from
        catAge)
select
    Pn.personname,
    Pn.gender,
    Pt.dob,
    concat((select max(cat) from cats where (dob >= glb or glb is null) and (dob < lub or lub is null)),initcap(Pn.gender)) as category
from
    patient as Pt left outer join person as Pn
    on Pt.PATIENTID = Pn.personid;



-- VIII.1

-- For each age(in years), how many patients have gone for treatment?

-- Meta-analysis:
-- treatment.patientid -1-> patient.patientid ;

select
    datediff(year,dob,date) as age,
    count(distinct T.patientid) as patients
from
    treatment as T inner join patient as P
    on P.patientid = T.patientid
group by
    datediff(year,dob,date);


-- VIII.2

-- For each city, Find the number of registered people, number of pharmacies, and number of insurance companies.

-- Meta-Analysis:
select addressid, count(distinct companyid) as ctr from insurancecompany group by addressid having ctr > 1; -- 0
select addressid, count(distinct pharmacyid) as ctr from pharmacy group by addressid having ctr > 1; -- 0


select
    T1.city,
    companies,
    pharmacies,
    people
from
    (select
        city,
        count(distinct companyid) as companies
    from
        address as A left outer join
        insurancecompany as I on A.addressid = I.addressid
    group by
        city) as T1 inner join
    (select
        city,
        count(distinct pharmacyid) as pharmacies
    from
        address as A left outer join
        pharmacy as P on A.addressid = P.addressid
    group by
        city) as T2 on T1.city = T2.city inner join
    (select
        city,
        count(distinct personid) as people
    from
        address as A left outer join
        person as P on A.addressid = P.addressid
    group by
        city) as T3 on T2.city = T3.city;



-- VIII.3




-- VII.4

with
cats (glb,lub,cat) as (select
        glb,
        (lead(glb) over(order by glb asc)),
        cat
    from
        catAge),
A as (select
    Pt.patientid,
    concat((select max(cat) from cats where (dob >= glb or glb is null) and (dob < lub or lub is null)),initcap(Pn.gender)) as category
from
    patient as Pt left outer join person as Pn
    on Pt.PATIENTID = Pn.personid)
select
    D.diseaseid,
    D.diseasename,
    X.category,
    X.patients
from
    (select
        T.diseaseid,
        A.category,
        count(distinct T.patientid) as patients,
        max(patients) over(partition by T.diseaseid) as MX
    from
        A inner join treatment as T on T.patientid = A.patientid
    group by
        T.diseaseid, A.category) as X
    right outer join disease as D on D.diseaseid = X.diseaseid
where
    X.patients = X.MX or X.patients is null;



-- X.1

CREATE PROCEDURE if not exists companyPerformance(id number(38,0))
  RETURNS TABLE()
  LANGUAGE SQL
  AS
  $$
  DECLARE
    T RESULTSET;
  BEGIN
    T := (
        select
            name,
            total,
            D.diseasename as disease,
            claims
        from
            (select
                B.planname as name,
                T.diseaseid,
                -- To count "claimid" or "treatmentid" ?
                count(T.claimid) as claims,

                -- Interestingly, 'claims' column must precede 'total'
                sum(claims) over(partition by name) as total,
                claims = max(claims) over(partition by name) as flag
            from
                (select
                    companyid,
                    companyname
                from
                    insurancecompany
                where
                    companyid = :id) as A left outer join
                insuranceplan as B on B.companyid = A.companyid
                left outer join claim as C on C.uin = B.uin
                left outer join treatment as T
                on T.claimid = C.claimid
            group by
                B.planname, T.diseaseid) as F left outer join
            disease as D on D.diseaseid = F.diseaseid
        where
            flag
        order by
            total desc
    );
    return table(T);
  END;
  $$
  ;

call companyperformance(6942);
