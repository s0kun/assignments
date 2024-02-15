drop database if exists `exercises`;
create database exercises;
use exercises;

-- Q. 1

drop table if exists `emp`;
CREATE TABLE emp (
    empid INT PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(100),
    gender ENUM('Male', 'Female'),
    department VARCHAR(100)
);
INSERT INTO emp (`name`, gender, department) VALUES
    ('John Doe', 'Male', 'Engineering'),
    ('Jane Smith', 'Female', 'Marketing'),
    ('Alex Johnson', 'Male', 'Finance'),
    ('Emma Lee', 'Female', 'Human Resources'),
    ('Chris Taylor', 'Male', 'Operations');

-- Solution:
select
	department,
    sum(if(gender = 'Male',1,0)) as "Num of males",
    sum(if(gender = 'Female',1,0)) as "Num of females"
from
	emp
group by department;

-- Q. 2
drop table if exists `salary`;
CREATE TABLE salary (
    ename VARCHAR(100),
    Jan DECIMAL(10, 2),
    Feb DECIMAL(10, 2),
    Mar DECIMAL(10, 2)
);
INSERT INTO salary (ename, Jan, Feb, Mar) VALUES
    ('John Doe', 5400.00, 5200.00, 5100.00),
    ('Jane Smith', 4800.00, 6900.00, 9050.00),
    ('Alex Johnson', 5500.00, 5900.00, 5700.00),
    ('Emma Lee', 4700.00, 9800.00, 4900.00),
    ('Chris Taylor', 5500.00, 5300.00, 5400.00);

-- Solution:
with
T as (
	select
		ename,
		"January" as mnth,
		Jan as sal
	from
		salary
	union
	select
		ename,
		"February" as mnth,
		Feb as sal
	from
		salary
	union
	select
		ename,
		"March" as mnth,
		Mar as sal
	from
		salary
)
select
	T.ename as "Name",
    T.sal as "Value",
	T.mnth as "Month"
from
	T left outer join 
    (select ename,max(sal) as mx from T group by ename) as R on R.ename = T.ename
where
	T.sal = R.mx
order by T.ename;


-- Q. 3
drop table if exists `score`;
CREATE TABLE score (
    cid INT,
    marks INT
);
INSERT INTO score (cid, marks) VALUES
    (81, 85),
    (52, 90),
    (35, 75),
    (34, 80),
    (54, 85),
    (16, 90),
    (74, 80),
    (82, 75),
    (49, 85),
    (100, 90);

select * from score;

-- Solution:
with
rnk_tbl as (
select
	marks,
    (select count(*) from (select distinct marks from score) as T where T.marks >= A.marks) as rnk
from
	(select distinct marks from score) as A
)
select
	r.marks as Marks,
    r.rnk as "Rank",
    group_concat(s.cid separator ', ') as Candidates 
from
	score as s left outer join rnk_tbl as r on r.marks = s.marks
group by r.marks, r.rnk
order by r.rnk;

-- Q. 4
drop table if exists cust_mail;
CREATE TABLE cust_mail (
    cid INT PRIMARY KEY,
    mailid VARCHAR(255)
);
INSERT INTO cust_mail (cid, mailid) VALUES
    (45, 'abc@gmail.com'),
    (23, 'def@yahoo.com'),
    (34, 'abc@gmail.com'),
    (21, 'bef@gmail.com'),
    (94, 'def@yahoo.com');


-- Solution:
SET SQL_SAFE_UPDATES = 0;

delete from cust_mail 
where
	cid is not null and
    mailid is not null and
	(cid, mailid) not in 
    (select
		*
	from
		(select
			min(c.cid),
			c.mailid
		from
			cust_mail as c
		group by c.mailid) as T
	);

SET SQL_SAFE_UPDATES = 1;

select * from cust_mail;