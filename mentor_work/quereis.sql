drop schema if exists mentor;

create schema mentor;
use mentor;

-- Q. 1
CREATE TABLE nameOcc (
  `id` int not null auto_increment primary key,
  `name` VARCHAR(255),
  `profession` VARCHAR(255)
);

INSERT INTO nameOcc (`name`, `profession`)
VALUES ('John', 'Engineer'),
       ('Jane', 'Doctor'),
       ('Michael', 'Teacher');

select
	concat(`name`,'(',substring(`profession`,1,1),')') as `output`
from
	nameOcc;
    
    
-- Q.2
drop table if exists actual;
drop table if exists recorded;

CREATE TABLE actual (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,  
  salary DECIMAL(10,2) NOT NULL,
  
  PRIMARY KEY (id)  
);
INSERT INTO actual (name, salary)
VALUES ('John Doe', 5000.00), 
       ('Jane Smith', 7200.75), 
       ('Michael Chen', 6125.50); 
       
create table recorded (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,  
  salary DECIMAL(10,2) NOT NULL,
  
  PRIMARY KEY(id)
);
INSERT INTO recorded (name, salary)
VALUES ('John Doe', 5), 
       ('Jane Smith', 72.75), 
       ('Michael Chen', 6125.5); 

select
	abs(avg(A.salary) - avg(R.salary)) as error
from
	actual as A inner join recorded as R on A.id = R.id;


-- Q.3
drop table if exists BST;

CREATE TABLE BST (
  id INT NOT NULL AUTO_INCREMENT, 
  value INT NOT NULL, 
  parent_id INT DEFAULT NULL,
  
  FOREIGN KEY (parent_id) REFERENCES BST(id),
  PRIMARY KEY (id)
);

INSERT INTO BST (value,parent_id)
VALUES (50,NULL),
       (25,1),
       (75,1);

select
	id,
	value,
    (case 1
		when parent_id is null then 'Root'
        when id in (select distinct parent_id from BST) then 'Inner'
        when 1 then 'Leaf'
	end) as vertex 
from
	BST
order by
	value asc;


-- Q.4
drop table if exists transaction;

CREATE TABLE transaction (
  transaction_id INT NOT NULL AUTO_INCREMENT,
  user_id INT NOT NULL,
  transaction_date DATE NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,

  PRIMARY KEY (transaction_id)
);

INSERT INTO transaction (user_id, transaction_date, product_id, quantity)
VALUES (1, '2023-01-01', 10, 1),
       (1, '2023-01-01', 20, 2),
       (2, '2023-01-02', 30, 1),
       (2, '2023-01-02', 40, 3),
       (3, '2023-01-03', 50, 1),
       (1, '2023-01-05', 10, 4),
       (3, '2023-01-05', 60, 2);

select
	count(*) as output
from
	(select
		user_id,
        count(distinct transaction_date) as ctr
	from transaction
	group by user_id) as T
where
	ctr > 1;
    
    
-- Q.5
drop table if exists subs ;

CREATE TABLE subs (
  user_id INT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  
  check (start_date <= end_date),
  PRIMARY KEY (user_id)
);

INSERT INTO subs (user_id, start_date, end_date)
VALUES (1, '2024-01-01', '2024-01-31'),
       (2, '2024-01-16', '2024-01-26'),
       (3, '2024-01-28', '2024-02-06'),
       (4, '2024-02-16', '2024-02-26');


-- Inefficient O(r*r) operations? ; r: Number of rows ;
select
	A.user_id as id,
	count(B.user_id)>0 as flag
from
	subs as A left outer join subs as B
	on 
		A.user_id!=B.user_id and (
			(A.start_date <= B.start_date and B.start_date <= A.end_date) or
			(B.start_date <= A.start_date and A.start_date <= B.end_date)
			)
group by
	A.user_id;
    

-- Efficient: [Assumption: (select count(distinct user_id) from subs) = (select count(user_id) from subs)]
select
	user_id,
	-- Doubt: How to specify only those window-rows(R) where (R.user_id != user_id)? (
    -- Asserting this assumption nullifies the prior assumption)
    (
		ifnull(
			(max(end_date) over (order by start_date asc rows between unbounded preceding and 1 preceding)) >= start_date,
            0)
		or
		ifnull(
			(min(start_date) over (order by end_date asc rows between 1 following and unbounded following)) <= end_date,
            0)
	) as flag
from
	subs;


-- Q.6
drop table if exists fb_eu_energy;
drop table if exists fb_na_energy;
drop table if exists fb_asia_energy;

CREATE TABLE fb_eu_energy (
  date DATETIME,
  consumption INT
);

INSERT INTO fb_eu_energy (date, consumption)
VALUES ('2020-01-01', 400),
       ('2020-01-02', 350),
       ('2020-01-03', 500),
       ('2020-01-04', 500),
       ('2020-01-07', 600);

CREATE TABLE fb_na_energy (
  date DATETIME,
  consumption INT
);

INSERT INTO fb_na_energy (date, consumption)
VALUES ('2020-01-01', 250),
       ('2020-01-02', 375),
       ('2020-01-03', 600),
       ('2020-01-06', 500),
       ('2020-01-07', 250);

CREATE TABLE fb_asia_energy (
  date DATETIME,
  consumption INT
);

INSERT INTO fb_na_energy (date, consumption)
VALUES ('2020-01-01', 400),
       ('2020-01-02', 400),
       ('2020-01-04', 675),
       ('2020-01-05', 1200),
       ('2020-01-06', 750);



-- Inefficient O(r*r) operations? ; r - Number of rows ;
with
T (date,consumption) as (
	select
		date,
        sum(consumption)
	from
		((select * from fb_eu_energy) 
		union all 
		(select * from fb_na_energy) 
		union all 
		(select * from fb_asia_energy)) as G
	group by
		date)
select
	T1.date,
	(select sum(consumption) from T as T2 where T1.date >= T2.date) as cumulative_consumption,
    round(100*(select sum(consumption) from T as T2 where T1.date >= T2.date)/(select sum(consumption) from T)) as cumulative_percent
from
	T as T1
order by
	date;
    
-- Efficient:
 with
T (date,consumption) as (
	select
		date,
        sum(consumption)
	from
		((select * from fb_eu_energy) 
		union all 
		(select * from fb_na_energy) 
		union all 
		(select * from fb_asia_energy)) as G
	group by
		date)
select
	date,
	run as cumulative_consumption,
    round(100*run/(select sum(consumption) from T)) as cumulative_percent
from
	(select
		*,
        (sum(consumption) over (order by date asc)) as run
	from
		T) as ``
order by
	date;

