-- tables: athlete, region
show shares;

create or replace database olympic from share SRGNYVR.WZ73622.s1;
use olympic;

show tables;

-- 1.How many olympics games have been held?

-- 2.List down all Olympics games held so far.

-- 3.Mention the total no of nations who participated in each olympics game?

-- 4. Which year saw the highest and lowest no of countries participating in olympics?
create view q4 as
with
counts as (select
    year as yr,
    count(distinct noc) as ctr
    from athletes
    group by year)
select
    yr as year,
    ctr as countries,
    (case true
        when ctr = (select max(ctr) from counts) then 'Highest'
        when ctr = (select min(ctr) from counts) then 'Lowest'
        when true then null
    end) as participation
from
    counts
having
    participation is not null;

-- 5. Which nation has participated in all of the olympic games?
create view q5 as
with
total_games as (select count(distinct year,season) from athletes)
select
    noc,
    team
from
    athletes
group by
    noc, team
having
    count(distinct year,season) = (select * from total_games);


-- 6. Identify the sport which was played in all summer olympics.
with
T as (select * from athletes where season='Summer'),
total as (select count(distinct year) from T)
select
    sport
from
    T
group by
    sport
having
    count(distinct year) = (select * from total);


-- 7. Which Sports were just played only once in the olympics?
select
    lastGame as games,
    sport
from
    (select
        sport,
        max(concat(year,' ',season)) as lastGame,
        count(distinct games) as occurs
    from
        athletes
    group by
        sport) as T
where
    T.occurs = 1;


-- 8. Fetch the total no of sports played in each olympic games.
select
    games,
    count(distinct sport) as total_sports
from
    athletes
group by
    games;


-- 9. Fetch details of the oldest athletes to win a gold medal.
with
T as (select * from athletes where medal = 'Gold')
select
    *
from
    T
where
    age = (select max(age) from T);


-- 10. Find the Ratio of male and female athletes participated in all olympic games.
select
    sex,
    count(distinct id)/(select count(distinct id) from athletes) as ratio
from
    athletes
group by
    sex;


-- 11. Fetch the top 5 athletes who have won the most gold medals.
select
    *
from
    (select
        id,
        name,
        sum(iff(medal='Gold',1,0)) as golds
    from
        athletes
    group by
        id,name) as T
order by
    golds desc
limit 5;


-- 12. Fetch the top 5 athletes who have won the most medals (gold/silver/bronze).
select
    *
from
    (select
        id,
        name,
        sum(iff(medal is not null,1,0)) as total_medals
    from
        athletes
    group by
        id,name) as T
order by
    total_medals desc
limit 5;

-- 13. Fetch the top 5 most successful countries in olympics. Success is defined by no of medals won.
select
    *
from
    (select
        noc,
        sum(iff(medal is not null,1,0)) as total_medals
    from
        athletes
    group by
        noc) as T
order by
    total_medals desc
limit 5;



-- 14. List down total gold, silver and broze medals won by each country.
select
    noc,
    count(distinct iff(medal='Gold',event,null)) as gold,
    count(distinct iff(medal='Silver',event,null)) as silver,
    count(distinct iff(medal='Bronze',event,null)) as bronze
from
    athletes
group by
    noc;


-- 15. List down total gold, silver and broze medals won by each country corresponding to each olympic games.
select
    games,
    noc,
    count(distinct iff(medal='Gold',event,null)) as gold,
    count(distinct iff(medal='Silver',event,null)) as silver,
    count(distinct iff(medal='Bronze',event,null)) as bronze
from
    athletes
group by
    games, noc;




-- 16. Identify which country won the most gold, most silver and most bronze medals in each olympic games
select
    games,
    -- Aggregate-function(Window-function(...)) does not compile?
    -- listagg(iff(gold = (max(gold) over (partition by games)),noc,null),', ') as MXGold,

    listagg(iff(gold = MXG,noc,null),', ') as MXGold,
    min(MXG) as Golds,
    listagg(iff(silver = MXS,noc,null),', ') as MXSilver,
    min(MXS) as Silvers,
    listagg(iff(bronze = MXB,noc,null),', ') as MXBronze,
    min(MXB) as Bronzes
from
    (select
        games,
        noc,
        count(distinct iff(medal='Gold',event,null)) as gold,
        -- Window-function(Aggregate-function(...)) compiles?
        (max(gold) over (partition by games)) as MXG, -- What is the unambiguous-identity of 'gold'?

        count(distinct iff(medal='Silver',event,null)) as silver,
        (max(silver) over (partition by games)) as MXS,
        count(distinct iff(medal='Bronze',event,null)) as bronze,
        (max(bronze) over (partition by games)) as MXB
    from
        athletes
    group by
        games, noc) as T
group by
    games
order by
    games;


-- 17. Identify which country won the most gold, most silver, most bronze medals and the most medals in each olympic games.
select
    games,
    -- Aggregate-function(Window-function(...)) does not compile?
    -- listagg(iff(gold = (max(gold) over (partition by games)),noc,null),', ') as MXGold,

    listagg(iff(gold = MXG,noc,null),', ') as MXGold,
    min(MXG) as Golds,
    listagg(iff(silver = MXS,noc,null),', ') as MXSilver,
    min(MXS) as Silvers,
    listagg(iff(bronze = MXB,noc,null),', ') as MXBronze,
    min(MXB) as Bronzes,
    listagg(iff(total = MXTOT,noc,null),', ') as MXTotal,
    min(MXTOT) as Total
from
    (select
        games,
        noc,
        count(distinct iff(medal='Gold',event,null)) as gold,
        -- Window-function(Aggregate-function(...)) compiles?
        (max(gold) over (partition by games)) as MXG, -- What is the unambiguous-identity of 'gold'?

        count(distinct iff(medal='Silver',event,null)) as silver,
        (max(silver) over (partition by games)) as MXS,
        count(distinct iff(medal='Bronze',event,null)) as bronze,
        (max(bronze) over (partition by games)) as MXB,

        (gold + bronze + silver) as total,
        max(total) over (partition by games) as MXTOT
    from
        athletes
    group by
        games, noc) as T
group by
    games
order by
    games;



-- 18. Which countries have never won gold medal but have won silver/bronze medals?
select distinct
    R.noc,
    R.region as country
from
    athletes as A left outer join region as R on
    A.NOC = R.NOC
where
    medal = 'Silver' or medal = 'Bronze';



-- 19. In which Sport/event, India has won highest medals.
select
    sport,
    event,
    medals
from
    (select
        sport,
        event,
        noc,
        count(distinct games) as medals,
        (max(medals) over (partition by sport,event)) as MX
    from
        athletes
    where
        medal is not null
    group by
        noc,event,sport) as T
where
    noc = 'IND' and medals = MX;
-- where/having
    -- noc = 'IND' and
    -- Compilation error for both 'having' and 'where'?!
    -- medals = (max(medals) over (partition by sport,event));


-- 20. Hockey Analysis:

-- For 'IND' players in sport 'Hockey':
-- player, events, medals,
select
    id,
    name,
    sex,
    count(distinct year,season) as games,
    count(medal) as medals
from
    (select
        *
    from
        athletes
    where
        sport='Hockey' and noc='IND') as T
group by
    id,name,sex
order by
    games;

-- Hockey Events, India Team Composition and their outcomes
select
    games,
    event,
    listagg(distinct concat(id,'(',sex,')'), ', ') as team,
    -- sum(iff(medal='Gold',1,0)) as gold,
    -- sum(iff(medal='Silver',1,0)) as silver,
    -- sum(iff(medal='Bronze',1,0)) as bronze,
    -- sum(iff(medal is null,1,0)) as participated,
    count(distinct id) as size,
    listagg(distinct medal,', ') as standing,
from
    athletes
where
    sport='Hockey' and noc='IND'
group by
    games,event;