-- select name, month(sighted) as month from sightings
select person, month, count(*) as month_count from (select person, month(sighted) as month from sightings) as t10_1
group by person, month

select person, sum(month_count) as total_sighted from (select person, month, count(*) as month_count from (select person, month(sighted) as month from sightings) as t10_1
group by person, month) as t10_2
group by person

select month, month_count from (select person, month, count(*) as month_count from (select person, month(sighted) as month from sightings) as t10_1
group by person, month) as t10_3
where person = 'Jennifer'

create view total_times as
    select person, sum(month_count) as total_sighted from (select person, month, count(*) as month_count from (select person, month(sighted) as month from sightings) as t10_1
    group by person, month) as t10_2
    group by person

-- select total_sighted from total_times where person = 'Jennifer'

select month, cast(month_count as float) / cast((select total_sighted from total_times where person = 'Jennifer') as float)
from (select person, month, count(*) as month_count from (select person, month(sighted) as month from sightings) as t10_1
group by person, month) as t10_3
where person = 'Jennifer'