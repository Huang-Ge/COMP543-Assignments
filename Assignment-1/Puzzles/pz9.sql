-- select distinct * from (select distinct location, person from sightings) as t
create view dictinct_places as
    select distinct * from (select distinct location, person from sightings) as t

create view visited_times as
    select person, count(location) as count from dictinct_places
    group by person

select person from visited_times
where count = (select max(count) from visited_times)

select distinct name from sightings
where person = (select person from visited_times where count = (select max(count) from visited_times))

select person, count(*) from (select * from sightings where person = (select person from visited_times
where count = (select max(count) from visited_times))) as T99
group by person