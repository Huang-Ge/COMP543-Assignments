select person, count(*) from visited_flowers
group by person

create view visited_flowers as
    select distinct * from (select person, name from sightings) as t9_1

create view all_finished_obervers as
    select p.person from people p
    where not exists (
        select f.comname from flowers f
        where f.comname not in (
            select v.name from visited_flowers v
            where v.person = p.person
            )
        )

create view all_observe_records as
    select person, name, sighted from sightings
    where person in (select person from all_finished_obervers)

-- select name, person, min(sighted) as first_sighted from all_observe_records
-- group by name, person

select person, max(first_sighted) as last_seen from (select name, person, min(sighted) as first_sighted from all_observe_records
group by name, person) as t9_2
group by person
