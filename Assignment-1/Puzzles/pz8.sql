select distinct person from features f, sightings s
where s.location = f.location and class = 'Tower'

select person from people
where person not in (
    select distinct person from features f, sightings s
    where s.location = f.location and class = 'Tower'
    )

select * from people