create view james_locations as
select s.person, s.location, latitude from features f, sightings s
where s.location = f.location and s.person = 'James'

select * from james_locations

select location from james_locations
where latitude = (select min(latitude) from james_locations)
