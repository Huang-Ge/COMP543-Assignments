-- select name, location, sighted from sightings
-- where month(sighted) = 8
select map from sightings s, features f
where month(s.sighted) = 8 and s.location = f.location and s.name = 'Alpine penstemon'