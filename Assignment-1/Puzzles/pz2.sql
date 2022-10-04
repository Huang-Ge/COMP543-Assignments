SELECT S1.PERSON FROM SIGHTINGS S1
WHERE S1.Location = 'Moreland Mill' and S1.Person in (
    SELECT PERSON FROM SIGHTINGS
                  WHERE LOCATION = 'Steve Spring' AND S1.NAME = NAME
    )
-- SELECT s.PERSON, s.NAME, s.LOCATION FROM SIGHTINGS s
-- Where (s.location = 'Steve Spring' or s.location = 'Moreland Mill')