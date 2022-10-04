select distinct * from (select person, name from sightings) as t12_1

select count(*) from john_flowers group by person

select * from john_flowers

select distinct * from (select person, name from sightings) as t12_1
where person <> 'John' and name not in (select name from john_flowers)

-- All flowers sighted by John
create view john_flowers as
select * from (select distinct * from (select person, name from sightings) as t12_1) as t12_2
where person = 'John'

-- All flowers for all other ppl that are not shared with John
create view not_shared as
select person, count(*) as none_shared
from(select distinct * from (select person, name from sightings) as t12_1
where person <> 'John' and name not in (select name from john_flowers)) as t12_2
group by person

-- All flowers for all other ppl that are shared with John
create view shared_with as
select person, count(*) as shared
from(select distinct * from (select person, name from sightings) as t12_1
where person <> 'John' and name in (select name from john_flowers)) as t12_2
group by person

--Calculate similarity for all other ppl according to Jaccard Index
-- (shared_flower_numbers / (not_shared_flower_numbers + flower_numbers_sighted_by_john))
create view similar_with_john as
select p.person, n.none_shared, s.shared, (select count(*) from john_flowers group by person) as john_num,
       cast(s.shared as float) / cast(n.none_shared + (select count(*) from john_flowers group by person) as float) as similarity
from people p, shared_with s, not_shared n
where p.person = s.person and p.person = n.person

-- find the person with highest similarity
select person from similar_with_john
where similarity = (select max(similarity) from similar_with_john)

select * from similar_with_john


