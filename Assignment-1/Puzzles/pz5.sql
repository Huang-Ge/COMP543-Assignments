select distinct genus from flowers
group by genus
having count(species) > 1


-- select distinct f1.genus from flowers f1
-- where exists(
--     select f2.genus from flowers f2
--         where f1.genus = f2.genus and f1.species <> f2.species
--
--           )

-- select * from flowers