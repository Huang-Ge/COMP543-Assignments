create view SM as
    select class from features
    where map = 'Sawmill Mountain'

select class, count(*) as count from SM
group by class
having class = 'Summit'
