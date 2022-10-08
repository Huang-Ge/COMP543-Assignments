create procedure step
as begin
    declare @d float = 0.85
    declare @n float = (select count(*) from nodes)
    declare @delta float = 1

    declare @prevProb table (id int, prob float)
    declare @updatedProb table (id int, prob float)
    declare @sinkSum float = 0
    -- initial probability tables
    insert into @prevProb select paperid, 1/@n from nodes
    insert into @updatedProb select paperid, 1/@n from nodes
    while (@delta >= 0.01)
    begin
        delete from @prevProb
        insert into @prevProb select * from @updatedProb

        UPDATE up SET up.prob = (1.0-@d)/@n + @d*ISNULL((SELECT SUM(s.sourceProb)
                FROM (SELECT prev.prob / (SELECT COUNT(*)
                FROM edges e WHERE e.paperID = prev.id) AS sourceProb
                FROM @prevProb prev, edges e2 WHERE prev.id = e2.paperID and e2.citedPaperID = up.id) AS s), 0)
        FROM @updatedProb up

    SET @sinkSum = (SELECT SUM(prev.prob)
        FROM @prevProb prev WHERE prev.id IN (SELECT id from sink)) / @n
    UPDATE @updatedProb SET prob = prob + @sinkSum*@d;

    set @delta =(select sum(com.dif) from
                        (select abs(nxt.prob - p.prob) as dif
                         from @updatedProb nxt join @prevProb p on nxt.id = p.id) as com)
    end
    select top 10 id, prob from @updatedProb order by prob desc

end

go
exec step

drop procedure step