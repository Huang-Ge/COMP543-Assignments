
-- Task 1: find all connected components
drop procedure bfs

CREATE TYPE [dbo].[visitedID] AS TABLE (id int)
-- Store all grouped papers
create table groupedPapers (
    id int,
    groupID int
)


CREATE PROCEDURE bfs
    @prevVisitedID  visitedID READONLY,
    @groupID int

    AS BEGIN
        declare @prevCnt int = (select count(*) from @prevVisitedID)

        declare @updatedVisitedID visitedID;
        insert into @updatedVisitedID(id) (
            select * from @prevVisitedID
            )

        -- select * from @updatedVisitedID
        -- insert related paperid nodes into @updatedVisitedID table
        insert into @updatedVisitedID(id) (
            select e.paperid from edges e join @prevVisitedID p on e.citedpaperid = p.id
                             where e.paperid not in (select * from @prevVisitedID)
            )
        -- insert related citedpaperid nodes into @updatedVisitedID table
        insert into @updatedVisitedID(id) (
            select e.citedpaperid from edges e join @prevVisitedID p on e.paperid = p.id
                             where e.citedpaperid not in (select * from @prevVisitedID)
            )
        declare @afterVisitedID visitedID
        insert into @afterVisitedID(id) (
        select distinct id from @updatedVisitedID)

        declare @afterCnt int = (select count(*) from @afterVisitedID)
        print @prevCnt
        print @afterCnt
        if (@prevCnt <> @afterCnt)
        begin
            exec bfs @afterVisitedID,  @groupID
        end
        else
        begin
            insert into groupedPapers(id, groupID) (
                select id, @groupID from @afterVisitedID
                )
            if ( @afterCnt <= 10 and @afterCnt >4)
            begin
                select n.paperID, n.papertitle from nodes n join @afterVisitedID av on n.paperid = av.id
            end
        end
    END


BEGIN
    delete from groupedPapers
    declare @curVisitedNum int = (
        select count(*) from groupedPapers
        )
    -- init 0
    declare @totalNodeNum int = (
        select count(*) from nodes
        )
    -- init 9

    declare @nxt_visit_node visitedID;


--     declare @all_edges citedPaper
--     insert into @all_edges(paperid, citedpaperid) (select * from edges)

    declare @groupID int = 0

    while (@curVisitedNum < @totalNodeNum)
    begin
        delete from @nxt_visit_node;
        insert into @nxt_visit_node(id) (
        select top(1) v.paperid from  (select paperid from nodes where paperid not in(select id from groupedPapers)) as v
        );
        exec bfs @nxt_visit_node,  @groupID
        set @groupID = @groupID + 1


        -- update
        set @curVisitedNum = (select count(*) from groupedPapers)
    end
END

--Task2: PageRank

delete from sink
insert into sink(id) (
    select distinct t.paperid from nodes t
    where not exists (
        select e.paperid from edges e
        where e.paperid = t.paperid
        )
    )
drop procedure step

CREATE TABLE probs
  (
     id    INTEGER,
     prob float
  );

declare @n float = (select count(*) from nodes)
-- initialize
-- delete from probs
insert into probs(id, prob) (
    select distinct paperid, 1/@n from nodes
    )

CREATE PROCEDURE step
    -- PR[i][j] = (1-d)/n + d * citingSum
    AS BEGIN
        declare @delta float = 0
        declare @newProb float = 0
        declare @prevProb float = 0
        declare @n float = (select count(*) from nodes)
        declare @d float = 0.85
        declare @curID int = 0
        declare @citingSum float = 0
        declare @sinkSum float = 0
        declare @nonSinkSum float = 0
        declare @citingProb float = 0
        declare @nxtSrc int = 0
        declare @citingNum int = 0
        declare @nextProbs table (id INTEGER, prob float)
        declare id_cursor cursor local
            for select paperid from nodes
        open id_cursor
        fetch next from id_cursor into @curID
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- PRINT @curID
            -- if (@curID in (select * from test_sink))
--             begin
--                 insert into @nextProbs(id, prob) (select @curID, prob from test_probs where test_probs.id = @curID)
--             end
--             else

            set @citingSum = 0
            -- set @citingNum = (select count(*) from test_edges e where e.citedpaperid = @curID)
            -- set @citingNum = (select count(*) from test_edges e where e.paperid = @curID)
            set @prevProb = (select prob from probs t where t.id = @curID)

            declare sources cursor local
                for select distinct paperid from edges e where e.citedpaperid = @curID
            open sources
            fetch next from sources into @nxtSrc
            WHILE @@FETCH_STATUS = 0
            BEGIN
                set @citingNum = (select count(*) from edges e where e.paperid = @nxtSrc)
                set @citingProb = (select prob from probs t where t.id = @nxtSrc)
                set @citingSum = @citingSum + @citingProb / @citingNum
                fetch next from sources into @nxtSrc
            END
            close sources
            deallocate sources




            set @sinkSum = (select sum(prob) from probs p, sink s where s.id = p.id) / @n
            -- set @nonSinkSum = (1-@d) / @n * (select sum(prob) from test_probs where id not in (select id from test_sink))
            set @nonSinkSum= (1-@d) / @n
            -- set @newProb = (1 - @d) / @n + @d * @citingSum
            set @newProb = @d * @sinkSum + @nonSinkSum + @d * @citingSum

            set @delta = @delta + ABS(@newProb - @prevProb)
            insert into @nextProbs(id, prob) values(@curID, @newProb)


            fetch next from id_cursor into @curID
        END;

        close id_cursor
        deallocate id_cursor

        delete from probs
        insert into probs(id, prob) (select * from @nextProbs)
        print @delta
        if (@delta >= 0.1)
            begin
                exec step
            end
END

go

exec step

drop procedure step

select sum(prob) from test_probs

select top 10 id, prob from probs order by prob desc

-- CREATE PROCEDURE step
--     -- PR[i][j] = (1-d)/n + d * citingSum
--     AS BEGIN
--         declare @delta float = 0
--         declare @newProb float = 0
--         declare @prevProb float = 0
--         declare @n float = (select count(*) from nodes)
--         declare @d float = 0.85
--         declare @curID int = 0
--         declare @citingSum float = 0
--         declare @citingProb float = 0
--         declare @nxtSrc int = 0
--         declare @citingNum int = 0
--         declare @nextProbs table (id INTEGER, prob float)
--         declare id_cursor cursor local
--             for select paperid from nodes
--         open id_cursor
--         fetch next from id_cursor into @curID
--         WHILE @@FETCH_STATUS = 0
--         BEGIN
--             -- PRINT @curID
--             if (@curID in (select * from sink))
--             begin
--                 insert into @nextProbs(id, prob) (select @curID, prob from probs where probs.id = @curID)
--             end
--             else
--             begin
--                 set @citingSum = 0
--                 set @citingNum = (select count(*) from edges e where e.citedpaperid = @curID)
--                 set @prevProb = (select prob from probs t where t.id = @curID)
--                 declare sources cursor local
--                     for select distinct paperid from edges e where e.citedpaperid = @curID
--                 open sources
--                 fetch next from sources into @nxtSrc
--                 WHILE @@FETCH_STATUS = 0
--                 BEGIN
--                     set @citingProb = (select prob from probs t where t.id = @nxtSrc)
--                     set @citingSum = @citingSum + @citingProb / @citingNum
--                     fetch next from sources into @nxtSrc
--                 END
--                 close sources
--                 deallocate sources
--                 set @newProb = (1 - @d) / @n + @d * @citingSum
--                 set @delta = @delta + ABS(@newProb - @prevProb)
--                 insert into @nextProbs(id, prob) values(@curID, @newProb)
--             end
--             fetch next from id_cursor into @curID
--         END;
--
--         close id_cursor
--         deallocate id_cursor
--
--         delete from probs
--         insert into probs(id, prob) (select * from @nextProbs)
--         print @delta
--         if (@delta >= 0.01)
--             begin
--                 exec step
--             end
-- END
--
-- exec step


