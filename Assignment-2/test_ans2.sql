CREATE TABLE test_probs
  (
     id    INTEGER,
     prob float
  );

declare @n float = (select count(*) from test_nodes)
declare @d float = 0.85
set @n = @n + 0.1
print @n

create table test_sink(id int)

delete from sink
insert into test_sink(id) (
    select distinct t.paperid from test_nodes t
    where not exists (
        select e.paperid from test_edges e
        where e.paperid = t.paperid
        )
    )

declare @n float = (select count(*) from test_nodes)
-- initialize
delete from test_probs
insert into test_probs(id, prob) (
    select distinct paperid, 1/@n from test_nodes
    )

drop procedure step

CREATE PROCEDURE step
    -- PR[i][j] = (1-d)/n + d * citingSum
    AS BEGIN
        declare @delta float = 0
        declare @newProb float = 0
        declare @prevProb float = 0
        declare @n float = (select count(*) from test_nodes)
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
            for select paperid from test_nodes
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
            set @prevProb = (select prob from test_probs t where t.id = @curID)

            declare sources cursor local
                for select distinct paperid from test_edges e where e.citedpaperid = @curID
            open sources
            fetch next from sources into @nxtSrc
            WHILE @@FETCH_STATUS = 0
            BEGIN
                set @citingNum = (select count(*) from test_edges e where e.paperid = @nxtSrc)
                set @citingProb = (select prob from test_probs t where t.id = @nxtSrc)
                set @citingSum = @citingSum + @citingProb / @citingNum
                fetch next from sources into @nxtSrc
            END
            close sources
            deallocate sources




            -- set @sinkSum = (select sum(prob) from test_probs p, test_sink s where s.id = p.id) / @n
            -- set @nonSinkSum = (1-@d) / @n * (select sum(prob) from test_probs where id not in (select id from test_sink))
            set @nonSinkSum= (1-@d) / @n
            -- set @newProb = (1 - @d) / @n + @d * @citingSum
            set @newProb = @nonSinkSum + @d * @citingSum

            set @delta = @delta + ABS(@newProb - @prevProb)
            insert into @nextProbs(id, prob) values(@curID, @newProb)


            fetch next from id_cursor into @curID
        END;

        close id_cursor
        deallocate id_cursor


        set @sinkSum = (select sum(prob) from @nextProbs where id in (select id from test_sink)) / @n
        update @nextProbs set prob = prob + @sinkSum
        set @delta = (select sum(com.diff) from
                        (select abs(nxt.prob - p.prob) as diff
                         from @nextProbs nxt join test_probs p on nxt.id = p.id) as com)

        delete from test_probs
        insert into test_probs(id, prob) (select * from @nextProbs)

        print @delta
        if (@delta >= 0.1)
            begin
                exec step
            end
END

go

exec step

    select sum(prob) from test_probs




    select sum(prob) from test_probs p, test_sink s where s.id = p.id

    select sum(prob) from test_probs where id not in (select id from test_sink)


    drop procedure cursorTest

CREATE PROCEDURE cursorTest
    -- PR[i][j] = (1-d)/n + d * citingSum
    AS BEGIN
    declare @curID int = 0
    declare id_cursor cursor LOCAL
            for select paperid from test_nodes
        open id_cursor
        fetch next from id_cursor into @curID
        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT @curID
            fetch next from id_cursor into @curID
        end
        close id_cursor
        deallocate id_cursor

end

exec cursorTest
exec cursorTest