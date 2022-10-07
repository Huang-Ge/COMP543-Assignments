
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
            if ( @afterCnt <= 10 and @afterCnt >=4)
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