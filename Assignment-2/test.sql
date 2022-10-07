CREATE TABLE test_edges
  (
     paperid      INTEGER,
     citedpaperid INTEGER
  );

INSERT INTO test_edges VALUES(0, 1)
INSERT INTO test_edges VALUES(1, 2)
INSERT INTO test_edges VALUES(1, 3)
-- INSERT INTO test_edges VALUES(3, 10)
INSERT INTO test_edges VALUES(9, 4)
INSERT INTO test_edges VALUES(5, 4)
INSERT INTO test_edges VALUES(10, 14)

CREATE TABLE test_nodes
  (
     paperid    INTEGER,
     papertitle VARCHAR (150)
  );

INSERT INTO test_nodes VALUES(0, 'paper 0')
INSERT INTO test_nodes VALUES(1, 'paper 1')
INSERT INTO test_nodes VALUES(2, 'paper 2')
INSERT INTO test_nodes VALUES(3, 'paper 3')
INSERT INTO test_nodes VALUES(4, 'paper 4')
INSERT INTO test_nodes VALUES(5, 'paper 5')
INSERT INTO test_nodes VALUES(9, 'paper 9')
INSERT INTO test_nodes VALUES(10, 'paper 10')
INSERT INTO test_nodes VALUES(14, 'paper 14')

drop procedure bfs

create table groupedPapers (
    id int,
    groupID int
)

CREATE TYPE [dbo].[visitedID] AS TABLE (id int)
CREATE TYPE [dbo].[citedPaper] AS TABLE(
     paperid      INTEGER,
     citedpaperid INTEGER);

begin

    CREATE PROCEDURE bfs
    @prevVisitedID  visitedID READONLY,
    @edges  citedPaper READONLY,
    @groupID int

    AS BEGIN
        declare @prevCnt int = (select count(*) from @prevVisitedID)

        declare @updatedVisitedID visitedID;
        insert into @updatedVisitedID(id) (
            select * from @prevVisitedID
            )

        select * from @updatedVisitedID
        -- insert related paperid nodes into @updatedVisitedID table
        insert into @updatedVisitedID(id) (
            select e.paperid from @edges e join @prevVisitedID p on e.citedpaperid = p.id
                             where e.paperid not in (select * from @prevVisitedID)
            )
        -- insert related citedpaperid nodes into @updatedVisitedID table
        insert into @updatedVisitedID(id) (
            select e.citedpaperid from @edges e join @prevVisitedID p on e.paperid = p.id
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
            exec bfs @afterVisitedID, @edges, @groupID
        end
        else
        begin
            insert into groupedPapers(id, groupID) (
                select id, @groupID from @afterVisitedID
                )
        end

    END

       -- End of stored procedure





    -- select top(1) v.paperid from  (select paperid from test_nodes where paperid not in (select * from @visited_nodes)) as v




    delete from groupedPapers

    declare @curVisitedNum int = (
        select count(*) from groupedPapers
        )
    -- init 0
    declare @totalNodeNum int = (
        select count(*) from test_nodes
        )
    -- init 9

    declare @nxt_visit_node visitedID;


    declare @all_edges citedPaper
    insert into @all_edges(paperid, citedpaperid) (select * from test_edges)

        declare @groupID int = 0

    while (@curVisitedNum < @totalNodeNum)
    begin
        delete from @nxt_visit_node;
        insert into @nxt_visit_node(id) (
        select top(1) v.paperid from  (select paperid from test_nodes where paperid not in(select id from groupedPapers)) as v
        );
        exec bfs @nxt_visit_node, @all_edges, @groupID
        set @groupID = @groupID + 1


        -- update
        set @curVisitedNum = (select count(*) from groupedPapers)
        set @totalNodeNum = (select count(*) from test_nodes)
    end


end

