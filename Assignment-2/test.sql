CREATE TABLE test_edges
  (
     paperid      INTEGER,
     citedpaperid INTEGER
  );

INSERT INTO test_edges VALUES(0, 1)
INSERT INTO test_edges VALUES(1, 2)
INSERT INTO test_edges VALUES(1, 3)
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

drop procedure dfs

CREATE TYPE [dbo].[visitedID] AS TABLE (id int)
CREATE TYPE [dbo].[citedPaper] AS TABLE(
     paperid      INTEGER,
     citedpaperid INTEGER);

begin

    CREATE PROCEDURE dfs
    @prevVisitedID  visitedID READONLY,
    @edges  citedPaper READONLY

    AS BEGIN
        declare @prevCnt int = (select count(*) from @prevVisitedID)
        print @prevCnt
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
        --select * from @updatedVisitedID
        -- insert related citedpaperid nodes into @updatedVisitedID table
        insert into @updatedVisitedID(id) (
            select e.citedpaperid from @edges e join @prevVisitedID p on e.paperid = p.id
                             where e.citedpaperid not in (select * from @prevVisitedID)
            )
        --select * from @updatedVisitedID

        select distinct id from @updatedVisitedID
--         declare @query varchar(50)
--         set @query = 'select * from ' + @tableName
--         exec (@query)
    end

       declare @visited_nodes visitedID;
        insert into @visited_nodes(id) values (1);


--     declare @visited_nodes visitedID;
--     insert into @visited_nodes(id) (
--         select paperid from test_nodes
--         );


    declare @curVisitedNum int = (
        select count(*) from @visited_nodes
        )
    -- init 0
    declare @totalNodeNum int = (
        select count(*) from test_nodes
        )
    -- init 9

    -- select top(1) v.paperid from  (select paperid from test_nodes where paperid not in (select * from @visited_nodes)) as v

    declare @all_edges citedPaper
    insert into @all_edges(paperid, citedpaperid) (select * from test_edges)
        select * from @all_edges
    exec dfs @visited_nodes, @all_edges;



--     while (@curVisitedNum < @totalNodeNum)
--     begin
--
--         declare @curVisitedNum int = (
--         select count(*) from @visited_nodes
--         )
--     end
end

