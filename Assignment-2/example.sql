delete from sink
insert into sink(id) (
    select distinct t.paperid from nodes t
    where not exists (
        select e.paperid from edges e
        where e.paperid = t.paperid
        )
    )

CREATE PROCEDURE calPageRank
@d FLOAT
AS BEGIN
    DECLARE @PaperRank TABLE (paperID INTEGER, paperRank FLOAT)
    DECLARE @NewPaperRank TABLE (paperID INTEGER, paperRank FLOAT)
    INSERT INTO @PaperRank SELECT n.paperID, 0 FROM nodes n
    INSERT INTO @NewPaperRank SELECT n.paperID, 1 FROM nodes n

    DECLARE @TotalNodesNum FLOAT;
    DECLARE @SinkTotal FLOAT;

    SET @TotalNodesNum = (SELECT COUNT(*) FROM NODES);
    UPDATE @PaperRank SET paperRank = 1/@TotalNodesNum;

    WHILE ((SELECT SUM(NOD.DIFF) FROM
            (SELECT ABS(ne.paperRank - p.paperRank) AS DIFF
            FROM @PaperRank p join @NewPaperRank ne ON ne.paperID = p.paperID) AS NOD) >= 0.01)
    BEGIN;
    Delete FROM @NewPaperRank;
    INSERT INTO @NewPaperRank SELECT * FROM @PaperRank;

    UPDATE p SET p.paperRank = (1.0-@d)/@TotalNodesNum + @d*ISNULL((SELECT SUM(NOD.DIVEDRANK)
                FROM (SELECT ne.paperRank / (SELECT COUNT(*)
                FROM edges e WHERE e.paperID = ne.paperID) AS DIVEDRANK
            FROM @NewPaperRank ne, edges e2 WHERE ne.paperID = e2.paperID and e2.citedPaperID = p.paperID) AS NOD), 0)
    FROM @PaperRank p

    SET @SinkTotal = (SELECT SUM(ne.paperRank)
        FROM @NewPaperRank ne WHERE ne.paperID NOT IN (SELECT e.paperID
        FROM edges e))/@TotalNodesNum
    UPDATE @PaperRank SET paperRank = paperRank + @SinkTotal*@d;
    END;
SELECT TOP 10 p.paperID, n.paperTitle, p.paperRank FROM @PaperRank p, NODES n WHERE p.paperID = n.paperID ORDER BY paperRank DESC
END;
GO
EXECUTE calPageRank
               @d = 0.85
DROP PROCEDURE calPageRank