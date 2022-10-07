create database hg_as2;
use hg_as2;

CREATE TABLE nodes
  (
     paperid    INTEGER,
     papertitle VARCHAR (150)
  );

CREATE TABLE edges
  (
     paperid      INTEGER,
     citedpaperid INTEGER
  );

select * from edges