DROP TABLE IF EXISTS s_paperauths cascade;
DROP TABLE IF EXISTS s_authors cascade;
DROP TABLE IF EXISTS s_papers cascade;
DROP TABLE IF EXISTS s_venue cascade;

CREATE TABLE s_paperauths AS
    SELECT * FROM paperauths 
    OFFSET floor(random()*8997872) LIMIT 1000;

CREATE TABLE s_papers AS
    SELECT DISTINCT p.id, p.name, p.venue
    FROM s_paperauths s_pa, papers p
    WHERE s_pa.paperid = p.id;

CREATE TABLE s_authors AS
    SELECT DISTINCT a.id, a.name
    FROM s_paperauths s_pa, authors a
    WHERE s_pa.authid = a.id;

CREATE TABLE s_venue AS
    SELECT DISTINCT v.id, v.name, v.year, v.school, v.type
    FROM s_papers s_p, venue v
    WHERE s_p.venue = v.id;

ALTER TABLE s_papers ADD CONSTRAINT s_papers_pk PRIMARY KEY (id);
ALTER TABLE s_authors ADD CONSTRAINT s_authors_pk PRIMARY KEY (id);
ALTER TABLE s_venue ADD CONSTRAINT s_venue_pk PRIMARY KEY (id);
ALTER TABLE s_paperauths ADD CONSTRAINT s_fk_pa_pid FOREIGN KEY (paperid) REFERENCES s_papers(id);
ALTER TABLE s_paperauths ADD CONSTRAINT s_fk_pa_aid FOREIGN KEY (authid) REFERENCES s_authors(id);
CREATE UNIQUE INDEX ON s_paperauths (paperid, authid);
