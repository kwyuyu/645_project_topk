DROP TABLE IF EXISTS paper_score_constraint cascade;
DROP VIEW IF EXISTS venue_paper_count CASCADE;

CREATE TEMP TABLE venue_paper_cnt AS 
    SELECT v.name, COUNT(p.id) AS paper_cnt 
    FROM venue v, papers p
    WHERE p.venue = v.id
    GROUP BY v.name
    ORDER BY paper_cnt DESC;

CREATE TABLE paper_score_constraint AS (
    with author_score(author_id, author_s) as (
        select authid, count(distinct paperid)
        from paperauths
        group by authid
    ),
    paper_score_temp(paper_id, paper_s) as (
        select pa.paperid, sum(a_s.author_s)
        from author_score as a_s, paperauths as pa
        where pa.authid = a_s.author_id
        group by pa.paperid
    )

    select p_s.paper_s as paper_score, 
        v.name as venue_name, v.year as venue_year
    from paper_score_temp as p_s, papers as p, venue as v, venue_paper_cnt as vp
    where p_s.paper_id = p.id and p.venue = v.id and 
		vp.name = v.name AND vp.paper_cnt > 10000 AND
		1995 <= v.year AND v.year <= 2015
);


