-- This code is the same as 'create_score_table.sql', 
-- except based on the small testing tables.

DROP TABLE IF EXISTS s_paper_score cascade;

CREATE TABLE s_paper_score AS (
    with author_score(author_id, author_s) as (
        select authid, count(distinct paperid)
        from s_paperauths
        group by authid
    ),
    paper_score_temp(paper_id, paper_s) as (
        select pa.paperid, sum(a_s.author_s)
        from author_score as a_s, s_paperauths as pa
        where pa.authid = a_s.author_id
        group by pa.paperid
    )

    select p_s.paper_s as paper_score, 
        v.name as venue_name, v.year as venue_year, v.type as venue_type
    from paper_score_temp as p_s, s_papers as p, s_venue as v
    where p_s.paper_id = p.id and p.venue = v.id
);

alter table s_paper_score add primary key (venue_name, venue_year, venue_type);


