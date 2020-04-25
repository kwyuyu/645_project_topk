-- Question 1 : Top-10 insights from DBLP: Try different values of 1, 2, 3.
DROP TABLE IF EXISTS q1 cascade;
CREATE TABLE q1 AS (
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

    select v.year as venue_year, v.school as school,
            sum(p_s.paper_s) as paper_score
    from paper_score_temp as p_s, papers as p, venue as v
    where p_s.paper_id = p.id and p.venue = v.id
    and v.year <> 0 and school <> ''
    group by venue_year, school
    order by paper_score DESC, venue_year, school
);
alter table q1 add primary key (venue_year, school);


DROP TABLE IF EXISTS s_q1 cascade;
CREATE TABLE s_q1 AS (
    select * from q1
    ORDER BY random() LIMIT 100
);
alter table s_q1 add primary key (venue_year, school);