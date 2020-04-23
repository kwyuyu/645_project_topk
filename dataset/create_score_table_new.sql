DROP TABLE IF EXISTS paper_score cascade;

CREATE TABLE paper_score_new AS (
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

    select sum(p_s.paper_s) as paper_score, v.name as venue_name, v.year as venue_year
    from paper_score_temp as p_s, papers as p, venue as v
    where p_s.paper_id = p.id and p.venue = v.id
    group by v.name, v.year
    order by v.name, v.year
);

alter table paper_score_new add primary key (venue_name, venue_year);


