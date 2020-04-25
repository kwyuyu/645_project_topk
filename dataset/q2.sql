-- Question 2 : insights on collaborations using a composite extractor.
--              Example: which authors have collaborated the most over time.
DROP TABLE IF EXISTS q2 cascade;
CREATE TABLE q2 AS (
    with paper_cnt(author_id, author_paper_cnt) as (
        select authid, count(distinct paperid)
        from paperauths
        group by authid
    ),
    distinct_collaborator_cnt(author_id, collaborations) as (
        select pa1.authid, count(distinct pa2.authid)
        from paperauths pa1, paperauths pa2
        where pa1.paperid = pa2.paperid and pa1.authid != pa2.authid
        group by pa1.authid
    ),
    paper_score_temp(paper_id, paper_s) as (
        select pa.paperid, sum(a_s.author_paper_cnt)
        from paper_cnt as a_s, paperauths as pa
        where pa.authid = a_s.author_id
        group by pa.paperid
    ),
    auth_score(author_id, auth_score) as (
        select pa.authid, sum(pst.paper_s)
        from paperauths as pa, paper_score_temp as pst
        where pa.paperid = pst.paper_id
        group by pa.authid
    )

    select pc.author_paper_cnt as paper_cnt, dcc.collaborations as collaborations,
            sum(a_s.auth_score) as auth_score
    from paper_cnt as pc, distinct_collaborator_cnt as dcc, auth_score as a_s
    where pc.author_id = dcc.author_id and pc.author_id = a_s.author_id
    group by paper_cnt, collaborations
    order by auth_score DESC, collaborations DESC, paper_cnt DESC
);
alter table q2 add primary key (paper_cnt, collaborations);

DROP TABLE IF EXISTS s_q2 cascade;
CREATE TABLE s_q2 AS (
    select * from q2
    ORDER BY random() LIMIT 20
);
alter table s_q2 add primary key (paper_cnt, collaborations);