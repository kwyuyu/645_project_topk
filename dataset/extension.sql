-- Extension : insights on specified keyword 'deep learning'
DROP TABLE IF EXISTS ext_dl cascade;
CREATE TABLE ext_dl AS (
    SELECT v.year, v.name as venue, SUM(ts_rank_cd(pname, query, 1|4|16)) as score
    FROM papers p, venue v, to_tsquery('english', 'deep & learning') query, to_tsvector('english', p.name) pname
    WHERE p.venue = v.id
        AND v.year <> 0 AND v.year < 2016
        AND pname @@ query
    GROUP BY v.year, v.name
    ORDER BY score DESC, year DESC
);
alter table ext_dl add primary key (year, venue);

DROP TABLE IF EXISTS s_ext_dl cascade;
CREATE TABLE s_ext_dl AS (
    select * from ext_dl
    ORDER BY random() LIMIT 100
);
alter table s_ext_dl add primary key (year, venue);
