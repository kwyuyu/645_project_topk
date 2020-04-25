-- Question 4 : Top-10 insights for words used in the titles.
DROP TABLE IF EXISTS q4 cascade;
CREATE TABLE q4 AS (
    SELECT word, ndoc, nentry
    FROM ts_stat('SELECT to_tsvector(name) from papers')
    ORDER BY nentry DESC, ndoc DESC, word
    limit 10000
);
alter table q4 add primary key (word, ndoc);

DROP TABLE IF EXISTS s_q4 cascade;
CREATE TABLE s_q4 AS (
    select word, ndoc, nentry from q4
    ORDER BY nentry DESC, ndoc DESC, word
    LIMIT 100
);
alter table s_q4 add primary key (word, ndoc);

