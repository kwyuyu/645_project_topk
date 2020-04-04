create table authors
(	id integer primary key,
	name varchar(200)
);

create table venue
(	id integer primary key,
	name varchar(200) not null,
	year integer not null,
	school varchar(200),
	volume varchar(50),
	number varchar(50),
	type integer not null
);

create table papers
(	id integer primary key,
	name varchar not null,
	venue integer references venue (id),
	pages varchar(50),
	url varchar
);

-- this is the unclean paperauths table
create table uncleanpaperauths
(	paperid integer,
	authid integer
);

-- copy file from csv to table
\copy authors from '/Users/roger/Desktop/645_project_topk/dataset/authors.csv' with csv;
\copy venue from '/Users/roger/Desktop/645_project_topk/dataset/venue.csv' with csv header;
\copy papers from '/Users/roger/Desktop/645_project_topk/dataset/papers.csv' with csv header;
\copy uncleanpaperauths from '/Users/roger/Desktop/645_project_topk/dataset/paperauths.csv' with csv header;



-- create clean paperauths
create table paperauths
as 
select distinct uncleanpaperauths.paperid, uncleanpaperauths.authid
from uncleanpaperauths, authors, papers
where uncleanpaperauths.authid = authors.id
and uncleanpaperauths.paperid = papers.id;


-- set primary key and foreign key
alter table paperauths add primary key (paperid, authid);
alter table paperauths add foreign key (authid) references authors (id);
alter table paperauths add foreign key (paperid) references papers (id);



-- create b+tree index
create index btreePaperauthsIndex on paperauths using btree (paperid, authid);
create index btreeAuthorsIndex on authors using btree (name);


-- join all tables
create table allpaperauths as
(select a_pa_p.*, v.name as venue_name, v.year, v.school, v.volume, v.number, v.type
from (select a_pa.*, p.name as paper_name, p.venue as venue_id, p.pages, p.url
from (select a.id as author_id, a.name as author_name, pa.paperid as paper_id
from authors as a join paperauths as pa on a.id = pa.authid) as a_pa join papers as p on a_pa.paper_id = p.id) as a_pa_p
join venue as v on a_pa_p.venue_id = v.id);