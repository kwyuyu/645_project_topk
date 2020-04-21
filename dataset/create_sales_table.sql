DROP TABLE IF EXISTS sales CASCADE;

CREATE TABLE sales (
    year integer,
    brand varchar(5),
    sales integer
);

\COPY sales FROM '/Users/ypc/UMass/645/project/dataset/sales.csv' DELIMITER ',' CSV HEADER;
