use default;

DROP TABLE IF EXISTS mapreduce_results;
DROP TABLE IF EXISTS films_metadata;
DROP TABLE IF EXISTS final_result;

CREATE EXTERNAL TABLE mapreduce_results (
  film_id STRING,
  platform STRING,
  total_views INT,
  avg_watch_time DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir3}';

CREATE EXTERNAL TABLE films_metadata (
  film_id STRING,
  film_title STRING,
  genre STRING,
  film_length DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir4}';

-- Wynikowa tabela (6)
CREATE EXTERNAL TABLE final_result (
  genre STRING,
  film_title STRING,
  total_views INT,
  pct_film_watch_time DOUBLE,
  pct_genre_watch_time DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

INSERT INTO TABLE final_result
SELECT
    m.genre AS genre,
    m.film_title AS film_title,
    SUM(r.total_views) AS total_views,
    AVG(r.avg_watch_time / m.film_length) AS pct_film_watch_time,
    g.pct_genre_watch_time AS pct_genre_watch_time
FROM
    mapreduce_results r
JOIN
    films_metadata m
    ON r.film_id = m.film_id
JOIN
    (
        SELECT
            m2.genre AS genre,
            AVG(r2.avg_watch_time / m2.film_length) AS pct_genre_watch_time
        FROM
            mapreduce_results r2
        JOIN
            films_metadata m2
            ON r2.film_id = m2.film_id
        GROUP BY m2.genre
    ) g
    ON m.genre = g.genre
GROUP BY
    m.genre,
    m.film_title,
    g.pct_genre_watch_time
HAVING
    AVG(r.avg_watch_time / m.film_length) > g.pct_genre_watch_time;