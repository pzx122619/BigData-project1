use default;
DROP TABLE IF EXISTS operator_departures;
DROP TABLE IF EXISTS operators;
DROP TABLE IF EXISTS region_rankings_json;

CREATE EXTERNAL TABLE operators(
  operator_id string,
  operator_name  string,
  region string,
  service_type string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir3}';


CREATE EXTERNAL TABLE operator_departures(
  operator_id string,
  departure_hour  int,
  total_passengers int,
  avg_ticket_price float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${input_dir4}';

CREATE EXTERNAL TABLE region_rankings_json (
    region STRING,
    service_type STRING,
    total_passengers BIGINT,
    avg_ticket_price DOUBLE,
    rank_in_region INT
)
STORED AS JSONFILE
LOCATION '${output_dir6}';

INSERT OVERWRITE TABLE region_rankings_json
SELECT
    o.region,
    o.service_type,
    SUM(d.total_passengers) AS total_passengers,
    AVG(d.avg_ticket_price) AS avg_ticket_price,
    RANK() OVER (PARTITION BY o.region ORDER BY SUM(d.total_passengers) DESC) AS rank_in_region
FROM operators o
JOIN operator_departures d
ON o.operator_id = d.operator_id
GROUP BY o.region, o.service_type;