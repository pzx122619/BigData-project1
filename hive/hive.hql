use default;
DROP TABLE IF EXISTS operator_departures;
DROP TABLE IF EXISTS operators;

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
