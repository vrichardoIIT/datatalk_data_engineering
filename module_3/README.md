# HW QUERY

sql
'''
CREATE OR REPLACE EXTERNAL TABLE 
`applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://vrichardo_module_3_hw/yellow_tripdata_2024-*.parquet']
);


CREATE OR REPLACE TABLE 
`applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`
AS
SELECT * 
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024_ext`;



SELECT COUNT(*) 
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024_ext`;

SELECT COUNT(*) 
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`;

SELECT PULocationID
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`;

SELECT PULocationID, DOLocationID
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`;

SELECT COUNT(*) FROM  `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`
WHERE fare_amount = 0;


CREATE OR REPLACE TABLE `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024`;

SELECT DISTINCT(VendorID) 
FROM `applied-shade-485904-c9.zoomcamp_module_3.yellow_taxi_2024_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
'''