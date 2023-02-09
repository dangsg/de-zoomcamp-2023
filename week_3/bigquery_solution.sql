CREATE OR REPLACE EXTERNAL TABLE `dtc-de-376507.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dtc-de-376507/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned` AS 
SELECT * FROM `dtc-de-376507.trips_data_all.external_fhv_tripdata`;

-- BEGIN Q1
SELECT COUNT(*) FROM `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned`;
-- END Q1

-- BEGIN Q2
SELECT COUNT(DISTINCT(`Affiliated_base_number`)) FROM `dtc-de-376507.trips_data_all.external_fhv_tripdata`;
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned`;
-- END Q2

-- BEGIN Q3
SELECT COUNT(*) FROM `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
-- END Q3

-- BEGIN Q4
CREATE OR REPLACE TABLE `dtc-de-376507.trips_data_all.fhv_tripdata_partitioned_clustered` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS (
  SELECT *
  FROM `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned`
);
-- END Q4

-- BEGIN Q5
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-376507.trips_data_all.fhv_tripdata_non_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-376507.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
-- END Q5