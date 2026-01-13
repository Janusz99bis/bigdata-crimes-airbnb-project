
USE bigdata;

-- T00
SELECT 'T00_DB_OK' AS test_name;

-- T01
SHOW TABLES;

-- T02
DESCRIBE airbnb_cleaned;
DESCRIBE police_unified;
DESCRIBE listings_with_police_stats;
DESCRIBE neighbourhood_stats;
DESCRIBE city_comparisons;
DESCRIBE police_geohash_agg;

-- T03
SELECT 'T03_airbnb_rows' AS test_name, COUNT(*) AS n_rows FROM airbnb_cleaned;
SELECT 'T03_police_rows' AS test_name, COUNT(*) AS n_rows FROM police_unified;
SELECT 'T03_joined_rows' AS test_name, COUNT(*) AS n_rows FROM listings_with_police_stats;
SELECT 'T03_neigh_rows'  AS test_name, COUNT(*) AS n_rows FROM neighbourhood_stats;
SELECT 'T03_city_rows'   AS test_name, COUNT(*) AS n_rows FROM city_comparisons;

SELECT 'T03_police_geo_rows' AS test_name, COUNT(*) AS n_rows FROM police_geohash_agg;

-- T04
SELECT 'T04_airbnb_sample' AS test_name, city_code, id, geohash5
FROM airbnb_cleaned
LIMIT 5;

SELECT 'T04_joined_sample' AS test_name, city_code, id, geohash5, crime_events_total, safety_index, crime_bucket
FROM listings_with_police_stats
LIMIT 5;

SELECT 'T04_city_comparisons' AS test_name, *
FROM city_comparisons;

-- T05
SELECT 'T05_airbnb_nulls' AS test_name,
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
  SUM(CASE WHEN city_code IS NULL THEN 1 ELSE 0 END) AS city_nulls,
  SUM(CASE WHEN geohash5 IS NULL THEN 1 ELSE 0 END) AS geohash_nulls
FROM airbnb_cleaned;

SELECT 'T05_joined_nulls' AS test_name,
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
  SUM(CASE WHEN city_code IS NULL THEN 1 ELSE 0 END) AS city_nulls,
  SUM(CASE WHEN geohash5 IS NULL THEN 1 ELSE 0 END) AS geohash_nulls,
  SUM(CASE WHEN crime_events_total IS NULL THEN 1 ELSE 0 END) AS crimes_nulls,
  SUM(CASE WHEN safety_index IS NULL THEN 1 ELSE 0 END) AS safety_nulls
FROM listings_with_police_stats;

-- T06
SELECT 'T06_listings_per_city' AS test_name, city_code, COUNT(*) AS listings
FROM airbnb_cleaned
GROUP BY city_code
ORDER BY city_code;

SELECT 'T06_joined_bucket_dist' AS test_name, city_code, crime_bucket, COUNT(*) AS n
FROM listings_with_police_stats
GROUP BY city_code, crime_bucket
ORDER BY city_code, crime_bucket;

-- T07
SELECT 'T07_sum_crimes_by_city' AS test_name, city_code, SUM(crime_events_total) AS sum_crimes
FROM listings_with_police_stats
GROUP BY city_code
ORDER BY city_code;
