-- ===============================
-- BIG DATA PROJECT — HIVE LAYER
-- External tables over Spark Parquet (HDFS)
-- ===============================

CREATE DATABASE IF NOT EXISTS bigdata;
USE bigdata;

-- ===============================
-- AIRBNB CLEANED (Parquet)
-- ===============================
DROP TABLE IF EXISTS airbnb_cleaned;

CREATE EXTERNAL TABLE airbnb_cleaned (
  id STRING,
  city_code STRING,
  neighbourhood_cleansed STRING,
  latitude_d DOUBLE,
  longitude_d DOUBLE,
  geohash5 STRING,
  price_num DOUBLE,
  room_type INT,
  property_type STRING,
  accommodates INT,
  bedrooms FLOAT,
  beds INT,
  number_of_reviews INT,
  review_scores_rating FLOAT,
  availability_365 INT,
  host_is_superhost BOOLEAN,
  host_response_rate FLOAT,
  host_acceptance_rate FLOAT
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/processed/airbnb_cleaned';


-- ===============================
-- POLICE UNIFIED (Parquet)
-- ===============================
DROP TABLE IF EXISTS police_unified;

CREATE EXTERNAL TABLE police_unified (
  event_id STRING,
  date_id INT,
  city_code STRING,
  source STRING,
  latitude_d DOUBLE,
  longitude_d DOUBLE,
  geohash5 STRING,
  crime_type_id STRING,
  crime_desc STRING
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/processed/police_unified';


-- ===============================
-- LISTINGS WITH POLICE STATS (Parquet)
-- FIX: counts must be BIGINT (Spark count() -> LongType)
-- ===============================
DROP TABLE IF EXISTS listings_with_police_stats;

CREATE EXTERNAL TABLE listings_with_police_stats (
  id STRING,
  city_code STRING,
  neighbourhood_cleansed STRING,
  latitude_d DOUBLE,
  longitude_d DOUBLE,
  geohash5 STRING,
  price_num DOUBLE,
  room_type INT,
  property_type STRING,
  accommodates INT,
  bedrooms FLOAT,
  beds INT,
  number_of_reviews INT,
  review_scores_rating FLOAT,
  availability_365 INT,
  host_is_superhost BOOLEAN,
  host_response_rate FLOAT,
  host_acceptance_rate FLOAT,

  crime_events_total BIGINT,
  crime_events_distinct BIGINT,
  crime_types_distinct BIGINT,
  top_crime_desc STRING,
  top_crime_desc_count BIGINT,
  safety_index DOUBLE,
  crime_bucket STRING
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/analytical/listings_with_police_stats';


-- ===============================
-- NEIGHBOURHOOD / LSOA STATS (Parquet)
-- ===============================
DROP TABLE IF EXISTS neighbourhood_stats;

CREATE EXTERNAL TABLE neighbourhood_stats (
  city_code STRING,
  neighbourhood_cleansed STRING,
  listing_count BIGINT,
  avg_price DOUBLE,
  median_price DOUBLE,
  avg_rating DOUBLE,
  median_rating DOUBLE,
  avg_crimes_nearby DOUBLE,
  median_crimes_nearby DOUBLE,
  avg_safety_index DOUBLE,
  median_safety_index DOUBLE,

  bucket_0_cnt BIGINT,
  bucket_1_5_cnt BIGINT,
  bucket_6_20_cnt BIGINT,
  bucket_21p_cnt BIGINT,

  crimes_per_100_listings DOUBLE
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/analytical/neighbourhood_stats';


-- ===============================
-- CITY COMPARISONS (Parquet)
-- ===============================
DROP TABLE IF EXISTS city_comparisons;

CREATE EXTERNAL TABLE city_comparisons (
  city_code STRING,
  listing_count BIGINT,
  avg_price DOUBLE,
  median_price DOUBLE,
  avg_rating DOUBLE,
  median_rating DOUBLE,
  avg_crimes_nearby DOUBLE,
  median_crimes_nearby DOUBLE,
  avg_safety_index DOUBLE,
  median_safety_index DOUBLE,

  bucket_0_cnt BIGINT,
  bucket_1_5_cnt BIGINT,
  bucket_6_20_cnt BIGINT,
  bucket_21p_cnt BIGINT,

  bucket_0_pct DOUBLE,
  bucket_1_5_pct DOUBLE,
  bucket_6_20_pct DOUBLE,
  bucket_21p_pct DOUBLE,

  corr_price_vs_crime DOUBLE,
  corr_rating_vs_crime DOUBLE,
  corr_price_vs_safety DOUBLE
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/analytical/city_comparisons';


-- ===============================
-- (OPTIONAL) POLICE GEOHASH AGG (Parquet)
-- Jeśli chcesz mieć SQL nad tym agregatem
-- ===============================
DROP TABLE IF EXISTS police_geohash_agg;

CREATE EXTERNAL TABLE police_geohash_agg (
  city_code STRING,
  geohash5 STRING,
  crime_events_total BIGINT,
  crime_events_distinct BIGINT,
  crime_types_distinct BIGINT,
  top_crime_desc STRING,
  top_crime_desc_count BIGINT
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/analytical/police_geohash_agg';

