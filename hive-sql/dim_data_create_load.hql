-- ============================================================
-- 1. Tabela DimDate
-- ============================================================
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INT,
    date_str STRING,
    year INT,
    month INT,
    day INT,
    day_name STRING,
    is_weekend BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_date.csv' 
OVERWRITE INTO TABLE dim_date;


-- ============================================================
-- 2. Tabela DimHostResponseTime
-- ============================================================
DROP TABLE IF EXISTS dim_host_response_time;
CREATE TABLE dim_host_response_time (
    response_time_key INT,
    response_time_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_host_response_time.csv' 
OVERWRITE INTO TABLE dim_host_response_time;


-- ============================================================
-- 3. Tabela DimRoomType
-- ============================================================
DROP TABLE IF EXISTS dim_room_type;
CREATE TABLE dim_room_type (
    room_type_key INT,
    room_type_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_room_type.csv' 
OVERWRITE INTO TABLE dim_room_type;


-- ============================================================
-- 4. Tabela DimLawCatCD
-- ============================================================
DROP TABLE IF EXISTS dim_law_cat_cd;
CREATE TABLE dim_law_cat_cd (
    LAW_CAT_CD_key INT,
    LAW_CAT_CD_code STRING,
    LAW_CAT_CD_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_LAW_CAT_CD.csv' 
OVERWRITE INTO TABLE dim_law_cat_cd;


-- ============================================================
-- 5. Tabela DimJurisdictionCode
-- ============================================================
DROP TABLE IF EXISTS dim_jurisdiction_code;
CREATE TABLE dim_jurisdiction_code (
    JURISDICTION_CODE_key INT,
    JURISDICTION_CODE_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_JURISDICTION_CODE.csv' 
OVERWRITE INTO TABLE dim_jurisdiction_code;


-- ============================================================
-- 6. Tabela DimArrestBoro
-- ============================================================
DROP TABLE IF EXISTS dim_arrest_boro;
CREATE TABLE dim_arrest_boro (
    ARREST_BORO_key INT,
    ARREST_BORO_code STRING,
    ARREST_BORO_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_ARREST_BORO.csv' 
OVERWRITE INTO TABLE dim_arrest_boro;


-- ============================================================
-- 7. Tabela DimAgeGroup
-- ============================================================
DROP TABLE IF EXISTS dim_age_group;
CREATE TABLE dim_age_group (
    age_group_key INT,
    age_group_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_age_group.csv' 
OVERWRITE INTO TABLE dim_age_group;


-- ============================================================
-- 8. Tabela DimCrimeType
-- ============================================================
DROP TABLE IF EXISTS dim_crime_type;
CREATE TABLE dim_crime_type (
    crime_type_key INT,
    crime_type_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_crime_type.csv' 
OVERWRITE INTO TABLE dim_crime_type;


-- ============================================================
-- 9. Tabela DimOutcomeCategory
-- ============================================================
DROP TABLE IF EXISTS dim_outcome_category;
CREATE TABLE dim_outcome_category (
    outcome_category_key INT,
    outcome_category_desc STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/vagrant/bigdata/dimensions/dim_outcome_category.csv' 
OVERWRITE INTO TABLE dim_outcome_category;


-- ============================================================
-- 10. Tabela airbnb_listings
-- ============================================================
DROP TABLE IF EXISTS airbnb_listings;
CREATE EXTERNAL TABLE airbnb_listings (
    id                              STRING,
    scrape_id                       STRING,
    last_scraped_id                 INT,           -- klucz do dim_date
    name                            STRING,
    description                     STRING,
    neighborhood_overview           STRING,
    host_id                         STRING,
    host_name                       STRING,
    host_since_id                   INT,           -- klucz do dim_date
    host_location_country           STRING,
    host_location_city              STRING,
    host_about                      STRING,
    host_response_time              INT,           -- klucz do dim_host_response_time
    host_response_rate              FLOAT,
    host_acceptance_rate            FLOAT,
    host_is_superhost               BOOLEAN,
    host_listings_count             INT,
    host_total_listings_count       INT,
    host_has_profile_pic            BOOLEAN,
    host_identity_verified          BOOLEAN,
    neighbourhood_cleansed          STRING,
    latitude                        FLOAT,
    longitude                       FLOAT,
    property_type                   STRING,
    room_type                       INT,           -- klucz do dim_room_type
    accommodates                    INT,
    bathrooms                       FLOAT,
    bedrooms                        FLOAT,
    beds                            INT,
    price                           FLOAT,
    minimum_nights                  INT,
    maximum_nights                  INT,
    availability_30                 INT,
    availability_60                 INT,
    availability_90                 INT,
    availability_365                INT,
    number_of_reviews               INT,
    number_of_reviews_ltm           INT,
    number_of_reviews_l30d          INT,
    estimated_revenue_l365d         FLOAT,
    first_review_id                 INT,           -- klucz do dim_date
    last_review_id                  INT,           -- klucz do dim_date
    review_scores_rating            FLOAT,
    review_scores_accuracy          FLOAT,
    review_scores_cleanliness       FLOAT,
    review_scores_checkin           FLOAT,
    review_scores_communication     FLOAT,
    review_scores_location          FLOAT,
    review_scores_value             FLOAT,
    reviews_per_month               FLOAT,
    has_verifications_email         BOOLEAN,
    has_verifications_phone         BOOLEAN,
    has_verifications_work_email    BOOLEAN,
    amenities_has_wifi              BOOLEAN,
    amenities_has_smoke_alarm       BOOLEAN,
    amenities_has_kitchen           INT,
    amenities_has_washer            BOOLEAN,
    amenities_has_essentials        BOOLEAN,
    amenities_has_iron              BOOLEAN,
    amenities_has_hot_water         BOOLEAN,
    amenities_has_hangers           BOOLEAN,
    amenities_has_cma               BOOLEAN,
    amenities_has_heating           BOOLEAN,
    amenities_has_tv                BOOLEAN,
    amenities_has_microwave         BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/vagrant/bigdata/processed/airbnb_listings';

