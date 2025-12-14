-- ============================================================
-- 1. Tabela DimDate (Zaktualizowana o BOOLEAN)
-- ============================================================
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key INT,
    date_str STRING,
    year INT,
    month INT,
    day INT,
    day_name STRING,
    is_weekend BOOLEAN  -- Teraz używamy poprawnego typu
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Ładowanie danych
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