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