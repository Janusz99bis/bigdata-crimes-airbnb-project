from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import happybase
import geohash2

# ============================================
# 1. Initialize Spark Session
# ============================================
spark = (
    SparkSession.builder.appName("PoliceKafkaToHBase")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session created")

# ============================================
# 2. Define Schema for Police Data
# ============================================

# Schema for NYPD data (with city_code and source)
nypd_schema = StructType(
    [
        StructField("arrest_key", LongType(), True),
        StructField("arrest_date_id", IntegerType(), True),
        StructField("pd_cd", IntegerType(), True),
        StructField("pd_desc", StringType(), True),
        StructField("ky_cd", IntegerType(), True),
        StructField("ky_desc", StringType(), True),
        StructField("law_cat_cd", IntegerType(), True),
        StructField("arrest_boro", IntegerType(), True),
        StructField("arrest_precinct", IntegerType(), True),
        StructField("jurisdiction_code", IntegerType(), True),
        StructField("age_group", IntegerType(), True),
        StructField("perp_sex", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("city_code", StringType(), True),  # Added
        StructField("source", StringType(), True),  # Added
    ]
)

# Schema for London Police data (with city_code and source)
london_schema = StructType(
    [
        StructField("crime_id", StringType(), True),
        StructField("date_id", IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("lsoa_code", StringType(), True),
        StructField("lsoa_name", StringType(), True),
        StructField("crime_type", IntegerType(), True),
        StructField("last_outcome_category", IntegerType(), True),
        StructField("city_code", StringType(), True),  # Added
        StructField("source", StringType(), True),  # Added
    ]
)

# ============================================
# 3. Read from Kafka
# ============================================
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "police-stream")
    .option("startingOffsets", "earliest")
    .load()
)

print("✅ Connected to Kafka topic: police-stream")

# ============================================
# 4. Parse JSON - Use city_code from data
# ============================================

# Convert Kafka value (binary) to string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")

# Parse JSON to get city_code field
# We'll use a temporary schema to extract city_code first
temp_schema = StructType(
    [
        StructField("city_code", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

kafka_with_city = (
    kafka_df.withColumn("temp", from_json(col("json_string"), temp_schema))
    .withColumn("city_code", col("temp.city_code"))
    .withColumn("source", col("temp.source"))
    .drop("temp")
)

# ============================================
# 5. Parse JSON based on city_code
# ============================================

# For NYC data
nyc_df = (
    kafka_with_city.filter(col("city_code") == "NYC")
    .select(from_json(col("json_string"), nypd_schema).alias("data"))
    .select("data.*")
)

# For London data
london_df = (
    kafka_with_city.filter(col("city_code") == "LON")
    .select(from_json(col("json_string"), london_schema).alias("data"))
    .select("data.*")
)

# For Bristol data (if you have it)
bristol_df = (
    kafka_with_city.filter(col("city_code") == "BRS")
    .select(from_json(col("json_string"), london_schema).alias("data"))
    .select("data.*")
)

# ============================================
# 6. Add geohash to each dataframe
# ============================================


# UDF for geohash
def generate_geohash(lat, lon):
    if lat and lon:
        try:
            return geohash2.encode(float(lat), float(lon), precision=5)
        except:
            return "XXXXX"
    return "XXXXX"


geohash_udf = udf(generate_geohash, StringType())

# Add geohash and normalize date_id field
nyc_with_geo = (
    nyc_df.withColumn("geohash", geohash_udf(col("latitude"), col("longitude")))
    .withColumn("date_id", col("arrest_date_id"))
    .withColumn("event_id", col("arrest_key").cast(StringType()))
)

london_with_geo = london_df.withColumn(
    "geohash", geohash_udf(col("latitude"), col("longitude"))
).withColumn("event_id", col("crime_id"))

bristol_with_geo = bristol_df.withColumn(
    "geohash", geohash_udf(col("latitude"), col("longitude"))
).withColumn("event_id", col("crime_id"))

# ============================================
# 7. Write to HBase Function
# ============================================


def write_batch_to_hbase(batch_df, batch_id):
    """Write each micro-batch to HBase"""

    count = batch_df.count()
    if count == 0:
        print(f"📦 Batch {batch_id}: No records to process")
        return

    print(f"📦 Processing batch {batch_id} with {count} records")

    def write_partition_to_hbase(partition):
        """Write partition to HBase"""
        try:
            # Connect to HBase
            connection = happybase.Connection("localhost", port=9090)
            table = connection.table("police_events")

            batch = table.batch()
            record_count = 0

            for row in partition:
                try:
                    city = row.city_code if row.city_code else "UNKNOWN"
                    event_id = str(row.event_id) if row.event_id else "UNKNOWN"
                    date_id = row.date_id if row.date_id else 0
                    geohash = row.geohash if row.geohash else "XXXXX"
                    source = row.source if row.source else "UNKNOWN"

                    # Create row key
                    row_key = f"{date_id}#{geohash}#{city}#{event_id}"

                    # Common columns
                    data = {
                        b"common:date_id": str(date_id).encode(),
                        b"common:latitude": str(row.latitude).encode()
                        if row.latitude
                        else b"",
                        b"common:longitude": str(row.longitude).encode()
                        if row.longitude
                        else b"",
                        b"common:geohash": geohash.encode(),
                        b"common:city_code": city.encode(),
                        b"common:data_source": source.encode(),
                        b"common:crime_category": b"UNKNOWN",  # Will add mapping later
                    }

                    # Add city-specific columns
                    if city == "NYC":
                        data.update(
                            {
                                b"source_specific:arrest_key": str(
                                    row.arrest_key
                                ).encode()
                                if row.arrest_key
                                else b"",
                                b"source_specific:pd_cd": str(row.pd_cd).encode()
                                if row.pd_cd
                                else b"",
                                b"source_specific:pd_desc": (
                                    row.pd_desc or ""
                                ).encode(),
                                b"source_specific:ky_cd": str(row.ky_cd).encode()
                                if row.ky_cd
                                else b"",
                                b"source_specific:ky_desc": (
                                    row.ky_desc or ""
                                ).encode(),
                                b"source_specific:law_cat_cd": str(
                                    row.law_cat_cd
                                ).encode()
                                if row.law_cat_cd
                                else b"",
                                b"source_specific:age_group": str(
                                    row.age_group
                                ).encode()
                                if row.age_group
                                else b"",
                                b"source_specific:perp_sex": (
                                    row.perp_sex or ""
                                ).encode(),
                                b"source_specific:arrest_boro": str(
                                    row.arrest_boro
                                ).encode()
                                if row.arrest_boro
                                else b"",
                                b"source_specific:arrest_precinct": str(
                                    row.arrest_precinct
                                ).encode()
                                if row.arrest_precinct
                                else b"",
                                b"source_specific:jurisdiction_code": str(
                                    row.jurisdiction_code
                                ).encode()
                                if row.jurisdiction_code
                                else b"",
                            }
                        )
                    elif city in ["LON", "BRS"]:  # London or Bristol
                        data.update(
                            {
                                b"source_specific:crime_id": (
                                    row.crime_id or ""
                                ).encode(),
                                b"source_specific:location": (
                                    row.location or ""
                                ).encode(),
                                b"source_specific:lsoa_code": (
                                    row.lsoa_code or ""
                                ).encode(),
                                b"source_specific:lsoa_name": (
                                    row.lsoa_name or ""
                                ).encode(),
                                b"source_specific:crime_type": str(
                                    row.crime_type
                                ).encode()
                                if row.crime_type
                                else b"",
                                b"source_specific:last_outcome_category": str(
                                    row.last_outcome_category
                                ).encode()
                                if row.last_outcome_category
                                else b"",
                            }
                        )

                    # Metadata
                    import time

                    data[b"metadata:ingested_at"] = str(int(time.time())).encode()
                    data[b"metadata:batch_id"] = str(batch_id).encode()

                    # Put to HBase
                    batch.put(row_key.encode(), data)
                    record_count += 1

                except Exception as e:
                    print(f"❌ Error processing row: {e}")
                    continue

            # Send batch
            batch.send()
            connection.close()

            if record_count > 0:
                print(f"✅ Wrote {record_count} records to HBase from partition")

        except Exception as e:
            print(f"❌ Error connecting to HBase: {e}")

    # Process each partition
    batch_df.foreachPartition(write_partition_to_hbase)


# ============================================
# 8. Union all dataframes and start streaming
# ============================================

# Union all city dataframes
all_cities_df = nyc_with_geo.unionByName(
    london_with_geo, allowMissingColumns=True
).unionByName(bristol_with_geo, allowMissingColumns=True)

print("🚀 Starting unified streaming job for all cities...")

query = (
    all_cities_df.writeStream.foreachBatch(write_batch_to_hbase)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kafka-hbase-checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Streaming job started. Processing data from Kafka...")
print("📊 Monitoring: Open http://localhost:4040 to see Spark UI")
print("⏹️  Press Ctrl+C to stop")

# Wait for termination
query.awaitTermination()
