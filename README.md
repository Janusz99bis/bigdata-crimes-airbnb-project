Here is a comprehensive, production-ready `README.md` tailored specifically for your project repository. It incorporates the complete architecture, data engineering pipelines, technologies, and exact catalog structures detailed in your project documentation.

---

# Big Data Analysis: Short-Term Rentals vs. Public Safety 🏙️🛡️

## Project Overview

This project establishes a hybrid Big Data engineering pipeline designed to ingest, process, and analyze the spatial correlation between short-term rental activities (Airbnb) and public safety indicators (crime and arrest metadata) across two major metropolitan areas: **London** and **New York City (NYC)**.

The system implements a unified spatial join over a distributed environment to determine how short-term tourism densities overlap with regional crime patterns. This analytical framework is built to support data-driven decision-making for municipal administrators, real estate investors, law enforcement allocation, and travel platforms.

---

## Architecture Diagram

The architecture combines a **Batch Layer** for housing asset distributions and a simulated **Streaming Layer** for real-time policy event delivery, feeding into a unified **Serving Layer** for multi-dimensional SQL querying.

```
                [ Inside Airbnb ]               [ NYC Open Data ]       [ data.police.uk ]
                       │                                │                        │
                (Batch Ingest)                    (Socrata API)            (Local Source)
                       │                                │                        │
                       ▼                                ▼                        ▼
               ┌───────────────┐               ┌──────────────────────────────────────────┐
               │  Apache NiFi  │               │               Apache NiFi                │
               │ (Raw Ingest)  │               │      (Validation, Avro & Streaming)      │
               └───────┬───────┘               └────────────────────┬─────────────────────┘
                       │                                            │
                       ▼ (Direct Write)                             ▼ (Event-Time Routing)
               ┌───────────────┐                            ┌─────────────────────────────┐
               │  HDFS Layer   │                            │        Apache Kafka         │
               │    (/raw)     │                            │    Topic: police-stream     │
               └───────┬───────┘                            └──────────────┬──────────────┘
                       │                                                   │
                       │                                                   ▼ (Structured Streaming)
                       │                                            ┌─────────────────────────────┐
                       │                                            │        Apache Spark         │
                       │                                            │     (Schema Unification)     │
                       │                                            └──────────────┬──────────────┘
                       │                                                           │
                       ▼                                                           ▼
         ┌───────────────────────────┐                               ┌─────────────────────────────┐
         │       Apache Spark        │◄──────────────────────────────┤        Apache HBase         │
         │  (Batch Execution Layer)  │   (Spatial Join via Geohash)  │  Namespace: `bigdata:cf_*`  │
         └─────────────┬─────────────┘                               └──────────────┬──────────────┘
                       │                                                           │
                       ▼ (Parquet Format)                                          │
               ┌───────────────┐                                                   │
               │  HDFS Layer   │                                                   │
               │ (/analytical) │                                                   │
               └───────┬───────┘                                                   │
                       │                                                           │
                       ▼                                                           ▼
               ┌──────────────────────────────────────────────────────────────────────────┐
               │                               Apache Hive                                │
               │               (External Tables & Hive-HBase Connector)                   │
               └───────────────────────────────────┬──────────────────────────────────────┘
                                                   │
                                                   ▼
                                       ┌──────────────────────┐
                                       │     Visualizations   │
                                       │  (Folium / Heatmaps) │
                                       └──────────────────────┘

```

---

## Core Technologies

* 
**Ingest & Preprocessing:** [Apache NiFi v1.23+](cite: 47) — Orchestrates raw data fetches, handles GZIP extraction, performs early field mutation (currency stripping, formatting, and pagination), and converts inputs into structured Avro streams.


* 
**Message Broker:** [Apache Kafka](cite: 48) — Manages the real-time event pipeline stream via the `police-stream` topic, isolating input workloads from processing nodes.


* 
**Distributed Compute Engine:** [Apache Spark v3.x](cite: 53) — Handled downstream execution through **Spark Structured Streaming** for continuous ingestion into NoSQL storage and **Spark Batch** for running dimensional data alignment and spatial joins.


* 
**NoSQL Storage Layer:** [Apache HBase](cite: 51) — Stores unified stream outputs under dynamic column families, indexed heavily via composite row keys to facilitate sub-second geographic ranges scans.


* 
**Data Lake Warehouse:** [Hadoop Distributed File System (HDFS)](cite: 50) — Houses raw, standardized, and aggregated analytical files serialized into columnar Apache Parquet configurations.


* 
**Analytical Engine:** [Apache Hive](cite: 55) — Provides schema-on-read abstractions using managed external definitions overlaid on top of HDFS nodes and maps HBase tables natively via the `Hive-HBase-Connector`.



---

## Data Repositories & Engine Schemas

### 1. Inside Airbnb (Batch Target)

* 
**Source:** [Inside Airbnb Portal](https://insideairbnb.com/get-the-data/) 


* 
**Profiles Ingested:** London (June 2025 Snapshots) & New York City (November 2025 Snapshots) 


* 
**Target Storage:** HDFS `/processed/airbnb_cleaned` via columnar Parquet.


* 
**Attributes:** Includes unique identifier strings (`id`), geographical coordinate locations (`latitude`/`longitude`), pricing floats (`price`), spatial bounds (`neighbourhood_cleansed`), property/room classifications, and review tracking parameters.



### 2. Public Safety Datasets (Simulated Streams)

* 
**UK Police Data:** Extracted from [data.police.uk](https://data.police.uk/data/), monitoring crime incidents registered by the Metropolitan Police Service across Greater London.


* 
**NYPD Arrest Data:** Real-time collection executed over the [NYC Open Data Socrata API](https://data.cityofnewyork.us/resource/uip8-fykc.json), collecting physical booking events tracked during equivalent reporting intervals.


* 
**Target Broker Layer:** Kafka `police-stream`.



---

## Unified Storage Designs

### Apache HBase Table Structure

To reconcile the schema differences between NYPD data structures and UK crime sheets, data is flattened into a single HBase framework mapping to standard column families:

* 
**Namespace:** `bigdata` 


* 
**Table:** `bigdata:police_events` 


* 
**Rowkey Format:** `YYYYMM_ <geohash7> _ <UUID>` (Optimizes for space-time region queries).


* **Column Families:**
* 
`cf_common`: Fields present in both sources (`crime_type`, `latitude`, `longitude`, `timestamp`).


* 
`cf_uk`: Fields specific to UK datasets (`lsoa_code`, `reported_by`, `falls_within`).


* 
`cf_nyc`: Fields specific to NYPD datasets (`pd_desc`, `law_cat_cd`, `arrest_boro`, `perp_race`).





---

## Repository Directory Architecture

This project's workspace is structured as shown below. Large files, datasets, and runtime binaries are excluded using the `.gitignore` configuration to maintain a lightweight repository.

```
[cite_start]/home/<user>/bigdata-project/           # Root Workspace Directory [cite: 76]
[cite_start]├── data/                               # Excluded from version control via .gitignore [cite: 76]
[cite_start]│   ├── raw/                            # For localized testing and manual execution checkpoints [cite: 76]
[cite_start]│   └── temp/                           # Temporary working buffers handled during NiFi executions [cite: 76]
[cite_start]├── scripts/                            # Automations and pipeline orchestrations [cite: 76]
[cite_start]│   ├── download_airbnb.sh              # Bash utility targeting Inside Airbnb raw downloads [cite: 76]
[cite_start]│   ├── download_police_uk.py           # Preprocessing script targeting local UK data extraction [cite: 76]
[cite_start]│   └── download_nyc_api.py             # Socrata API handler for incremental batch pagination [cite: 76]
[cite_start]├── nifi-templates/                     # XML template exports detailing NiFi pipeline paths [cite: 76]
[cite_start]├── spark-jobs/                         # Native PySpark script configurations [cite: 76]
[cite_start]├── hive-sql/                           # DDL/DML queries containing .hql analytical schemas [cite: 76]
[cite_start]├── notebooks/                          # Interactive exploratory notebooks (Jupyter/Zeppelin) [cite: 76]
[cite_start]├── docs/                               # Detailed technical documentation [cite: 76]
[cite_start]├── environment.yml                     # Conda package deployment specifications [cite: 76]
[cite_start]└── README.md                           # Repository introduction profile [cite: 76]

```

### Distributed File System (HDFS) Targets

```
[cite_start]/user/<user>/bigdata/                   # HDFS System Root [cite: 78]
[cite_start]├── raw/                                # Unaltered landing assets [cite: 78]
[cite_start]│   ├── airbnb/                         # Raw property inventory CSV tables [cite: 78]
[cite_start]│   └── police/                         # Raw streaming data reference sets [cite: 78]
[cite_start]├── processed/                          # Schema-validated and clean working files [cite: 78]
[cite_start]│   ├── airbnb/                         # Validated Airbnb listings containing structural repairs [cite: 78]
[cite_start]│   ├── police_nyc/                     # Transformed Avro/Parquet data for NYPD [cite: 78]
[cite_start]│   └── police_uk/                      # Standardized crime outputs for London [cite: 78]
[cite_start]└── analytical/                         # Aggregated matrices ready for reporting layers [cite: 78]
    [cite_start]├── dimensions/                     # Core system dimension tables [cite: 78]
    [cite_start]├── agg_neighbourhood/              # Metric profiles aggregated by city districts [cite: 78]
    [cite_start]└── agg_geohash/                    # High-resolution geographic metric cells [cite: 78]

```

---

## Data Engineering Pipelines

### 1. Spatial Joining Using Geohash Strings

To bypass computationally expensive polygon overlay calculations, coordinates are resolved into spatial string hashes using a customized User Defined Function (UDF) inside Apache Spark:

```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import geohash2

# Resolve location values into an indexed 5-character string boundary precision map
geohash_udf = udf(lambda lat, lon: geohash2.encode(lat, lon, precision=5), StringType())

df = df.withColumn("geohash5", geohash_udf(col("latitude_d"), col("longitude_d")))

```

A 5-character precision geohash aggregates events within a grid cell of roughly 4.9km x 4.9km, enabling highly efficient bucket joins between the datasets.

### 2. Star-Schema Dimensional Modeling

To improve storage efficiency and query performance across Hive data lake partitions, descriptive properties are decoupled into explicit dimension tables:

* 
`dim_date`: Time intelligence dimension tracking calendar periods from 2000 to 2030.


* 
`dim_room_type`: Accommodation configurations mapped into fast numerical indexes.


* 
`dim_LAW_CAT_CD` / `dim_crime_type`: Unified classification references that standardize reporting terms across different legal systems (NYPD vs. UKPD).



---

## Deployment & Setup Instructions

### Step 1: Initialize the Distributed Environment

Construct the required directory structures inside HDFS and apply secure permission masks to authorize multi-agent read/write executions:

```bash
hdfs dfs -mkdir -p /user/$USER/bigdata/{raw/{airbnb,police_uk,police_nyc},processed/{airbnb_clean,police_events,geo},analytical/{agg_neighbourhood,agg_geohash,reports}}
hdfs dfs -chmod -R 755 /user/$USER/bigdata

```

### Step 2: Establish HBase Tables

Launch the HBase interactive shell to provision the core analytical storage tables:

```bash
hbase shell
create 'bigdata:police_events', 'cf_common', 'cf_uk', 'cf_nyc'

```

### Step 3: Run the Pipeline Components

1. 
**Apache NiFi:** Import the templates located under `nifi-templates/` into the user interface canvas and start the processor groups to begin ingesting the data.


2. 
**Streaming Processing:** Launch the Spark Structured Streaming application to start processing incoming Kafka event records.


3. 
**Batch Processing:** Run the pipeline notebooks sequentially to extract features, clean data, and execute the spatial joins:


```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark-jobs/process_airbnb_batch.py

```



---

## Authors

* 
**Jan Cwalina** 


* 
**Mikołaj Guzik** 


* 
*Faculty of Mathematics and Information Science, Warsaw University of Technology*
