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

**Ingest & Preprocessing:** [Apache NiFi v1.23+](cite: 47) — Orchestrates raw data fetches, handles GZIP extraction, performs early field mutation (currency stripping, formatting, and pagination), and converts inputs into structured Avro streams.

**Message Broker:** [Apache Kafka](cite: 48) — Manages the real-time event pipeline stream via the `police-stream` topic, isolating input workloads from processing nodes.

**Distributed Compute Engine:** [Apache Spark v3.x](cite: 53) — Handled downstream execution through **Spark Structured Streaming** for continuous ingestion into NoSQL storage and **Spark Batch** for running dimensional data alignment and spatial joins.

**NoSQL Storage Layer:** [Apache HBase](cite: 51) — Stores unified stream outputs under dynamic column families, indexed heavily via composite row keys to facilitate sub-second geographic ranges scans.

**Data Lake Warehouse:** [Hadoop Distributed File System (HDFS)](cite: 50) — Houses raw, standardized, and aggregated analytical files serialized into columnar Apache Parquet configurations.

**Analytical Engine:** [Apache Hive](cite: 55) — Provides schema-on-read abstractions using managed external definitions overlaid on top of HDFS nodes and maps HBase tables natively via the `Hive-HBase-Connector`.


---

## Data Repositories & Engine Schemas

### 1. Inside Airbnb (Batch Target)

**Source:** [Inside Airbnb Portal](https://insideairbnb.com/get-the-data/) 
**Profiles Ingested:** London (June 2025 Snapshots) & New York City (November 2025 Snapshots) 
**Target Storage:** HDFS `/processed/airbnb_cleaned` via columnar Parquet.
**Attributes:** Includes unique identifier strings (`id`), geographical coordinate locations (`latitude`/`longitude`), pricing floats (`price`), spatial bounds (`neighbourhood_cleansed`), property/room classifications, and review tracking parameters.

### 2. Public Safety Datasets (Simulated Streams)

**UK Police Data:** Extracted from [data.police.uk](https://data.police.uk/data/), monitoring crime incidents registered by the Metropolitan Police Service across Greater London.
**NYPD Arrest Data:** Real-time collection executed over the [NYC Open Data Socrata API](https://data.cityofnewyork.us/resource/uip8-fykc.json), collecting physical booking events tracked during equivalent reporting intervals.
**Target Broker Layer:** Kafka `police-stream`.

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
