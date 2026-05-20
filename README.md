# Big Data: Airbnb & Crime Analysis — London & New York City
---

## Overview

This project builds a hybrid Big Data pipeline to ingest, process, and analyse the spatial relationship between short-term rental activity (Airbnb) and public safety data (crime/arrest records) in **London** and **New York City**.

The goal is to determine whether areas of high Airbnb density correlate with elevated crime intensity at the neighbourhood level — a question relevant to city administrators, law enforcement, real-estate investors, and travel platforms.

The system combines **batch processing** (Airbnb listings) with a **simulated streaming layer** (police data routed through Kafka), all running on a single-node Hadoop environment while preserving a typical multi-layer Big Data architecture.

---

## Architecture

```
[ Inside Airbnb ]          [ NYC Open Data ]       [ data.police.uk ]
       │                          │                        │
  (Batch CSV)               (Socrata API)            (Local CSV)
       │                          └──────────┬─────────────┘
       ▼                                     ▼
┌─────────────────────────────────────────────────────────────┐
│                        Apache NiFi                          │
│  Ingest · Validation · Avro conversion · Stream simulation  │
└──────────────────┬──────────────────────────────────────────┘
                   │                          │
            (Direct write)             (Event-time routing)
                   ▼                          ▼
           ┌──────────────┐         ┌──────────────────────┐
           │     HDFS     │         │     Apache Kafka      │
           │   /raw/…     │         │  topic: police-stream │
           └──────┬───────┘         └──────────┬───────────┘
                  │                             │
                  │                     (Structured Streaming)
                  │                             ▼
                  │                  ┌──────────────────────┐
                  │                  │     Apache Spark      │
                  │                  │  Schema unification   │
                  │                  └──────────┬───────────┘
                  │                             │
                  │                             ▼
                  │                  ┌──────────────────────┐
                  │                  │     Apache HBase      │
                  │◄─────────────────┤  bigdata:police_events│
                  │  (Geohash join)  └──────────────────────┘
                  ▼
        ┌──────────────────┐
        │   Apache Spark   │  ← Batch: clean · join · aggregate
        │   (Batch Layer)  │
        └────────┬─────────┘
                 │  Parquet
                 ▼
        ┌──────────────────┐
        │  HDFS /analytical│
        └────────┬─────────┘
                 ▼
        ┌──────────────────┐
        │   Apache Hive    │  ← External tables · SQL analytics
        └────────┬─────────┘
                 ▼
        ┌──────────────────┐
        │  Visualizations  │  ← Folium heatmaps · choropleth maps
        └──────────────────┘
```

---

## Repository Structure

```
bigdata-crimes-airbnb-project/
├── nifi-templates/          # Exported NiFi flow templates (Airbnb, NYPD, UKPD)
├── spark-batch/             # PySpark batch notebooks & jobs
│   ├── 1_Airbnb_cleaned+geohash.ipynb
│   ├── 2_police_unified.ipynb
│   ├── 3_listings_with_police_stats.ipynb
│   └── 4_neighbourhood_stats.ipynb
├── notebooks/               # Jupyter / Zeppelin exploration notebooks
├── hive-sql/                # HiveQL DDL and analytical queries (.hql)
├── hbase/                   # HBase schema setup and test scripts
├── kafka/                   # Kafka topic configuration
├── scripts/                 # Data download helpers
│   ├── download_airbnb.sh
│   ├── download_police_uk.py
│   └── download_nyc_api.py
├── tests/                   # Functional test suites
│   ├── test_spark_pipeline.py
│   ├── hive_functional_tests.hql
│   └── hbase_test.py
├── visualizations/          # Folium map notebooks and screenshots
├── environment.yml          # Conda environment definition
├── .gitignore               # Excludes data/ directory from version control
└── bigdata_report.pdf       # Full project report (Polish)
```

> **Note:** The `data/` directory is excluded from version control via `.gitignore`. All raw and processed datasets live exclusively in HDFS.

---

## Data Sources

| Source | City | Format | Records | Update frequency |
|---|---|---|---|---|
| [Inside Airbnb](https://insideairbnb.com/get-the-data/) | London (Jun 2025), NYC (Nov 2025) | CSV + GeoJSON | ~96k (LON) | Every 3–4 months |
| [data.police.uk](https://data.police.uk/data/) | London (Jun 2025) | CSV | ~100k | Monthly |
| [NYC Open Data — NYPD Arrests](https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-) | New York City (2025 YTD) | CSV / JSON | ~212k | Daily |

---

## Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Ingest & preprocessing | Apache NiFi | HTTP fetch, GZIP extraction, Avro conversion, Kafka routing |
| Message broker | Apache Kafka | Simulated streaming via `police-stream` topic |
| Distributed storage | HDFS / Hadoop | Raw, processed, and analytical Parquet files |
| NoSQL storage | Apache HBase | Unified police events indexed by `date#geohash#city#event_id` |
| Batch & stream processing | Apache Spark | Schema unification, geohash spatial joins, aggregations |
| SQL analytics | Apache Hive | External tables over HDFS Parquet + Hive-HBase connector |
| Visualisation | Python / Folium | Interactive heatmaps and choropleth district maps |

---

## Getting Started

### Prerequisites

- Hadoop / HDFS running locally (single-node pseudo-distributed)
- Apache NiFi, Kafka, Spark, HBase, and Hive installed and configured
- Python 3.x with the Conda environment from `environment.yml`

```bash
conda env create -f environment.yml
conda activate bigdata
```

### 1. Initialise HDFS directories

```bash
hdfs dfs -mkdir -p /user/$USER/bigdata/{raw/{airbnb,police_uk,police_nyc},\
processed/{airbnb_clean,police_events,geo},\
analytical/{agg_neighbourhood,agg_geohash,reports}}
hdfs dfs -chmod -R 755 /user/$USER/bigdata
```

### 2. Create HBase table

```bash
hbase shell
> create 'bigdata:police_events', 'cf_common', 'cf_uk', 'cf_nyc'
```

### 3. Download source data

```bash
bash scripts/download_airbnb.sh
python scripts/download_police_uk.py
python scripts/download_nyc_api.py
```

### 4. Start NiFi flows

Import the templates from `nifi-templates/` into the NiFi UI and start the three processor groups:
- `Airbnb_Ingest_Flow` — batch ingest to HDFS
- `NYPD_Stream_Flow` — NYC arrests → Kafka
- `UKPD_Stream_Flow` — London crimes → Kafka

### 5. Run Spark batch pipeline

```bash
spark-submit tests/test_spark_pipeline.py   # optional smoke test first
```

Then run the notebooks in order (1 → 4) from `spark-batch/`.

### 6. Register Hive external tables

```bash
hive -f hive-sql/create_external_tables.hql
```

### 7. Generate visualisations

Run the notebooks in `visualizations/` to produce the heatmap and choropleth maps.

---

## Key Analytical Results

All analyses used a random sample of 200 Airbnb listings per city for performance reasons in a single-node environment.

### Price comparison

| City | Avg. price/night | Median price/night |
|---|---|---|
| London | £153.23 | £105.00 |
| New York City | $210.05 | $149.80 |

### Crime intensity around listings

| City | Avg. crimes nearby | Median crimes nearby | Avg. safety index |
|---|---|---|---|
| London | 1954.31 | 1739.73 | 0.00093 |
| New York City | 17.11 | 11.93 | 0.09293 |

The large difference is a direct consequence of the far higher reporting density in the UKPD dataset (~79k events) versus the NYC sample (~400 events) used in analysis.

### Correlations (price / rating vs. crime)

| City | Price vs. crime | Rating vs. crime | Price vs. safety |
|---|---|---|---|
| London | +0.173 | +0.160 | −0.078 |
| New York City | −0.099 | +0.053 | +0.084 |

In London, pricier listings tend to cluster in central, high-crime areas. In NYC the opposite holds — higher prices correlate with safer surroundings.

---

## Tests

All functional tests can be run independently:

```bash
# Spark pipeline integrity
spark-submit tests/test_spark_pipeline.py

# Hive external tables
hive -f tests/hive_functional_tests.hql

# HBase CRUD and schema
python tests/hbase_test.py
```

All 8 Spark tests, 8 Hive tests, and 6 HBase tests pass (PASS) on the reference environment.

---

## HDFS Layout Reference

```
/user/<user>/bigdata/
├── raw/
│   ├── airbnb/
│   ├── police_uk/
│   └── police_nyc/
├── processed/
│   ├── airbnb_clean/
│   ├── police_events/
│   └── geo/
└── analytical/
    ├── agg_neighbourhood/
    ├── agg_geohash/
    └── reports/
```

---

## HBase Schema Reference

```
Namespace : bigdata
Table     : bigdata:police_events

Column families:
  cf_common  →  crime_type, latitude, longitude, timestamp
  cf_uk      →  lsoa_code, reported_by, falls_within, ...
  cf_nyc     →  pd_desc, law_cat_cd, arrest_boro, ...

Row key format: YYYYMM_<geohash7>_<uuid>
```

---

## Authors

**Jan Cwalina** · **Mikołaj Guzik**  
Faculty of Mathematics and Information Science, Warsaw University of Technology  
Academic year 2025/2026
