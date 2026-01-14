#!/usr/bin/env python3

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ---------- helpers ----------
def fail(msg: str) -> None:
    print("\n[FAIL]", msg)
    sys.exit(1)


def ok(msg: str) -> None:
    print("[OK]  ", msg)


def require_columns(df, cols, df_name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        fail(f"{df_name}: brakuje kolumn: {missing}")
    ok(f"{df_name}: wymagane kolumny istnieją")


def assert_non_empty(df, df_name: str):
    n = df.count()
    if n <= 0:
        fail(f"{df_name}: dataset jest pusty (0 wierszy)")
    ok(f"{df_name}: liczba wierszy > 0 ({n})")
    return n


def assert_no_nulls(df, df_name: str, cols):
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]
    row = df.select(*exprs).collect()[0].asDict()
    bad = {k: v for k, v in row.items() if v and v > 0}
    if bad:
        fail(f"{df_name}: wykryto NULL-e w kolumnach: {bad}")
    ok(f"{df_name}: brak NULL-i w kolumnach {cols}")


def assert_range(df, df_name: str, col: str, lo: float, hi: float):
    bad = df.filter((F.col(col) < lo) | (F.col(col) > hi) | F.col(col).isNull()).count()
    if bad > 0:
        fail(f"{df_name}: {col} poza zakresem [{lo},{hi}] lub NULL. Bad rows: {bad}")
    ok(f"{df_name}: {col} w zakresie [{lo},{hi}]")


def assert_geohash_len(df, df_name: str, col="geohash5", length=5):
    bad = df.filter(F.length(F.col(col)) != length).count()
    if bad > 0:
        fail(f"{df_name}: {col} nie ma długości {length} dla {bad} wierszy")
    ok(f"{df_name}: {col} ma długość {length}")


def assert_distinct_values_subset(df, df_name: str, col: str, allowed):
    bad = df.filter(~F.col(col).isin(list(allowed))).count()
    if bad > 0:
        fail(f"{df_name}: {col} ma wartości spoza {allowed} (bad rows={bad})")
    ok(f"{df_name}: {col} ma poprawne wartości (subset {allowed})")


# ---------- main tests ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--airbnb", default="hdfs:///user/vagrant/bigdata/processed/airbnb_cleaned")
    parser.add_argument("--police", default="hdfs:///user/vagrant/bigdata/processed/police_unified")
    parser.add_argument("--joined", default="hdfs:///user/vagrant/bigdata/analytical/listings_with_police_stats")
    parser.add_argument("--skip_police", action="store_true", help="Pomiń test police_unified (jeśli jest problem z danymi).")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BigData-Functional-Tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Spark functional tests: START ===")
    print("airbnb:", args.airbnb)
    print("police:", args.police)
    print("joined:", args.joined)
    print("skip_police:", args.skip_police)
    print("====================================\n")

    # ---------- T01: Read datasets ----------
    try:
        airbnb = spark.read.parquet(args.airbnb)
    except Exception as e:
        fail(f"Nie mogę wczytać airbnb_cleaned z {args.airbnb}: {e}")

    try:
        joined = spark.read.parquet(args.joined)
    except Exception as e:
        fail(f"Nie mogę wczytać listings_with_police_stats z {args.joined}: {e}")

    police = None
    if not args.skip_police:
        try:
            police = spark.read.parquet(args.police)
        except Exception as e:
            fail(f"Nie mogę wczytać police_unified z {args.police}: {e}")

    ok("T01: wczytanie Parquet z HDFS")

    # ---------- T02: Schema checks ----------
    airbnb_required = [
        "id", "city_code", "latitude_d", "longitude_d", "geohash5",
        "price_num", "review_scores_rating", "number_of_reviews"
    ]
    require_columns(airbnb, airbnb_required, "airbnb_cleaned")

    joined_required = [
        "id", "city_code", "geohash5",
        "crime_events_total", "crime_events_distinct", "crime_types_distinct",
        "safety_index", "crime_bucket"
    ]
    require_columns(joined, joined_required, "listings_with_police_stats")

    if police is not None:
        police_required = [
            "event_id", "date_id", "city_code", "source",
            "latitude_d", "longitude_d", "geohash5",
            "crime_type_id", "crime_desc"
        ]
        require_columns(police, police_required, "police_unified")

    ok("T02: schema OK")

    # ---------- T03: Non-empty ----------
    airbnb_n = assert_non_empty(airbnb, "airbnb_cleaned")
    joined_n = assert_non_empty(joined, "listings_with_police_stats")
    if police is not None:
        _ = assert_non_empty(police, "police_unified")

    # ---------- T04: Basic null/range/geohash checks ----------
    assert_no_nulls(airbnb, "airbnb_cleaned", ["id", "city_code", "geohash5", "latitude_d", "longitude_d"])
    assert_range(airbnb, "airbnb_cleaned", "latitude_d", -90.0, 90.0)
    assert_range(airbnb, "airbnb_cleaned", "longitude_d", -180.0, 180.0)
    assert_geohash_len(airbnb, "airbnb_cleaned", "geohash5", 5)

    # joined: crime fields should be not null
    assert_no_nulls(joined, "listings_with_police_stats", ["id", "city_code", "geohash5", "crime_events_total", "safety_index", "crime_bucket"])
    assert_geohash_len(joined, "listings_with_police_stats", "geohash5", 5)

    # allowed buckets
    allowed_buckets = {"0", "1-5", "6-20", "21+"}
    assert_distinct_values_subset(joined, "listings_with_police_stats", "crime_bucket", allowed_buckets)

    if police is not None:
        assert_no_nulls(police, "police_unified", ["event_id", "city_code", "source", "geohash5", "latitude_d", "longitude_d"])
        assert_geohash_len(police, "police_unified", "geohash5", 5)

    ok("T04: jakość danych OK")

    # ---------- T05: Join logic checks ----------
    # 1) join output should have same number of rows as airbnb
    if joined_n != airbnb_n:
        print(f"[WARN] joined rows ({joined_n}) != airbnb rows ({airbnb_n}). "
              f"To może być OK, jeśli po drodze filtrowałeś listingi.")
    else:
        ok("T05a: joined rows == airbnb rows (expected for left join)")

    # 2) safety_index formula check: safety_index = 1/(1+crime_events_total)
    diff = (
        joined
        .withColumn("expected", F.lit(1.0) / (F.lit(1.0) + F.col("crime_events_total").cast("double")))
        .withColumn("abs_err", F.abs(F.col("safety_index") - F.col("expected")))
        .agg(F.max("abs_err").alias("max_abs_err"))
        .collect()[0]["max_abs_err"]
    )

    if diff is None:
        fail("T05b: safety_index error check failed (diff=None)")
    if diff > 1e-9:
        fail(f"T05b: safety_index nie zgadza się z 1/(1+crime_events_total). max_abs_err={diff}")
    ok("T05b: safety_index zgodny z definicją")

    # 3) Bucket rule sanity
    bad_bucket0 = joined.filter((F.col("crime_events_total") == 0) & (F.col("crime_bucket") != F.lit("0"))).count()
    if bad_bucket0 > 0:
        fail(f"T05c: crime_bucket nie jest '0' dla crime_events_total==0 (bad rows={bad_bucket0})")
    ok("T05c: bucket '0' dla crime_events_total==0")

    ok("T05: logika joinu OK")

    # ---------- T06: City coverage ----------
    cities_airbnb = [r["city_code"] for r in airbnb.select("city_code").distinct().collect()]
    cities_joined = [r["city_code"] for r in joined.select("city_code").distinct().collect()]

    if set(cities_airbnb) != set(cities_joined):
        print(f"[WARN] city_code w airbnb != city_code w joined: airbnb={cities_airbnb}, joined={cities_joined}")
    else:
        ok("T06: city_code spójne między airbnb i joined")

    print("\n=== Spark functional tests: PASS ===\n")
    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
