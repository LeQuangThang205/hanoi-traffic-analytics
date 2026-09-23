"""Phase 5.1-5.2 - SparkSession + schemas + traffic validation (READ-ONLY raw).

- Spark Local Mode, Windows-safe (PYSPARK_PYTHON = sys.executable).
- Session timezone UTC (ETL canonical; scheduling van Asia/Ho_Chi_Minh).
- Explicit StructType cho traffic (9 fields) + weather (12 fields).
- validate_and_normalize_traffic(): fail-fast validation + event_timestamp
  (TimestampType), giu nguyen 9 cot raw. KHONG congestion/join/export.
- Khong tao SparkSession o import time.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

try:
    from config import (
        SPARK_APP_NAME,
        SPARK_MASTER,
        TRAFFIC_DATA_CSV,
        WEATHER_DATA_CSV,
    )
except ImportError:
    SPARK_APP_NAME = "HanoiTrafficAnalytics"
    SPARK_MASTER = "local[*]"
    TRAFFIC_DATA_CSV = None
    WEATHER_DATA_CSV = None

TRAFFIC_SCHEMA = (
    StructType()
    .add("timestamp", StringType(), True)
    .add("road_name", StringType(), True)
    .add("lat", DoubleType(), True)
    .add("lon", DoubleType(), True)
    .add("current_speed", DoubleType(), True)
    .add("free_flow_speed", DoubleType(), True)
    .add("current_travel_time", DoubleType(), True)
    .add("free_flow_travel_time", DoubleType(), True)
    .add("confidence", DoubleType(), True)
)

WEATHER_SCHEMA = (
    StructType()
    .add("timestamp", StringType(), True)
    .add("road_name", StringType(), True)
    .add("lat", DoubleType(), True)
    .add("lon", DoubleType(), True)
    .add("weather_lat", DoubleType(), True)
    .add("weather_lon", DoubleType(), True)
    .add("temperature", DoubleType(), True)
    .add("humidity", DoubleType(), True)
    .add("precipitation", DoubleType(), True)
    .add("rain", DoubleType(), True)
    .add("wind_speed", DoubleType(), True)
    .add("weather_code", IntegerType(), True)
)


def create_spark_session(app_name=None, master=None):
    """Tao SparkSession local (WARN log, session timezone UTC).

    Chi goi khi can, khong o import.
    """
    spark = (
        SparkSession.builder
        .appName(app_name or SPARK_APP_NAME)
        .master(master or SPARK_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def load_traffic_data(spark, path=None):
    """Doc raw traffic CSV voi explicit schema (header, khong inferSchema)."""
    target = str(path) if path is not None else str(TRAFFIC_DATA_CSV)
    if not target or target == "None":
        raise RuntimeError("Khong xac dinh duoc duong dan traffic_data.csv.")
    return spark.read.option("header", True).schema(TRAFFIC_SCHEMA).csv(target)


def load_weather_data(spark, path=None):
    """Doc raw weather CSV voi explicit schema (header, khong inferSchema)."""
    target = str(path) if path is not None else str(WEATHER_DATA_CSV)
    if not target or target == "None":
        raise RuntimeError("Khong xac dinh duoc duong dan weather_data.csv.")
    return spark.read.option("header", True).schema(WEATHER_SCHEMA).csv(target)


# Raw traffic timestamp contract (evidence: toan bo raw hien tai dung 1 format
# ISO 8601 co offset, vd "2026-09-23T04:45:21+00:00"); default ISO parsing
# cua try_to_timestamp la du, khong can format framework rieng.

TRAFFIC_REQUIRED_COLUMNS = [f.name for f in TRAFFIC_SCHEMA.fields]


def validate_and_normalize_traffic(df):
    """Validate + parse timestamp cho raw traffic DataFrame (pure Spark).

    - Khong tao session/doc CSV/ghi file/goi API/mutate ben ngoai.
    - Fail-fast: neu co row invalid -> raise ValueError kem counts/reasons;
      KHONG sua/drop/fill rows. Neu valid -> tra ve DataFrame 10 cot
      (9 raw goc + event_timestamp TimestampType, instant UTC dung).
    """
    from functools import reduce

    from pyspark.sql import functions as F

    actual = list(df.columns)
    if actual != TRAFFIC_REQUIRED_COLUMNS:
        raise ValueError(
            f"Traffic schema drift: got {actual!r}, "
            f"expected {TRAFFIC_REQUIRED_COLUMNS!r}."
        )

    parsed = df.withColumn(
        # try_to_timestamp (default ISO parsing): malformed -> NULL thay vi
        # throw nhu ANSI to_timestamp, de parse failure duoc phat hien bang
        # unparseable_timestamp reason. Raw contract chi chua ISO offset
        # (vd "...+00:00") nen default parsing la du va co bang chung.
        "event_timestamp",
        F.try_to_timestamp(F.col("timestamp")),
    )

    reasons = {
        "null_or_empty_timestamp": F.col("timestamp").isNull()
        | (F.trim(F.col("timestamp")) == ""),
        "unparseable_timestamp": F.col("timestamp").isNotNull()
        & F.col("event_timestamp").isNull(),
        "null_or_empty_road_name": F.col("road_name").isNull()
        | (F.trim(F.col("road_name")) == ""),
        "null_lat": F.col("lat").isNull(),
        "lat_out_of_range": F.col("lat").isNotNull()
        & ((F.col("lat") < -90) | (F.col("lat") > 90)),
        "null_lon": F.col("lon").isNull(),
        "lon_out_of_range": F.col("lon").isNotNull()
        & ((F.col("lon") < -180) | (F.col("lon") > 180)),
        "null_current_speed": F.col("current_speed").isNull(),
        "negative_current_speed": F.col("current_speed").isNotNull()
        & (F.col("current_speed") < 0),
        "null_free_flow_speed": F.col("free_flow_speed").isNull(),
        "non_positive_free_flow_speed": F.col("free_flow_speed").isNotNull()
        & (F.col("free_flow_speed") <= 0),
        "null_current_travel_time": F.col("current_travel_time").isNull(),
        "negative_current_travel_time":
            F.col("current_travel_time").isNotNull()
            & (F.col("current_travel_time") < 0),
        "null_free_flow_travel_time":
            F.col("free_flow_travel_time").isNull(),
        "non_positive_free_flow_travel_time":
            F.col("free_flow_travel_time").isNotNull()
            & (F.col("free_flow_travel_time") <= 0),
        "null_confidence": F.col("confidence").isNull(),
        "confidence_out_of_range": F.col("confidence").isNotNull()
        & ((F.col("confidence") < 0) | (F.col("confidence") > 1)),
    }

    invalid_total = parsed.filter(
        reduce(lambda a, b: a | b, reasons.values())
    ).count()
    if invalid_total > 0:
        summary = parsed.select(
            *[F.sum(F.when(cond, 1).otherwise(0)).alias(name)
              for name, cond in reasons.items()]
        ).first().asDict()
        bad = {k: v for k, v in summary.items() if v > 0}
        raise ValueError(
            f"Traffic validation failed: {invalid_total} invalid rows; "
            f"reasons={bad}."
        )
    return parsed


WEATHER_REQUIRED_COLUMNS = [f.name for f in WEATHER_SCHEMA.fields]

# Raw timestamp phai timezone-aware: ket thuc bang Z hoac offset so
# (vd ...Z, ...+00:00, ...+0700, ...+07). Naive nhu "2026-09-23T06:00:00"
# bi reject ngay ca khi Spark parse duoc no.
TZ_AWARE_PATTERN = r"Z$|[+-]\d{2}:?\d{2}$|[+-]\d{2}$"


def validate_and_normalize_weather(df):
    """Validate + parse timestamp cho raw weather DataFrame (pure Spark).

    - Khong tao session/doc CSV/ghi file/goi API/mutate ben ngoai.
    - Fail-fast: row invalid -> raise ValueError kem counts/reasons;
      KHONG sua/drop/fill/dedup rows. Valid -> DataFrame 13 cot
      (12 raw goc + event_timestamp TimestampType, instant UTC dung).
    - weather_code chi validate NOT NULL (khong gioi han WMO range).
    """
    from functools import reduce

    from pyspark.sql import functions as F

    actual = list(df.columns)
    if actual != WEATHER_REQUIRED_COLUMNS:
        raise ValueError(
            f"Weather schema drift: got {actual!r}, "
            f"expected {WEATHER_REQUIRED_COLUMNS!r}."
        )

    parsed = df.withColumn(
        "event_timestamp",
        F.try_to_timestamp(F.col("timestamp")),
    )

    has_tz = F.col("timestamp").rlike(TZ_AWARE_PATTERN)
    reasons = {
        "null_or_empty_timestamp": F.col("timestamp").isNull()
        | (F.trim(F.col("timestamp")) == ""),
        "missing_timezone_indicator": F.col("timestamp").isNotNull()
        & ~has_tz,
        "unparseable_timestamp": F.col("timestamp").isNotNull()
        & F.col("event_timestamp").isNull(),
        "null_or_empty_road_name": F.col("road_name").isNull()
        | (F.trim(F.col("road_name")) == ""),
        "null_lat": F.col("lat").isNull(),
        "lat_out_of_range": F.col("lat").isNotNull()
        & ((F.col("lat") < -90) | (F.col("lat") > 90)),
        "null_lon": F.col("lon").isNull(),
        "lon_out_of_range": F.col("lon").isNotNull()
        & ((F.col("lon") < -180) | (F.col("lon") > 180)),
        "null_weather_lat": F.col("weather_lat").isNull(),
        "weather_lat_out_of_range": F.col("weather_lat").isNotNull()
        & ((F.col("weather_lat") < -90) | (F.col("weather_lat") > 90)),
        "null_weather_lon": F.col("weather_lon").isNull(),
        "weather_lon_out_of_range": F.col("weather_lon").isNotNull()
        & ((F.col("weather_lon") < -180) | (F.col("weather_lon") > 180)),
        "null_temperature": F.col("temperature").isNull(),
        "null_humidity": F.col("humidity").isNull(),
        "humidity_out_of_range": F.col("humidity").isNotNull()
        & ((F.col("humidity") < 0) | (F.col("humidity") > 100)),
        "null_precipitation": F.col("precipitation").isNull(),
        "negative_precipitation": F.col("precipitation").isNotNull()
        & (F.col("precipitation") < 0),
        "null_rain": F.col("rain").isNull(),
        "negative_rain": F.col("rain").isNotNull() & (F.col("rain") < 0),
        "null_wind_speed": F.col("wind_speed").isNull(),
        "negative_wind_speed": F.col("wind_speed").isNotNull()
        & (F.col("wind_speed") < 0),
        "null_weather_code": F.col("weather_code").isNull(),
    }

    invalid_total = parsed.filter(
        reduce(lambda a, b: a | b, reasons.values())
    ).count()
    if invalid_total > 0:
        summary = parsed.select(
            *[F.sum(F.when(cond, 1).otherwise(0)).alias(name)
              for name, cond in reasons.items()]
        ).first().asDict()
        bad = {k: v for k, v in summary.items() if v > 0}
        raise ValueError(
            f"Weather validation failed: {invalid_total} invalid rows; "
            f"reasons={bad}."
        )
    return parsed


if __name__ == "__main__":
    # Smoke read-only Buoc 5.1: load + count, khong transform/export.
    spark = create_spark_session()
    try:
        traffic_df = load_traffic_data(spark)
        weather_df = load_weather_data(spark)
        print(f"traffic rows: {traffic_df.count()}")
        print(f"weather rows: {weather_df.count()}")
    finally:
        spark.stop()
