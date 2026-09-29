"""Phase 5.1-5.5 - SparkSession + schemas + traffic/weather validation + traffic derived fields + weather dedup (READ-ONLY raw).

- Spark Local Mode, Windows-safe (PYSPARK_PYTHON = sys.executable).
- Session timezone UTC (ETL canonical; scheduling van Asia/Ho_Chi_Minh).
- Explicit StructType cho traffic (9 fields) + weather (12 fields).
- validate_and_normalize_traffic(): fail-fast validation + event_timestamp
  (TimestampType), giu nguyen 9 cot raw. KHONG congestion/join/export.
- validate_and_normalize_weather(): fail-fast + event_timestamp.
- derive_traffic_fields(): pure Spark transform them congestion_percent
  (locked raw formula, khong clamp), congestion_level (locked boundaries),
  local_hour (Asia/Ho_Chi_Minh, khong doi session tz). KHONG join/dedup/export.
- deduplicate_weather(): deterministic dedup theo (road_name, event_timestamp)
  bang Window row_number, canonical tie-break tren existing fields.
  KHONG join/analytics/export.
- temporal_join_traffic_weather(): deterministic temporal join traffic-weather
  (same road, nearest weather trong <=60 phut inclusive, tie uu tien past,
  canonical tie-break cuoi; LEFT semantics, unmatched giu lai voi
  weather_matched=false). KHONG analytics/export.
- export_processed_data(): export joined 25-col DataFrame ra single-file CSV
  (Spark orderBy + UTC format; Python stdlib csv chi serialize snapshot nho
  MVP vi Windows local Spark .write.csv can winutils — approved Option 2,
  KHONG scalable, KHONG Pandas). run_spark_etl(): orchestrate full Phase 5
  pipeline (load -> validate -> derive/dedup -> join -> export).
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


# Buoc 5.5 input contract: output hop le cua validate_and_normalize_weather()
# = 12 raw columns + event_timestamp (TimestampType, instant UTC).
DEDUP_REQUIRED_COLUMNS = WEATHER_REQUIRED_COLUMNS + ["event_timestamp"]

# Canonical tie-break order (deterministic, existing fields only, all ASC).
# KHONG phai ingestion order / do moi / do chinh xac khi tuong; chi la
# canonicalization vi raw khong co ingestion sequence metadata.
DEDUP_ORDER_COLUMNS = [
    "timestamp",
    "lat",
    "lon",
    "weather_lat",
    "weather_lon",
    "temperature",
    "humidity",
    "precipitation",
    "rain",
    "wind_speed",
    "weather_code",
]


def deduplicate_weather(df):
    """Dedup deterministic cho validated weather DataFrame (pure Spark).

    - Input: output cua validate_and_normalize_weather() (12 raw +
      event_timestamp). Thieu cot -> raise ValueError.
    - Duplicate key: (road_name, event_timestamp). Shared timestamp giua
      cac road khac nhau la hop le, KHONG dedup theo event_timestamp alone.
    - Trong moi partition: Window orderBy DEDUP_ORDER_COLUMNS ASC,
      row_number() == 1 giu lai. Exact duplicates -> giu 1; conflicting
      duplicates -> chon canonical row theo documented order (khong claim
      moi/chinh xac hon, khong dung input order hay
      monotonically_increasing_id).
    - Output: 13 cot (12 raw + event_timestamp), cung order/types nhu input,
      khong cot phu tro. Idempotent. Khong session/IO/collect/UDF/Pandas/API.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    missing = [c for c in DEDUP_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"deduplicate_weather missing required columns: {missing}. "
            f"Expected at minimum {DEDUP_REQUIRED_COLUMNS!r} "
            "(output cua validate_and_normalize_weather())."
        )

    w = Window.partitionBy("road_name", "event_timestamp").orderBy(
        *[F.col(c).asc() for c in DEDUP_ORDER_COLUMNS]
    )
    ranked = df.withColumn("__dedup_rn", F.row_number().over(w))
    return ranked.filter(F.col("__dedup_rn") == 1).drop("__dedup_rn").select(
        *DEDUP_REQUIRED_COLUMNS
    )


# Buoc 5.4 input contract: output hop le cua validate_and_normalize_traffic()
# = 9 raw columns + event_timestamp (TimestampType, instant UTC).
DERIVE_REQUIRED_COLUMNS = TRAFFIC_REQUIRED_COLUMNS + ["event_timestamp"]

HANOI_TIMEZONE = "Asia/Ho_Chi_Minh"


def classify_congestion(congestion_col):
    """Classification expression cho congestion value (pure Spark Column).

    Extraction cua locked classification logic (KHONG doi semantics):
    <20 -> "Thông thoáng"; <=40 -> "Đông"; <=60 -> "Ùn tắc";
    otherwise -> "Ùn tắc nghiêm trọng".
    Nhan mot Spark Column (vd F.col("congestion_percent") hoac
    F.lit(20.0)) va tra ve when/otherwise Column de test boundary
    doc lap voi floating-point formula generation.
    """
    from pyspark.sql import functions as F

    return (
        F.when(congestion_col < 20, "Thông thoáng")
        .when(congestion_col <= 40, "Đông")
        .when(congestion_col <= 60, "Ùn tắc")
        .otherwise("Ùn tắc nghiêm trọng")
    )


def derive_traffic_fields(df):
    """Them derived fields cho validated traffic DataFrame (pure Spark).

    - Input: output cua validate_and_normalize_traffic() (9 raw + event_timestamp).
    - Output: 13 cot (9 raw + event_timestamp + congestion_percent +
      congestion_level + local_hour), khong drop row.
    - congestion_percent = (1 - current_speed / free_flow_speed) * 100
      (locked formula, KHONG clamp/round/abs; am la hop le).
    - congestion_level: <20 Thong thoang; <=40 Dong; <=60 Un tac;
      otherwise Un tac nghiem trong (deterministic when/otherwise).
    - local_hour: gio wall-clock Hanoi tu event_timestamp (UTC instant)
      qua Asia/Ho_Chi_Minh, khong doi session tz, khong +7 thu cong.
    - Chi dung Spark column expressions; khong session/IO/collect/UDF.
    """
    from pyspark.sql import functions as F

    missing = [c for c in DERIVE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"derive_traffic_fields missing required columns: {missing}. "
            f"Expected at minimum {DERIVE_REQUIRED_COLUMNS!r} "
            "(output cua validate_and_normalize_traffic())."
        )

    congestion = (F.lit(1) - F.col("current_speed") / F.col("free_flow_speed")) * 100

    derived = (
        df.withColumn("congestion_percent", congestion)
        .withColumn(
            "congestion_level",
            classify_congestion(F.col("congestion_percent")),
        )
        .withColumn(
            "local_hour",
            F.hour(F.from_utc_timestamp(F.col("event_timestamp"), HANOI_TIMEZONE)),
        )
    )
    return derived.select(
        *(TRAFFIC_REQUIRED_COLUMNS
          + ["event_timestamp", "congestion_percent",
             "congestion_level", "local_hour"])
    )


# Buoc 5.6 input contracts.
# traffic_df: output cua derive_traffic_fields() = 9 raw + event_timestamp
#   + congestion_percent + congestion_level + local_hour (13 cot).
# weather_df: output cua deduplicate_weather() = 12 raw + event_timestamp
#   (13 cot, unique theo (road_name, event_timestamp)).
JOIN_TRAFFIC_REQUIRED_COLUMNS = (
    TRAFFIC_REQUIRED_COLUMNS
    + ["event_timestamp", "congestion_percent",
       "congestion_level", "local_hour"]
)
JOIN_WEATHER_REQUIRED_COLUMNS = DEDUP_REQUIRED_COLUMNS

# Tolerance cua temporal join (khoa tu Phase 3 / Buoc 3.5), inclusive.
WEATHER_JOIN_TOLERANCE_MINUTES = 60

# Weather fields attach sang output (renamed de tranh ambiguous voi traffic).
JOIN_WEATHER_OUTPUT_COLUMNS = [
    "weather_timestamp",
    "weather_event_timestamp",
    "weather_lat",
    "weather_lon",
    "temperature",
    "humidity",
    "precipitation",
    "rain",
    "wind_speed",
    "weather_code",
]

JOIN_OUTPUT_COLUMNS = (
    JOIN_TRAFFIC_REQUIRED_COLUMNS
    + JOIN_WEATHER_OUTPUT_COLUMNS
    + ["weather_matched", "weather_time_diff_minutes"]
)


def temporal_join_traffic_weather(traffic_df, weather_df):
    """Deterministic temporal join traffic-weather (pure Spark).

    - Input traffic: output cua derive_traffic_fields() (13 cot).
      Input weather: output cua deduplicate_weather() (13 cot, unique theo
      (road_name, event_timestamp)). Thieu cot -> raise ValueError (ro side).
    - Candidate: traffic.road_name == weather.road_name VA
      abs(weather_event - traffic_event) <= 60 phut (inclusive, tinh bang
      epoch seconds, khong so sanh timestamp strings).
    - Chon 1 weather/observation: (1) abs diff ASC, (2) past uu tien
      (weather_event <= traffic_event rank truoc khi abs diff tie),
      (3) canonical weather fields ASC (khong input order/random).
    - LEFT semantics: traffic khong co eligible weather van giu lai,
      weather fields NULL, weather_matched=false, diff NULL.
    - Traffic identity: content hash SHA-256 tren toan bo 13 cot traffic;
      exact duplicate traffic rows -> fail-fast ValueError (schema hien tai
      khong co ingestion ID, khong duoc collapse multiplicity am tham).
      Weather input co duplicate (road_name, event_timestamp) -> fail-fast,
      caller phai chay deduplicate_weather() truoc (khong redo 5.5 o day).
    - Output 25 cot theo JOIN_OUTPUT_COLUMNS; khong cot helper/ranking/hash.
      Khong session/IO/collect-matching/UDF/Pandas/API.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    missing_t = [c for c in JOIN_TRAFFIC_REQUIRED_COLUMNS
                 if c not in traffic_df.columns]
    if missing_t:
        raise ValueError(
            "temporal_join_traffic_weather missing traffic columns: "
            f"{missing_t}. Expected at minimum "
            f"{JOIN_TRAFFIC_REQUIRED_COLUMNS!r} "
            "(output cua derive_traffic_fields())."
        )
    missing_w = [c for c in JOIN_WEATHER_REQUIRED_COLUMNS
                 if c not in weather_df.columns]
    if missing_w:
        raise ValueError(
            "temporal_join_traffic_weather missing weather columns: "
            f"{missing_w}. Expected at minimum "
            f"{JOIN_WEATHER_REQUIRED_COLUMNS!r} "
            "(output cua deduplicate_weather())."
        )

    # Weather phai unique theo (road_name, event_timestamp) — 5.5 guarantee.
    dup_w = (
        weather_df.groupBy("road_name", "event_timestamp").count()
        .filter(F.col("count") > 1).count()
    )
    if dup_w > 0:
        raise ValueError(
            "temporal_join_traffic_weather: weather input has duplicate "
            "(road_name, event_timestamp) groups; run deduplicate_weather() "
            "first."
        )

    # Deterministic traffic identity tu full content (khong monotonic id,
    # khong input order). Identical rows -> cung hash -> fail-fast.
    keyed = traffic_df.withColumn(
        "__tkey",
        F.sha2(
            F.concat_ws(
                "|",
                *[F.col(c).cast("string")
                  for c in JOIN_TRAFFIC_REQUIRED_COLUMNS],
            ),
            256,
        ),
    )
    dup_t = (
        keyed.groupBy("__tkey").count()
        .filter(F.col("count") > 1).count()
    )
    if dup_t > 0:
        raise ValueError(
            "temporal_join_traffic_weather: exact duplicate traffic "
            "observations detected; deterministic per-observation identity "
            "cannot be established under current schema (no ingestion ID). "
            "Do not silently collapse multiplicity."
        )

    # Namespace weather truoc join (traffic giu nguyen ten).
    w = weather_df.select(
        F.col("road_name").alias("__w_road"),
        F.col("timestamp").alias("weather_timestamp"),
        F.col("event_timestamp").alias("weather_event_timestamp"),
        F.col("weather_lat"),
        F.col("weather_lon"),
        F.col("temperature"),
        F.col("humidity"),
        F.col("precipitation"),
        F.col("rain"),
        F.col("wind_speed"),
        F.col("weather_code"),
    )

    t = keyed.select(
        "__tkey",
        F.col("road_name").alias("__t_road"),
        F.col("event_timestamp").alias("__t_event"),
    )

    signed_min = (
        F.unix_timestamp(F.col("weather_event_timestamp"))
        - F.unix_timestamp(F.col("__t_event"))
    ) / 60.0
    candidates = (
        t.join(w, F.col("__t_road") == F.col("__w_road"), "inner")
        .withColumn("__signed_min", signed_min)
        .withColumn("__abs_min", F.abs(F.col("__signed_min")))
        .filter(
            F.col("__abs_min") <= WEATHER_JOIN_TOLERANCE_MINUTES
        )
        .withColumn(
            "__past_rank",
            F.when(
                F.col("weather_event_timestamp") <= F.col("__t_event"), 0
            ).otherwise(1),
        )
    )

    rank_w = Window.partitionBy("__tkey").orderBy(
        F.col("__abs_min").asc(),
        F.col("__past_rank").asc(),
        F.col("weather_timestamp").asc(),
        F.col("weather_event_timestamp").asc(),
        F.col("weather_lat").asc(),
        F.col("weather_lon").asc(),
        F.col("temperature").asc(),
        F.col("humidity").asc(),
        F.col("precipitation").asc(),
        F.col("rain").asc(),
        F.col("wind_speed").asc(),
        F.col("weather_code").asc(),
    )
    best = (
        candidates.withColumn("__rn", F.row_number().over(rank_w))
        .filter(F.col("__rn") == 1)
        .select(
            "__tkey",
            *JOIN_WEATHER_OUTPUT_COLUMNS,
            F.col("__abs_min").alias("weather_time_diff_minutes"),
        )
    )

    joined = keyed.join(best, on="__tkey", how="left").withColumn(
        "weather_matched",
        F.col("weather_event_timestamp").isNotNull(),
    )
    return joined.select(*JOIN_OUTPUT_COLUMNS)


# Buoc 5.7 export contract: input la exact 25-column output cua 5.6.
EXPORT_REQUIRED_COLUMNS = list(JOIN_OUTPUT_COLUMNS)

# Deterministic row order truoc export (khong phu thuoc partition order).
EXPORT_ORDER_COLUMNS = [
    "event_timestamp",
    "road_name",
    "timestamp",
    "lat",
    "lon",
    "current_speed",
    "free_flow_speed",
    "current_travel_time",
    "free_flow_travel_time",
    "confidence",
]

PROCESSED_FILENAME = "combined_data.csv"

# APPROVED Option 2 (user decision): Windows local Spark .write.csv can
# Hadoop native/winutils nen khong dung duoc. Spark van thuc hien MOI ETL,
# join, orderBy va UTC timestamp formatting; Python stdlib csv CHI serialize
# final snapshot NHO cho MVP (khong scalable, khong Pandas, khong thay Spark).
EXPORT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssxxx"


def _export_value(value):
    """Serialize 1 gia tri ra CSV field (deterministic, UTF-8 safe).

    None -> "" (empty field, khong viet None/null/NULL).
    bool -> "true"/"false" (kiem tra truoc int vi bool la subclass cua int).
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def default_processed_path():
    """Default output data/processed/combined_data.csv (portable, pathlib).

    Khong hard-code drive/path may; resolve tu vi tri module.
    """
    from pathlib import Path

    return Path(__file__).resolve().parent / "data" / "processed" / PROCESSED_FILENAME


def _write_snapshot_csv(final_path, header, rows):
    """Ghi snapshot CSV single-file (temp sibling + os.replace, UTF-8).

    - Helper chung cho Phase 5.7 va Phase 6.1 (approved MVP driver-side
      serialization; KHONG scalable, KHONG Pandas, KHONG thay Spark).
    - `rows` la iterable cac sequence gia tri tho (se serialize qua
      _export_value); header ghi dung 1 lan, khong index col.
    - Snapshot replacement: chi os.replace sang final sau khi write +
      flush + close thanh cong; loi -> file cu giu nguyen, temp duoc don.
    """
    import csv as _csv
    import os
    import uuid
    from pathlib import Path

    final = Path(final_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f"{final.name}.tmp-{uuid.uuid4().hex[:8]}"
    try:
        with open(tmp, "w", encoding="utf-8", newline="") as fh:
            writer = _csv.writer(fh)
            writer.writerow(list(header))
            for row in rows:
                writer.writerow([_export_value(v) for v in row])
        os.replace(tmp, final)
    except Exception:
        raise
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
    return final


def export_processed_data(df, output_path=None):
    """Export final joined DataFrame ra single-file CSV (snapshot replacement).

    - Input phai co exact 25 cot theo JOIN_OUTPUT_COLUMNS (sai -> ValueError
      truoc moi materialization/write; khong bao gio export malformed DF).
    - ORDERING DO SPARK: orderBy deterministic theo EXPORT_ORDER_COLUMNS.
      Timestamp columns duoc Spark format UTC (session tz UTC) qua date_format
      thanh canonical ISO "+00:00"; header names giu nguyen event_timestamp /
      weather_event_timestamp; raw strings (timestamp/weather_timestamp) giu
      nguyen; khong +-7h thu cong, khong doi instant.
    - Driver-side collect CHI cho MVP snapshot nho (da duoc user approve do
      Windows .write.csv can winutils); KHONG scalable, KHONG Pandas, KHONG
      dung cho dataset lon. Khong collect-based matching o production ETL.
    - Viet bang Python stdlib csv (encoding utf-8, newline=""): 1 header dung
      25 cot/order, khong index col; None -> ""; bool -> true/false.
    - Snapshot replacement: ghi temp sibling file truoc, chi os.replace sang
      final sau khi write+flush+close thanh cong (khong append); loi -> file
      cu giu nguyen, temp duoc don. Tao parent dir neu can.
    - Tra ve metadata (output_path, output_rows, header, export_strategy).
      Khong session/API. Caller quan ly SparkSession.
    """
    from pathlib import Path

    from pyspark.sql import functions as F

    if list(df.columns) != EXPORT_REQUIRED_COLUMNS:
        raise ValueError(
            "export_processed_data requires exact 25-column 5.6 output; got "
            f"{list(df.columns)!r}, expected {EXPORT_REQUIRED_COLUMNS!r}."
        )

    final = Path(output_path) if output_path is not None else default_processed_path()
    ordered = (
        df.orderBy(*[F.col(c).asc() for c in EXPORT_ORDER_COLUMNS])
        .withColumn("event_timestamp",
                    F.date_format("event_timestamp", EXPORT_TS_FORMAT))
        .withColumn("weather_event_timestamp",
                    F.date_format("weather_event_timestamp", EXPORT_TS_FORMAT))
    )
    # Collect nay CHI thuoc final export layer cho snapshot MVP nho;
    # moi ETL/matching van la Spark column expressions.
    rows = ordered.collect()
    final = _write_snapshot_csv(
        final, EXPORT_REQUIRED_COLUMNS,
        [[row[c] for c in EXPORT_REQUIRED_COLUMNS] for row in rows])

    return {"output_path": str(final), "output_rows": len(rows),
            "header": list(EXPORT_REQUIRED_COLUMNS),
            "export_strategy": "driver_csv_mvp"}


def run_spark_etl(output_path=None):
    """Orchestrate full Phase 5 pipeline roi export (don gian, fail-fast).

    traffic: load -> validate -> derive; weather: load -> validate -> dedup;
    joined = temporal_join_traffic_weather(); export combined_data.csv.
    Bat loi o bat ky stage nao -> raise (khong tao CSV gia), temp duoc don,
    file final cu duoc giu neu replacement chua xong. Luon stop session.
    Tra ve metadata (traffic/weather/output/matched/unmatched rows + path).
    """
    from pyspark.sql import functions as F

    spark = create_spark_session()
    try:
        traffic = derive_traffic_fields(
            validate_and_normalize_traffic(load_traffic_data(spark))
        )
        weather = deduplicate_weather(
            validate_and_normalize_weather(load_weather_data(spark))
        )
        traffic.cache()
        weather.cache()
        joined = temporal_join_traffic_weather(traffic, weather)
        joined.cache()
        n_traffic = traffic.count()
        n_weather = weather.count()
        n_out = joined.count()
        n_matched = joined.filter(F.col("weather_matched") == True).count()
        meta = export_processed_data(joined, output_path=output_path)
        try:
            traffic.unpersist()
            weather.unpersist()
            joined.unpersist()
        except Exception:
            pass
        meta.update(
            {
                "traffic_rows": n_traffic,
                "weather_rows": n_weather,
                "matched_rows": n_matched,
                "unmatched_rows": n_out - n_matched,
            }
        )
        return meta
    finally:
        spark.stop()


# ============================================================
# Phase 6.1 — Road-level analytics (Spark engine, MVP export).
#
# - Input: data/processed/combined_data.csv (Phase 5 boundary artifact).
#   KHONG doc raw traffic/weather, KHONG rerun validation/dedup/join.
# - Aggregation + ordering DO SPARK (groupBy/count/avg/min/max/sum-when,
#   khong UDF, khong collect-based aggregation, khong round).
# - Final single-file serialization tai dung _write_snapshot_csv
#   (approved MVP driver-side strategy nhu 5.7; khong .write.csv,
#   khong Pandas).
# - KHONG hourly/rain-no-rain/dashboard (buoc 6.2+).
# ============================================================

ROAD_SUMMARY_FILENAME = "road_summary.csv"

# Explicit schema cho Phase 5 artifact (25 cot). Timestamp strings giu
# StringType (analytics road-level khong can timestamp arithmetic);
# numerics dung Double/Integer de Spark cast; malformed -> NULL va bi
# fail-fast o load_processed_data (khong silently coerce).
def _processed_schema():
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructType,
    )

    return (
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
        .add("event_timestamp", StringType(), True)
        .add("congestion_percent", DoubleType(), True)
        .add("congestion_level", StringType(), True)
        .add("local_hour", IntegerType(), True)
        .add("weather_timestamp", StringType(), True)
        .add("weather_event_timestamp", StringType(), True)
        .add("weather_lat", DoubleType(), True)
        .add("weather_lon", DoubleType(), True)
        .add("temperature", DoubleType(), True)
        .add("humidity", DoubleType(), True)
        .add("precipitation", DoubleType(), True)
        .add("rain", DoubleType(), True)
        .add("wind_speed", DoubleType(), True)
        .add("weather_code", IntegerType(), True)
        .add("weather_matched", BooleanType(), True)
        .add("weather_time_diff_minutes", DoubleType(), True)
    )


PROCESSED_SCHEMA = _processed_schema()

# Subset cot toi thieu cho road analytics.
ANALYZE_REQUIRED_COLUMNS = [
    "road_name",
    "current_speed",
    "free_flow_speed",
    "congestion_percent",
    "congestion_level",
]

# Locked congestion labels tu Phase 5.4 (co dau).
ROAD_CONGESTION_LEVELS = ["Thông thoáng", "Đông", "Ùn tắc", "Ùn tắc nghiêm trọng"]

ROAD_SUMMARY_COLUMNS = [
    "road_name",
    "observation_count",
    "avg_speed",
    "avg_free_flow_speed",
    "avg_congestion_percent",
    "max_congestion_percent",
    "min_congestion_percent",
    "clear_count",
    "busy_count",
    "congested_count",
    "severe_count",
]


def default_road_summary_path():
    """Default output data/processed/road_summary.csv (portable, pathlib)."""
    from pathlib import Path

    return Path(__file__).resolve().parent / "data" / "processed" / ROAD_SUMMARY_FILENAME


def load_processed_data(spark, path=None):
    """Doc Phase 5 artifact combined_data.csv voi explicit schema (pure load).

    - Header kiem tra bang stdlib csv (chi dong header, khong aggregate):
      sai 25 cot/order -> ValueError. Thieu file -> RuntimeError.
    - Spark doc voi PROCESSED_SCHEMA (header, khong inferSchema).
    - Fail-fast neu cot analytics-critical bi NULL (do malformed coerce):
      road_name/current_speed/free_flow_speed/congestion_percent/
      congestion_level. KHONG sua/drop rows.
    - Khong tao session/doc raw/goi API. Khong Pandas.
    """
    from functools import reduce
    from pathlib import Path

    from pyspark.sql import functions as F

    target = Path(path) if path is not None else default_processed_path()
    if not target.is_file():
        raise RuntimeError(
            f"Khong tim thay processed artifact: {target}. Chay Phase 5 ETL truoc."
        )

    import csv as _csv

    with open(target, "r", encoding="utf-8", newline="") as fh:
        header = next(_csv.reader(fh), None)
    if header != EXPORT_REQUIRED_COLUMNS:
        raise ValueError(
            "Processed header mismatch: got "
            f"{header!r}, expected {EXPORT_REQUIRED_COLUMNS!r}."
        )

    df = spark.read.option("header", True).schema(PROCESSED_SCHEMA).csv(str(target))

    critical = list(ANALYZE_REQUIRED_COLUMNS)
    n_bad = df.filter(
        reduce(lambda a, b: a | b, (F.col(c).isNull() for c in critical))
    ).count()
    if n_bad > 0:
        raise ValueError(
            f"Processed analytics columns contain {n_bad} NULL rows "
            f"(columns={critical}); refusing to coerce malformed values."
        )
    return df


def analyze_roads(df):
    """Road-level aggregation bang pure Spark (khong UDF, khong Pandas).

    - Input can toi thieu ANALYZE_REQUIRED_COLUMNS (thieu -> ValueError).
    - Fail-fast neu co road_name null/empty hoac congestion_level
      null/unknown (ngoai 4 locked labels): khong tong hop sai lech.
    - GroupBy road_name: count/avg/min/max + sum(when(...)) cho 4 level
      counts; giu Double precision, KHONG round (presentation round sau).
    - Output 11 cot theo ROAD_SUMMARY_COLUMNS (chua order; export order).
    - Khong session/IO/collect/UDF/API.
    """
    from pyspark.sql import functions as F

    missing = [c for c in ANALYZE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"analyze_roads missing required columns: {missing}. "
            f"Expected at minimum {ANALYZE_REQUIRED_COLUMNS!r}."
        )

    n_bad = df.filter(
        F.col("road_name").isNull()
        | (F.trim(F.col("road_name")) == "")
        | F.col("congestion_level").isNull()
        | ~F.col("congestion_level").isin(ROAD_CONGESTION_LEVELS)
    ).count()
    if n_bad > 0:
        raise ValueError(
            f"analyze_roads contract violation: {n_bad} rows with "
            "null/empty road_name or unknown/null congestion_level; "
            "refusing to produce a misleading summary."
        )

    grouped = df.groupBy("road_name").agg(
        F.count("*").alias("observation_count"),
        F.avg("current_speed").alias("avg_speed"),
        F.avg("free_flow_speed").alias("avg_free_flow_speed"),
        F.avg("congestion_percent").alias("avg_congestion_percent"),
        F.max("congestion_percent").alias("max_congestion_percent"),
        F.min("congestion_percent").alias("min_congestion_percent"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[0], 1)
              .otherwise(0)).alias("clear_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[1], 1)
              .otherwise(0)).alias("busy_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[2], 1)
              .otherwise(0)).alias("congested_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[3], 1)
              .otherwise(0)).alias("severe_count"),
    )
    return grouped.select(*ROAD_SUMMARY_COLUMNS)


def export_road_summary(df, output_path=None):
    """Export road summary ra single-file CSV (snapshot replacement).

    - Input phai co exact 11 cot theo ROAD_SUMMARY_COLUMNS (sai ->
      ValueError truoc moi materialization).
    - ORDERING DO SPARK: avg_congestion_percent DESC, road_name ASC
      (duong un tac nhat len dau, deterministic).
    - Serialization qua _write_snapshot_csv (approved MVP driver-side;
      KHONG .write.csv, KHONG Pandas). Temp sibling + os.replace;
      loi -> file cu giu nguyen, temp duoc don.
    - Tra ve metadata (output_path, output_rows, header, export_strategy).
      Khong session/API. Caller quan ly SparkSession.
    """
    from pathlib import Path

    from pyspark.sql import functions as F

    if list(df.columns) != ROAD_SUMMARY_COLUMNS:
        raise ValueError(
            "export_road_summary requires exact 11-column analyze_roads output; got "
            f"{list(df.columns)!r}, expected {ROAD_SUMMARY_COLUMNS!r}."
        )

    final = Path(output_path) if output_path is not None else default_road_summary_path()
    ordered = df.orderBy(
        F.col("avg_congestion_percent").desc(), F.col("road_name").asc()
    )
    rows = ordered.collect()
    final = _write_snapshot_csv(
        final, ROAD_SUMMARY_COLUMNS,
        [[row[c] for c in ROAD_SUMMARY_COLUMNS] for row in rows])

    return {"output_path": str(final), "output_rows": len(rows),
            "header": list(ROAD_SUMMARY_COLUMNS),
            "export_strategy": "driver_csv_mvp"}


def run_road_analytics(output_path=None):
    """Orchestrate Phase 6.1: load processed -> analyze -> export (fail-fast).

    Doc Phase 5 boundary artifact (KHONG raw, KHONG re-validation/join),
    aggregate theo road bang Spark, export road_summary.csv. Loi o bat ky
    stage nao -> raise (khong tao CSV gia). Luon stop session.
    Tra ve metadata (input_rows/roads/output/sum/top road + avg).
    Ket qua la descriptive cua collected observations, khong claim causality.
    """
    import csv as _csv

    spark = create_spark_session()
    try:
        df = load_processed_data(spark)
        summary = analyze_roads(df)
        n_in = df.count()
        meta = export_road_summary(summary, output_path=output_path)
        total_obs = 0
        top_road = None
        top_avg = None
        with open(meta["output_path"], "r", encoding="utf-8", newline="") as fh:
            for i, row in enumerate(_csv.DictReader(fh)):
                total_obs += int(row["observation_count"])
                if i == 0:
                    top_road = row["road_name"]
                    top_avg = float(row["avg_congestion_percent"])
        meta.update(
            {
                "input_rows": n_in,
                "road_count": meta["output_rows"],
                "sum_observation_count": total_obs,
                "top_road": top_road,
                "top_avg_congestion_percent": top_avg,
            }
        )
        return meta
    finally:
        spark.stop()


# ============================================================
# Phase 6.2 — Hourly/time-window analytics (Spark engine, MVP export).
#
# - Input: data/processed/combined_data.csv qua load_processed_data()
#   (KHONG raw, KHONG rerun ETL, KHONG road_summary lam input).
# - Dung Phase 5 local_hour (Asia/Ho_Chi_Minh, KHONG recompute timezone,
#   KHONG aggregate theo UTC hour).
# - Aggregation + ordering DO SPARK (groupBy/count/avg/min/max/sum-when,
#   khong UDF, khong collect-based aggregation, khong round).
# - Final single-file serialization qua _write_snapshot_csv
#   (approved MVP driver-side; khong .write.csv, khong Pandas).
# - KHONG rain/no-rain/dashboard (buoc 6.3+).
# ============================================================

HOURLY_SUMMARY_FILENAME = "hourly_summary.csv"

# Subset cot toi thieu cho hourly analytics.
HOURLY_REQUIRED_COLUMNS = [
    "local_hour",
    "current_speed",
    "free_flow_speed",
    "congestion_percent",
    "congestion_level",
]

HOURLY_SUMMARY_COLUMNS = [
    "local_hour",
    "time_window",
    "observation_count",
    "avg_speed",
    "avg_free_flow_speed",
    "avg_congestion_percent",
    "max_congestion_percent",
    "min_congestion_percent",
    "clear_count",
    "busy_count",
    "congested_count",
    "severe_count",
]


def classify_time_window(hour_col):
    """Descriptive time-window expression cho local_hour (pure Spark Column).

    Boundaries (khoa): 00-05 Dem; 06-10 Sang cao diem; 11-15 Trua;
    16-20 Chieu cao diem; 21-23 Toi. Nhan mot Spark Column so nguyen
    va tra ve when/otherwise Column (khong UDF), de test boundary
    doc lap voi aggregation.
    """
    from pyspark.sql import functions as F

    return (
        F.when(hour_col <= 5, "Đêm")
        .when(hour_col <= 10, "Sáng cao điểm")
        .when(hour_col <= 15, "Trưa")
        .when(hour_col <= 20, "Chiều cao điểm")
        .otherwise("Tối")
    )


def default_hourly_summary_path():
    """Default output data/processed/hourly_summary.csv (portable, pathlib)."""
    from pathlib import Path

    return Path(__file__).resolve().parent / "data" / "processed" / HOURLY_SUMMARY_FILENAME


def analyze_hourly(df):
    """Hourly aggregation bang pure Spark (khong UDF, khong Pandas).

    - Input can toi thieu HOURLY_REQUIRED_COLUMNS (thieu -> ValueError).
    - Fail-fast neu local_hour null hoac ngoai [0, 23] (non-integer da
      thanh NULL khi doc schema), hoac congestion_level null/unknown
      (ngoai 4 locked labels): khong tong hop sai lech.
    - time_window = classify_time_window(local_hour); groupBy
      (local_hour, time_window): count/avg/min/max + sum(when(...)) cho
      4 level counts; giu Double precision, KHONG round.
    - Output 12 cot theo HOURLY_SUMMARY_COLUMNS (chua order; export order
      local_hour ASC). Chi cac gio quan sat duoc xuat hien (khong tao
      gio 0-observation). Khong session/IO/collect/UDF/API.
    """
    from pyspark.sql import functions as F

    missing = [c for c in HOURLY_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"analyze_hourly missing required columns: {missing}. "
            f"Expected at minimum {HOURLY_REQUIRED_COLUMNS!r}."
        )

    n_bad_hour = df.filter(
        F.col("local_hour").isNull()
        | (F.col("local_hour") < 0)
        | (F.col("local_hour") > 23)
    ).count()
    if n_bad_hour > 0:
        raise ValueError(
            f"analyze_hourly contract violation: {n_bad_hour} rows with "
            "null or out-of-range local_hour (expected integer 0-23); "
            "refusing to produce a misleading summary."
        )

    n_bad_level = df.filter(
        F.col("congestion_level").isNull()
        | ~F.col("congestion_level").isin(ROAD_CONGESTION_LEVELS)
    ).count()
    if n_bad_level > 0:
        raise ValueError(
            f"analyze_hourly contract violation: {n_bad_level} rows with "
            "unknown/null congestion_level; "
            "refusing to produce a misleading summary."
        )

    with_window = df.withColumn(
        "time_window", classify_time_window(F.col("local_hour"))
    )
    grouped = with_window.groupBy("local_hour", "time_window").agg(
        F.count("*").alias("observation_count"),
        F.avg("current_speed").alias("avg_speed"),
        F.avg("free_flow_speed").alias("avg_free_flow_speed"),
        F.avg("congestion_percent").alias("avg_congestion_percent"),
        F.max("congestion_percent").alias("max_congestion_percent"),
        F.min("congestion_percent").alias("min_congestion_percent"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[0], 1)
              .otherwise(0)).alias("clear_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[1], 1)
              .otherwise(0)).alias("busy_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[2], 1)
              .otherwise(0)).alias("congested_count"),
        F.sum(F.when(F.col("congestion_level") == ROAD_CONGESTION_LEVELS[3], 1)
              .otherwise(0)).alias("severe_count"),
    )
    return grouped.select(*HOURLY_SUMMARY_COLUMNS)


def export_hourly_summary(df, output_path=None):
    """Export hourly summary ra single-file CSV (snapshot replacement).

    - Input phai co exact 12 cot theo HOURLY_SUMMARY_COLUMNS (sai ->
      ValueError truoc moi materialization).
    - ORDERING DO SPARK: local_hour ASC (deterministic; khong sort theo
      congestion, khong rank column).
    - Serialization qua _write_snapshot_csv (approved MVP driver-side;
      KHONG .write.csv, KHONG Pandas). Temp sibling + os.replace;
      loi -> file cu giu nguyen, temp duoc don.
    - Tra ve metadata (output_path, output_rows, header, export_strategy).
      Khong session/API. Caller quan ly SparkSession.
    """
    from pathlib import Path

    from pyspark.sql import functions as F

    if list(df.columns) != HOURLY_SUMMARY_COLUMNS:
        raise ValueError(
            "export_hourly_summary requires exact 12-column analyze_hourly output; got "
            f"{list(df.columns)!r}, expected {HOURLY_SUMMARY_COLUMNS!r}."
        )

    final = Path(output_path) if output_path is not None else default_hourly_summary_path()
    ordered = df.orderBy(F.col("local_hour").asc())
    rows = ordered.collect()
    final = _write_snapshot_csv(
        final, HOURLY_SUMMARY_COLUMNS,
        [[row[c] for c in HOURLY_SUMMARY_COLUMNS] for row in rows])

    return {"output_path": str(final), "output_rows": len(rows),
            "header": list(HOURLY_SUMMARY_COLUMNS),
            "export_strategy": "driver_csv_mvp"}


def run_hourly_analytics(output_path=None):
    """Orchestrate Phase 6.2: load processed -> analyze -> export (fail-fast).

    Doc Phase 5 boundary artifact (KHONG raw, KHONG road_summary lam input),
    aggregate theo local_hour bang Spark, export hourly_summary.csv. Loi o
    bat ky stage nao -> raise (khong tao CSV gia). Luon stop session.
    Tra ve metadata (input_rows/distinct_hours/output/sum/top hours + avg).
    Ket qua la descriptive cua collected observations ("trong du lieu da
    thu thap"), khong claim causality hay Hanoi-wide.
    """
    import csv as _csv

    spark = create_spark_session()
    try:
        df = load_processed_data(spark)
        summary = analyze_hourly(df)
        n_in = df.count()
        meta = export_hourly_summary(summary, output_path=output_path)
        total_obs = 0
        top_avg = None
        top_hours = []
        with open(meta["output_path"], "r", encoding="utf-8", newline="") as fh:
            for row in _csv.DictReader(fh):
                total_obs += int(row["observation_count"])
                avg = float(row["avg_congestion_percent"])
                if top_avg is None or avg > top_avg:
                    top_avg = avg
                    top_hours = [{
                        "local_hour": int(row["local_hour"]),
                        "time_window": row["time_window"],
                        "observation_count": int(row["observation_count"]),
                    }]
                elif avg == top_avg:
                    top_hours.append({
                        "local_hour": int(row["local_hour"]),
                        "time_window": row["time_window"],
                        "observation_count": int(row["observation_count"]),
                    })
        meta.update(
            {
                "input_rows": n_in,
                "distinct_hours": meta["output_rows"],
                "sum_observation_count": total_obs,
                "top_hours": top_hours,
                "top_avg_congestion_percent": top_avg,
            }
        )
        return meta
    finally:
        spark.stop()


# ============================================================
# Phase 6.3 — Rain vs no-rain analytics (Spark engine, MVP export).
#
# - Input: data/processed/combined_data.csv qua load_processed_data()
#   (KHONG raw, KHONG road/hourly summary lam input).
# - Chi dung observations co weather_matched == true; unmatched bi
#   loai khoi aggregation (KHONG coi la "Khong mua").
# - rain > 0 -> "Mua"; rain == 0 -> "Khong mua" (khong threshold tu che,
#   khong dung weather_code/precipitation override).
# - Aggregation + ordering DO SPARK (filter/groupBy/count/avg/min/max,
#   khong UDF, khong collect-based aggregation, khong round).
# - Final single-file serialization qua _write_snapshot_csv
#   (approved MVP driver-side; khong .write.csv, khong Pandas).
# - Ket qua la DESCRIPTIVE ASSOCIATION ("trong du lieu da thu thap"),
#   KHONG causality, KHONG Hanoi-wide.
# ============================================================

WEATHER_SUMMARY_FILENAME = "weather_summary.csv"

# Subset cot toi thieu cho weather analytics.
# (Ten rieng biet WEATHER_REQUIRED_COLUMNS raw cua Phase 5.3: fix shadowing
# 9.1C — global nay tung ghi de raw contract tai runtime.)
WEATHER_ANALYTICS_REQUIRED_COLUMNS = [
    "weather_matched",
    "rain",
    "current_speed",
    "free_flow_speed",
    "congestion_percent",
]

# Semantic order hien thi: "Khong mua" truoc, "Mua" sau (explicit sort
# key, khong dua vao Unicode sorting).
WEATHER_CONDITIONS = ["Không mưa", "Mưa"]

WEATHER_SUMMARY_COLUMNS = [
    "weather_condition",
    "observation_count",
    "avg_speed",
    "avg_free_flow_speed",
    "avg_congestion_percent",
    "max_congestion_percent",
    "min_congestion_percent",
]


def classify_rain_condition(rain_col):
    """Rain classification expression (pure Spark Column, khong UDF).

    rain > 0 -> "Mua"; otherwise -> "Khong mua". Chi ap dung tren
    matched rows da validate (rain non-null, >= 0), nen otherwise o day
    nghia la rain == 0. Khong threshold tu che (0.0001 van la Mua),
    khong weather_code/precipitation override.
    """
    from pyspark.sql import functions as F

    return F.when(rain_col > 0, WEATHER_CONDITIONS[1]).otherwise(WEATHER_CONDITIONS[0])


def default_weather_summary_path():
    """Default output data/processed/weather_summary.csv (portable, pathlib)."""
    from pathlib import Path

    return Path(__file__).resolve().parent / "data" / "processed" / WEATHER_SUMMARY_FILENAME


def analyze_weather_conditions(df):
    """Rain vs no-rain aggregation bang pure Spark (khong UDF, khong Pandas).

    - Input can toi thieu WEATHER_ANALYTICS_REQUIRED_COLUMNS (thieu -> ValueError).
    - weather_matched null -> ValueError (khong suy doan trang thai).
    - Chi matched rows duoc phan tich; matched validate: current_speed
      non-null va >= 0, free_flow_speed non-null va > 0,
      congestion_percent non-null, rain non-null va >= 0. Vi pham ->
      ValueError (khong clean/drop am tham).
    - Zero matched rows -> ValueError (khong export summary gay hieu lam).
    - Unmatched rows (false, ke ca rain NULL) la hop le: bi loai khoi
      aggregation, KHONG thanh "Khong mua".
    - GroupBy weather_condition: count/avg/min/max; giu Double precision,
      KHONG round. Output 7 cot theo WEATHER_SUMMARY_COLUMNS (chua order;
      export order). Chi condition quan sat duoc xuat hien. Khong
      session/IO/collect/UDF/API.
    """
    from pyspark.sql import functions as F

    missing = [c for c in WEATHER_ANALYTICS_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"analyze_weather_conditions missing required columns: {missing}. "
            f"Expected at minimum {WEATHER_ANALYTICS_REQUIRED_COLUMNS!r}."
        )

    n_null_matched = df.filter(F.col("weather_matched").isNull()).count()
    if n_null_matched > 0:
        raise ValueError(
            f"analyze_weather_conditions contract violation: {n_null_matched} rows "
            "with null weather_matched; refusing to guess match state."
        )

    matched = df.filter(F.col("weather_matched") == True)
    n_matched = matched.count()
    if n_matched == 0:
        raise ValueError(
            "analyze_weather_conditions: no matched weather observations "
            "available for analysis; refusing to export a misleading summary."
        )

    n_bad = matched.filter(
        F.col("current_speed").isNull()
        | (F.col("current_speed") < 0)
        | F.col("free_flow_speed").isNull()
        | (F.col("free_flow_speed") <= 0)
        | F.col("congestion_percent").isNull()
        | F.col("rain").isNull()
        | (F.col("rain") < 0)
    ).count()
    if n_bad > 0:
        raise ValueError(
            f"analyze_weather_conditions contract violation: {n_bad} matched rows "
            "with null/negative speed, non-positive free-flow speed, null "
            "congestion, or null/negative rain; refusing to coerce."
        )

    with_condition = matched.withColumn(
        "weather_condition", classify_rain_condition(F.col("rain"))
    )
    grouped = with_condition.groupBy("weather_condition").agg(
        F.count("*").alias("observation_count"),
        F.avg("current_speed").alias("avg_speed"),
        F.avg("free_flow_speed").alias("avg_free_flow_speed"),
        F.avg("congestion_percent").alias("avg_congestion_percent"),
        F.max("congestion_percent").alias("max_congestion_percent"),
        F.min("congestion_percent").alias("min_congestion_percent"),
    )
    return grouped.select(*WEATHER_SUMMARY_COLUMNS)


def export_weather_summary(df, output_path=None):
    """Export weather summary ra single-file CSV (snapshot replacement).

    - Input phai co exact 7 cot theo WEATHER_SUMMARY_COLUMNS (sai ->
      ValueError truoc moi materialization).
    - ORDERING DO SPARK: explicit semantic sort key ("Khong mua" = 0,
      "Mua" = 1), drop helper truoc export; khong dua Unicode sort.
    - Serialization qua _write_snapshot_csv (approved MVP driver-side;
      KHONG .write.csv, KHONG Pandas). Temp sibling + os.replace;
      loi -> file cu giu nguyen, temp duoc don.
    - Tra ve metadata (output_path, output_rows, header, export_strategy).
      Khong session/API. Caller quan ly SparkSession.
    """
    from pathlib import Path

    from pyspark.sql import functions as F

    if list(df.columns) != WEATHER_SUMMARY_COLUMNS:
        raise ValueError(
            "export_weather_summary requires exact 7-column analyze_weather_conditions "
            f"output; got {list(df.columns)!r}, expected {WEATHER_SUMMARY_COLUMNS!r}."
        )

    final = Path(output_path) if output_path is not None else default_weather_summary_path()
    ordered = (
        df.withColumn(
            "__worder",
            F.when(F.col("weather_condition") == WEATHER_CONDITIONS[0], 0).otherwise(1),
        )
        .orderBy(F.col("__worder").asc())
        .drop("__worder")
    )
    rows = ordered.collect()
    final = _write_snapshot_csv(
        final, WEATHER_SUMMARY_COLUMNS,
        [[row[c] for c in WEATHER_SUMMARY_COLUMNS] for row in rows])

    return {"output_path": str(final), "output_rows": len(rows),
            "header": list(WEATHER_SUMMARY_COLUMNS),
            "export_strategy": "driver_csv_mvp"}


def run_weather_analytics(output_path=None):
    """Orchestrate Phase 6.3: load processed -> analyze -> export (fail-fast).

    Doc Phase 5 boundary artifact (KHONG raw, KHONG summary khac lam input),
    so sanh Mua/Khong mua bang Spark tren matched observations, export
    weather_summary.csv. Loi o bat ky stage nao -> raise (khong tao CSV gia).
    Luon stop session. Tra ve metadata (input/matched/unmatched/analyzed/
    output rows + path). Ket qua la descriptive association, khong causality.
    """
    import csv as _csv

    from pyspark.sql import functions as F

    spark = create_spark_session()
    try:
        df = load_processed_data(spark)
        summary = analyze_weather_conditions(df)
        n_in = df.count()
        n_matched = df.filter(F.col("weather_matched") == True).count()
        n_unmatched = df.filter(F.col("weather_matched") == False).count()
        meta = export_weather_summary(summary, output_path=output_path)
        conditions = {}
        with open(meta["output_path"], "r", encoding="utf-8", newline="") as fh:
            for row in _csv.DictReader(fh):
                conditions[row["weather_condition"]] = {
                    "observation_count": int(row["observation_count"]),
                    "avg_speed": float(row["avg_speed"]),
                    "avg_congestion_percent": float(row["avg_congestion_percent"]),
                }
        meta.update(
            {
                "input_rows": n_in,
                "matched_weather_rows": n_matched,
                "unmatched_weather_rows": n_unmatched,
                "analyzed_rows": n_matched,
                "conditions": conditions,
            }
        )
        return meta
    finally:
        spark.stop()


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
