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
    import csv as _csv
    import os
    import uuid
    from pathlib import Path

    from pyspark.sql import functions as F

    if list(df.columns) != EXPORT_REQUIRED_COLUMNS:
        raise ValueError(
            "export_processed_data requires exact 25-column 5.6 output; got "
            f"{list(df.columns)!r}, expected {EXPORT_REQUIRED_COLUMNS!r}."
        )

    final = Path(output_path) if output_path is not None else default_processed_path()
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f"{final.name}.tmp-{uuid.uuid4().hex[:8]}"
    try:
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
        with open(tmp, "w", encoding="utf-8", newline="") as fh:
            writer = _csv.writer(fh)
            writer.writerow(EXPORT_REQUIRED_COLUMNS)
            for row in rows:
                writer.writerow([_export_value(row[c]) for c in EXPORT_REQUIRED_COLUMNS])
        os.replace(tmp, final)
    except Exception:
        raise
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass

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
