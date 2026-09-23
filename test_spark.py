from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Khởi tạo Apache Spark chạy Local Mode
spark = (
    SparkSession.builder
    .appName("HanoiTrafficTest")
    .master("local[*]")
    .getOrCreate()
)

# Giảm log cho dễ nhìn
spark.sparkContext.setLogLevel("ERROR")

# Dữ liệu giao thông thử nghiệm
data = [
    ("Nguyen Trai", 20, 45),
    ("Truong Chinh", 25, 40),
    ("Giai Phong", 28, 45),
    ("Cau Giay", 35, 50),
    ("Xuan Thuy", 32, 50),
]

columns = [
    "road_name",
    "current_speed",
    "free_flow_speed"
]

df = spark.createDataFrame(data, columns)

# Tính mức độ ùn tắc
df = df.withColumn(
    "congestion",
    (1 - col("current_speed") / col("free_flow_speed")) * 100
)

print("\n=== HANOI TRAFFIC TEST ===")

df.show(truncate=False)

print("Spark version:", spark.version)

spark.stop()