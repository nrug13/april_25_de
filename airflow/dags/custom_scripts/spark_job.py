from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/files/data.csv"
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/files/output/spark_result"

spark = SparkSession.builder.appName("airflow_spark_example").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)

print(f"Oxunan sətir sayı: {df.count()}")
df.printSchema()
df.show(5)

result = df.agg(
    F.count("id").alias("total_records"),
    F.round(F.avg("value"), 2).alias("avg_value"),
    F.round(F.max("value"), 2).alias("max_value"),
    F.round(F.min("value"), 2).alias("min_value"),
)

result.show()

result.write.mode("overwrite").csv(OUTPUT_PATH, header=True)
print(f"Nəticə yazıldı: {OUTPUT_PATH}")

spark.stop()
