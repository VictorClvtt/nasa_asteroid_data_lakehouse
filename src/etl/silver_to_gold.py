# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pathlib import Path
from dotenv import load_dotenv
import os

from datetime import datetime

# Carregando e salvando variaveis do .env
env_path = Path(__file__).parent.parent.parent / ".env"

load_dotenv(env_path)

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")
bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

spark = SparkSession.builder \
    .appName("silver_to_gold") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698") \
    .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

today_str = datetime.today().strftime("%Y-%m-%d")
df = spark.read.parquet(f"s3a://{bucket_name}/silver/nasa_asteroids/_processing_date={today_str}/")

# %%
dim_asteroid = df.select(
    "asteroid_id",
    "neo_id",
    "asteroid_name",
    "magnitude",
    "is_hazardous",
    "is_sentry",
    "diameter_min_km",
    "diameter_max_km",
    "diameter_min_m",
    "diameter_max_m",
    "jpl_url"
).dropDuplicates(["asteroid_id"])

dim_date = df.select(
    F.date_format("approach_date", "yyyyMMdd").cast("int").alias("date_id"),
    "approach_date"
).dropDuplicates(["date_id"]) \
 .withColumn("year", F.year("approach_date")) \
 .withColumn("month", F.month("approach_date")) \
 .withColumn("day", F.dayofmonth("approach_date")) \
 .withColumn("weekday", F.date_format("approach_date", "E"))

dim_celestial_body = df.select(
    F.monotonically_increasing_id().alias("celestial_body_id"),
    "approaching_body"
).dropDuplicates(["approaching_body"])

fact_asteroid_approach = df \
    .join(dim_date, df.approach_date == dim_date.approach_date, "left") \
    .join(dim_celestial_body, "approaching_body", "left") \
    .select(
        F.monotonically_increasing_id().alias("approach_event_id"),
        "asteroid_id",
        "date_id",
        "celestial_body_id",
        "approach_datetime",
        "velocity_km_s",
        "velocity_km_h",
        "miss_distance_km",
        "miss_distance_au",
        "miss_distance_lunar",
        "_etl_batch_id",
        "_processing_timestamp"
    )

# %%
dim_asteroid.write.mode("append").parquet(
    f"s3a://{bucket_name}/gold/dim_asteroid"
)

dim_date.write.mode("append").parquet(
    f"s3a://{bucket_name}/gold/dim_date"
)

dim_celestial_body.write.mode("append").parquet(
    f"s3a://{bucket_name}/gold/dim_celestial_body"
)

fact_asteroid_approach.write.mode("append").parquet(
    f"s3a://{bucket_name}/gold/fact_asteroid_approach"
)

# %%
