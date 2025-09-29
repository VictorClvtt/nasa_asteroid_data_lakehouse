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
    .appName("bronze_to_silver") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.698") \
    .config("spark.hadoop.fs.s3a.endpoint", bucket_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


# Ler o JSON
today_str = datetime.today().strftime("%Y-%m-%d")
df = spark.read.option("multiline", "true").json(f"s3a://{bucket_name}/bronze/JSON/nasa-asteroid-data-{today_str}.json")

# %%
# Transformar em estrutura tabular
df = df.select(
    F.explode(F.col("near_earth_objects.2025-09-28")).alias("asteroid")
).select(
    # Identificação
    F.col("asteroid.id").alias("asteroid_id"),
    F.col("asteroid.neo_reference_id").alias("neo_id"),
    F.col("asteroid.name").alias("asteroid_name"),
    
    # Características físicas
    F.col("asteroid.absolute_magnitude_h").alias("magnitude"),
    F.col("asteroid.is_potentially_hazardous_asteroid").alias("is_hazardous"),
    F.col("asteroid.is_sentry_object").alias("is_sentry"),
    
    # Diâmetros em diferentes unidades
    F.col("asteroid.estimated_diameter.kilometers.estimated_diameter_min").alias("diameter_min_km"),
    F.col("asteroid.estimated_diameter.kilometers.estimated_diameter_max").alias("diameter_max_km"),
    F.col("asteroid.estimated_diameter.meters.estimated_diameter_min").alias("diameter_min_m"),
    F.col("asteroid.estimated_diameter.meters.estimated_diameter_max").alias("diameter_max_m"),
    
    # URL para mais informações
    F.col("asteroid.nasa_jpl_url").alias("jpl_url"),

    # Dados de close approach (primeira abordagem)
    F.col("asteroid.close_approach_data")[0].alias("approach")
).select(
    "*",
    # Extrair dados da abordagem
    F.col("approach.close_approach_date").alias("approach_date").cast('date'),
    F.to_timestamp(F.col("approach.close_approach_date_full"), "yyyy-MMM-dd HH:mm").alias("approach_datetime"),
    F.col("approach.orbiting_body").alias("approaching_body"),
    
    # Velocidade relativa
    F.col("approach.relative_velocity.kilometers_per_second").cast("double").alias("velocity_km_s"),
    F.col("approach.relative_velocity.kilometers_per_hour").cast("double").alias("velocity_km_h"),
    
    # Distância de miss
    F.col("approach.miss_distance.kilometers").cast("double").alias("miss_distance_km"),
    F.col("approach.miss_distance.astronomical").cast("double").alias("miss_distance_au"),
    F.col("approach.miss_distance.lunar").cast("double").alias("miss_distance_lunar")

).drop("approach")

# %%
# Adicionando metadados
df = df.withColumn(
        "_processing_timestamp", F.current_timestamp()  # Quando foi processado
    ).withColumn(
        "_processing_date", F.current_date()            # Data do processamento
    ).withColumn(
        "_source_system", F.lit('NeoWs API')          # Fonte dos dados (NASA, ESA, etc.)
    ).withColumn(
        "_etl_batch_id", F.unix_timestamp()      # ID único do batch
    )

# %%
# Salvar particionado por data de processamento - MELHOR PERFORMANCE
(df.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("_processing_date")  # Partição por dia
    .save(f"s3a://{bucket_name}/silver/nasa_asteroids"))

print("✅ Dados salvos como Parquet particionado!")
# %%
