# %%
import duckdb
import s3fs
import os
from dotenv import load_dotenv

# Caminho do .env (ajuste se necessário)
env_path = "/home/victor/Documentos/Code/Python/Data_Engineering/nasa_meteor_data_warehouse/.env"
load_dotenv(env_path)

# Variáveis de ambiente
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")
bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

# %%
# Cria conexão persistente com o catálogo DuckDB
catalog_path = "./gold_catalog.duckdb"
con = duckdb.connect(catalog_path)

# Instala e carrega extensão httpfs (para acessar S3/MinIO)
con.sql("INSTALL httpfs;")
con.sql("LOAD httpfs;")

# Configura o acesso ao MinIO
endpoint = bucket_endpoint.replace("http://", "").replace("https://", "")

con.sql(f"""
SET s3_endpoint='{endpoint}';
SET s3_access_key_id='{access_key}';
SET s3_secret_access_key='{secret_key}';
SET s3_use_ssl=false;
SET s3_url_style='path';  -- IMPORTANTE para MinIO
""")

# %%
# Descobre todos os arquivos Parquet na camada gold
fs = s3fs.S3FileSystem(
    key=access_key,
    secret=secret_key,
    client_kwargs={"endpoint_url": bucket_endpoint},
)

gold_files = fs.glob(f"{bucket_name}/gold/*/*.parquet")

if not gold_files:
    raise FileNotFoundError("❌ Nenhum arquivo Parquet encontrado em gold/")

# %%
# Registra cada tabela no catálogo
for file_path in gold_files:
    table_name = file_path.split("/")[-2]  # <- nome da pasta
    s3_path = f"s3://{file_path}"

    print(f"🔗 Registrando tabela '{table_name}' em: {s3_path}")

    con.sql(f"""
    CREATE OR REPLACE VIEW {table_name} AS
    SELECT * FROM read_parquet('{s3_path}');
    """)
    print(f"✅ Registrada tabela: {table_name}")


# %%
# Lista as tabelas disponíveis no catálogo
print("\n📚 Tabelas catalogadas:")
tables = con.sql("SHOW TABLES").fetchall()
for t in tables:
    print(f" - {t[0]}")

# %%
# Exemplo: visualizar o esquema de uma tabela
print("\n🧩 Esquema da tabela fact_asteroid_approach:")
print(con.sql("DESCRIBE fact_asteroid_approach").df())

# %%
# Exemplo: consulta SQL diretamente do catálogo
print("\n🚀 Consulta exemplo (TOP 5 registros):")
print(con.sql("""
SELECT asteroid_id, velocity_km_s, miss_distance_km
FROM fact_asteroid_approach
LIMIT 5
""").df())
