#%%
import os
from datetime import datetime
from dotenv import load_dotenv
import requests
import boto3

# Carregando e salvando variaveis do .env
env_path = "/home/victor/Documentos/Code/Python/Data_Engineering/nasa_meteor_data_warehouse/.env"

load_dotenv(env_path)

api_key = os.getenv("NASA_API_KEY")
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")
bucket_endpoint = os.getenv("BUCKET_ENDPOINT")
#%%
url = "https://api.nasa.gov/neo/rest/v1/feed"
today_str = datetime.today().strftime("%Y-%m-%d")
params = {
    "start_date": today_str,
    "end_date": today_str,
    "api_key": api_key
}

response = requests.get(url, params=params)
data = response.json()
print(data)

# %%
import json

# Configuração do cliente MinIO (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=bucket_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Nome do arquivo (pode incluir a data do "feed" da API)
file_name = f"bronze/JSON/nasa-asteroid-data-{today_str}.json"

# Serializa o JSON
json_bytes = json.dumps(data, indent=2).encode("utf-8")

# Faz upload para o bucket / bronze
s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_bytes)

print(f"✅ Arquivo salvo em s3://{bucket_name}/{file_name}")