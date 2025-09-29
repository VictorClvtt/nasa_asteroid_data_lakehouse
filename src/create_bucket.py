import boto3
from botocore.exceptions import ClientError
import os
from pathlib import Path
from dotenv import load_dotenv

# Carregando e salvando variaveis do .env
env_path = Path(__file__).parent.parent / ".env"
print("Carregando .env de:", env_path)
print("Existe?", env_path.exists())

load_dotenv(env_path)

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")
bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

# Configuração do cliente MinIO (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=bucket_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Cria o bucket se não existir
try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"✅ Bucket '{bucket_name}' criado com sucesso!")
except ClientError as e:
    if e.response['Error']['Code'] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        print(f"⚠️ Bucket '{bucket_name}' já existe.")
    else:
        raise
