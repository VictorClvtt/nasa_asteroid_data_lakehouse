def bronze_to_silver():
    # %%
    import pandas as pd
    from pandas import json_normalize
    from dotenv import load_dotenv
    from pathlib import Path
    import os
    from datetime import datetime

    # Carregar variáveis do .env
    env_path = "/usr/local/airflow/include/.env"
    load_dotenv(env_path)

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

    today_str = datetime.today().strftime("%Y-%m-%d")

    # Caminho no MinIO
    file_key = f"bronze/JSON/nasa-asteroid-data-{today_str}.json"

    # %%
    # Ler JSON do MinIO usando s3fs
    df_raw = pd.read_json(
        f"s3://{bucket_name}/{file_key}",
        storage_options={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": bucket_endpoint}
        }
    )

    # %%
    # Explodir o campo "near_earth_objects[today_str]"
    asteroids = df_raw["near_earth_objects"][today_str]
    df = json_normalize(asteroids)

    # %%
    # Selecionar e renomear colunas para imitar o Spark
    df = df.rename(columns={
        "id": "asteroid_id",
        "neo_reference_id": "neo_id",
        "name": "asteroid_name",
        "absolute_magnitude_h": "magnitude",
        "is_potentially_hazardous_asteroid": "is_hazardous",
        "is_sentry_object": "is_sentry",
        "estimated_diameter.kilometers.estimated_diameter_min": "diameter_min_km",
        "estimated_diameter.kilometers.estimated_diameter_max": "diameter_max_km",
        "estimated_diameter.meters.estimated_diameter_min": "diameter_min_m",
        "estimated_diameter.meters.estimated_diameter_max": "diameter_max_m",
        "nasa_jpl_url": "jpl_url"
    })

    # Pegar só a primeira abordagem (close_approach_data[0])
    approach = json_normalize(df["close_approach_data"].str[0])
    approach = approach.rename(columns={
        "close_approach_date": "approach_date",
        "close_approach_date_full": "approach_datetime",
        "orbiting_body": "approaching_body",
        "relative_velocity.kilometers_per_second": "velocity_km_s",
        "relative_velocity.kilometers_per_hour": "velocity_km_h",
        "miss_distance.kilometers": "miss_distance_km",
        "miss_distance.astronomical": "miss_distance_au",
        "miss_distance.lunar": "miss_distance_lunar"
    })

    # Juntar com o df principal
    df = pd.concat([df.drop(columns=["close_approach_data"]), approach], axis=1)

    # Converter tipos
    df["approach_date"] = pd.to_datetime(df["approach_date"], errors="coerce").dt.date
    df["approach_datetime"] = pd.to_datetime(df["approach_datetime"], errors="coerce")

    df["velocity_km_s"] = pd.to_numeric(df["velocity_km_s"], errors="coerce")
    df["velocity_km_h"] = pd.to_numeric(df["velocity_km_h"], errors="coerce")
    df["miss_distance_km"] = pd.to_numeric(df["miss_distance_km"], errors="coerce")
    df["miss_distance_au"] = pd.to_numeric(df["miss_distance_au"], errors="coerce")
    df["miss_distance_lunar"] = pd.to_numeric(df["miss_distance_lunar"], errors="coerce")

    # %%
    # Adicionar metadados
    df["_processing_timestamp"] = pd.Timestamp.now()
    df["_processing_date"] = pd.to_datetime("today").normalize().date()
    df["_source_system"] = "NeoWs API"
    df["_etl_batch_id"] = int(pd.Timestamp.now().timestamp())

    # %%
    # Salvar em parquet no MinIO, particionado por data
    output_path = f"s3://{bucket_name}/silver/nasa_asteroids/"

    df.to_parquet(
        output_path,
        engine="pyarrow",
        partition_cols=["_processing_date"],
        storage_options={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": bucket_endpoint}
        }
    )

    print("✅ Dados salvos como Parquet particionado!")
