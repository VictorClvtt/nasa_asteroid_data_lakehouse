def silver_to_gold():
    # %%
    import pandas as pd
    import os
    from dotenv import load_dotenv
    from datetime import datetime

    # Carregando variáveis do .env
    env_path = "/usr/local/airflow/include/.env"
    load_dotenv(env_path)

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

    today_str = datetime.today().strftime("%Y-%m-%d")

    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": bucket_endpoint},
    }

    # Lê todos os arquivos parquet da partição silver
    df = pd.read_parquet(
        f"s3://{bucket_name}/silver/nasa_asteroids/_processing_date={today_str}/",
        storage_options=storage_options
    )

    # %%
    # Dimension tables
    dim_asteroid = (
        df[[
            "asteroid_id", "neo_id", "asteroid_name", "magnitude",
            "is_hazardous", "is_sentry",
            "diameter_min_km", "diameter_max_km",
            "diameter_min_m", "diameter_max_m",
            "jpl_url"
        ]]
        .drop_duplicates(subset=["asteroid_id"])
        .reset_index(drop=True)
    )

    dim_date = (
        df[["approach_date"]]
        .dropna()
        .drop_duplicates()
        .assign(
            date_id=lambda x: pd.to_datetime(x["approach_date"]).dt.strftime("%Y%m%d").astype(int),
            year=lambda x: pd.to_datetime(x["approach_date"]).dt.year,
            month=lambda x: pd.to_datetime(x["approach_date"]).dt.month,
            day=lambda x: pd.to_datetime(x["approach_date"]).dt.day,
            weekday=lambda x: pd.to_datetime(x["approach_date"]).dt.strftime("%a"),
        )
        .reset_index(drop=True)
    )

    dim_celestial_body = (
        df[["approaching_body"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_celestial_body["celestial_body_id"] = dim_celestial_body.index + 1

    # %%
    # Fact table
    fact_asteroid_approach = (
        df.merge(dim_date, on="approach_date", how="left")
        .merge(dim_celestial_body, on="approaching_body", how="left")
    )

    fact_asteroid_approach = fact_asteroid_approach[[
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
    ]].reset_index(drop=True)
    fact_asteroid_approach["approach_event_id"] = fact_asteroid_approach.index + 1

    # %%
    # Salvar de volta no MinIO em parquet
    batch_id = fact_asteroid_approach["_etl_batch_id"].iloc[0]

    dim_asteroid.to_parquet(
        f"s3://{bucket_name}/gold/dim_asteroid/data_{batch_id}.parquet",
        index=False,
        storage_options=storage_options
    )

    dim_date.to_parquet(
        f"s3://{bucket_name}/gold/dim_date/data_{batch_id}.parquet",
        index=False,
        storage_options=storage_options
    )

    dim_celestial_body.to_parquet(
        f"s3://{bucket_name}/gold/dim_celestial_body/data_{batch_id}.parquet",
        index=False,
        storage_options=storage_options
    )

    fact_asteroid_approach.to_parquet(
        f"s3://{bucket_name}/gold/fact_asteroid_approach/data_{batch_id}.parquet",
        index=False,
        storage_options=storage_options
    )

    print(f"✅ Dataframes gold salvos com sucesso! (batch_id={batch_id})")