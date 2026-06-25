from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from minio import Minio


RAW_DATA_DIR = Path("/opt/airflow/data/raw")
FILE_PATTERN = "yellow_tripdata_*.parquet"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "nyc-taxi-raw")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_RAW_PREFIX = os.getenv("MINIO_RAW_PREFIX", "nyc_taxi/raw")

MAX_FILES_TO_UPLOAD = int(os.getenv("MAX_FILES_TO_UPLOAD", "1"))
MINIO_UPLOAD_PART_SIZE_MB = int(os.getenv("MINIO_UPLOAD_PART_SIZE_MB", "5"))
def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


@dag(
    dag_id="upload_nyc_taxi_to_minio",
    description="Upload NYC Taxi raw parquet files from local raw zone to MinIO object storage",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "minio", "object-storage", "raw-zone"],
)
def upload_nyc_taxi_to_minio():
    @task
    def check_raw_files() -> list[str]:
        if not RAW_DATA_DIR.exists():
            raise FileNotFoundError(f"Raw data folder not found: {RAW_DATA_DIR}")

        files = sorted(RAW_DATA_DIR.glob(FILE_PATTERN))

        if not files:
            raise FileNotFoundError(
                f"No parquet files found in {RAW_DATA_DIR} with pattern {FILE_PATTERN}"
            )

        file_paths = [str(file) for file in files]

        print(f"Found {len(file_paths)} parquet file(s):")
        for file_path in file_paths:
            print(f"- {file_path}")

        return file_paths

    @task
    def ensure_bucket_exists() -> str:
        client = get_minio_client()

        if not client.bucket_exists(MINIO_BUCKET_NAME):
            client.make_bucket(MINIO_BUCKET_NAME)
            print(f"Created bucket: {MINIO_BUCKET_NAME}")
        else:
            print(f"Bucket already exists: {MINIO_BUCKET_NAME}")

        return MINIO_BUCKET_NAME

    @task
    def upload_files_to_minio(file_paths: list[str], bucket_name: str) -> list[dict]:
        client = get_minio_client()
        upload_results = []

        part_size = MINIO_UPLOAD_PART_SIZE_MB * 1024 * 1024

        for file_path in file_paths:
            local_file = Path(file_path)
            object_name = f"{MINIO_RAW_PREFIX}/{local_file.name}"
            file_size_mb = local_file.stat().st_size / (1024 * 1024)

            print(
                f"Uploading {local_file.name} "
                f"({file_size_mb:.2f} MB) "
                f"to minio://{bucket_name}/{object_name}"
            )

            client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(local_file),
                content_type="application/octet-stream",
                part_size=part_size,
                num_parallel_uploads=1,
            )

            result = {
                "local_file": str(local_file),
                "file_size_mb": round(file_size_mb, 2),
                "bucket": bucket_name,
                "object_name": object_name,
            }

            print(f"Uploaded successfully: minio://{bucket_name}/{object_name}")
            upload_results.append(result)

        return upload_results

    @task
    def summarize_upload(upload_results: list[dict]) -> dict:
        summary = {
            "uploaded_file_count": len(upload_results),
            "bucket": MINIO_BUCKET_NAME,
            "raw_prefix": MINIO_RAW_PREFIX,
            "objects": [item["object_name"] for item in upload_results],
        }

        print("Upload summary:")
        print(summary)

        return summary

    raw_files = check_raw_files()
    bucket = ensure_bucket_exists()
    uploaded = upload_files_to_minio(raw_files, bucket)
    summarize_upload(uploaded)


upload_nyc_taxi_to_minio()