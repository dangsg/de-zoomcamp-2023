from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import os


@task(retries=3)
# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, data_file_name: str) -> Path:
    """Fetch and write fhv data to local"""
    os.system(f"wget {dataset_url} -O data/fhv/{data_file_name}")
    return Path(f"data/fhv/{data_file_name}")


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year: int = 2019, month: int = 3) -> None:
    """Sub flow"""
    data_file_name = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{data_file_name}"
    path = fetch(dataset_url, data_file_name)
    write_gcs(path)


@flow(log_prints=True)
def main_flow(years: list[int], months: list[int]) -> None:
    """Main flow"""
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month)


if __name__ == "__main__":
    years = [2019]
    months = list(range(1, 13))
    main_flow(years, months)
