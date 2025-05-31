import urllib.request
from prefect import task, flow


@task
def download_data(year: int = 2023, month: int = 2):
    """
    Downloads the data from the given URL and saves it to a file.
    """
    filename = f'green_tripdata_{year}-{month:02d}.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    urllib.request.urlretrieve(url, filename)
    print(f"Downloaded {filename}")


@flow
def download_data_flow(year: int = 2023, month: int = 2):
    """
    Flow to download data for a specific year and month.
    """
    download_data(year, month)


if __name__ == "__main__":
    download_data_flow(year=2023, month=2)
    # You can change the year and month parameters to download different data.
    # For example, to download data for March 2023, call:
    # download_data_flow(year=2023, month=3)