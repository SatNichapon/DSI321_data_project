import requests
import pandas as pd
from prefect import flow, task

# Pipeline script for data collection, processing, and loading into lakeFS storage (ETL).
@task
def get_aqi_data() -> list[dict]:
    """
    Make an API request to the endpoint using URL.

    Parameters:
    None

    Returns:
    data: extracted data for further processing.
    """

    # API endpoint
    AQI_ENDPOINT = 'http://air4thai.pcd.go.th/services/getNewAQI_JSON.php'

    try:
        # Make API request
        response = requests.get(url=AQI_ENDPOINT)
        response.raise_for_status()

        # Transform data into dataframe
        response_json = response.json()
        data = response_json['stations']

        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None

@task
def data_processing(data: list[dict]) -> pd.DataFrame:
    """
    Process data, convert data types, minor cleaning and keeping necessary columns.

    Parameters: 
    data: extracted data.

    Returns:
    df: DataFrame contain processed data.
    """

    df = pd.DataFrame(data)

    expanded_aqi = pd.json_normalize(df['AQILast'])
    df = pd.concat([df, expanded_aqi], axis=1)

    # Convert data types
    numeric_cols = ['PM25.color_id', 'PM25.value']

    df[numeric_cols] = df[numeric_cols].astype(float)
    df['time'] = df['time'].mode()[0]
    df['date'] = df['date'].mode()[0]
    df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['time'])

    # Make Datetime attribute columns for partitioning in parquet file.
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour

    # Keep necessary columns
    datetime_attibute_cols = ['timestamp', 'year', 'month', 'day', 'hour']
    location_attribute_cols = ['stationID', 'nameTH', 'nameEN', 'areaTH', 'areaEN', 'stationType', 'lat', 'long']
    df = df[datetime_attibute_cols + location_attribute_cols + numeric_cols]

    return df

@task
def load_to_lakefs(df: pd.DataFrame, lakefs_s3_path: str, storage_options: dict):
    """
    Load data into lakeFS storage in S3 (Amazon Simple Storage Service) and in a parquet format.
    Partitions the parquet file based on 'year', 'month', 'day' and 'hour'.

    Parameters: 
    df: Target DataFrame that will be loaded into the storage.
    lakefs_s3_path: S3-Supported API Gateway, layer in lakeFS responsible for the compatibility with S3.
    storage_options: configured options for accessing lakeFS storage.

    Returns:
    None
    """
    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=['year','month','day','hour'],
    )

@flow(name='main-flow', log_prints=True)
def main_flow():

    # Task 1: Get data
    data = get_aqi_data()
    
    # Task 2: Process data
    df = data_processing(data)

    # laekFS storage
    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"

    # lakeFS endpoint (running locally)
    lakefs_endpoint = "http://lakefs-dev:8000/"

    # lakeFS repository, branch, and file path
    repo = "air-quality"
    branch = "main"
    path = "airquality.parquet"

    # Construct the full lakeFS S3-compatible path
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    # Configure storage_options for lakeFS (S3-compatible)
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }

    # Task 3: Load data into lakeFS storage in a parquet format
    load_to_lakefs(df=df, lakefs_s3_path=lakefs_s3_path, storage_options=storage_options)
