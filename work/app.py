import os
import s3fs
import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo


# Setting up environments from LakeFS
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")

# Setting S3FileSystem to access LakeFS
fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

@st.cache_data()
def load_data():
    lakefs_path = "s3://air-quality/main/airquality.parquet/year=2025"
    data_list = fs.glob(f"{lakefs_path}/*/*/*/*")
    df_all = pd.concat([pd.read_parquet(f"s3://{path}", engine="pyarrow", filesystem=fs) for path in data_list], ignore_index=True)
    # Change Data Type
    df_all['lat'] = pd.to_numeric(df_all['lat'], errors='coerce')
    df_all['long'] = pd.to_numeric(df_all['long'], errors='coerce')
    df_all['year'] = df_all['year'].astype(int) 
    df_all['month'] = df_all['month'].astype(int)
    columns_to_convert = ['stationID', 'nameTH', 'nameEN', 'areaTH', 'areaEN', 'stationType']
    for col in columns_to_convert:
        df_all[col] = df_all[col].astype(pd.StringDtype())

    df_all.drop_duplicates(inplace=True)
    df_all['PM25.value'] = df_all['PM25.value'].mask(df_all['PM25.value'] < 0, pd.NA)
    # Fill value "Previous Record" Group By stationID
    df_all['PM25.value'] = df_all.groupby('stationID')['PM25.value'].transform(lambda x: x.fillna(method='ffill'))
    return df_all

def filter_data(df, start_date, end_date, station):
    df_filtered = df.copy()

    # Filter by date
    df_filtered = df_filtered[
        (df_filtered['timestamp'].dt.date >= start_date) &
        (df_filtered['timestamp'].dt.date <= end_date)
    ]

    # Filter by station
    if station != "ทั้งหมด":
        df_filtered = df_filtered[df_filtered['nameTH'] == station]

    # Remove invalid AQI
    df_filtered = df_filtered[df_filtered['PM25.aqi'] >= 0]

    return df_filtered

st.set_page_config(
    page_title = 'Real-Time Air Quality Dashboard',
    page_icon = '🦄',
    layout = 'wide'
)
st.title("Air Quality Dashboard from LakeFS 🌎")
df = load_data()
thai_time = datetime.now(ZoneInfo("Asia/Bangkok"))
st.caption(f"อัปเดตล่าสุด: {thai_time.strftime('%Y-%m-%d %H:%M:%S')}")

if "analyzed" not in st.session_state:
    st.session_state.analyzed = False

if "insight_output" not in st.session_state:
    st.session_state.insight_output = ""

if "prev_start_date" not in st.session_state:
    st.session_state.prev_start_date = None
if "prev_end_date" not in st.session_state:
    st.session_state.prev_end_date = None
if "prev_station" not in st.session_state:
    st.session_state.prev_station = None
