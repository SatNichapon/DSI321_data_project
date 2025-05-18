import os
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import streamlit as st
import pyarrow.parquet as pq
import s3fs
from zoneinfo import ZoneInfo
from datetime import datetime

# Set up environments of LakeFS
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")
ACCESS_KEY = 'access_key'
SECRET_KEY = 'secret_key'

# Setting S3FileSystem for access LakeFS
fs = s3fs.S3FileSystem(
    key=ACCESS_KEY,
    secret=SECRET_KEY,
    client_kwargs={'endpoint_url': lakefs_endpoint}
)

@st.cache_data()
def load_data():
    lakefs_path = "s3://air-quality/main/airquality.parquet/year=2025"
    data_list = fs.glob(f"{lakefs_path}/*/*/*/*")
    df_all = pd.concat([pd.read_parquet(f"s3://{path}", filesystem=fs) for path in data_list], ignore_index=True)
    df_all['lat'] = pd.to_numeric(df_all['lat'], errors='coerce')
    df_all['long'] = pd.to_numeric(df_all['long'], errors='coerce')
    df_all['year'] = df_all['year'].astype(int)
    df_all['month'] = df_all['month'].astype(int)
    df_all.drop_duplicates(inplace=True)
    df_all['PM25.value'] = df_all['PM25.value'].mask(df_all['PM25.value'] < 0, pd.NA)
    # Fill value "Previous Record" Group By stationID
    df_all['PM25.value'] = df_all.groupby('stationID')['PM25.value'].transform(lambda x: x.ffill())
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

    # Remove invalid values
    df_filtered = df_filtered[df_filtered['PM25.value'] >= 0]

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

# Sidebar settings
with st.sidebar:
    st.title("Air4Thai Dashboard")
    st.header("⚙️ Settings")

    max_date = df['timestamp'].max().date()
    min_date = df['timestamp'].min().date()
    default_start_date = min_date
    default_end_date = max_date

    start_date = st.date_input(
        "Start date",
        default_start_date,
        min_value=min_date,
        max_value=max_date
    )

    end_date = st.date_input(
        "End date",
        default_end_date,
        min_value=min_date,
        max_value=max_date
    )

    station_name = df['nameTH'].dropna().unique().tolist()
    station_name.sort()
    station_name.insert(0, "ทั้งหมด")
    station = st.selectbox("Select Station", station_name)

if (
    st.session_state.prev_start_date != start_date or
    st.session_state.prev_end_date != end_date or
    st.session_state.prev_station != station
):
    st.session_state.analyzed = False
    st.session_state.insight_output = ""

st.session_state.prev_start_date = start_date
st.session_state.prev_end_date = end_date
st.session_state.prev_station = station

df_filtered = filter_data(df, start_date, end_date, station)

# Container for KPI and main content
placeholder = st.empty()

with placeholder.container():

    if not df_filtered.empty:
        # AVG for Selection Interval
        avg_value = df_filtered['PM25.value'].mean()
        avg_color = df_filtered['PM25.color_id'].mean()

        # Previous Day
        prev_day = end_date - pd.Timedelta(days=1)
        df_prev_day = filter_data(df, prev_day, prev_day, station)

        # AVG of Previous Day
        prev_avg_value = df_prev_day['PM25.value'].mean()
        prev_avg_color = df_prev_day['PM25.color_id'].mean()

        # Delta
        delta_value = None if pd.isna(prev_avg_value) else avg_value - prev_avg_value
        delta_color = None if pd.isna(prev_avg_color) else avg_color - prev_avg_color

        # Area that have the Most value
        area_highest_value_ind = df_filtered.groupby('areaTH')['PM25.value'].mean().idxmax()
        area_highest_value = df_filtered.groupby('areaTH')['PM25.value'].mean().max()

        # Area Most value of Previous
        if not df_prev_day.empty:
            area_prev_highest_value = df_prev_day.groupby('areaTH')['PM25.value'].mean().max()
            delta_area_value = area_highest_value - area_prev_highest_value
        else:
            delta_area_value = None

        # Scorecards
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(
            label="🌡️ ค่าเฉลี่ยคุณภาพ PM2.5 ในอากาศ",
            value=f"{avg_value:.2f}",
            delta=f"{delta_value:+.2f}" if delta_value is not None else None
        )
        kpi2.metric(
            label="🔥 ค่าเฉลี่ยระดับ PM2.5 ของประเทศไทย",
            value=f"{avg_color:.2f}",
            delta=f"{delta_color:+.2f}" if delta_color is not None else None
        )
        kpi3.metric(
            label="📍 พื้นที่ที่มีระดับ PM2.5 สูงสุด",
            value=area_highest_value,
            delta=f"{delta_area_value:+.2f}" if delta_area_value is not None else None
        )
    else:
        st.warning("ไม่พบข้อมูลในช่วงเวลาหรือสถานีที่เลือก")

# Visualization section
fig_col1, fig_col2 = st.columns([1.2, 1.8], gap='medium')

# Filter by station
if station == "ทั้งหมด":
    df_selected = df_filtered.copy()
    title = "PM2.5 ของทุกสถานี"
else:
    df_selected = df_filtered[df_filtered['nameTH'] == station]
    title = f"PM2.5 - สถานี {station}"

# Left column: Thailand map with PM2.5 value
with fig_col1:
    if not df_selected.empty:
        df_map = df_selected.groupby(['stationID', 'nameTH', 'lat', 'long'], as_index=False)['PM25.value'].mean()

        fig_map = px.scatter_geo(
            df_map,
            lat='lat',
            lon='long',
            color='PM25.value',
            hover_name='nameTH',
            color_continuous_scale='Turbo',
            title='แผนที่สถานีตรวจวัด PM2.5 ในประเทศไทย',
            projection='natural earth'
        )

        fig_map.update_geos(
            visible=True,
            resolution=50,
            showcountries=True,
            countrycolor="grey",
            showsubunits=True,
            subunitcolor="lightgray",
            showocean=True,
            oceancolor="LightBlue",
            showland=True,
            landcolor="whitesmoke",
            lakecolor="LightBlue",
            showlakes=True,
            lataxis_range=[5, 21],  # กำหนดขอบเขตละติจูด
            lonaxis_range=[93, 110] # กำหนดขอบเขตลองจิจูด
        )

        fig_map.update_layout(
            template="plotly_dark",
            margin={"r":0,"t":40,"l":0,"b":0},
            coloraxis_colorbar=dict(title="PM2.5 value")
        )

        st.plotly_chart(fig_map)
    else:
        st.warning("ไม่พบข้อมูลสำหรับสถานีที่เลือก")


# Right column: Line chart
with fig_col2:
    if not df_selected.empty:
        if station == "ทั้งหมด":
            # Filter the top 5 stations with highest average PM2.5 value
            top_5_stations = df_selected.groupby('nameTH')['PM25.value'].mean().nlargest(5).index
            df_selected_top5 = df_selected[df_selected['nameTH'].isin(top_5_stations)]
            
            fig = px.line(
                df_selected_top5.sort_values("timestamp"),
                x='timestamp',
                y='PM25.value',
                color='nameTH',
                title=f"Top 5 สถานี PM2.5 สูงสุดในช่วง {start_date} ถึง {end_date}",
            )
        else:
            fig = px.line(
                df_selected.sort_values("timestamp"),
                x='timestamp',
                y='PM25.value',
                color=None,
                title=title,
            )
        
        fig.update_layout(xaxis_title='Time', yaxis_title='PM2.5 value')
        st.plotly_chart(fig)
    else:
        st.warning("ไม่พบข้อมูลสำหรับสถานีที่เลือก")

# CSS
st.markdown("""
<style>
.stButton button {
  background-color: rgb(124 58 237);
  color: white;
  border-radius: 0.5rem;
  box-shadow:
    inset 0 1px 0 0 rgba(255,255,255,0.3),
    0 2px 0 0 rgb(109 40 217),
    0 4px 0 0 rgb(91 33 182),
    0 6px 0 0 rgb(76 29 149),
    0 8px 0 0 rgb(67 26 131),
    0 8px 16px 0 rgba(147,51,234,0.5);
  overflow: hidden;
  padding: 0.75rem 1.5rem;
  font-family: system-ui;
  font-weight: 400;
  font-size: 1.875rem;
  align-items: center;
  border: none;
  cursor: pointer;
  position: relative;
  transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out; 
}

.stButton button::before {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(to bottom, rgba(255,255,255,0.2), transparent);
}

.stButton button:hover {
  transform: translateY(4px);
  box-shadow:
    inset 0 1px 0 0 rgba(255,255,255,0.3),
    0 1px 0 0 rgb(109 40 217),
    0 2px 0 0 rgb(91 33 182),
    0 3px 0 0 rgb(76 29 149),
    0 4px 0 0 rgb(67 26 131),
    0 4px 8px 0 rgba(147,51,234,0.5);
}

.stButton button:hover i {
  display: inline-block;
  animation: bounce 1s infinite;
}

@keyframes bounce {
  0%, 20%, 50%, 80%, 100% {
    transform: translateY(0);
  }
  40% {
    transform: translateY(-5px);
  }
  60% {
    transform: translateY(-3px);
  }
}
</style>
""", unsafe_allow_html=True)