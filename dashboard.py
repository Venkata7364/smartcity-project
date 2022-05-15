import pandas as pd
import psycopg2
import streamlit as st


st.set_page_config(page_title="Smart City Dashboard", layout="wide")


@st.cache_data(ttl=5)
def load_events():
    conn = psycopg2.connect(
        host="localhost",
        database="smartcity",
        user="admin",
        password="admin"
    )

    query = """
        SELECT *
        FROM iot_events
        ORDER BY event_time DESC
        LIMIT 1000;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


df = load_events()

st.title("Smart City Real-Time Dashboard")

if df.empty:
    st.warning("No events found yet. Start Spark and the producers to load data.")
    st.stop()

total_events = len(df)
vehicle_df = df[df["type"] == "vehicle"]
traffic_df = df[df["type"] == "traffic"]
weather_df = df[df["type"] == "weather"]
emergency_df = df[df["type"] == "emergency"]

avg_speed = vehicle_df["speed"].dropna().mean()
overspeeding_count = len(vehicle_df[vehicle_df["speed_status"] == "overspeeding"])
critical_count = len(emergency_df[emergency_df["priority"] == "critical"])
latest_weather = weather_df.sort_values("event_time", ascending=False).head(1)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Events", total_events)
col2.metric("Average Vehicle Speed", f"{avg_speed:.2f}" if pd.notna(avg_speed) else "N/A")
col3.metric("Overspeeding Vehicles", overspeeding_count)
col4.metric("Critical Emergencies", critical_count)

if not latest_weather.empty:
    weather_row = latest_weather.iloc[0]
    col5, col6 = st.columns(2)
    col5.metric("Latest Temperature", f"{int(weather_row['temperature'])}")
    col6.metric("Latest Humidity", f"{int(weather_row['humidity'])}%")

left, right = st.columns(2)

with left:
    st.subheader("Events By Type")
    st.bar_chart(df["type"].value_counts())

with right:
    st.subheader("Traffic Levels")
    if traffic_df.empty:
        st.info("No traffic events yet.")
    else:
        st.bar_chart(traffic_df["level"].value_counts())

st.subheader("Latest Events")
st.dataframe(df.head(50), use_container_width=True)

st.subheader("Overspeeding Vehicles")
st.dataframe(
    vehicle_df[vehicle_df["speed_status"] == "overspeeding"]
    .sort_values("event_time", ascending=False)
    .head(25),
    use_container_width=True
)

st.subheader("Critical Emergency Events")
st.dataframe(
    emergency_df[emergency_df["priority"] == "critical"]
    .sort_values("event_time", ascending=False)
    .head(25),
    use_container_width=True
)
