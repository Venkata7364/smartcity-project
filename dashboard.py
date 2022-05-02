import streamlit as st
import psycopg2
import pandas as pd

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="smartcity",
    user="admin",
    password="admin"
)

# Query latest data
query = "SELECT * FROM vehicle_data ORDER BY timestamp DESC LIMIT 50;"
df = pd.read_sql(query, conn)

# UI
st.title("🚗 Smart City Real-Time Dashboard")

st.subheader("Latest Vehicle Data")
st.dataframe(df)

# Average speed
avg_speed = df["speed"].mean()
st.metric("Average Speed", f"{avg_speed:.2f}")

# Vehicles per location
st.subheader("Vehicles per Location")
location_counts = df["location"].value_counts()
st.bar_chart(location_counts)