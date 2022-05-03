# 🚗 Smart City Real-Time Data Pipeline

##  Project Overview
This project simulates a **real-time data engineering pipeline** for a smart city system.

It generates live vehicle data, streams it through Kafka, processes it using Python, stores it in PostgreSQL, and visualizes it using a Streamlit dashboard.

---

##  Architecture

Producer (Python) → Kafka → Consumer (Python) → PostgreSQL → Streamlit Dashboard

---

##  Technologies Used

- Python
- Apache Kafka
- Docker & Docker Compose
- PostgreSQL
- Streamlit
- Pandas

---

##  Features

- Real-time vehicle data simulation
- Kafka-based streaming pipeline
- Data storage in PostgreSQL
- Live dashboard visualization
- Average speed calculation
- Vehicles distribution by location

---

##  Project Structure


smartcity-project/
smartcity-project/
│
├── dashboard.png   # Dashboard screenshot
├── producer.py     # Generates and sends data to Kafka
├── consumer.py     # Reads from Kafka and stores in DB
├── dashboard.py    # Streamlit dashboard
├── docker-compose.yml
├── data.json       # Raw streamed data
└── README.md


---

##  Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/smartcity-project.git
cd smartcity-project
2. Start Docker Services
docker-compose up -d
3. Install Dependencies
pip install kafka-python psycopg2-binary streamlit pandas
4. Run Producer
python producer.py
5. Run Consumer
python consumer.py
6. Run Dashboard
streamlit run dashboard.py
📊 Dashboard Output

The dashboard shows:

Latest vehicle data
Average speed
Vehicles per location (bar chart)

Challenges Faced & Solutions
1. Kafka Connection Issues
Problem: Broker not connecting
Solution: Verified Docker containers using docker ps
2. Data Serialization Error
Problem: Consumer not reading data properly
Solution: Used JSON encoding/decoding
3. Continuous Streaming Data Confusion
Problem: Data kept printing infinitely
Solution: Used Ctrl + C to stop producer
4. PostgreSQL Connection Setup
Problem: Could not connect to DB
Solution: Installed psycopg2-binary and verified credentials
5. GitHub Push Authentication
Problem: Could not push code
Solution: Used browser authentication (Git Credential Manager)

Future Improvements
Add Apache Spark Streaming
Integrate AWS S3, Glue, Athena
Deploy dashboard online
Add alert system for traffic anomalies
Conclusion

This project demonstrates:

Real-time data streaming
End-to-end pipeline building
Integration of multiple tools
Practical data engineering skills

🙌 Author
Venkata Lakshminarayana Katta
