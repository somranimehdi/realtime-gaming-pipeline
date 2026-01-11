# Real-Time Gaming Analytics Pipeline
**High-Throughput Stream Processing with Kafka, Spark, and the ELK Stack**

## Introduction
In the world of competitive gaming, real-time data is the difference between a seamless experience and a system-wide failure. This project implements a production-grade data engineering pipeline designed to ingest, transform, and visualize player telemetry data as it happens. 

By leveraging Apache Kafka for resilient message queuing and Apache Spark Structured Streaming for complex windowed aggregations, the system converts a chaotic stream of raw JSON events into actionable business intelligence within seconds.

---

## System Architecture
The pipeline is designed for scalability and fault tolerance, utilizing a fully containerized infrastructure.



* **Ingestion:** Distributed event streaming via Apache Kafka.
* **Processing:** Micro-batch aggregation via Apache Spark 3.5.
* **Storage:** Document-based indexing in Elasticsearch 7.17.
* **Analytics:** Real-time monitoring with Kibana.

---

## How to Execute

### 1. Infrastructure Deployment
Launch the core services (Kafka, Zookeeper, and the ELK Stack) using Docker:

cd docker
docker-compose up -d

### 2. Schema and Mapping Initialization
Initialize the Elasticsearch index with the correct date mappings to enable time-series visualization:

# While in the docker folder
chmod +x elastic_mapping.sh
./elastic_mapping.sh

### 3. Running the Data Engine
Open two separate terminals to observe the flow:

**Terminal 1 (Producer):**
cd producers
python3 producer.py

**Terminal 2 (Consumer):**
cd consumers
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16 consumer.py

---

## Real-Time Data Simulation
To simulate a live environment, I developed a custom Python-based Event Simulator. This is the engine that allows for real-time data fetching and stress-testing.

* **Dynamic Generation:** Mimics 1,000+ players across regions (NA, EU, ASIA) generating event types like player_join, item_purchase, and enemy_defeated.
* **High-Velocity Ingestion:** Events are injected into the Kafka broker at intervals as low as 100ms to simulate a high-traffic production game server.
* **Precision Timestamps:** Each event is tagged with an ISO-8601 timestamp at the source, enabling accurate Event-Time Processing within Spark.

---

## System Demonstration

### I. Real-Time Dashboard (IRL)
This video shows the Kibana dashboard refreshing every second. Watch the 'Events Per Region' and 'Live Event Count' charts react instantly to the producer's activity.
[![Watch the video](https://img.youtube.com/vi/9g3DMH9RLcY/maxresdefault.jpg)](https://youtu.be/9g3DMH9RLcY)




### II. Data Pipeline Flow (Terminals)
This video shows the interaction between the Simulation (left) and the Spark Engine (right). You can see Spark processing micro-batches and committing them to the sink in real-time.

[![Watch the video](https://img.youtube.com/vi/_qiy4xC3o54/maxresdefault.jpg)](https://youtu.be/_qiy4xC3o54)



---

## Core Engineering Logic
* **Windowed Aggregations:** Used 1-minute tumbling windows to calculate player activity trends.
* **Watermarking:** Implemented a 10-minute watermark to ensure data integrity despite potential network latency.
* **Idempotent Writes:** Engineered a composite doc_id strategy to prevent data duplication in the event of a Spark task retry.

---

## Contact
**Mehdi Somrani** - Data Engineer 
[LinkedIn](https://linkedin.com/in/somranimehdi)
