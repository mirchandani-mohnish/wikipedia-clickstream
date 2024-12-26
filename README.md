# Wikipedia Clickstream Analysis

## Overview

Wikipedia Clickstream Analysis is a tool for studying user navigation patterns across Wikipedia articles. It leverages technologies like Apache Kafka, Apache Spark, and Cassandra to process and analyze large-scale clickstream data. The system is designed for scalability, fault tolerance, and high availability, providing insights into user behavior, article popularity, and navigation paths.

## Features

**Data Ingestion:** Downloads and processes Wikipedia clickstream datasets.

**Real-Time Streaming**: Streams data to Kafka and processes it using Apache Spark.

**Data Storage**: Stores processed data in Cassandra for efficient querying and updates.

**Visualization**: Displays insights through interactive dashboards powered by Node.js and Chart.js.

**Scalable Architecture**: Uses Docker for containerization and deployment across multiple instances.

## System Architecture
![image](https://github.com/user-attachments/assets/8808c6ae-c57f-472a-a1c0-7a65b946ad86)

### Kafka Integration

Kafka acts as the backbone for real-time data ingestion. It processes raw clickstream data and distributes it across partitions for parallel processing.

### Spark Processing

Spark processes the data streams in real-time, cleaning, normalizing, and aggregating data for analysis.

### Cassandra Storage

Processed data is stored in Cassandra, providing high availability and scalability for queries.

### Visualization

Interactive dashboards display navigation patterns, incoming/outgoing traffic, and related search terms.

### Technologies Used

Apache Kafka: Real-time data ingestion and message streaming.

Apache Spark: Distributed data processing and stream analytics.

Apache Cassandra: NoSQL database for scalable and fault-tolerant storage.

Docker: Containerization and deployment across environments.

Node.js and Chart.js: Visualization tools for interactive dashboards.

## Installation and Setup

### Prerequisites

- Docker and Docker Compose installed.
- Python 3.9 or higher.
- pip and virtualenv.

### Steps

1. Clone Repository
```
git clone <repository-url>
cd wikipedia-clickstream
```
2. Set Up Environment
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
3. Build and Start Services
```
docker-compose up --build
```
4. Load Data into Kafka
- Download the dataset, extract it, and produce it into Kafka topics.

5. Monitor Spark and Cassandra

- Access Spark UI at http://localhost:8080.
- Access Cassandra using CQL Shell:
```
docker exec -it cassandra cqlsh
```
6. Access Visualizations

- Open the browser and visit http://localhost:5001.
