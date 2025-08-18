# Kafka_to_MongoDB_Spark_streaming_GCP


## What is this project?

This project demonstrates a real-time data pipeline built with **Confluent Kafka**, **Spark Structured Streaming**, and **MongoDB**, deployed and tested on **Google Cloud Platform (GCP)**.

It includes:
- Two Python scripts of streaming **orders** and **payments** data into Kafka topics.
- A Spark Structured Streaming job that reads data from the Kafka topics, joins them in real-time using stateful processing, and writes the enriched data to **MongoDB**.

## Why this project?

Kafka is widely used for building real-time streaming data pipelines. This project is intended to demonstrate:
- My understanding of **stateful stream processing** using Apache Spark.
- Integration of **Kafka**, **MongoDB**, and **Spark** for near real-time ETL.
- Practical skills in designing event-driven architectures on cloud platforms.


## Project Structure

- `producer_join_stream.py`, `producer_payments_join_stream.py` – Sends order and payment data to two Kafka topics (`order_data` and `payment_data`).
- `join_stream.py` – Spark Structured Streaming job that:
  - Reads from Kafka topics.
  - Parses and joins order and payment data using a **stateful join**.
  - Writes the resulting enriched data to a **MongoDB Atlas** collection.

## Requirements

- Python 3.0+
- Confluent Kafka (via Confluent Cloud or locally)
- Apache Spark (configured with Kafka and MongoDB connector packages)
- MongoDB (Atlas or local)
- GCP (for running the streaming job, or local testing)
- Required Python libraries:
  - `pyspark`
  - `confluent_kafka`
  - `pandas`
- IDE like **VS Code** or **PyCharm**
- download necessary dependencies from Maven repository

