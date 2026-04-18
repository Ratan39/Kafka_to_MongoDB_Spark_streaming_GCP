# Kafka_to_MongoDB_Spark_streaming_GCP

## Project Overview

In e-commerce systems, order events and payment events often arrive at different times through different channels. A simple SQL join in a streaming context often fails if the data arrives out of order or outside of a specific time window.

This project solves this by:
1.  **Ingesting** two separate streams from Confluent Kafka.
2.  **Maintaining State** of "Pending Orders" using Spark's `applyInPandasWithState`.
3.  **Enriching** records only when both the order and payment info are present.
4.  **Handling Timeouts** to clear memory for orders that never receive a payment.
5.  **Persisting** the final enriched data into MongoDB for downstream analytics.

This project demonstrates a real-time data pipeline built with **Confluent Kafka**, **Spark Structured Streaming**, and **MongoDB**, deployed and tested on **Google Cloud Platform (GCP)**.

It includes:
- Two Python scripts of streaming **orders** and **payments** data into Kafka topics.
- A Spark Structured Streaming job that reads data from the Kafka topics, joins them in real-time using stateful processing, and writes the enriched data to **MongoDB**.

## Why this project?

Kafka is widely used for building real-time streaming data pipelines. This project is intended to demonstrate:
- My understanding of **stateful stream processing** using Apache Spark.
- Integration of **Kafka**, **MongoDB**, and **Spark** for near real-time ETL.
- Practical skills in designing event-driven architectures on cloud platforms.

- ## Tech Stack

* **Language:** Python
* **Stream Processing:** PySpark (Spark Structured Streaming)
* **Message Broker:** Confluent Kafka (Cloud-based SASL/SSL)
* **Data Handling:** Pandas (for stateful mapping)
* **NoSQL Database:** MongoDB Atlas
* **Cloud Storage:** Google Cloud Storage (GCS) for checkpointing

## Architecture

1.  **Producers:** Two Python scripts simulate real-time traffic.
    * `Producer_ojin_stream.py`: Generates orders with occasional duplicates to test idempotency.
    * `producer_payments_join_streams.py`: Generates payment events linked to existing orders.
2.  **Consumer/Processor:** `join_stream.py`
    * Creates a `SparkSession` with Kafka and MongoDB connectors.
    * Unions the streams into a unified processing flow.
    * Uses a custom state function to track `order_id` lifecycle.
3.  **Sink:** The processed, joined records are written to a MongoDB collection.


## Key Logic: Stateful Processing

The core logic resides in the `process_stateful` function. It manages the lifecycle of a transaction:
-   **Order arrives first:** Store order details in the state and set a 15-minute timeout.
-   **Payment arrives:** Retrieve order details from the state, produce a joined record, and clear the state.
-   **Timeout:** If no payment arrives within 15 minutes, the state is automatically removed to prevent memory bloat.

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
