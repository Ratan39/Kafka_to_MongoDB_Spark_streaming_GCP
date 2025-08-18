import pandas as pd
from spark.sql import sparkSession
from spark.sql.functions import from_json, col, lit
from spark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType, stateStructType
from pyspark.sql.streaming.state import GroupStateTimeout
from typing import Iterator, Tuple
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format=("%(asctime)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("StatefulOrdersPaymentsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3, org.mangodb.spark:mango-spark-connector_2.12:10.3.0") \
    .config("spark.sql.shuffle.paertitions", "2") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

order_schema = StructType([
    StructField("order_id", StringType, True),
    StructField("order_date", StringType, True),
    StructField("created_at", StringType, True),
    StructField("customer_id", StringType, True),
    StructField("amount", IntegerType, True)
])

payments_schema = StructType([
    StructField = ("payment_id", StringType, True),
    StructField = ("order_id", StringType, True),
    StructField = ("payment_date", StringType, True),
    StructField = ("created_at", StringType, True),
    StructField = ("amount", IntegerType, True)
])

orders_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-419q3.us-east4.gcp.confluent.cloud:9092") \
    .option("subscribe", "order_data") \
    .option("startingOffset", "latest") \
    .option("kafka.secuity.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='H3Z7EI22PVDMBQTG' password='i8hWmySg+tYngeJijzwx7w0sLAdRklTI/77lKz37f2JIPLX+0wP7ojaZJYIeHKWv';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), order_schema.alias("data"))) \
    .select("data.*") \
    .withColumn("type", lit("payment"))

logger.info("Order Read Stream Started")

payment_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-419q3.us-east4.gcp.confluent.cloud:9092") \
    .option("subscribe", "payment_data") \
    .option("startingOffset", "latest") \
    .option("kafka.secuity.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='H3Z7EI22PVDMBQTG' password='i8hWmySg+tYngeJijzwx7w0sLAdRklTI/77lKz37f2JIPLX+0wP7ojaZJYIeHKWv';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), order_schema.alias("data"))) \
    .select("data.*") \
    .withColumn("type", lit("payment"))

logger.info("Payment Read Stream Started")

combined_stream = orders_stream.unionByName(payment_stream, allowMissingColumns = True)

def process_stateful(
        key: Tuple[str], pdfs: Iterator[pd.DataFrame], state
) -> Iterator[pd.dataFrame]:
    (order_id,) = key
    logger.info(f"Current key for process {key}")

    if state.exists:
        (order_date, created_at, customer_id, order_amount) = state.get
    else: 
        order_date, created_at, customer_id, order_amount = None, None, None, None

    output_data = []

    logger.info(f"Iterable dataframes for process {pdfs}")

    for pdf in pdfs:
        for _, row in pdf.iterrows():
            if row["type"] == "order":
                order_date = row["order_date"]
                created_at = row["created_at"]
                customer_id = row["customer_id"]
                order_amount = row["order_amount"]
                order_tup = (order_date, created_at, customer_id, order_amount)
                logger.info(f"Processing order event {order_tup}")
                state.update((order_date, created_at, customer_id, order_amount))
                state.setTimeoutDuration(15 * 60 * 1000)
            
            elif row["type"] == "payment" and order_date:
                payment_date = row["payment_date"]
                payment_amount = row["payment_amount"]
                payment_id = row["payment_id"]
                payment_tup = (payment_date, payment_amount, payment_id)
                logger.info(f"Processing payment event {payment_tup}")

                output_data.append({
                    "order_id": order_id,
                    "order_date": order_date,
                    "created_at": created_at,
                    "customer_id": customer_id,
                    "order_amount": order_amount,
                    "payment_id": payment_id,
                    "payment_date": payment_date,
                    "payment_amount": payment_amount
                    })
                
                state.remove()

    if state.hasTimedOut:
        print(f"state for order_id {order_id} has timed out.")
        state.remove()

     

stateful_query = combined_stream.groupBy("order_id".applyInPandasWithState(
    func=process_stateful),
    outputStructType="""
    order_id STRING,
    order_date STRING,
    created_at STRING,
    customer_id STRING,
    order_amount INT,
    payment_id STRING,
    payment_date STRING,
    payment_amount INT
    """,
    stateStructType="""
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        amount INT
    """,
    outputMode="update",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)

logger.info("state update query has started...")

checkpoint_dir = "gs://streaming-checkpointing3/stateful_joinstream"

query = stateful_query.writeStream \
    .outputMode("update") \
    .format("mondodb") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://<ratansaiuser>:<ratan12345>@mongo-db-cluster.k6tj5.mongodb.net/") \
    .option('spark.mongodb.database', "ecomm") \
    .option("spark.mongodb.collection", "order_data") \
    .option("truncate", "false") \
    .option("checkpoint_dir", checkpoint_dir) \
    .start()

logger.info("Mongo write stream started")

query.awaitTermination()
    