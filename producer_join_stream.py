import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='pkc-419q3.us-east4.gcp.confluent.cloud:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='H3Z7EI22PVDMBQTG',
    sasl_plain_password='i8hWmySg+tYngeJijzwx7w0sLAdRklTI/77lKz37f2JIPLX+0wP7ojaZJYIeHKWv',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate order data
def generate_order(order_id):
    return {
        "order_id": order_id,
        "order_date": str((datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()),
        "created_at": str(datetime.now().isoformat()),
        "customer_id": f"customer_{random.randint(1, 100)}",
        "amount": random.randint(100, 1000)
    }

# Publish orders with duplicates
try:
    order_id_counter = 1
    for _ in range(20):
        order_id = f"order_{order_id_counter}"
        order = generate_order(order_id)
        producer.send('order_data', value=order)
        print(f"Sent order: {order}")

        # Randomly duplicate the same order
        if random.choice([True, False]):
            producer.send('order_data', value=order)
            print(f"Sent duplicate order: {order}")

        order_id_counter += 1
        time.sleep(6)
finally:
    producer.flush()
    producer.close()