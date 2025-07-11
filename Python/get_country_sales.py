import random
import pandas as pd
import time
from Sale_generator import generate_sales
import json
from confluent_kafka import Producer
import datetime


# Callback to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        #Remove the comment of necessary
        #print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        pass

# Convert DataFrame to JSON and send to Kafka
def send_dataframe_to_kafka(df, topic, producer):
    # Convert DataFrame to a list of dictionaries (one per row)
    records = df.to_dict(orient='records')
    
    # Send each record as a JSON string
    for record in records:
        # Serialize record to JSON
        message = json.dumps(record)
        # Produce message to Kafka
        producer.produce(topic, value=message.encode('utf-8'), callback=delivery_report)
    
    # Wait for messages to be delivered
    producer.flush()

def send_sales(country_code, products, product_sale_frequency, overall_sale_frequency, selling_price_range, currencies, delay, max_iterations):

    # Kafka consumer configuration
    kafka_config = {
    'bootstrap.servers': '54.90.90.125:9092'
    }

    # Create Kafka producer
    producer = Producer(kafka_config)

    #create topic name
    topic = f"{country_code}_topic"

    # Simulate a delay in fetching sales data
    for i in range(max_iterations):
        time.sleep(random.uniform(delay[0], delay[1]))  # Simulate network delay
        results = generate_sales(country_code, products, product_sale_frequency, overall_sale_frequency, selling_price_range, currencies)
        print(results)
        #need to work here
        send_dataframe_to_kafka(results, topic, producer)
        with open("../logs/log_1.txt", 'a') as log_file:
            log_file.write(f"{country_code},{len(results)},{datetime.datetime.now()}\n")
        log_file.close()
