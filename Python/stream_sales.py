from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import multiprocessing
import datetime
import boto3
import shutil

countries = ['US', 'UK', 'DE', 'FR', 'IT']

# Initialize S3 client
s3_client = boto3.client('s3') 

# Define S3 bucket name
bucket_name = 'raw-data-store-de-project-1'


# List to store received records
records = []

def stream(country):

    consumer_config = {
    'bootstrap.servers': '54.82.6.85:9092',
    'group.id': f"{country}_consumer-group",
    'auto.offset.reset': 'earliest'
    }
    
    # Create Kafka consumer
    consumer = Consumer(consumer_config)

    #Subscribe to the topics    
    topic = f"{country}_topic"
    consumer.subscribe([topic])

    record_count = 0
    data_file = f"../Inbound/{country}/{country}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S%f')}.csv"

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Wait for message
            if msg is None:
                print('no msg')
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('partition error')
                    continue
                else: 
                   print(f'Error: {msg.error()}')
                   break
            # Decode and parse JSON message
            record = json.loads(msg.value().decode('utf-8'))
            record_count+=1
            df = pd.DataFrame([record])
            if record_count % 100 == 0:
                s3_client.upload_file(data_file, bucket_name, data_file[3:])  # Upload to S3, removing '../' from the path
                
                # Move the file to a Archived folder
                temp = data_file.split('/')
                archived_file = '/'.join(temp[:-1]) + '/Archived/' + temp[-1]
                shutil.move(data_file, archived_file)

                # Create a new data file for the next batch
                data_file = f"../Inbound/{country}/{country}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S%f')}.csv"

            df.to_csv(data_file, mode='a', index=False, header=False)
            

    except KeyboardInterrupt:
        pass
    finally:
       s3_client.upload_file(data_file, bucket_name, data_file[3:])  # Upload to S3, removing '../' from the path
       # Move the file to a Archived folder
       temp = data_file.split('/')
       archived_file = '/'.join(temp[:-1]) + '/Archived/' + temp[-1]
       shutil.move(data_file, archived_file)

       consumer.close()

""" 
# Convert received records to DataFrame
received_df = pd.DataFrame(records)
print("\nReceived DataFrame:")
print(received_df)
"""

# Create consumers and subscribe to the topics
processes = [None]*len(countries)
for i in range(len(countries)):
    processes[i] = multiprocessing.Process(target=stream, args=(countries[i],))
    processes[i].start()

for process in processes:
    process.join()