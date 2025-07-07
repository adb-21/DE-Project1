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

def upload_to_s3(file_path, bucket_name):
    """
    Uploads a file to the specified S3 bucket.
    """
    try:
        s3_client.upload_file(file_path, bucket_name, file_path[3:])  # Remove '../' from the path
        print(f"File {file_path} uploaded to S3 bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file {file_path} to S3: {e}")

def move_to_archived(file_path):
    """
    Moves a file to the Archived folder.
    """
    try:
        temp = file_path.split('/')
        archived_file = '/'.join(temp[:-1]) + '/Archived/' + temp[-1]
        shutil.move(file_path, archived_file)
        print(f"File {file_path} moved to Archived folder.")
    except Exception as e:
        print(f"Error moving file {file_path} to Archived: {e}")

def stream(country):

    consumer_config = {
    'bootstrap.servers': '75.101.201.118:9092',
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
        start_time = datetime.datetime.now()
        current_time = None
        while True:
            msg = consumer.poll(timeout=1.0)  # Wait for message
            if msg is None:
                #print('no msg')
                current_time = datetime.datetime.now()
                if (current_time - start_time).seconds > 45:  # If no message
                    upload_to_s3(data_file, bucket_name)  # Upload to S3   
                    move_to_archived(data_file)
                    data_file = f"../Inbound/{country}/{country}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S%f')}.csv"
                    start_time = current_time  # Reset start time                
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

                # Upload the data file to S3
                upload_to_s3(data_file, bucket_name)

                # Move the file to a Archived folder
                move_to_archived(data_file)

                # Create a new data file for the next batch
                data_file = f"../Inbound/{country}/{country}_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S%f')}.csv"

            df.to_csv(data_file, mode='a', index=False, header=False)  

    except KeyboardInterrupt:
        pass
    finally:
       upload_to_s3(data_file, bucket_name)
       # Move the file to a Archived folder
       move_to_archived(data_file)

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