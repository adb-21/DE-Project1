# Product Sales Data Streaming Pipeline

## Overview
This project implements a data engineering pipeline to generate, stream, and store product sales data. The pipeline simulates continuous sales data generation, streams it through Apache Kafka, stores it in CSV files, and loads it into AWS DynamoDB. The entire process is orchestrated using Apache Airflow and runs on an AWS EC2 instance (Ubuntu).

## Architecture
The pipeline consists of the following components:
1. **Sales Data Generation**: A Python script (`Sale_generator.py`) generates synthetic sales data for multiple products across different countries.
2. **Kafka Streaming**: The generated data is published to Apache Kafka topics by country (`publish_sales.py` and `get_country_sales.py`).
3. **Data Storage in CSV**: A consumer script (`stream_sales.py`) subscribes to Kafka topics, processes the streamed data, and saves it to CSV files in an AWS S3 bucket.
4. **Data Loading to DynamoDB**: An Airflow DAG (`load_s3_to_db.py`) reads the CSV files from S3 and loads the data into an AWS DynamoDB table.
5. **Orchestration**: Apache Airflow schedules and manages the pipeline execution.

The pipeline runs on an AWS EC2 instance running Ubuntu, leveraging multiprocessing for parallel processing of data by country.

## Prerequisites
- **AWS Account**: Access to AWS services (S3, DynamoDB, EC2).
- **Apache Kafka**: A running Kafka instance (e.g., hosted on EC2 with broker at `54.90.90.125:9092`).
- **Apache Airflow**: Installed and configured on the EC2 instance.
- **Python**: Version 3.8+ with required libraries (`pandas`, `boto3`, `confluent-kafka`, `airflow`).
- **EC2 Instance**: Ubuntu-based instance with sufficient resources to run Kafka, Airflow, and Python scripts.
- **S3 Bucket**: Configured with the name `raw-data-store-de-project-1`.
- **DynamoDB Table**: A table named `stage.sales` with appropriate schema.

## Project Structure
The project includes the following Python scripts:

- **`Sale_generator.py`**: Generates synthetic sales data for specified products and countries. It creates a DataFrame with columns: `sale_id`, `product_code`, `country_of_sale`, `datetime`, `selling_price`, and `currency`.
- **`publish_sales.py`**: Orchestrates the generation of sales data for multiple countries using multiprocessing. It calls `generate_sales` and sends the data to Kafka topics.
- **`get_country_sales.py`**: Publishes the generated sales data to Kafka topics (one per country) using a Kafka producer.
- **`stream_sales.py`**: Consumes data from Kafka topics, aggregates it into CSV files, and uploads them to an S3 bucket (`raw-data-store-de-project-1`). Files are archived after processing.
- **`load_s3_to_db.py`**: An Airflow DAG that reads CSV files from S3, processes them, and loads the data into a DynamoDB table (`stage.sales`).

## Setup Instructions
1. **Set Up EC2 Instance**:
   - Launch an Ubuntu-based EC2 instance with sufficient resources.
   - Install Python 3.8+, pip, and required libraries:
     ```bash
     sudo apt update
     sudo apt install python3 python3-pip
     pip3 install pandas boto3 confluent-kafka apache-airflow
     ```

2. **Install and Configure Apache Kafka**:
   - Install Kafka on the EC2 instance or use an existing Kafka cluster.
   - Ensure the Kafka broker is accessible at `54.90.90.125:9092`.
   - Create topics for each country (`US_topic`, `UK_topic`, `DE_topic`, `FR_topic`, `IT_topic`).

3. **Configure AWS Credentials**:
   - Set up AWS CLI and configure credentials for S3 and DynamoDB access:
     ```bash
     aws configure
     ```
   - Ensure the S3 bucket `raw-data-store-de-project-1` exists with an `Inbound/` prefix and subfolders for each country (`US/`, `UK/`, etc.).
   - Create a DynamoDB table named `stage.sales` with a schema matching the CSV fields: `sale_id`, `product_code`, `country_of_sale`, `datetime`, `selling_price`, `currency`.

4. **Set Up Apache Airflow**:
   - Initialize Airflow and configure the DAGs folder:
     ```bash
     airflow db init
     ```
   - Copy `load_s3_to_db.py` to the Airflow DAGs folder (e.g., `~/airflow/dags/`).
   - Start the Airflow webserver and scheduler:
     ```bash
     airflow webserver -p 8080 &
     airflow scheduler &
     ```

5. **Directory Structure**:
   - Create directories for CSV storage and archiving:
     ```bash
     mkdir -p ../Inbound/{US,UK,DE,FR,IT}/Archived
     mkdir -p ../logs
     ```

6. **Install Project Dependencies**:
   - Ensure all Python scripts are in the same directory.
   - Install additional dependencies if needed:
     ```bash
     pip3 install python-dateutil
     ```

## Usage
1. **Run the Sales Data Publisher**:
   - Execute `publish_sales.py` to start generating and publishing sales data to Kafka:
     ```bash
     python3 publish_sales.py
     ```
   - This script uses multiprocessing to generate data for each country (`US`, `UK`, `DE`, `FR`, `IT`) and publishes it to respective Kafka topics.

2. **Run the Kafka Consumer**:
   - Execute `stream_sales.py` to consume data from Kafka topics and save it to CSV files in S3:
     ```bash
     python3 stream_sales.py
     ```
   - The script creates CSV files in `../Inbound/<country>/` and uploads them to the S3 bucket (`raw-data-store-de-project-1/Inbound/<country>/`). After uploading, files are moved to an `Archived/` subfolder.

3. **Run the Airflow DAG**:
   - Enable the `load_s3_to_db` DAG in the Airflow UI or trigger it manually:
     ```bash
     airflow dags trigger -d load_s3_to_db
     ```
   - The DAG processes CSV files from S3, loads them into DynamoDB, and moves processed files to a `Processed/` subfolder in S3.

## Configuration Details
- **Countries**: `US`, `UK`, `DE`, `FR`, `IT`.
- **Products**: 10 products (`p1` to `p10`) with predefined sale frequencies and price ranges (see `publish_sales.py`).
- **Kafka**: Broker at `54.90.90.125:9092` with topics named `<country>_topic`.
- **S3**: Bucket `raw-data-store-de-project-1` with prefix `Inbound/<country>/`.
- **DynamoDB**: Table `stage.sales` with fields matching the CSV structure.
- **Airflow**: DAG `load_s3_to_db` runs tasks sequentially for each country.

## Notes
- The pipeline is designed for continuous data generation, but `publish_sales.py` currently uses a `max_iterations` limit (set to 20). Remove this limit for true continuous streaming.
- The `stream_sales.py` script batches records into CSV files and uploads them to S3 every 10,000 records or after a 45-second timeout.
- Ensure sufficient disk space on the EC2 instance for temporary CSV files and logs (`../logs/log_1.txt`).
- The `load_s3_to_db.py` script includes commented-out code for logging processed rows to a CSV file. Uncomment and implement if needed.

## Troubleshooting
- **Kafka Connection Issues**: Verify the Kafka broker address and ensure topics are created.
- **S3 Access Errors**: Check AWS credentials and bucket permissions.
- **DynamoDB Write Failures**: Ensure the table schema matches the CSV data and that the EC2 instance has DynamoDB write permissions.
- **Airflow DAG Failures**: Check the Airflow logs (`~/airflow/logs/`) for errors and verify the DAG file is in the correct folder.

## Future Improvements
- Implement continuous streaming by removing the `max_iterations` limit in `publish_sales.py`.
- Add error handling and retry logic for failed S3 uploads or DynamoDB writes.
- Enhance logging to track failed records in `load_s3_to_db.py`.
- Add monitoring and alerting for pipeline failures using Airflow or AWS CloudWatch.