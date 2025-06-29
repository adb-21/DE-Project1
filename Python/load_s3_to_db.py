import boto3
import csv
import io
from botocore.exceptions import ClientError
from decimal import Decimal
import json

def convert_to_dynamodb_type(value):
    """Convert Python types to DynamoDB compatible types"""
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    elif value in ['true', 'false']:
        return value.lower() == 'true'
    elif value == '':
        return None
    return value

def read_csv_from_s3(bucket_name, file_key):
    """Read CSV file from S3 bucket"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        return csv.reader(io.StringIO(csv_content))
    except ClientError as e:
        print(f"Error reading from S3: {e}")
        return None

def write_to_dynamodb(table_name, rows, field_names):
    """Write items to DynamoDB table"""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    for row in rows:
        # Map row values to field names
        if len(row) != len(field_names):
            print(f"Skipping row with mismatched columns: {row}")
            continue
        item = {field_names[i]: convert_to_dynamodb_type(value) for i, value in enumerate(row)}
        
        try:
            table.put_item(Item=item)
            print(f"Successfully inserted item: {json.dumps(item, default=str)}")
        except ClientError as e:
            print(f"Error writing to DynamoDB: {e}")

def process_s3_csv_to_dynamodb(bucket_name, prefix, table_name, field_names, country):
    """Process all CSV files in S3 bucket and store in DynamoDB"""
    s3_client = boto3.client('s3')
    
    try:
        # List objects in the S3 bucket with the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix + country + '/')

        if 'Contents' not in response:
            print(f"No files found in bucket {bucket_name} with prefix {prefix + country + '/'}")
            return
        
        # Process each CSV file
        for obj in response['Contents']:
            file_key = obj['Key']
            print( f"Found file: {file_key}")
            if file_key.endswith('.csv') and file_key.startswith(country):
                print(f"Processing file: {file_key}")
                csv_reader = read_csv_from_s3(bucket_name, file_key)
                
                if csv_reader:
                    write_to_dynamodb(table_name, csv_reader, field_names)
                
    except ClientError as e:
        print(f"Error listing S3 objects: {e}")

if __name__ == "__main__":
    # List of countries to process
    countries = ['US', 'UK', 'DE', 'FR', 'IT']

    # Configuration
    S3_BUCKET_NAME = 'raw-data-store-de-project-1'
    S3_PREFIX = 'Inbound/'  
    DYNAMODB_TABLE_NAME = 'stage.sales' 
    
    # Define field names for your CSV columns
    FIELD_NAMES = ['sale_id', 'product_code', 'country_of_sale', 'datetime', 'selling_price', 'currency']

    # Process CSV files
    for country in countries:
        process_s3_csv_to_dynamodb(S3_BUCKET_NAME, S3_PREFIX, DYNAMODB_TABLE_NAME, FIELD_NAMES, country)

