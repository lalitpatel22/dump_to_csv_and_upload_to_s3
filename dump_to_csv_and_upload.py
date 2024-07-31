import mysql.connector
import pandas as pd
import boto3
from sqlalchemy import create_engine
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

s3_bucket = os.getenv('S3_BUCKET')
aws_region = os.getenv('AWS_REGION')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

def get_all_tables():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        connection.close()
        return tables
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []

def fetch_data(table_name):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
        query = f"SELECT * FROM `{table_name}`"
        df = pd.read_sql(query, engine)
        return df
    except Exception as err:
        print(f"Error: {err}")
        return None

def write_csv(dataframe, file_name):
    dataframe.to_csv(file_name, index=False)

def upload_to_s3(file_name):
    s3 = boto3.client('s3', 
                      region_name=aws_region,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    try:
        s3.upload_file(file_name, s3_bucket, file_name)
        print(f"File {file_name} uploaded to S3 bucket {s3_bucket}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    
def delete_file(file_name):
    try:
        os.remove(file_name)
        print(f"File {file_name} deleted from local filesystem")
    except FileNotFoundError:
        print(f"The file {file_name} was not found in local filesystem")
    except Exception as e:
        print(f"Error deleting file {file_name}: {e}")
            
def main():
    tables = get_all_tables()
    if not tables:
        print("No tables found in the database")
        return
    for table in tables:
        print(f"Fetching data from table {table}...")
        data = fetch_data(table)
        if data is not None:
            csv_file_name = f"{table}.csv"
            print(f"Writing data from table {table} to CSV...")
            write_csv(data, csv_file_name)
            print(f"Uploading {csv_file_name} to S3...")
            if upload_to_s3(csv_file_name):
                print("Deleting CSV file from local filesystem...")
                delete_file(csv_file_name)
            else:
                print("Failed to upload CSV file to S3")
        else:
            print("Failed to fetch data from table")

if __name__ == "__main__":
    main()
    print("Operation completed successfully")
