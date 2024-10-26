# %%
import os
import subprocess
import requests
import pandas as pd
from hdfs import InsecureClient
from pyhive import hive
from dotenv import load_dotenv

# %%
# load environment variables
load_dotenv()

# %%
# HDFS Client setup
hdfs_client = InsecureClient(os.getenv('HDFS_URL'), user=os.getenv('HDFS_USER'))
url = os.getenv('DATA_URL')
HDFS_PATH = os.getenv('HDFS_PATH')
HIVE_TABLE_NAME = os.getenv('HIVE_TABLE_NAME')
HIVE_DATABASE=os.getenv('HIVE_DATABASE')

# %%
# Hive Connection setup
hive_conn = hive.Connection(
    host=os.getenv('HIVE_HOST'),
    port=int(os.getenv('HIVE_PORT')),
    username=os.getenv('HIVE_USER')
)

# %%
def create_hive_database(database_name):
    # Create a cursor object
    cursor = hive_conn.cursor()

    # SQL command to create a database
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    try:
        # Execute the query
        cursor.execute(create_db_query)
        print(f"Database '{database_name}' created successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")

# %%
# Function to download data from the URL and upload directly to HDFS
def download_and_upload_to_hdfs(url, hdfs_path):
    try:
        response = requests.get(url, stream=True)  # Use stream to download in chunks
        response.raise_for_status()

        # Write the data directly to HDFS
        with hdfs_client.write(hdfs_path, overwrite=True) as hdfs_file:
            for chunk in response.iter_content(chunk_size=8192):
                hdfs_file.write(chunk)

        print("Data downloaded and uploaded to HDFS successfully.")
    except requests.RequestException as e:
        print(f"Error downloading data: {e}")
    except Exception as e:
        print(f"Error uploading to HDFS: {e}")

# %%
# Function to create a Hive table
def create_hive_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {HIVE_DATABASE}.{HIVE_TABLE_NAME} (
        SUMLEV STRING,
        REGION INT,
        DIVISION INT,
        STATE STRING,
        COUNTY STRING,
        STNAME STRING,
        CTYNAME STRING,
        ESTIMATESBASE2020 INT,
        POPESTIMATE2020 INT,
        POPESTIMATE2021 INT,
        POPESTIMATE2022 INT,
        POPESTIMATE2023 INT,
        NPOPCHG2020 INT,
        NPOPCHG2021 INT,
        NPOPCHG2022 INT,
        NPOPCHG2023 INT,
        BIRTHS2020 INT,
        BIRTHS2021 INT,
        BIRTHS2022 INT,
        BIRTHS2023 INT,
        DEATHS2020 INT,
        DEATHS2021 INT,
        DEATHS2022 INT,
        DEATHS2023 INT,
        NATURALCHG2020 INT,
        NATURALCHG2021 INT,
        NATURALCHG2022 INT,
        NATURALCHG2023 INT,
        INTERNATIONALMIG2020 INT,
        INTERNATIONALMIG2021 INT,
        INTERNATIONALMIG2022 INT,
        INTERNATIONALMIG2023 INT,
        DOMESTICMIG2020 INT,
        DOMESTICMIG2021 INT,
        DOMESTICMIG2022 INT,
        DOMESTICMIG2023 INT,
        NETMIG2020 INT,
        NETMIG2021 INT,
        NETMIG2022 INT,
        NETMIG2023 INT,
        RESIDUAL2020 INT,
        RESIDUAL2021 INT,
        RESIDUAL2022 INT,
        RESIDUAL2023 INT,
        GQESTIMATESBASE2020 INT,
        GQESTIMATES2020 INT,
        GQESTIMATES2021 INT,
        GQESTIMATES2022 INT,
        GQESTIMATES2023 INT,
        RBIRTH2021 DOUBLE,
        RBIRTH2022 DOUBLE,
        RBIRTH2023 DOUBLE,
        RDEATH2021 DOUBLE,
        RDEATH2022 DOUBLE,
        RDEATH2023 DOUBLE,
        RNATURALCHG2021 DOUBLE,
        RNATURALCHG2022 DOUBLE,
        RNATURALCHG2023 DOUBLE,
        RINTERNATIONALMIG2021 DOUBLE,
        RINTERNATIONALMIG2022 DOUBLE,
        RINTERNATIONALMIG2023 DOUBLE,
        RDOMESTICMIG2021 DOUBLE,
        RDOMESTICMIG2022 DOUBLE,
        RDOMESTICMIG2023 DOUBLE,
        RNETMIG2021 DOUBLE,
        RNETMIG2022 DOUBLE,
        RNETMIG2023 DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    """
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(create_table_query)
        print(f"Hive table '{HIVE_DATABASE}.{HIVE_TABLE_NAME}' created successfully.")
    except Exception as e:
        print(f"Error creating Hive table: {e}")


# %%
# Function to load data into Hive table
def load_data_into_hive():
    load_data_query = f"LOAD DATA INPATH '{HDFS_PATH}' INTO TABLE {HIVE_DATABASE}.{HIVE_TABLE_NAME}"
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(load_data_query)
        print("Data loaded into Hive table successfully.")
    except Exception as e:
        print(f"Error loading data into Hive: {e}")


# %%
# Function to verify data integrity in Hive table
def verify_data_in_hive():

    verify_query = f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE_NAME} LIMIT 10"

    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(verify_query)
            result = cursor.fetchall()
            print("Data in Hive table:")
            for row in result:
                print(row)
    except Exception as e:
        print(f"Error verifying data in Hive: {e}")
    finally:
        # Close the connection
        hive_conn.close()

# %%
# Main execution flow
if __name__ == "__main__":
    download_and_upload_to_hdfs(url, HDFS_PATH)
    create_hive_database(HIVE_DATABASE)
    create_hive_table()
    load_data_into_hive()
    verify_data_in_hive()


