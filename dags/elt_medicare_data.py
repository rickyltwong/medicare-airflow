from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
import requests
import json
import pendulum
import logging
import boto3

@dag(
    description="Incremental Medicare data pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 2, 14, tz="UTC"),
)
def medicare_data_pipeline():

    aws_conn = BaseHook.get_connection('s3_default')
    s3_hook = S3Hook(aws_conn_id='s3_default')

    @task(retries=3)
    def get_offset_count():
        try:
            files = s3_hook.list_keys(
                bucket_name='pharmaceutical-data-dashboard',
                prefix='medicare/spending_data/spending_data_'
            )

            if not files:
                return 0

            max_offset = 0
            for file in files:
                try:
                    parts = file.split('/')[-1].split('_')
                    end_offset = int(parts[3].split('.')[0])
                    max_offset = max(max_offset, end_offset)
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipping malformed filename {file}: {str(e)}")
                    continue

            logging.info(f"Latest offset count from files: {max_offset}")
            return max_offset

        except Exception as e:
            raise Exception(f"Error accessing S3: {str(e)}")
    
    @task
    def get_processed_files():
        """
        Get the list of already processed files from the manifest
        """
        import json
        from airflow.hooks.base import BaseHook
        import boto3
        import logging
        
        s3_conn = BaseHook.get_connection('s3_default')
        bucket = "pharmaceutical-data-dashboard"
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            endpoint_url='https://tor1.digitaloceanspaces.com'  # Adjust as needed
        )
        
        try:
            # Try to get the manifest file
            response = s3_client.get_object(
                Bucket=bucket,
                Key="medicare/manifest/processed_files.json"
            )
            
            # Parse the manifest
            manifest_data = json.loads(response['Body'].read().decode('utf-8'))
            processed_files = manifest_data.get("processed_files", [])
            logging.info(f"Found {len(processed_files)} previously processed files")
            
            return processed_files
        except Exception as e:
            logging.warning(f"Failed to read manifest file, assuming no files processed: {e}")
            return []

    @task
    def update_manifest(processed_files, new_files):
        """
        Update the manifest with newly processed files
        """
        import json
        from airflow.hooks.base import BaseHook
        import boto3
        import logging
        
        s3_conn = BaseHook.get_connection('s3_default')
        bucket = "pharmaceutical-data-dashboard"
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            endpoint_url='https://tor1.digitaloceanspaces.com'  # Adjust as needed
        )
        
        # Combine existing and new files
        all_processed_files = list(set(processed_files + new_files))
        
        try:
            # Create manifest data
            manifest_data = json.dumps({"processed_files": all_processed_files})
            
            # Write to S3
            s3_client.put_object(
                Bucket=bucket,
                Key="medicare/manifest/processed_files.json",
                Body=manifest_data
            )
            
            logging.info(f"Successfully updated manifest with {len(all_processed_files)} processed files")
            return True
        except Exception as e:
            logging.error(f"Failed to update manifest: {e}")
            raise

    @task
    def extract_spending_data(offset: int):
        params = {  
            "offset": offset,
            "size": 1000
        }

        # Extract spending data from API
        api_url = "https://data.cms.gov/data-api/v1/dataset/7e0b4365-fd63-4a29-8f5e-e0ac9f66a81b/data"
        response = requests.get(api_url, params=params)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")
        
        data = response.json()

        # If no data, log it and return the current offset (no change)
        if len(data) == 0:
            logging.info(f"No more data available beyond offset {offset}")
            return offset
        
        # Save data to S3
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=f'medicare/spending_data/spending_data_{offset}_{offset + len(data)}_{pendulum.now().strftime("%Y-%m-%d")}.json',
            bucket_name='pharmaceutical-data-dashboard',
            replace=False
        )

        # Return the new offset
        return offset + len(data)

    # Get the list of processed files
    processed_files = get_processed_files()
    
    # Convert processed files to JSON string for passing to Spark
    @task
    def prepare_spark_args(processed_files):
        import json
        return json.dumps(processed_files)
    
    spark_args = prepare_spark_args(processed_files)
    
    # Modify the SparkSubmitOperator to accept the processed files as an argument
    unpivot_medicare_spending_data = SparkSubmitOperator(
            task_id='unpivot_medicare_spending_data',
            application='dags/scripts/unpivot_medicare_spending_data.py',
            conn_id='spark_default',
            packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261',
            application_args=["{{ ti.xcom_pull(task_ids='prepare_spark_args') }}"],
            conf={
            "spark.driver.bindAddress": "0.0.0.0",
            # "spark.driver.host": "172.20.0.6", # airflow's worker pod IP
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.endpoint': 'tor1.digitaloceanspaces.com',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.access.key': '{{ conn.s3_default.login }}',
            'spark.hadoop.fs.s3a.secret.key': '{{ conn.s3_default.password }}',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.memory.fraction': '0.7',
            'spark.memory.storageFraction': '0.3',
            'spark.shuffle.file.buffer': '1mb',
            'spark.shuffle.spill.compress': 'true',
            'spark.shuffle.compress': 'true',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s'
        }
    )
    
    # Task to capture the output of the Spark job (new files processed)
    @task
    def process_spark_output(ti=None):
        """
        Process the output of the Spark job to get the list of newly processed files
        """
        import json
        
        # Get the Spark task output
        spark_output = ti.xcom_pull(task_ids='unpivot_medicare_spending_data')
        
        # Parse the JSON output (list of new files)
        try:
            new_files = json.loads(spark_output)
            return new_files
        except Exception as e:
            # If parsing fails, return empty list
            return []
    
    # Get the new files processed by Spark
    new_files = process_spark_output()
    
    # Update the manifest with the new files
    update_manifest_task = update_manifest(processed_files, new_files)
    
    # Define the task dependencies
    extract_spending_data(get_offset_count()) >> unpivot_medicare_spending_data >> update_manifest_task

medicare_data_pipeline()