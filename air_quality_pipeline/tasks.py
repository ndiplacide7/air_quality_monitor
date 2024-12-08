import os
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import requests
import json
from django.utils import timezone
from datetime import datetime
import pytz
from hdfs import InsecureClient

from air_quality_monitor.settings import AIR_QUALITY_API_URL, NAMENODE_CONTAINER_NAME, HDFS_BASE_PATH


class AirQualityDataPipeline:
    def __init__(self):

        self.api_url = AIR_QUALITY_API_URL
        self.hdfs_base_path = '/air-quality-data/'
        self.hdfs_url = os.getenv('HDFS_URL', 'http://localhost:9870')
        self.hdfs_user = os.getenv('HDFS_USER', 'root')
        self.hdfs_base_path = os.getenv('HDFS_BASE_PATH', '/air-quality-data/')
        # Create HDFS client
        self.hdfs_client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
        self.namenode_container_name = NAMENODE_CONTAINER_NAME

    def fetch_air_quality_data(self):
        """
        Fetch air quality data from an API
        """
        try:
            # API call
            response = requests.get(self.api_url)
            response.raise_for_status()

            # Parse JSON data
            data = response.json()

            df = pd.DataFrame(data)
            print(df.columns)

            # Convert to list of dictionaries if needed
            if not isinstance(data, list):
                data = [data]

            return data
        except Exception as e:
            print(f"Error fetching air quality data: {e}")
            return []

    def process_data(self, raw_data):
        """
        Process raw air quality data
        """
        processed_records = []

        for record in raw_data:
            # Create a new dictionary to avoid modification issues
            processed_record = {
                'station_id': record.get('name', 'Unknown'),
                'pm25': record.get('pm2_5', 0.0),
                'pm10': record.get('pm10', 0.0),
                'ozone': record.get('o3_4hr', 0.0),
                'nitrogen_dioxide': record.get('no2', 0.0),
                'carbon_monoxide': record.get('co', 0.0),
                'timestamp': timezone.now(),  # Use Django's timezone.now()
                'pollutant': record.get('aqi_site', 'Unknown'),
                'concentration': float(record.get('concentration', 0.0)),
                'units': record.get('units', 'Unknown'),
                'source': record.get('source', 'ACT API')
            }
            processed_records.append(processed_record)

        return processed_records

    def save_to_hdfs(self, processed_data):
        """
        Save processed data to HDFS using docker cp.
        """
        try:
            # Create DataFrame with explicit index
            df = pd.DataFrame(processed_data)

            # Ensure timestamp is properly handled
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['ozone'] = pd.to_numeric(df['ozone'], errors='coerce')
            df['nitrogen_dioxide'] = pd.to_numeric(df['nitrogen_dioxide'], errors='coerce')
            df['carbon_monoxide'] = pd.to_numeric(df['carbon_monoxide'], errors='coerce')

            # Generate a unique filename
            filename = f"air_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            print(f"Filename.......: {filename}")
            hdfs_path = os.path.join(self.hdfs_base_path, filename)
            print(f"HDFS path......: {hdfs_path}")

            # Convert to PyArrow table
            table = pa.Table.from_pandas(df)

            # Use a temporary local file to write first
            local_temp_file = f"/tmp/{filename}"
            print(f"Data size.......: {table.num_rows} rows")

            # Write to local temporary file
            pq.write_table(table, local_temp_file)

            # Copy the file to the namenode container
            container_temp_path = f"/{filename}"

            # Create the directory inside the namenode container (if not exists)
            subprocess.run(
                ["docker", "exec", self.namenode_container_name, "mkdir", "-p", HDFS_BASE_PATH],
                check=True
            )

            print(f"Directory created in container: {HDFS_BASE_PATH}")

            # Copy the file to the container's air-quality-data directory
            container_temp_path = os.path.join(HDFS_BASE_PATH, filename)

            subprocess.run(
                ["docker", "cp", local_temp_file, f"{self.namenode_container_name}:{container_temp_path}"],
                check=True
            )
            print(f"File copied to container: {self.namenode_container_name}:{container_temp_path}")

            # # Use HDFS client to move the file to the correct HDFS path
            # self.hdfs_client.write(hdfs_path, container_temp_path)

            # Optional: Remove local temporary file
            os.remove(local_temp_file)
            print(f"Local temp file removed: {local_temp_file}")

            print(f"Data saved to HDFS: {hdfs_path}")
            return hdfs_path

        except subprocess.CalledProcessError as e:
            print(f"Error during docker cp: {e}")
            return None
        except Exception as e:
            print(f"HDFS storage error: {e}")
            return None


def fetch_and_process_air_quality_data():
    """
    Main pipeline function
    """
    try:
        # Initialize pipeline
        pipeline = AirQualityDataPipeline()

        # Fetch raw data
        raw_data = pipeline.fetch_air_quality_data()

        # Process data
        processed_data = pipeline.process_data(raw_data)

        # Save to HDFS
        hdfs_path = pipeline.save_to_hdfs(processed_data)

        # Optionally, save to database
        if processed_data:
            from air_quality_pipeline.models import AirQualityRecord

            for record in processed_data:
                AirQualityRecord.objects.create(**record)

        return {
            'raw_records': len(raw_data),
            'processed_records': len(processed_data),
            'hdfs_path': hdfs_path
        }

    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        raise
