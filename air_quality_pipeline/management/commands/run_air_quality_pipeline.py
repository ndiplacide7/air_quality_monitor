from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Manually trigger air quality data pipeline'

    def handle(self, *args, **options):
        self.stdout.write('Starting Air Quality Data Pipeline...')

        try:
            # Direct function call instead of Celery task
            from air_quality_pipeline.tasks import fetch_and_process_air_quality_data

            # Execute the task directly
            result = fetch_and_process_air_quality_data()

            self.stdout.write(
                self.style.SUCCESS(
                    f'Pipeline task completed successfully.'
                )
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Pipeline execution failed: {str(e)}')
            )


# import pandas as pd
# import requests
# import json
# from django.utils import timezone
# from datetime import datetime
# import pytz
#
# from air_quality_monitor.settings import AIR_QUALITY_API_URL, HDFS_BASE_PATH
#
#
# class AirQualityDataPipeline:
#     def __init__(self):
#         # Simplified configuration
#         self.api_url = AIR_QUALITY_API_URL
#         self.hdfs_base_path = HDFS_BASE_PATH
#
#     def fetch_air_quality_data(self):
#         """
#         Fetch air quality data from an API
#         """
#         try:
#             # API call to get data
#             response = requests.get(self.api_url)
#             response.raise_for_status()
#
#             # Parse JSON data
#             data = response.json()
#
#             # Convert to list of dictionaries if needed
#             if not isinstance(data, list):
#                 data = [data]
#
#             return data
#         except Exception as e:
#             print(f"Error fetching air quality data: {e}")
#             return []
#
#     def process_data(self, raw_data):
#         """
#         Process raw air quality data
#         """
#         processed_records = []
#
#         for record in raw_data:
#             # Create a new dictionary to avoid modification issues
#             processed_record = {
#                 'station_id': record.get('station_id', 'Unknown'),
#                 'timestamp': timezone.now(),  # Use Django's timezone.now()
#                 'pollutant': record.get('pollutant', 'Unknown'),
#                 'concentration': float(record.get('concentration', 0.0)),
#                 'units': record.get('units', 'Unknown'),
#                 'source': record.get('source', 'API')
#             }
#             processed_records.append(processed_record)
#
#         return processed_records
#
#     def save_to_hdfs(self, processed_data):
#         """
#         Save processed data to HDFS
#         """
#         try:
#             # Create DataFrame with explicit index
#             df = pd.DataFrame(processed_data)
#
#             # Ensure timestamp is properly handled
#             df['timestamp'] = pd.to_datetime(df['timestamp'])
#
#             # Generate a unique filename
#             filename = f"air_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
#             hdfs_path = f"{self.hdfs_base_path}{filename}"
#
#             # Save to HDFS using pyarrow
#             import pyarrow.parquet as pq
#             import pyarrow as pa
#
#             # Convert to PyArrow table
#             table = pa.Table.from_pandas(df)
#
#             # Write to HDFS
#             pq.write_table(table, hdfs_path)
#
#             print(f"Data saved to HDFS: {hdfs_path}")
#             return hdfs_path
#
#         except Exception as e:
#             print(f"HDFS storage error: {e}")
#             return None
#
#
# def fetch_and_process_air_quality_data():
#     """
#     Main pipeline function
#     """
#     try:
#         # Initialize pipeline
#         pipeline = AirQualityDataPipeline()
#
#         # Fetch raw data
#         raw_data = pipeline.fetch_air_quality_data()
#
#         # Process data
#         processed_data = pipeline.process_data(raw_data)
#
#         # Save to HDFS
#         hdfs_path = pipeline.save_to_hdfs(processed_data)
#
#         # Optionally, save to database
#         if processed_data:
#             from air_quality_pipeline.models import AirQualityRecord
#
#             for record in processed_data:
#                 AirQualityRecord.objects.create(**record)
#
#         return {
#             'raw_records': len(raw_data),
#             'processed_records': len(processed_data),
#             'hdfs_path': hdfs_path
#         }
#
#     except Exception as e:
#         print(f"Pipeline execution failed: {e}")
#         raise