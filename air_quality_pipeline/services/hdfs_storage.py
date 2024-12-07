import os
import time
import pandas as pd
from hdfs import InsecureClient
from django.conf import settings
from typing import List, Dict


class HDFSStorage:
    def __init__(self):
        self.hdfs_client = InsecureClient(
            settings.HDFS_URL,
            user=settings.HDFS_USER
        )

    def store_records(self, records: List[Dict]):
        """
        Store records in HDFS as Parquet
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)

            # Generate unique filename
            filename = f"air_quality_{int(time.time())}.parquet"
            hdfs_path = f"/air_quality_data/{filename}"

            # Temporary local file
            local_temp_file = f"/tmp/{filename}"

            # Save as Parquet
            df.to_parquet(local_temp_file, index=False)

            # Upload to HDFS
            with open(local_temp_file, 'rb') as f:
                self.hdfs_client.write(hdfs_path, f, overwrite=True)

            print(f"Uploaded {filename} to HDFS")

            # Remove local temp file
            os.remove(local_temp_file)

            return hdfs_path

        except Exception as e:
            print(f"HDFS storage error: {e}")
            return None