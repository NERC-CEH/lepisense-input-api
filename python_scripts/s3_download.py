#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import boto3
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
import json


# Load AWS credentials and S3 bucket name from config file
with open('../credentials.json') as config_file:
    aws_credentials = json.load(config_file)

AWS_ACCESS_KEY_ID = aws_credentials['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_credentials['AWS_SECRET_ACCESS_KEY']
AWS_URL_ENDPOINT = aws_credentials['AWS_URL_ENDPOINT']

# Initialize S3 resource
s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    endpoint_url=AWS_URL_ENDPOINT)


def download_object(key):
    path, filename = os.path.split(key)
    os.makedirs(os.path.join(LOCAL_DOWNLOAD_PATH, path), exist_ok=True)
    download_path = Path(LOCAL_DOWNLOAD_PATH) / path / filename
    print(f"Downloading {key} to {download_path}")
    BUCKET_NAME.download_file(key, str(download_path))
    return "Success"


def download_parallel_multithreading(prefix):
    # Dispatch work tasks
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_key = {
            executor.submit(download_object, object.key): object.key for object in BUCKET_NAME.objects.filter(Prefix=prefix)
        }

        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield key, future.result()
            else:
                yield key, exception


if __name__ == "__main__":
    now = datetime.now()
    start_time = now.strftime("%H:%M:%S")

    # Select bucket
    BUCKET_NAME = s3.Bucket('test-upload')
    PREFIX = ""
    LOCAL_DOWNLOAD_PATH = r"./test_download"

    for key, result in download_parallel_multithreading(PREFIX):
        print(f"{key} result: {result}")

    now = datetime.now()
    end_time = now.strftime("%H:%M:%S")

    print("Start Time =", start_time)
    print("End Time =", end_time)
