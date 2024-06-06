#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import boto3
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import json


# Load AWS credentials and S3 bucket name from config file
with open('../credentials.json') as config_file:
    aws_credentials = json.load(config_file)

AWS_ACCESS_KEY_ID = aws_credentials['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_credentials['AWS_SECRET_ACCESS_KEY']
AWS_URL_ENDPOINT = aws_credentials['AWS_URL_ENDPOINT']

# Initialize S3 client
s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    endpoint_url=AWS_URL_ENDPOINT)

bucket = s3.Bucket('test-upload') # need resource
prefix = ""

def delete_file_from_bucket(obj):
    # print(obj)
    obj.delete()
    return True

# use loop and count increment
count_objs = 0

for i in bucket.objects.filter(Prefix=""):
    count_objs = count_objs + 1

print(f"Total number of files {count_objs}\n")

# Create a ThreadPoolExecutor
with tqdm(desc='Deleting files', ncols=60, total=count_objs, unit='B', unit_scale=1) as pbar:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(delete_file_from_bucket, obj) for obj in bucket.objects.filter(Prefix=prefix)]
        for future in as_completed(futures):
            pbar.update(1)
