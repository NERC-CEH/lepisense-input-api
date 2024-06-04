#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import boto3
import os
import json


# Load AWS credentials and S3 bucket name from config file
with open('../credentials.json') as config_file:
    aws_credentials = json.load(config_file)

AWS_ACCESS_KEY_ID = aws_credentials['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_credentials['AWS_SECRET_ACCESS_KEY']
AWS_URL_ENDPOINT = aws_credentials['AWS_URL_ENDPOINT']

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=AWS_URL_ENDPOINT
)

# Create a paginator helper for list_objects_v2
paginator = s3_client.get_paginator('list_objects_v2')

operation_parameters = {'Bucket': 'test-upload', 'Prefix': ''}

page_iterator = paginator.paginate(**operation_parameters)

# Keep a running total
count = 0
motion_filenames = []
snapshot_filenames = []

# Work through the response pages, add to the running total
for page in page_iterator:
    print(page.keys())
    print(page['Contents'][-1]['Key'])
    for obj in page['Contents']:
        print(obj['Key'])
        filename = obj['Key']
        if "motion" in filename: 
            motion_filenames.append(os.path.basename(filename))
        elif "snapshot" in filename:
            snapshot_filenames.append(os.path.basename(filename))
    count += page['KeyCount']
    print(count)
