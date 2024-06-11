#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from datetime import datetime
import csv
import json
import boto3
import boto3.s3.transfer as s3transfer
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import PartialCredentialsError


# Load AWS credentials and S3 bucket name from config file
with open('credentials.json') as config_file:
    aws_credentials = json.load(config_file)

AWS_ACCESS_KEY_ID = aws_credentials['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = aws_credentials['AWS_SECRET_ACCESS_KEY']
AWS_REGION = aws_credentials['AWS_REGION']
AWS_URL_ENDPOINT = aws_credentials['AWS_URL_ENDPOINT']

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=AWS_URL_ENDPOINT
)

transfer_config = s3transfer.TransferConfig(
    multipart_threshold=1024 * 25,  # 25MB threshold for multipart uploads
    max_concurrency=20,             # Increase the number of concurrent threads
    multipart_chunksize=1024 * 25,  # 25MB chunk size
    use_threads=True
)


def load_deployments_info():
    deployments = []
    with open('deployments_info.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployments.append(row)
    return deployments


def divide_data_into_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def fast_upload(s3_bucket_name, files, key, uploaded_files):
    s3t = s3transfer.create_transfer_manager(s3_client, transfer_config)
    try:
        for file in files:
            try:
                dst = key + file.filename
                s3t.upload(file.file, s3_bucket_name, dst)
                uploaded_files.append(file.filename)
            except Exception as e:
                return JSONResponse(status_code=400, content={"message": f"Error uploading {key}/{file.filename}: {e}"})
    finally:
        s3t.shutdown()  # wait for all the upload tasks to finish
    return uploaded_files


def download_logs(s3_bucket_name, log_key):
    try:
        file = s3_client.get_object(Bucket=s3_bucket_name, Key=log_key)
        json_data = file['Body'].read().decode('utf-8')
        return json_data
    except NoCredentialsError:
        raise HTTPException(status_code=401, detail="Credentials not available")
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="JSON not found")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Encoding issue occurred while reading the file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


def write_logs(uploaded_files, s3_bucket_name, host, name, deployment, data_type):
    # Create a temporary log file
    log_key = "logs/upload_summary.log"
    json_data = download_logs(s3_bucket_name, log_key)

    # Append new log entries to the temporary log file following CLF format
    ident = "-"
    authuser = "-"
    for file_name in uploaded_files:
        date_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
        request_line = f'POST /upload/ HTTP/1.1'
        status = 200
        bytes_sent = '-'  # You can replace this with the actual byte size of the response if needed
        log_entry = (f'{host} {ident} {authuser} [{date_str}] "{request_line}" {status} {bytes_sent} Name="{name}" '
                     f'Country="{s3_bucket_name}" Deployment="{deployment}" DataType="{data_type}" FileName="{file_name}"\n')
        json_data.update(log_entry)

    # Upload the log file to S3
    try:
        s3_client.upload_fileobj(json_data, s3_bucket_name, log_key)
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def s3_list_objects(s3_bucket_name, prefix):
    # Create a paginator helper for list_objects_v2
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': s3_bucket_name, 'Prefix': prefix}
    files = []
    page_iterator = paginator.paginate(**operation_parameters)
    # Work through the response pages, add to the running total
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files


def s3_create_bucket(bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        return JSONResponse(status_code=200, content={"message": f"Bucket '{bucket_name}' created successfully"})
    except s3_client.exceptions.BucketAlreadyExists:
        return JSONResponse(status_code=400, content={"message": "Bucket already exists"})
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        return JSONResponse(status_code=400, content={"message": "Bucket already owned by you"})