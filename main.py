from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Request, Body, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from typing import List
import json
import os
import csv
from tempfile import NamedTemporaryFile
from pydantic import BaseModel
from datetime import datetime


app = FastAPI()

# Set up CORS middleware
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "https://connect-apps.ceh.ac.uk/ami-data-upload/"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
# Mount the static directory to serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


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


def load_deployments_info():
    deployments = []
    with open('deployments_info.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            deployments.append(row)
    return deployments


deployments_info = load_deployments_info()
valid_countries_location_names = {f"{d['country']} - {d['location_name']}" for d in deployments_info
                                  if d['status'] == 'active'}
valid_data_types = {"motion_images", "snapshot_images", "audible_recordings", "ultrasound_recordings"}


class UploadResponse(BaseModel):
    uploaded_files: List[str]


@app.get("/", response_class=HTMLResponse)
async def main():
    with open("templates/upload.html") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/get-deployments/")
async def get_deployments():
    return JSONResponse(content=deployments_info)


@app.get("/list-data/")
async def list_data(
        country_location_name: str = Query("", enum=sorted(list(valid_countries_location_names)), description="Country and location names."),
        data_type: str = Query("", enum=list(valid_data_types), description="")
):
    country, location_name = country_location_name.split(" - ")
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country][0]
    s3_bucket_name = country_code.lower()
    deployment_id = [d['deployment_id'] for d in deployments_info if d['country'] == country
                    and d['location_name'] == location_name][0]
    prefix = f"{deployment_id}/{data_type}/"
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return JSONResponse(status_code=404, content={"message": "No data found"})
        files = [content['Key'] for content in response['Contents']]
        return JSONResponse(status_code=200, content={"files": files})
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/get-logs/")
async def get_logs(
        country_location_name: str = Query("", enum=sorted(list(valid_countries_location_names)), description="Country and location names."),
):
    country, location_name = country_location_name.split(" - ")
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country][0]
    s3_bucket_name = country_code.lower()
    try:
        file = s3_client.get_object(Bucket=s3_bucket_name, Key="logs/upload_summary.json")
        json_data = file['Body'].read().decode('utf-8')
        return Response(content=json_data, media_type="application/json")
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="JSON not found")
    except NoCredentialsError:
        raise HTTPException(status_code=401, detail="Credentials not available")


@app.post("/upload/")
async def upload_file(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        files: List[UploadFile] = File(...)
):
    s3_bucket_name = country.lower()
    uploaded_files = []
    key = deployment + "/" + data_type + "/"
    for file in files:
        try:
            s3_client.upload_fileobj(
                file.file,
                s3_bucket_name,
                key + file.filename)
            uploaded_files.append(file.filename)
        except NoCredentialsError:
            return JSONResponse(status_code=400, content={"message": "Credentials not available"})
        except PartialCredentialsError:
            return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
        except Exception as e:
            return JSONResponse(status_code=500, content={"message": str(e)})

    # Create a temporary log file
    log_key = f"logs/upload_summary.log"
    tmp_log_name = None

    # Check if the log file exists in the bucket
    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=log_key)
        # If the log file exists, download it
        with NamedTemporaryFile(delete=False, mode='wb') as tmp_log:
            s3_client.download_fileobj(s3_bucket_name, log_key, tmp_log)
            tmp_log_name = tmp_log.name
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # The log file does not exist, create a new one
            with NamedTemporaryFile(delete=False, mode='w') as tmp_log:
                tmp_log_name = tmp_log.name
        else:
            raise HTTPException(status_code=500, detail="Could not access S3 bucket")

    # Append new log entries to the temporary log file following CLF format
    host = request.client.host
    ident = "-"
    authuser = "-"
    with open(tmp_log_name, 'a') as tmp_log:
        for file_name in uploaded_files:
            date_str = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
            request_line = f'POST /upload/ HTTP/1.1'
            status = 200
            bytes_sent = '-'  # You can replace this with the actual byte size of the response if needed
            log_entry = (f'{host} {ident} {authuser} [{date_str}] "{request_line}" {status} {bytes_sent} Name="{name}" '
                         f'Country="{country}" Deployment="{deployment}" DataType="{data_type}" FileName="{file_name}"\n')
            tmp_log.write(log_entry)

    # Upload the log file to S3
    try:
        with open(tmp_log_name, 'rb') as f:
            s3_client.upload_fileobj(f, s3_bucket_name, log_key)
    except NoCredentialsError:
        raise HTTPException(status_code=403, detail="Credentials not available")
    except PartialCredentialsError:
        raise HTTPException(status_code=400, detail="Incomplete credentials")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Clean up the temporary file
        os.remove(tmp_log_name)

    response = UploadResponse(uploaded_files=uploaded_files)
    return JSONResponse(content=response.dict())


@app.post("/create-bucket/")
async def create_bucket(bucket_name: str = Body(..., embed=True)):
    try:
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        return JSONResponse(status_code=200, content={"message": f"Bucket '{bucket_name}' created successfully"})
    except s3_client.exceptions.BucketAlreadyExists:
        return JSONResponse(status_code=400, content={"message": "Bucket already exists"})
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        return JSONResponse(status_code=400, content={"message": "Bucket already owned by you"})
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
