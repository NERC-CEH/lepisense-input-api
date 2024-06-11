from fastapi import FastAPI, Form, File, UploadFile, Request, Body, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from concurrent.futures import ThreadPoolExecutor, wait
from pydantic import BaseModel, Field
from functions import *


tags_metadata = [
    {
        "name": "Data",
        "description": "Operations for data management in the server, uploading and downloading."
    },
    {
        "name": "Deployments",
        "description": "Operations with deployments."
    },
    {
        "name": "Other",
        "description": "Other operations."
    }
]

app = FastAPI(
    title="AMI Data Management API",
    version="1.0.1",
    contact={
        "name": "AMI system team at UKCEH",
        "url": "https://www.ceh.ac.uk/solutions/equipment/automated-monitoring-insects-trap",
        "email": "ami-system@ceh.ac.uk",
    },
    license_info={
        "name": "Apache 2.0",
        "identifier": "MIT",
    },
    openapi_tags=tags_metadata
)

# Set up CORS middleware
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "https://connect-apps.ceh.ac.uk/ami-data-upload/"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Mount the static directory to serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

deployments_info = load_deployments_info()
valid_countries_names = {d['country'] for d in deployments_info if d['status'] == 'active'}
valid_countries_location_names = {f"{d['country']} - {d['location_name']}" for d in deployments_info
                                  if d['status'] == 'active'}
valid_data_types = {"motion_images", "snapshot_images", "audible_recordings", "ultrasound_recordings"}


class UploadResponse(BaseModel):
    uploaded_files: List[str]


class Deployment(BaseModel):
    country: str
    country_code: str
    location_name: str
    lat: str
    lon: str
    location_id: str
    camera_id: str
    system_id: str
    hardware_id: str
    deployment_id: str
    data_type: str = Field(default="motion_images,snapshot_images,audible_recordings,ultrasound_recordings")
    s3_key: str = Field(default="country_code/deployment_id/data_type")
    status: str = Field(default="inactive")


@app.get("/", response_class=HTMLResponse, tags=["Data"])
async def main():
    with open("templates/upload.html") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


@app.get("/get-deployments/", tags=["Deployments"])
async def get_deployments():
    return JSONResponse(content=deployments_info)


@app.post("/create-deployment/", tags=["Deployments"])
async def create_deployment(deployment: Deployment):
    try:
        # Append the new deployment to the CSV file
        with open('deployments_info.csv', 'a', newline='') as csvfile:
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'data_type', "s3_key", 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(deployment.dict())

        # Reload deployments info
        global deployments_info
        deployments_info = load_deployments_info()

        return JSONResponse(status_code=201, content={"message": "Deployment created successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.put("/update-deployment/", tags=["Deployments"])
async def update_deployment(deployment: Deployment):
    try:
        # Load existing deployments
        deployments = load_deployments_info()

        # Find the deployment to update
        updated = False
        for i, existing_deployment in enumerate(deployments):
            if existing_deployment['deployment_id'] == deployment.deployment_id:
                deployments[i] = deployment.dict()
                updated = True
                break

        if not updated:
            return JSONResponse(status_code=404, content={"message": "Deployment not found"})

        # Write the updated deployments back to the CSV file
        with open('deployments_info.csv', 'w', newline='') as csvfile:
            fieldnames = ['country', 'country_code', 'location_name', "lat", "lon", "location_id", 'camera_id',
                          "system_id", 'hardware_id', 'deployment_id', 'data_type', "s3_key", 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dep in deployments:
                writer.writerow(dep)

        # Reload deployments info
        global deployments_info
        deployments_info = load_deployments_info()

        return JSONResponse(status_code=200, content={"message": "Deployment updated successfully"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/list-data/", tags=["Other"])
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
        files = s3_list_objects(s3_bucket_name, prefix)
        return JSONResponse(status_code=200, content={"files": files})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


@app.get("/get-logs/", tags=["Other"])
async def get_logs(country_name: str = Query("", enum=sorted(list(valid_countries_names)),
                                             description="Country names.")):
    country_code = [d['country_code'] for d in deployments_info if d['country'] == country_name][0]
    s3_bucket_name = country_code.lower()

    try:
        log_key = "logs/upload_summary.log"
        json_data = download_logs(s3_bucket_name, log_key)
        return Response(content=json_data, media_type="application/json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


@app.post("/upload/", tags=["Data"])
async def upload_file(
        request: Request,
        name: str = Form(...),
        country: str = Form(...),
        deployment: str = Form(...),
        data_type: str = Form(...),
        files: List[UploadFile] = File(...)
):
    s3_bucket_name = country.lower()
    key = deployment + "/" + data_type + "/"
    uploaded_files = []

    try:
        # Divide the large volume data into smaller chunks
        batch_size = 100
        chunks = divide_data_into_chunks(files, batch_size)
        # Process each chunk in parallel using a thread pool executor
        executor = ThreadPoolExecutor(max_workers=20)
        futures = [executor.submit(fast_upload, s3_bucket_name, chunk, key, uploaded_files) for chunk in chunks]
        # Wait for all tasks to complete
        wait(futures)
    except NoCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Credentials not available"})
    except PartialCredentialsError:
        return JSONResponse(status_code=400, content={"message": "Incomplete credentials"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

    host = request.client.host
    write_logs(uploaded_files, s3_bucket_name, host, name, deployment, data_type)

    response = UploadResponse(uploaded_files=uploaded_files)
    return JSONResponse(content=response.dict())


@app.post("/create-bucket/", tags=["Other"])
async def create_bucket(bucket_name: str = Body(..., embed=True)):
    try:
        s3_create_bucket(bucket_name)
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
