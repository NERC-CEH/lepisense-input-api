# AMI Data Upload

Website to push data (images and audio files) into the server.

## Installation 

Create an environment just for AMI and the data companion using conda 
```sh
conda create -n ami-api python=3.9 
```

Activate the conda environment
```sh
conda activate ami-api
```

Clone the repository using the command line or the GitHub desktop app.
```sh
git clone https://github.com/AMI-system/ami-api.git
```

Install the dependencies.
```sh
cd ami-api
pip install -e .
```
## Run the app

Create the credential.json file and save it in the root folder:
```sh
{
  "AWS_ACCESS_KEY_ID": "",
  "AWS_SECRET_ACCESS_KEY": "",
  "AWS_REGION": "",
  "AWS_URL_ENDPOINT": ""
}
```

Start the app
```sh
uvicorn main:app --port 8080 --reload
```
