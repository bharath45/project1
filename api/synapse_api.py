import os
from flask import Flask, request, jsonify
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import requests, uuid

app = Flask(__name__)

@app.route("/run-job", methods=["POST"])
def run_job():
    file = request.files["file"]
    job_id = str(uuid.uuid4())
    filename = f"{job_id}.csv"

    # Upload CSV to blob
    blob_service = BlobServiceClient(account_url=os.getenv("BLOB_URL"), credential=DefaultAzureCredential())
    container = blob_service.get_container_client("input-data")
    container.upload_blob(filename, file, overwrite=True)

    # Trigger Synapse pipeline
    synapse_url = os.getenv("SYNAPSE_URL")
    pipeline_name = os.getenv("PIPELINE_NAME")
    token = DefaultAzureCredential().get_token("https://dev.azuresynapse.net/.default").token

    resp = requests.post(
        f"{synapse_url}/pipelines/{pipeline_name}/createRun?api-version=2020-12-01",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"parameters": {"input_path": filename, "job_id": job_id}}
    )
    return jsonify({"job_id": job_id, "pipeline_run": resp.json()})

@app.route("/result/<job_id>", methods=["GET"])
def result(job_id):
    blob_service = BlobServiceClient(account_url=os.getenv("BLOB_URL"), credential=DefaultAzureCredential())
    container = blob_service.get_container_client("output-data")
    blob = container.get_blob_client(f"{job_id}.csv")
    if not blob.exists():
        return jsonify({"status": "pending"})
    data = blob.download_blob().readall().decode("utf-8")
    return data

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)