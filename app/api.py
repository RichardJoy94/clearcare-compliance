from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import os, uuid, shutil, json, zipfile, io, datetime as dt
import polars as pl
from jinja2 import Environment, FileSystemLoader

# Optional S3 (only used if creds are present & work)
import boto3, botocore

app = FastAPI(title="ClearCare Compliance API")

# --- CORS Setup ---
origins = [os.getenv("ALLOWED_ORIGINS", "*")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -------------------

DATA_DIR = "/app/data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
EV_DIR  = os.path.join(DATA_DIR, "evidence")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)
os.makedirs(EV_DIR,  exist_ok=True)

# Initialize Jinja2 environment
jinja_env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))

# Run store file path
RUNS_STORE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'runs.json')

def get_s3():
    access = os.getenv("S3_ACCESS_KEY")
    secret = os.getenv("S3_SECRET_KEY")
    bucket = os.getenv("S3_BUCKET")
    if not (access and secret and bucket):
        return None, None
    session = boto3.session.Session()
    client = session.client(
        "s3",
        region_name=os.getenv("S3_REGION", "us-east-1"),
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
    )
    return client, bucket

def presign(key: str, expires=3600):
    s3, bucket = get_s3()
    if not (s3 and bucket):
        return None
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires
    )


def load_runs_store() -> list:
    """Load the runs store from JSON file."""
    try:
        if os.path.exists(RUNS_STORE_PATH):
            with open(RUNS_STORE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    except Exception as e:
        print(f"Error loading runs store: {e}")
        return []

def save_runs_store(runs: list) -> None:
    """Save the runs store to JSON file."""
    try:
        with open(RUNS_STORE_PATH, 'w', encoding='utf-8') as f:
            json.dump(runs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving runs store: {e}")

def add_run_to_store(run_id: str, status: str = "uploaded", filename: str = None) -> None:
    """Add a new run to the store."""
    runs = load_runs_store()
    new_run = {
        "run_id": run_id,
        "status": status,
        "filename": filename,
        "created_at": dt.datetime.utcnow().isoformat() + "Z",
        "updated_at": dt.datetime.utcnow().isoformat() + "Z"
    }
    runs.append(new_run)
    save_runs_store(runs)

def update_run_status(run_id: str, status: str) -> None:
    """Update the status of an existing run."""
    runs = load_runs_store()
    for run in runs:
        if run["run_id"] == run_id:
            run["status"] = status
            run["updated_at"] = dt.datetime.utcnow().isoformat() + "Z"
            break
    save_runs_store(runs)

def stream_csv_to_parquet(local_csv_path: str, local_parquet_path: str) -> str:
    """Stream CSV to Parquet using Polars with error handling.
    
    Args:
        local_csv_path: Path to the input CSV file
        local_parquet_path: Path for the output Parquet file
        
    Returns:
        str: Path to the created Parquet file
    """
    try:
        # Use Polars to scan CSV with error handling and stream to Parquet
        df = pl.scan_csv(local_csv_path, ignore_errors=True)
        df.sink_parquet(local_parquet_path, compression='zstd')
        return local_parquet_path
    except Exception as e:
        raise Exception(f"Failed to convert CSV to Parquet: {e}")

@app.post("/upload", summary="Upload Document")
async def upload(document: UploadFile = File(...)):
    try:
        run_id = str(uuid.uuid4())
        safe_name = f"{run_id}__{os.path.basename(document.filename)}"
        local_csv_path = os.path.join(RAW_DIR, safe_name)
        local_parquet_path = os.path.join(PARQUET_DIR, f"{run_id}.parquet")

        # stream save CSV
        with open(local_csv_path, "wb") as f:
            shutil.copyfileobj(document.file, f)

        # convert CSV to Parquet using streaming
        stream_csv_to_parquet(local_csv_path, local_parquet_path)

        # Add run to store
        add_run_to_store(run_id, "uploaded", document.filename)

        s3_keys = {}
        # try S3, but don't fail upload if S3 not configured
        try:
            s3, bucket = get_s3()
            if s3 and bucket:
                # Upload raw CSV to raw/ path
                csv_s3_key = f"raw/{safe_name}"
                s3.upload_file(local_csv_path, bucket, csv_s3_key)
                s3_keys["csv"] = csv_s3_key
                
                # Upload Parquet to parquet/ path
                parquet_s3_key = f"parquet/{run_id}.parquet"
                s3.upload_file(local_parquet_path, bucket, parquet_s3_key)
                s3_keys["parquet"] = parquet_s3_key
        except Exception as e:
            # log-only, stay successful for local dev
            print(f"[upload] S3 upload skipped: {e}")

        return JSONResponse({
            "run_id": run_id, 
            "local_csv_path": local_csv_path,
            "local_parquet_path": local_parquet_path,
            "s3_keys": s3_keys
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"upload_failed: {e}")

@app.post("/validate", summary="Validate Data")
async def validate(payload: dict):
    run_id = payload.get("run_id")
    if not run_id:
        raise HTTPException(status_code=422, detail="run_id required")

    # Find the Parquet file for this run_id
    parquet_path = os.path.join(PARQUET_DIR, f"{run_id}.parquet")
    if not os.path.exists(parquet_path):
        raise HTTPException(status_code=404, detail=f"No Parquet file found for run_id: {run_id}")

    # Run validation rules
    registry_path = os.path.join(os.path.dirname(__file__), "..", "rules", "registry.yaml")
    if not os.path.exists(registry_path):
        raise HTTPException(status_code=500, detail="Rules registry not found")

    try:
        from app.validator import run_rules
        result = run_rules(parquet_path, registry_path)
        result["run_id"] = run_id
        
        # Update run status based on validation results
        if result.get("summary", {}).get("failed", 0) > 0:
            update_run_status(run_id, "validation_failed")
        else:
            update_run_status(run_id, "validated")
        
        # Save JSON evidence
        json_path = os.path.join(EV_DIR, f"{run_id}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

@app.get("/generate_evidence_pack/{run_id}", summary="Generate Evidence Pack")
async def generate_pack(run_id: str):
    json_path = os.path.join(EV_DIR, f"{run_id}.json")
    if not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail="no validation json for run_id")

    # Load validation results
    with open(json_path, 'r', encoding='utf-8') as f:
        validation_data = json.load(f)

    # Generate HTML report using Jinja2 template
    template = jinja_env.get_template('report.html')
    html_content = template.render(
        run_id=run_id,
        timestamp=validation_data.get('timestamp', dt.datetime.utcnow().isoformat() + 'Z'),
        total_rows=validation_data.get('total_rows', 0),
        summary=validation_data.get('summary', {}),
        checks=validation_data.get('checks', []),
        rules_version=validation_data.get('rules_version', 'v1.0.0'),
        file_path=validation_data.get('file_path', 'Unknown')
    )
    
    html_path = os.path.join(EV_DIR, f"{run_id}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Generate CSV summary
    csv_content = "rule,status,message\n"
    for check in validation_data.get('checks', []):
        csv_content += (
            f"\"{check.get('rule', '')}\","
            f"\"{check.get('status', '')}\","
            f"\"{check.get('message', '').replace('\"', '\"\"')}\"\n"
        )

    csv_path = os.path.join(EV_DIR, f"{run_id}.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(csv_content)


    # pack zip in-memory
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(json_path, arcname=f"{run_id}/result.json")
        z.write(html_path, arcname=f"{run_id}/report.html")
        z.write(csv_path, arcname=f"{run_id}/summary.csv")
    mem.seek(0)

    # write zip to disk for publishing/serve
    zip_path = os.path.join(EV_DIR, f"{run_id}.zip")
    with open(zip_path, "wb") as f:
        f.write(mem.read())

    # optional S3 publish + presigned URL
    s3_key = f"evidence/{run_id}.zip"
    try:
        s3, bucket = get_s3()
        if s3 and bucket:
            s3.upload_file(zip_path, bucket, s3_key)
            return {
                "run_id": run_id,
                "local_zip": zip_path,
                "s3_key": s3_key,
                "s3_presigned_url": presign(s3_key, 3600)
            }
    except Exception as e:
        print(f"[pack] S3 publish skipped: {e}")

    # fallback to returning the local file if S3 isn't configured
    return FileResponse(zip_path, media_type="application/zip", filename=f"{run_id}.zip")

@app.get("/runs", summary="List All Runs")
async def list_runs():
    """List all compliance runs."""
    runs = load_runs_store()
    return {"runs": runs, "total": len(runs)}

@app.get("/runs/{run_id}", summary="Get Run Details")
async def get_run_details(run_id: str):
    """Get details for a specific run."""
    runs = load_runs_store()
    for run in runs:
        if run["run_id"] == run_id:
            return run
    raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

@app.post("/publish", summary="Publish Data")
async def publish(payload: dict):
    run_id = payload.get("run_id")
    if not run_id:
        raise HTTPException(status_code=422, detail="run_id required")
    
    # Update run status to published
    update_run_status(run_id, "published")
    
    return {"run_id": run_id, "status": "published"}
