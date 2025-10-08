from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import os, uuid, shutil, json, zipfile, io, datetime as dt
import polars as pl
import csv
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
JSON_DIR = os.path.join(DATA_DIR, "json")
EV_DIR  = os.path.join(DATA_DIR, "evidence")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)
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

def detect_file_type(file_path: str) -> str:
    """Detect if a file is JSON or CSV based on content.
    
    Args:
        file_path: Path to the file to analyze
        
    Returns:
        String indicating file type: 'json', 'csv', or 'unknown'
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read first few characters to detect type
            first_chars = f.read(10).strip()
            
            # Check if it starts with JSON characters
            if first_chars.startswith(('{', '[')):
                return 'json'
            
            # Check if it looks like CSV (has comma or tab separation)
            f.seek(0)
            first_line = f.readline().strip()
            if ',' in first_line or '\t' in first_line:
                return 'csv'
                
    except Exception:
        pass
    
    # Fallback to file extension
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.json':
        return 'json'
    elif ext == '.csv':
        return 'csv'
    
    return 'unknown'

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
        local_raw_path = os.path.join(RAW_DIR, safe_name)

        # stream save file
        with open(local_raw_path, "wb") as f:
            shutil.copyfileobj(document.file, f)

        # detect file type
        file_type = detect_file_type(local_raw_path)
        
        result = {
            "run_id": run_id,
            "local_raw_path": local_raw_path,
            "file_type": file_type,
            "filename": document.filename
        }
        
        # process based on file type
        if file_type == 'csv':
            # CSV processing - convert to Parquet
            local_parquet_path = os.path.join(PARQUET_DIR, f"{run_id}.parquet")
            stream_csv_to_parquet(local_raw_path, local_parquet_path)
            result["local_parquet_path"] = local_parquet_path
            
            # Add run to store with CSV type
            add_run_to_store(run_id, "uploaded", document.filename)
            
        elif file_type == 'json':
            # JSON processing - copy to JSON directory
            local_json_path = os.path.join(JSON_DIR, f"{run_id}.json")
            shutil.copy2(local_raw_path, local_json_path)
            result["local_json_path"] = local_json_path
            
            # Add run to store with JSON type
            add_run_to_store(run_id, "uploaded", document.filename)
            
        else:
            # Unknown file type
            add_run_to_store(run_id, "uploaded", document.filename)
            result["warning"] = f"Unknown file type detected: {file_type}"

        # S3 upload
        s3_keys = {}
        try:
            s3, bucket = get_s3()
            if s3 and bucket:
                # Upload raw file
                raw_s3_key = f"raw/{safe_name}"
                s3.upload_file(local_raw_path, bucket, raw_s3_key)
                s3_keys["raw"] = raw_s3_key
                
                # Upload processed file if applicable
                if file_type == 'csv' and 'local_parquet_path' in result:
                    parquet_s3_key = f"parquet/{run_id}.parquet"
                    s3.upload_file(result["local_parquet_path"], bucket, parquet_s3_key)
                    s3_keys["parquet"] = parquet_s3_key
                elif file_type == 'json' and 'local_json_path' in result:
                    json_s3_key = f"json/{run_id}.json"
                    s3.upload_file(result["local_json_path"], bucket, json_s3_key)
                    s3_keys["json"] = json_s3_key
                    
        except Exception as e:
            print(f"[upload] S3 upload skipped: {e}")

        result["s3_keys"] = s3_keys
        return JSONResponse(result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"upload_failed: {e}")

@app.post("/validate", summary="Validate Data")
async def validate(payload: dict):
    run_id = payload.get("run_id")
    if not run_id:
        raise HTTPException(status_code=422, detail="run_id required")

    # Check what files exist for this run_id
    parquet_path = os.path.join(PARQUET_DIR, f"{run_id}.parquet")
    json_path = os.path.join(JSON_DIR, f"{run_id}.json")
    
    validation_results = {
        "run_id": run_id,
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "csv_validation": None,
        "json_validation": None,
        "combined_summary": {
            "total_checks": 0,
            "passed": 0,
            "failed": 0,
            "errors": 0
        }
    }

    # Validate CSV/Parquet if exists
    if os.path.exists(parquet_path):
        try:
            registry_path = os.path.join(os.path.dirname(__file__), "..", "rules", "registry.yaml")
            if os.path.exists(registry_path):
                from app.validator import run_rules
                csv_result = run_rules(parquet_path, registry_path)
                validation_results["csv_validation"] = csv_result
                
                # Update combined summary
                csv_summary = csv_result.get("summary", {})
                validation_results["combined_summary"]["total_checks"] += csv_summary.get("total_checks", 0)
                validation_results["combined_summary"]["passed"] += csv_summary.get("passed", 0)
                validation_results["combined_summary"]["failed"] += csv_summary.get("failed", 0)
                validation_results["combined_summary"]["errors"] += csv_summary.get("errors", 0)
        except Exception as e:
            validation_results["csv_validation"] = {
                "error": f"CSV validation failed: {str(e)}",
                "summary": {"total_checks": 0, "passed": 0, "failed": 0, "errors": 1}
            }
            validation_results["combined_summary"]["errors"] += 1

    # Validate JSON if exists
    if os.path.exists(json_path):
        try:
            from app.json_validator import run_json_schema_validation
            json_result = run_json_schema_validation(json_path)
            validation_results["json_validation"] = json_result
            
            # Update combined summary
            json_summary = json_result.get("summary", {})
            validation_results["combined_summary"]["total_checks"] += json_summary.get("total_schemas_checked", 0)
            validation_results["combined_summary"]["passed"] += json_summary.get("passed", 0)
            validation_results["combined_summary"]["failed"] += json_summary.get("failed", 0)
            validation_results["combined_summary"]["errors"] += json_summary.get("errors", 0)
        except Exception as e:
            validation_results["json_validation"] = {
                "error": f"JSON validation failed: {str(e)}",
                "summary": {"total_schemas_checked": 0, "passed": 0, "failed": 0, "errors": 1}
            }
            validation_results["combined_summary"]["errors"] += 1

    # Check if any files were found
    if not os.path.exists(parquet_path) and not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail=f"No files found for run_id: {run_id}")

    # Update run status based on combined validation results
    if validation_results["combined_summary"]["failed"] > 0 or validation_results["combined_summary"]["errors"] > 0:
        update_run_status(run_id, "validation_failed")
    else:
        update_run_status(run_id, "validated")
    
    # Save combined validation evidence
    evidence_path = os.path.join(EV_DIR, f"{run_id}.json")
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(validation_results, f, ensure_ascii=False, indent=2)
    
    return validation_results

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
        csv_validation=validation_data.get('csv_validation'),
        json_validation=validation_data.get('json_validation'),
        combined_summary=validation_data.get('combined_summary', {}),
        rules_version=validation_data.get('rules_version', 'v1.0.0'),
        file_path=validation_data.get('file_path', 'Unknown')
    )
    
    html_path = os.path.join(EV_DIR, f"{run_id}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Generate CSV summary (use csv module to handle quoting safely)
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["validation_type", "rule", "status", "message"])
    
    # Add CSV validation results
    csv_validation = validation_data.get("csv_validation")
    if csv_validation and csv_validation.get("checks"):
        for check in csv_validation["checks"]:
            writer.writerow([
                "CSV Rule",
                check.get("rule", ""),
                check.get("status", ""),
                check.get("message", ""),
            ])
    
    # Add JSON validation results
    json_validation = validation_data.get("json_validation")
    if json_validation:
        # Schema detection
        schema_type = json_validation.get("detected_schema_type", "unknown")
        detection_status = "pass" if schema_type != "unknown" else "fail"
        writer.writerow([
            "JSON Schema",
            "Schema Detection",
            detection_status,
            f"Detected schema type: {schema_type}"
        ])
        
        # Schema validation
        schema_validation = json_validation.get("schema_validation", {})
        validation_status = "pass" if schema_validation.get("valid") else "fail"
        validation_message = "JSON validates against schema" if schema_validation.get("valid") else "JSON validation failed"
        writer.writerow([
            "JSON Schema",
            "Schema Validation",
            validation_status,
            validation_message
        ])
    
    csv_content = buf.getvalue()

    csv_path = os.path.join(EV_DIR, f"{run_id}.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
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

@app.get("/runs/{run_id}/validation", summary="Get Run Validation Details")
async def get_run_validation_details(run_id: str):
    """Get validation details for a specific run."""
    evidence_path = os.path.join(EV_DIR, f"{run_id}.json")
    if not os.path.exists(evidence_path):
        return {"run_id": run_id, "csv_validation": None, "json_validation": None}
    
    try:
        with open(evidence_path, 'r', encoding='utf-8') as f:
            validation_data = json.load(f)
        
        # Extract summary information for UI badges
        result = {"run_id": run_id}
        
        # CSV validation badge
        csv_validation = validation_data.get("csv_validation")
        if csv_validation:
            csv_summary = csv_validation.get("summary", {})
            if csv_summary.get("failed", 0) > 0 or csv_summary.get("errors", 0) > 0:
                result["csv_validation"] = "FAIL"
            elif csv_summary.get("passed", 0) > 0:
                result["csv_validation"] = "PASS"
            else:
                result["csv_validation"] = "-"
        else:
            result["csv_validation"] = "-"
        
        # JSON validation badge
        json_validation = validation_data.get("json_validation")
        if json_validation:
            schema_validation = json_validation.get("schema_validation", {})
            if schema_validation.get("valid"):
                result["json_validation"] = "PASS"
            else:
                result["json_validation"] = "FAIL"
        else:
            result["json_validation"] = "-"
        
        return result
        
    except Exception as e:
        return {"run_id": run_id, "error": str(e), "csv_validation": "-", "json_validation": "-"}

@app.post("/publish", summary="Publish Data")
async def publish(payload: dict):
    run_id = payload.get("run_id")
    if not run_id:
        raise HTTPException(status_code=422, detail="run_id required")
    
    # Update run status to published
    update_run_status(run_id, "published")
    
    return {"run_id": run_id, "status": "published"}
