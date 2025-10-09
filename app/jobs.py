"""
Background job processing using RQ (Redis Queue)
"""

import os
import uuid
import json
from typing import Optional
from rq import Queue, Worker
from rq.job import Job
import redis
from sqlmodel import Session

# Import database and models
from app.db import get_db_session
from app.models import Run, RunStatus

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def get_redis_connection():
    """Get Redis connection from URL."""
    try:
        return redis.from_url(REDIS_URL)
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return None

def get_queue(name: str = "default") -> Optional[Queue]:
    """Get RQ queue instance."""
    redis_conn = get_redis_connection()
    if redis_conn:
        return Queue(name, connection=redis_conn)
    return None

def run_validation(run_id: str) -> dict:
    """
    Background job to run validation for a run.
    
    Args:
        run_id: UUID string of the run to validate
        
    Returns:
        Dict containing validation results
    """
    print(f"Starting validation job for run_id: {run_id}")
    
    try:
        # Get database session
        session = get_db_session()
        
        # Get the run
        run = session.get(Run, uuid.UUID(run_id))
        if not run:
            raise ValueError(f"Run {run_id} not found")
        
        # Update status to running
        run.status = RunStatus.RUNNING
        session.add(run)
        session.commit()
        
        # Import validation functions
        from app.validator import run_rules
        from app.json_validator import run_json_schema_validation
        
        # Check what files exist for this run_id
        DATA_DIR = "/app/data"
        PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
        JSON_DIR = os.path.join(DATA_DIR, "json")
        EV_DIR = os.path.join(DATA_DIR, "evidence")
        
        parquet_path = os.path.join(PARQUET_DIR, f"{run_id}.parquet")
        json_path = os.path.join(JSON_DIR, f"{run_id}.json")
        
        validation_results = {
            "run_id": run_id,
            "timestamp": None,
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
            raise ValueError(f"No files found for run_id: {run_id}")

        # Update run status and validation results in database
        combined_summary = validation_results["combined_summary"]
        csv_validation = validation_results.get("csv_validation")
        json_validation = validation_results.get("json_validation")
        
        # Determine final status
        if combined_summary["failed"] > 0 or combined_summary["errors"] > 0:
            final_status = RunStatus.FAILED
        else:
            final_status = RunStatus.VALIDATED
        
        # Extract validation counts
        rules_passed = combined_summary["passed"]
        rules_failed = combined_summary["failed"]
        
        # Extract schema validation result
        schema_ok = None
        if json_validation and json_validation.get("schema_validation"):
            schema_ok = json_validation["schema_validation"].get("valid")
        
        # Update run in database
        run.status = final_status
        if schema_ok is not None:
            run.schema_ok = schema_ok
        run.rules_passed = rules_passed
        run.rules_failed = rules_failed
        session.add(run)
        session.commit()
        
        # Save combined validation evidence
        import datetime as dt
        validation_results["timestamp"] = dt.datetime.utcnow().isoformat() + "Z"
        
        evidence_path = os.path.join(EV_DIR, f"{run_id}.json")
        os.makedirs(os.path.dirname(evidence_path), exist_ok=True)
        with open(evidence_path, "w", encoding="utf-8") as f:
            json.dump(validation_results, f, ensure_ascii=False, indent=2)
        
        print(f"Validation completed for run_id: {run_id}, status: {final_status}")
        
        session.close()
        return validation_results
        
    except Exception as e:
        print(f"Validation job failed for run_id {run_id}: {str(e)}")
        
        # Update run status to failed
        try:
            session = get_db_session()
            run = session.get(Run, uuid.UUID(run_id))
            if run:
                run.status = RunStatus.FAILED
                session.add(run)
                session.commit()
            session.close()
        except Exception:
            pass
        
        raise e

def generate_pack(run_id: str) -> dict:
    """
    Background job to generate evidence pack for a run.
    
    Args:
        run_id: UUID string of the run
        
    Returns:
        Dict containing pack generation results
    """
    print(f"Starting evidence pack generation for run_id: {run_id}")
    
    try:
        # Import the evidence pack generation logic from api.py
        import os
        import json
        import zipfile
        import io
        import csv
        import datetime as dt
        from jinja2 import Environment, FileSystemLoader
        
        DATA_DIR = "/app/data"
        EV_DIR = os.path.join(DATA_DIR, "evidence")
        
        # Initialize Jinja2 environment
        jinja_env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))
        
        json_path = os.path.join(EV_DIR, f"{run_id}.json")
        if not os.path.exists(json_path):
            raise ValueError(f"No validation json for run_id: {run_id}")

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

        # Optional S3 publish + presigned URL
        s3_key = f"evidence/{run_id}.zip"
        result = {
            "run_id": run_id,
            "local_zip": zip_path,
            "s3_key": s3_key,
            "s3_presigned_url": None
        }
        
        try:
            from app.api import get_s3, presign
            s3, bucket = get_s3()
            if s3 and bucket:
                s3.upload_file(zip_path, bucket, s3_key)
                result["s3_presigned_url"] = presign(s3_key, 3600)
        except Exception as e:
            print(f"[pack] S3 publish skipped: {e}")

        print(f"Evidence pack generated for run_id: {run_id}")
        return result
        
    except Exception as e:
        print(f"Evidence pack generation failed for run_id {run_id}: {str(e)}")
        raise e

def enqueue_validation(run_id: str) -> Optional[str]:
    """
    Enqueue a validation job.
    
    Args:
        run_id: UUID string of the run to validate
        
    Returns:
        Job ID if successful, None if Redis not available
    """
    queue = get_queue("validation")
    if queue:
        job = queue.enqueue(run_validation, run_id, job_timeout="10m")
        return job.id
    return None

def enqueue_pack_generation(run_id: str) -> Optional[str]:
    """
    Enqueue an evidence pack generation job.
    
    Args:
        run_id: UUID string of the run
        
    Returns:
        Job ID if successful, None if Redis not available
    """
    queue = get_queue("pack_generation")
    if queue:
        job = queue.enqueue(generate_pack, run_id, job_timeout="5m")
        return job.id
    return None

def get_job_status(job_id: str) -> dict:
    """
    Get the status and result of a job.
    
    Args:
        job_id: The job ID
        
    Returns:
        Dict containing job status and result
    """
    redis_conn = get_redis_connection()
    if not redis_conn:
        return {"status": "error", "error": "Redis not available"}
    
    try:
        job = Job.fetch(job_id, connection=redis_conn)
        
        result = {
            "job_id": job_id,
            "status": job.get_status(),
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
        }
        
        if job.is_finished:
            if job.result:
                result["result"] = job.result
            if job.exc_info:
                result["error"] = str(job.exc_info)
        elif job.is_failed:
            result["error"] = str(job.exc_info) if job.exc_info else "Job failed"
        
        return result
        
    except Exception as e:
        return {"status": "error", "error": f"Job not found: {str(e)}"}
