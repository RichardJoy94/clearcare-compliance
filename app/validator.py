"""
ClearCare Compliance Validator
Implements rule-based validation for compliance data using Polars
"""

import os
import yaml
import polars as pl
from datetime import datetime, timedelta
from typing import Dict, Any, List


def load_rules_registry(registry_path: str) -> Dict[str, Any]:
    """Load rules from the YAML registry file.
    
    Args:
        registry_path: Path to the rules registry YAML file
        
    Returns:
        Dict containing the loaded rules
    """
    try:
        with open(registry_path, 'r', encoding='utf-8') as f:
            rules = yaml.safe_load(f)
        return rules
    except Exception as e:
        raise Exception(f"Failed to load rules registry: {e}")


def run_rules(parquet_path: str, registry_path: str) -> Dict[str, Any]:
    """Run compliance rules against a Parquet file.
    
    Args:
        parquet_path: Path to the Parquet file to validate
        registry_path: Path to the rules registry YAML file
        
    Returns:
        Dict containing rule results and validation summary
    """
    try:
        # Load the rules
        rules = load_rules_registry(registry_path)
        
        # Load the Parquet file
        df = pl.read_parquet(parquet_path)
        
        # Initialize results
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "rules_version": "v1.0.0",
            "file_path": parquet_path,
            "total_rows": len(df),
            "checks": [],
            "summary": {
                "total_checks": 0,
                "passed": 0,
                "failed": 0,
                "errors": 0
            }
        }
        
        # Rule 1: Check required columns
        required_columns = rules.get("required_columns", [])
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            results["checks"].append({
                "rule": "required_columns",
                "status": "fail",
                "message": f"Missing required columns: {missing_columns}",
                "details": {"missing_columns": missing_columns}
            })
            results["summary"]["failed"] += 1
        else:
            results["checks"].append({
                "rule": "required_columns",
                "status": "pass",
                "message": "All required columns present",
                "details": {"required_columns": required_columns}
            })
            results["summary"]["passed"] += 1
        results["summary"]["total_checks"] += 1
        
        # Rule 2: Check date freshness (if date column exists)
        if "date" in df.columns and rules.get("date_freshness_max_days"):
            max_days = rules["date_freshness_max_days"]
            cutoff_date = datetime.now() - timedelta(days=max_days)
            
            try:
                # Convert date column to datetime if it's not already
                df_with_dates = df.with_columns(
                    pl.col("date").str.to_datetime().alias("date_parsed")
                )
                
                # Count rows with dates older than cutoff
                old_dates = df_with_dates.filter(
                    pl.col("date_parsed") < cutoff_date
                ).height
                
                if old_dates > 0:
                    results["checks"].append({
                        "rule": "date_freshness",
                        "status": "fail",
                        "message": f"{old_dates} rows have dates older than {max_days} days",
                        "details": {"old_rows": old_dates, "max_days": max_days}
                    })
                    results["summary"]["failed"] += 1
                else:
                    results["checks"].append({
                        "rule": "date_freshness",
                        "status": "pass",
                        "message": f"All dates are within {max_days} days",
                        "details": {"max_days": max_days}
                    })
                    results["summary"]["passed"] += 1
            except Exception as e:
                results["checks"].append({
                    "rule": "date_freshness",
                    "status": "error",
                    "message": f"Error checking date freshness: {str(e)}",
                    "details": {"error": str(e)}
                })
                results["summary"]["errors"] += 1
            results["summary"]["total_checks"] += 1
        
        # Rule 3: Check cash <= gross price (if both columns exist)
        if "cash_price" in df.columns and "gross_price" in df.columns and rules.get("cash_leq_gross"):
            try:
                # Convert to numeric, handling any non-numeric values
                df_numeric = df.with_columns([
                    pl.col("cash_price").cast(pl.Float64, strict=False),
                    pl.col("gross_price").cast(pl.Float64, strict=False)
                ])
                
                # Count rows where cash_price > gross_price
                invalid_prices = df_numeric.filter(
                    pl.col("cash_price") > pl.col("gross_price")
                ).height
                
                if invalid_prices > 0:
                    results["checks"].append({
                        "rule": "cash_leq_gross",
                        "status": "fail",
                        "message": f"{invalid_prices} rows have cash_price > gross_price",
                        "details": {"invalid_rows": invalid_prices}
                    })
                    results["summary"]["failed"] += 1
                else:
                    results["checks"].append({
                        "rule": "cash_leq_gross",
                        "status": "pass",
                        "message": "All cash prices are <= gross prices",
                        "details": {}
                    })
                    results["summary"]["passed"] += 1
            except Exception as e:
                results["checks"].append({
                    "rule": "cash_leq_gross",
                    "status": "error",
                    "message": f"Error checking cash <= gross rule: {str(e)}",
                    "details": {"error": str(e)}
                })
                results["summary"]["errors"] += 1
            results["summary"]["total_checks"] += 1
        
        # Rule 4: Check non-negative prices (if price columns exist)
        if rules.get("non_negative_prices"):
            price_columns = [col for col in ["cash_price", "gross_price"] if col in df.columns]
            
            if price_columns:
                try:
                    # Convert price columns to numeric
                    df_numeric = df
                    for col in price_columns:
                        df_numeric = df_numeric.with_columns(
                            pl.col(col).cast(pl.Float64, strict=False)
                        )
                    
                    # Count rows with negative prices
                    negative_prices = 0
                    for col in price_columns:
                        negative_prices += df_numeric.filter(
                            pl.col(col) < 0
                        ).height
                    
                    if negative_prices > 0:
                        results["checks"].append({
                            "rule": "non_negative_prices",
                            "status": "fail",
                            "message": f"{negative_prices} rows have negative prices",
                            "details": {"negative_rows": negative_prices, "price_columns": price_columns}
                        })
                        results["summary"]["failed"] += 1
                    else:
                        results["checks"].append({
                            "rule": "non_negative_prices",
                            "status": "pass",
                            "message": "All prices are non-negative",
                            "details": {"price_columns": price_columns}
                        })
                        results["summary"]["passed"] += 1
                except Exception as e:
                    results["checks"].append({
                        "rule": "non_negative_prices",
                        "status": "error",
                        "message": f"Error checking non-negative prices: {str(e)}",
                        "details": {"error": str(e)}
                    })
                    results["summary"]["errors"] += 1
                results["summary"]["total_checks"] += 1
        
        return results
        
    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "rules_version": "v1.0.0",
            "file_path": parquet_path,
            "error": f"Validation failed: {str(e)}",
            "checks": [],
            "summary": {
                "total_checks": 0,
                "passed": 0,
                "failed": 0,
                "errors": 1
            }
        }

