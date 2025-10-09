"""
ClearCare Compliance Validator
Implements comprehensive rule-based validation for compliance data using Polars
"""

import os
import yaml
import polars as pl
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Literal

# Import profile detection
from app.profiles import detect_profile, map_to_internal, get_profile_description, validate_cms_headers
from app.csv_header_sniffer import find_header_row, extract_header
from app.validator_utils import load_run_meta, parquet_columns


def validate_csv_run(run_id: str, parquet_path: str | None, raw_csv_path: str | None):
    """Validate CSV run using CMS CSV analyzer (preferred) or Parquet schema columns (fallback)."""
    # Load meta, prefer parquet_path from meta
    meta = load_run_meta(run_id) or {}
    parquet_path = parquet_path or meta.get("parquet_path")
    raw_csv_path = raw_csv_path or meta.get("csv_path")
    
    # Try CMS CSV analyzer first (for proper preamble detection)
    if raw_csv_path and os.path.exists(raw_csv_path):
        try:
            from app.cms_csv import analyze_cms_csv
            from pathlib import Path
            
            # Create a temporary parquet path if none exists
            temp_parquet_path = parquet_path
            if not temp_parquet_path:
                temp_parquet_path = raw_csv_path.replace('.csv', '.parquet')
            
            cms_result = analyze_cms_csv(Path(raw_csv_path), Path(temp_parquet_path))
            
            # Use CMS CSV results if successful
            if cms_result.get("ok") is not None:  # CMS analysis completed
                actual_cols = cms_result.get("present_columns", [])
                detected_profile = f"cms_csv_{cms_result.get('layout', 'unknown')}"
                header_row = cms_result.get("header_row", 0)
                headers = cms_result.get("headers", [])
                
                result = {
                    "profile": detected_profile,
                    "detected_profile": detected_profile,
                    "detected_header_row": header_row,
                    "detected_headers": headers,
                    "present_columns": actual_cols,
                    "cms_csv_result": cms_result  # Include full CMS results
                }
                return result
        except Exception as e:
            print(f"Warning: CMS CSV analysis failed: {e}")
    
    # Fallback to original logic
    header_row = meta.get("header_row")
    headers = meta.get("headers")
    
    # Prefer Parquet schema for 'actual_cols'
    actual_cols = []
    if parquet_path:
        actual_cols = parquet_columns(parquet_path)
    
    # If we have no Parquet or schema looks empty, fall back to sniffer
    if not actual_cols and raw_csv_path:
        # cheap re-sniff path
        with open(raw_csv_path, "rb") as f:
            head = f.read(150_000).decode("utf-8-sig", errors="ignore")
        header_row = find_header_row(head)
        headers = extract_header(raw_csv_path, header_row)
        actual_cols = headers[:] if headers else []
    
    # Detect profile using REAL columns
    detected_profile = detect_profile(actual_cols)
    
    result = {
        "profile": detected_profile,
        "detected_profile": detected_profile,
        "detected_header_row": header_row,
        "detected_headers": headers,
        "present_columns": actual_cols,
    }
    
    return result


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


def get_failing_rows(df: pl.DataFrame, condition: pl.Expr, max_rows: int = 5) -> List[Dict]:
    """Extract failing rows as dictionaries for reporting.
    
    Args:
        df: Polars DataFrame
        condition: Boolean expression identifying failing rows
        max_rows: Maximum number of failing rows to return
        
    Returns:
        List of dictionaries representing failing rows
    """
    try:
        failing_df = df.filter(condition).head(max_rows)
        return failing_df.to_dicts()
    except Exception as e:
        return [{"error": f"Failed to extract failing rows: {str(e)}"}]


def check_profile_headers(actual_cols: List[str], profile: Literal["cms_csv", "simple_csv"], 
                         rules: Dict, max_failing_rows: int) -> Dict:
    """Check headers based on detected profile using actual columns from Parquet schema."""
    profile_config = rules.get("profiles", {}).get(profile, {})
    
    if profile == "cms_csv":
        # For CMS CSV, check required_headers
        required_headers = profile_config.get("required_headers", [])
        
        # Normalize both the actual headers and required headers for comparison
        actual_headers = {h.lower().strip().replace(" ", "_").replace("-", "_") for h in actual_cols}
        normalized_required = {h.lower().strip().replace(" ", "_").replace("-", "_") for h in required_headers}
        
        missing_headers = normalized_required - actual_headers
        
        if missing_headers:
            return {
                "rule": "required_headers",
                "status": "fail",
                "message": f"Missing required CMS headers: {', '.join(missing_headers)}",
                "details": {
                    "profile": profile,
                    "missing_headers": list(missing_headers),
                    "present_headers": list(actual_headers),
                    "profile_description": get_profile_description(profile)
                }
            }
        else:
            return {
                "rule": "required_headers",
                "status": "pass",
                "message": f"All required CMS headers present",
                "details": {
                    "profile": profile,
                    "required_headers": required_headers,
                    "profile_description": get_profile_description(profile)
                }
            }
    else:
        # For simple CSV, check required_columns
        required_columns = profile_config.get("required_columns", [])
        missing_columns = [col for col in required_columns if col not in actual_cols]
        
        if missing_columns:
            return {
                "rule": "required_columns",
                "status": "fail",
                "message": f"Missing required columns: {', '.join(missing_columns)}",
                "details": {
                    "profile": profile,
                    "missing_columns": missing_columns,
                    "present_columns": actual_cols,
                    "profile_description": get_profile_description(profile)
                }
            }
        else:
            return {
                "rule": "required_columns",
                "status": "pass",
                "message": "All required columns present",
                "details": {
                    "profile": profile,
                    "required_columns": required_columns,
                    "profile_description": get_profile_description(profile)
                }
            }


def check_column_types(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that columns have the expected data types."""
    column_types = rules.get("column_types", {})
    if not column_types:
        return []
    
    results = []
    type_mapping = {
        "string": pl.Utf8,
        "float": pl.Float64,
        "int": pl.Int64,
        "date": pl.Date,
        "datetime": pl.Datetime,
        "bool": pl.Boolean
    }
    
    for col, expected_type_str in column_types.items():
        if col not in df.columns:
            continue  # Skip if column doesn't exist
        
        expected_type = type_mapping.get(expected_type_str)
        actual_type = df[col].dtype
        
        # Check if types match (allowing for compatible types)
        type_match = False
        if expected_type == pl.Float64 and actual_type in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            type_match = True
        elif expected_type == pl.Utf8 and actual_type == pl.Utf8:
            type_match = True
        elif expected_type == actual_type:
            type_match = True
        
        if not type_match:
            results.append({
                "rule": f"column_types.{col}",
                "status": "fail",
                "message": f"Column '{col}' has type {actual_type}, expected {expected_type_str}",
                "details": {
                    "column": col,
                    "expected_type": expected_type_str,
                    "actual_type": str(actual_type)
                }
            })
        else:
            results.append({
                "rule": f"column_types.{col}",
                "status": "pass",
                "message": f"Column '{col}' has correct type",
                "details": {"column": col, "type": expected_type_str}
            })
    
    return results


def check_value_ranges(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that numeric columns are within specified ranges."""
    value_ranges = rules.get("value_ranges", {})
    if not value_ranges:
        return []
    
    results = []
    for col, range_spec in value_ranges.items():
        if col not in df.columns:
            continue
        
        min_val = range_spec.get("min")
        max_val = range_spec.get("max")
        description = range_spec.get("description", f"{col} range check")
        
        try:
            # Cast to numeric
            df_numeric = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(f"{col}_numeric")
            )
            
            # Check for values outside range
            condition = (
                (pl.col(f"{col}_numeric").is_not_null()) &
                ((pl.col(f"{col}_numeric") < min_val) | (pl.col(f"{col}_numeric") > max_val))
            )
            
            out_of_range_count = df_numeric.filter(condition).height
            
            if out_of_range_count > 0:
                failing_rows = get_failing_rows(df, condition, max_failing_rows)
                results.append({
                    "rule": f"value_ranges.{col}",
                    "status": "fail",
                    "message": f"{out_of_range_count} rows have {col} outside range [{min_val}, {max_val}]",
                    "details": {
                        "column": col,
                        "min": min_val,
                        "max": max_val,
                        "out_of_range_count": out_of_range_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"value_ranges.{col}",
                    "status": "pass",
                    "message": description,
                    "details": {"column": col, "min": min_val, "max": max_val}
                })
        except Exception as e:
            results.append({
                "rule": f"value_ranges.{col}",
                "status": "error",
                "message": f"Error checking range for {col}: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def check_non_negative(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that specified columns have non-negative values."""
    non_negative_cols = rules.get("non_negative", [])
    if not non_negative_cols:
        return []
    
    results = []
    for col in non_negative_cols:
        if col not in df.columns:
            continue
        
        try:
            # Cast to numeric and check for negative values
            df_numeric = df.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(f"{col}_numeric")
            )
            
            condition = (
                pl.col(f"{col}_numeric").is_not_null() &
                (pl.col(f"{col}_numeric") < 0)
            )
            
            negative_count = df_numeric.filter(condition).height
            
            if negative_count > 0:
                failing_rows = get_failing_rows(df, condition, max_failing_rows)
                results.append({
                    "rule": f"non_negative.{col}",
                    "status": "fail",
                    "message": f"{negative_count} rows have negative {col}",
                    "details": {
                        "column": col,
                        "negative_count": negative_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"non_negative.{col}",
                    "status": "pass",
                    "message": f"All {col} values are non-negative",
                    "details": {"column": col}
                })
        except Exception as e:
            results.append({
                "rule": f"non_negative.{col}",
                "status": "error",
                "message": f"Error checking non-negative for {col}: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def check_duplicates_by(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check for duplicate rows based on specified column combinations."""
    duplicates_by = rules.get("duplicates_by", [])
    if not duplicates_by:
        return []
    
    results = []
    for dup_spec in duplicates_by:
        columns = dup_spec.get("columns", [])
        description = dup_spec.get("description", f"Duplicate check on {', '.join(columns)}")
        
        # Check if all columns exist
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            results.append({
                "rule": f"duplicates_by.{'+'.join(columns)}",
                "status": "error",
                "message": f"Missing columns for duplicate check: {missing_cols}",
                "details": {"missing_columns": missing_cols}
            })
            continue
        
        try:
            # Find duplicates
            duplicates = df.group_by(columns).agg(
                pl.count().alias("count")
            ).filter(pl.col("count") > 1)
            
            duplicate_count = duplicates.height
            
            if duplicate_count > 0:
                # Get sample failing rows
                failing_rows = duplicates.head(max_failing_rows).to_dicts()
                results.append({
                    "rule": f"duplicates_by.{'+'.join(columns)}",
                    "status": "fail",
                    "message": f"{duplicate_count} duplicate combinations found",
                    "details": {
                        "columns": columns,
                        "duplicate_combinations": duplicate_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"duplicates_by.{'+'.join(columns)}",
                    "status": "pass",
                    "message": description,
                    "details": {"columns": columns}
                })
        except Exception as e:
            results.append({
                "rule": f"duplicates_by.{'+'.join(columns)}",
                "status": "error",
                "message": f"Error checking duplicates: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def check_date_within_days(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> Optional[Dict]:
    """Check that dates are within specified number of days from today."""
    date_rule = rules.get("date_within_days")
    if not date_rule:
        return None
    
    column = date_rule.get("column")
    max_days = date_rule.get("max_days")
    description = date_rule.get("description", f"Date must be within {max_days} days")
    
    if not column or column not in df.columns:
        return None
    
    try:
        cutoff_date = datetime.now() - timedelta(days=max_days)
        
        # Try to parse date column
        df_with_dates = df.with_columns(
            pl.col(column).str.to_datetime().alias("date_parsed")
        )
        
        condition = pl.col("date_parsed") < cutoff_date
        old_dates_count = df_with_dates.filter(condition).height
        
        if old_dates_count > 0:
            failing_rows = get_failing_rows(df, condition, max_failing_rows)
            return {
                "rule": "date_within_days",
                "status": "fail",
                "message": f"{old_dates_count} rows have dates older than {max_days} days",
                "details": {
                    "column": column,
                    "max_days": max_days,
                    "cutoff_date": cutoff_date.isoformat(),
                    "old_dates_count": old_dates_count,
                    "failing_rows": failing_rows
                }
            }
        else:
            return {
                "rule": "date_within_days",
                "status": "pass",
                "message": description,
                "details": {"column": column, "max_days": max_days}
            }
    except Exception as e:
        return {
            "rule": "date_within_days",
            "status": "error",
            "message": f"Error checking date freshness: {str(e)}",
            "details": {"error": str(e)}
        }


def check_cash_leq_gross(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> Optional[Dict]:
    """Check that cash price is less than or equal to gross price."""
    cash_rule = rules.get("cash_leq_gross")
    if not cash_rule or not cash_rule.get("enabled"):
        return None
    
    cash_col = cash_rule.get("cash_column", "cash_price")
    gross_col = cash_rule.get("gross_column", "gross_price")
    description = cash_rule.get("description", "Cash price must be <= gross price")
    
    if cash_col not in df.columns or gross_col not in df.columns:
        return None
    
    try:
        # Cast to numeric
        df_numeric = df.with_columns([
            pl.col(cash_col).cast(pl.Float64, strict=False).alias("cash_numeric"),
            pl.col(gross_col).cast(pl.Float64, strict=False).alias("gross_numeric")
        ])
        
        condition = (
            pl.col("cash_numeric").is_not_null() &
            pl.col("gross_numeric").is_not_null() &
            (pl.col("cash_numeric") > pl.col("gross_numeric"))
        )
        
        invalid_count = df_numeric.filter(condition).height
        
        if invalid_count > 0:
            failing_rows = get_failing_rows(df, condition, max_failing_rows)
            return {
                "rule": "cash_leq_gross",
                "status": "fail",
                "message": f"{invalid_count} rows have cash_price > gross_price",
                "details": {
                    "cash_column": cash_col,
                    "gross_column": gross_col,
                    "invalid_count": invalid_count,
                    "failing_rows": failing_rows
                }
            }
        else:
            return {
                "rule": "cash_leq_gross",
                "status": "pass",
                "message": description,
                "details": {"cash_column": cash_col, "gross_column": gross_col}
            }
    except Exception as e:
        return {
            "rule": "cash_leq_gross",
            "status": "error",
            "message": f"Error checking cash <= gross: {str(e)}",
            "details": {"error": str(e)}
        }


def check_enum_values(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that columns contain only allowed values."""
    enum_values = rules.get("enum_values", {})
    if not enum_values:
        return []
    
    results = []
    for col, enum_spec in enum_values.items():
        if col not in df.columns:
            continue
        
        allowed = enum_spec.get("allowed", [])
        case_sensitive = enum_spec.get("case_sensitive", True)
        description = enum_spec.get("description", f"{col} must be one of allowed values")
        
        try:
            # Prepare allowed values
            if not case_sensitive:
                allowed_set = set(v.upper() for v in allowed)
                df_check = df.with_columns(
                    pl.col(col).str.to_uppercase().alias(f"{col}_upper")
                )
                condition = ~pl.col(f"{col}_upper").is_in(allowed_set)
            else:
                allowed_set = set(allowed)
                condition = ~pl.col(col).is_in(allowed_set)
            
            # Filter for non-null values that aren't in allowed set
            condition = pl.col(col).is_not_null() & condition
            
            if not case_sensitive:
                invalid_count = df_check.filter(condition).height
            else:
                invalid_count = df.filter(condition).height
            
            if invalid_count > 0:
                failing_rows = get_failing_rows(df, condition, max_failing_rows)
                results.append({
                    "rule": f"enum_values.{col}",
                    "status": "fail",
                    "message": f"{invalid_count} rows have invalid {col} values",
                    "details": {
                        "column": col,
                        "allowed_values": allowed,
                        "case_sensitive": case_sensitive,
                        "invalid_count": invalid_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"enum_values.{col}",
                    "status": "pass",
                    "message": description,
                    "details": {"column": col, "allowed_values": allowed}
                })
        except Exception as e:
            results.append({
                "rule": f"enum_values.{col}",
                "status": "error",
                "message": f"Error checking enum values for {col}: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def check_pattern_match(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that string columns match specified regex patterns."""
    pattern_match = rules.get("pattern_match", {})
    if not pattern_match:
        return []
    
    results = []
    for col, pattern_spec in pattern_match.items():
        if col not in df.columns:
            continue
        
        pattern = pattern_spec.get("pattern")
        description = pattern_spec.get("description", f"{col} must match pattern")
        
        try:
            # Use Polars regex matching
            condition = (
                pl.col(col).is_not_null() &
                (~pl.col(col).str.contains(f"^{pattern}$"))
            )
            
            invalid_count = df.filter(condition).height
            
            if invalid_count > 0:
                failing_rows = get_failing_rows(df, condition, max_failing_rows)
                results.append({
                    "rule": f"pattern_match.{col}",
                    "status": "fail",
                    "message": f"{invalid_count} rows have {col} not matching pattern",
                    "details": {
                        "column": col,
                        "pattern": pattern,
                        "invalid_count": invalid_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"pattern_match.{col}",
                    "status": "pass",
                    "message": description,
                    "details": {"column": col, "pattern": pattern}
                })
        except Exception as e:
            results.append({
                "rule": f"pattern_match.{col}",
                "status": "error",
                "message": f"Error checking pattern for {col}: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def check_not_null(df: pl.DataFrame, rules: Dict, max_failing_rows: int) -> List[Dict]:
    """Check that specified columns do not contain null values."""
    not_null_cols = rules.get("not_null", [])
    if not not_null_cols:
        return []
    
    results = []
    for col in not_null_cols:
        if col not in df.columns:
            continue
        
        try:
            condition = pl.col(col).is_null()
            null_count = df.filter(condition).height
            
            if null_count > 0:
                failing_rows = get_failing_rows(df, condition, max_failing_rows)
                results.append({
                    "rule": f"not_null.{col}",
                    "status": "fail",
                    "message": f"{null_count} rows have null {col}",
                    "details": {
                        "column": col,
                        "null_count": null_count,
                        "failing_rows": failing_rows
                    }
                })
            else:
                results.append({
                    "rule": f"not_null.{col}",
                    "status": "pass",
                    "message": f"No null values in {col}",
                    "details": {"column": col}
                })
        except Exception as e:
            results.append({
                "rule": f"not_null.{col}",
                "status": "error",
                "message": f"Error checking nulls for {col}: {str(e)}",
                "details": {"error": str(e)}
            })
    
    return results


def run_rules(parquet_path: str, registry_path: str, profile: Optional[Literal["cms_csv", "simple_csv"]] = None) -> Dict[str, Any]:
    """Run comprehensive compliance rules against a Parquet file.
    
    Args:
        parquet_path: Path to the Parquet file to validate
        registry_path: Path to the rules registry YAML file
        profile: Optional profile hint ("cms_csv" or "simple_csv")
        
    Returns:
        Dict containing rule results and validation summary
    """
    try:
        # Load the rules
        rules = load_rules_registry(registry_path)
        
        # Use validate_csv_run to get actual columns (CMS CSV analyzer preferred)
        run_id = os.path.basename(parquet_path).replace('.parquet', '')
        csv_path = parquet_path.replace('.parquet', '.csv')
        csv_result = validate_csv_run(run_id, parquet_path, csv_path if os.path.exists(csv_path) else None)
        
        # Extract the actual columns and metadata
        actual_cols = csv_result["present_columns"]
        detected_profile = csv_result["detected_profile"]
        header_row = csv_result["detected_header_row"]
        headers = csv_result["detected_headers"]
        
        # Check if we have CMS CSV results
        cms_csv_result = csv_result.get("cms_csv_result")
        
        # Load the Parquet file
        df = pl.read_parquet(parquet_path)
        
        # Use the detected profile from actual columns
        if profile is None:
            profile = detected_profile
        
        # Get column mapping for this profile using actual columns
        column_mapping = map_to_internal(actual_cols, profile)
        
        # Create a mapped DataFrame with internal column names (for rule evaluation)
        mapped_df = df
        if profile == "cms_csv" and column_mapping:
            # Rename columns to internal schema for rule evaluation
            rename_dict = {v: k for k, v in column_mapping.items() if v in df.columns}
            if rename_dict:
                mapped_df = df.rename(rename_dict)
        
        # Get error reporting configuration
        error_config = rules.get("error_reporting", {})
        max_failing_rows = error_config.get("max_failing_rows_per_rule", 5)
        
        # Initialize results
        results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "rules_version": rules.get("version", "1.0.0"),
            "file_path": parquet_path,
            "total_rows": len(df),
            "profile": profile,
            "profile_description": get_profile_description(profile),
            "column_mapping": column_mapping,
            "detected_profile": detected_profile,
            "detected_header_row": header_row,
            "detected_headers": headers,
            "schema_ok": None,  # Will be set based on header validation
            "checks": [],
            "summary": {
                "total_checks": 0,
                "passed": 0,
                "failed": 0,
                "errors": 0
            }
        }
        
        # Add CMS CSV information if available
        if cms_csv_result:
            results["cms_csv"] = cms_csv_result
        
        # Run all rule checks
        all_checks = []
        
        # 1. Profile-aware header check using actual columns
        header_check = check_profile_headers(actual_cols, profile, rules, max_failing_rows)
        all_checks.append(header_check)
        
        # Set schema_ok based on header validation
        results["schema_ok"] = (header_check.get("status") == "pass")
        
        # 2. Column types (use mapped_df for CMS, original df for simple)
        all_checks.extend(check_column_types(mapped_df, rules, max_failing_rows))
        
        # 3. Value ranges
        all_checks.extend(check_value_ranges(mapped_df, rules, max_failing_rows))
        
        # 4. Non-negative values
        all_checks.extend(check_non_negative(mapped_df, rules, max_failing_rows))
        
        # 5. Duplicates check (use original df to check actual columns)
        all_checks.extend(check_duplicates_by(mapped_df, rules, max_failing_rows))
        
        # 6. Date within days
        date_check = check_date_within_days(mapped_df, rules, max_failing_rows)
        if date_check:
            all_checks.append(date_check)
        
        # 7. Cash <= Gross
        cash_check = check_cash_leq_gross(mapped_df, rules, max_failing_rows)
        if cash_check:
            all_checks.append(cash_check)
        
        # 8. Enum values
        all_checks.extend(check_enum_values(mapped_df, rules, max_failing_rows))
        
        # 9. Pattern matching
        all_checks.extend(check_pattern_match(mapped_df, rules, max_failing_rows))
        
        # 10. Not null
        all_checks.extend(check_not_null(mapped_df, rules, max_failing_rows))
        
        # Compile results
        results["checks"] = all_checks
        
        # Calculate summary
        for check in all_checks:
            results["summary"]["total_checks"] += 1
            status = check.get("status", "error")
            if status == "pass":
                results["summary"]["passed"] += 1
            elif status == "fail":
                results["summary"]["failed"] += 1
            elif status == "error":
                results["summary"]["errors"] += 1
        
        return results
        
    except Exception as e:
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "rules_version": "1.0.0",
            "file_path": parquet_path,
            "profile": profile if profile else "unknown",
            "profile_description": get_profile_description(profile) if profile else "Unknown",
            "schema_ok": False,
            "error": f"Validation failed: {str(e)}",
            "checks": [],
            "summary": {
                "total_checks": 0,
                "passed": 0,
                "failed": 0,
                "errors": 1
            }
        }
