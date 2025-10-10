#!/usr/bin/env python3
"""
Vendor CMS Hospital Price Transparency JSON schemas and build manifest.
"""

import subprocess
import tempfile
import shutil
import json
import hashlib
import os
import sys
import time
import re
import pathlib

# Paths
ROOT = pathlib.Path(__file__).resolve().parents[1]
DEST_JSON = ROOT / "clearcare_compliance" / "schemas" / "json"
MANIFEST = DEST_JSON / "VERSION.json"
CMS_REPO = "https://github.com/CMSgov/hospital-price-transparency.git"

def sha256(fp: pathlib.Path) -> str:
    """Calculate SHA256 hash of a file."""
    with open(fp, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def run(cmd, cwd=None):
    """Run a shell command."""
    print(f"+ {' '.join(cmd)}")
    subprocess.check_call(cmd, cwd=cwd)

def main():
    """Main vendor function."""
    print("Vendoring CMS Hospital Price Transparency schemas...")
    
    # Create temp directory
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="cms_vendor_"))
    try:
        # Clone CMS repo
        run(["git", "clone", "--depth", "1", CMS_REPO, str(tmp)])
        
        # Find schema files
        candidates = list(tmp.rglob("*.schema.json"))
        print(f"Found {len(candidates)} potential schema files")
        
        # Process schemas
        files = {}
        for fp in candidates:
            name = fp.name
            if "hospital" in name.lower() and "schema" in name.lower():
                # Extract version
                version_match = re.search(r"v(\d+\.\d+\.\d+)", name, re.I)
                version = version_match.group(1) if version_match else "unknown"
                
                # Copy to destination
                dest = DEST_JSON / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(fp, dest)
                
                # Calculate checksum
                checksum = sha256(dest)
                
                # Store in files dict
                if version not in files:
                    files[version] = {}
                files[version][name] = checksum
                
                print(f"  {name} -> {version}")
        
        # Determine latest version
        if files:
            latest = max(files.keys(), key=lambda v: tuple(int(x) for x in re.findall(r"\d+", v)))
        else:
            latest = None
        
        # Create manifest
        manifest = {
            "latest": latest,
            "files": files,
            "source": CMS_REPO,
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp).decode().strip(),
            "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        
        # Write manifest
        MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Manifest written to {MANIFEST}")
        
        if latest:
            print(f"Latest version: {latest}")
            print(f"Files: {sum(len(v) for v in files.values())}")
        else:
            print("No schemas found")
            
    finally:
        # Cleanup
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
