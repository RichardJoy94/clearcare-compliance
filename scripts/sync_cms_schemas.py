#!/usr/bin/env python3
"""
Sync script to create CMS Hospital Price Transparency JSON schemas
based on CMS requirements and vendor them locally.
"""

import json
from pathlib import Path

SCHEMAS_DIR = Path(__file__).parent.parent / "rules" / "cms" / "json"

def create_in_network_rates_schema():
    """Create the in-network rates schema based on CMS requirements."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "In-Network Rates",
        "description": "Schema for in-network rate information for hospital price transparency",
        "type": "object",
        "required": ["provider_references", "in_network"],
        "properties": {
            "provider_references": {
                "type": "array",
                "description": "Array of provider reference objects",
                "items": {
                    "type": "object",
                    "required": ["provider_group_id", "provider_groups"],
                    "properties": {
                        "provider_group_id": {
                            "type": "string",
                            "description": "Unique identifier for the provider group"
                        },
                        "provider_groups": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["npi"],
                                "properties": {
                                    "npi": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "National Provider Identifier numbers"
                                    },
                                    "tin": {
                                        "type": "object",
                                        "properties": {
                                            "type": {"type": "string"},
                                            "value": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "in_network": {
                "type": "array",
                "description": "Array of in-network rate information",
                "items": {
                    "type": "object",
                    "required": ["negotiated_rates"],
                    "properties": {
                        "negotiated_rates": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["provider_references", "negotiated_prices"],
                                "properties": {
                                    "provider_references": {
                                        "type": "array",
                                        "items": {"type": "integer"}
                                    },
                                    "negotiated_prices": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "required": ["negotiated_type", "negotiated_rate"],
                                            "properties": {
                                                "negotiated_type": {
                                                    "type": "string",
                                                    "enum": ["negotiated", "fee schedule", "percentage"]
                                                },
                                                "negotiated_rate": {
                                                    "type": "number",
                                                    "minimum": 0
                                                },
                                                "expiration_date": {
                                                    "type": "string",
                                                    "format": "date"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "billing_code_type": {
                            "type": "string",
                            "enum": ["CPT", "HCPCS", "DRG", "ICD10", "MS-DRG"]
                        },
                        "billing_code_type_version": {
                            "type": "string"
                        },
                        "billing_code": {
                            "type": "string"
                        }
                    }
                }
            }
        }
    }

def create_allowed_amounts_schema():
    """Create the allowed amounts schema based on CMS requirements."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Allowed Amounts",
        "description": "Schema for allowed amount information for hospital price transparency",
        "type": "object",
        "required": ["provider_references", "allowed_amounts"],
        "properties": {
            "provider_references": {
                "type": "array",
                "description": "Array of provider reference objects",
                "items": {
                    "type": "object",
                    "required": ["provider_group_id", "provider_groups"],
                    "properties": {
                        "provider_group_id": {
                            "type": "string"
                        },
                        "provider_groups": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["npi"],
                                "properties": {
                                    "npi": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    },
                                    "tin": {
                                        "type": "object",
                                        "properties": {
                                            "type": {"type": "string"},
                                            "value": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "allowed_amounts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["provider_references", "allowed_amounts"],
                    "properties": {
                        "provider_references": {
                            "type": "array",
                            "items": {"type": "integer"}
                        },
                        "allowed_amounts": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["payer_name", "allowed_amount"],
                                "properties": {
                                    "payer_name": {"type": "string"},
                                    "allowed_amount": {
                                        "type": "number",
                                        "minimum": 0
                                    },
                                    "billing_class": {
                                        "type": "string",
                                        "enum": ["professional", "facility", "institutional"]
                                    }
                                }
                            }
                        },
                        "billing_code_type": {
                            "type": "string",
                            "enum": ["CPT", "HCPCS", "DRG", "ICD10", "MS-DRG"]
                        },
                        "billing_code_type_version": {"type": "string"},
                        "billing_code": {"type": "string"}
                    }
                }
            }
        }
    }

def create_provider_reference_schema():
    """Create the provider reference schema based on CMS requirements."""
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Provider Reference",
        "description": "Schema for provider reference information",
        "type": "object",
        "required": ["provider_group_id", "provider_groups"],
        "properties": {
            "provider_group_id": {
                "type": "string",
                "description": "Unique identifier for the provider group"
            },
            "provider_groups": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["npi"],
                    "properties": {
                        "npi": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "pattern": "^[0-9]{10}$"
                            },
                            "description": "National Provider Identifier numbers (10 digits)"
                        },
                        "tin": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["ein", "npi"]
                                },
                                "value": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
    }

def sync_schemas():
    """Create all CMS schemas locally."""
    print(f"Creating CMS schemas in {SCHEMAS_DIR}")
    
    # Ensure directory exists
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    
    schemas = {
        "in-network-rates.schema.json": create_in_network_rates_schema(),
        "allowed-amounts.schema.json": create_allowed_amounts_schema(),
        "provider-reference.schema.json": create_provider_reference_schema()
    }
    
    for schema_name, schema_data in schemas.items():
        local_path = SCHEMAS_DIR / schema_name
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=2)
        
        print(f"[OK] Created {schema_name}")
    
    print("Schema creation completed!")

if __name__ == "__main__":
    sync_schemas()
