# migrate_to_oas31.py
import json
from pathlib import Path

def migrate_openapi_schema(input_file: str, output_file: str):
    """Migrate OpenAPI 3.0.x schema to 3.1.0"""
    
    with open(input_file, 'r') as f:
        schema = json.load(f)
    
    # 1. Update version
    schema["openapi"] = "3.1.0"
    
    # 2. Add $schema references if not present
    if "components" in schema and "schemas" in schema["components"]:
        for name, component in schema["components"]["schemas"].items():
            if "$schema" not in component:
                component["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    
    # 3. Convert nullable to type array (if using old format)
    def update_nullable(obj):
        if isinstance(obj, dict):
            if "nullable" in obj and obj["nullable"]:
                # Convert to type array
                if "type" in obj:
                    if isinstance(obj["type"], list):
                        if "null" not in obj["type"]:
                            obj["type"].append("null")
                    else:
                        obj["type"] = [obj["type"], "null"]
                del obj["nullable"]
            
            # Recursively update nested objects
            for key, value in obj.items():
                obj[key] = update_nullable(value)
        
        elif isinstance(obj, list):
            return [update_nullable(item) for item in obj]
        
        return obj
    
    schema = update_nullable(schema)
    
    # 4. Add webhooks section if needed
    if "webhooks" not in schema:
        schema["webhooks"] = {}
    
    # 5. Save migrated schema
    with open(output_file, 'w') as f:
        json.dump(schema, f, indent=2)
    
    print(f"✅ Migrated {input_file} to OAS 3.1.0")
    print(f"📁 Output: {output_file}")
    
    return schema

# Usage
if __name__ == "__main__":
    migrate_openapi_schema("openapi.json", "openapi-3.1.0.json")
