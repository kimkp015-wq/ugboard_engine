# test_oas31.py
import pytest
from fastapi.testclient import TestClient
from app.main import app
import json

client = TestClient(app)

def test_openapi_version():
    """Test that OpenAPI version is 3.1.0"""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    
    schema = response.json()
    assert schema["openapi"] == "3.1.0", f"Expected 3.1.0, got {schema['openapi']}"
    
    # Check for OAS 3.1 features
    assert "webhooks" in schema, "Webhooks should be present in OAS 3.1"
    
    # Check JSON Schema version
    if "components" in schema and "schemas" in schema["components"]:
        first_schema = list(schema["components"]["schemas"].values())[0]
        assert "$schema" in first_schema, "Should have $schema in OAS 3.1"

def test_oas31_features():
    """Test specific OAS 3.1 features"""
    response = client.get("/openapi.json")
    schema = response.json()
    
    # Test webhooks
    assert "scraper-completed" in schema.get("webhooks", {})
    
    # Test discriminators (if using)
    if "components" in schema and "schemas" in schema["components"]:
        for name, component in schema["components"]["schemas"].items():
            if "discriminator" in component:
                print(f"✅ Found discriminator in {name}: {component['discriminator']}")

def test_api_endpoints():
    """Test that endpoints still work with OAS 3.1"""
    # Test YouTube ingest
    response = client.post(
        "/ingest/youtube",
        json={
            "platform": "youtube",
            "token": "test_token_123",
            "client_ip": "192.168.1.1"
        }
    )
    assert response.status_code in [200, 401], f"Unexpected status: {response.status_code}"
