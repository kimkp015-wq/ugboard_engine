"""
Unit tests for health check endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_check():
    """Test basic health check endpoint."""
    response = client.get("/health")
    
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    
def test_api_health_check():
    """Test API health check endpoint."""
    response = client.get("/api/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert data["service"] == "ugboard-engine"
    
def test_detailed_health_check():
    """Test detailed health check endpoint."""
    response = client.get("/api/health/detailed")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "dependencies" in data
    
def test_root_endpoint():
    """Test root endpoint."""
    response = client.get("/")
    
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "UG Board Engine API"
    assert "health" in data
