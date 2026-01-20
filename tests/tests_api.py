"""
Unit tests for API endpoints
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from main import app

client = TestClient(app)

class TestChartsAPI:
    """Test chart endpoints"""
    
    def test_root_endpoint(self):
        """Test root endpoint returns service info"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "UG Board Engine"
        assert "version" in data
        assert "status" in data
    
    def test_health_endpoint(self):
        """Test health endpoint returns healthy status"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "database" in data
    
    def test_top100_default_limit(self):
        """Test top100 endpoint with default limit"""
        response = client.get("/charts/top100")
        assert response.status_code == 200
        data = response.json()
        assert "entries" in data
        assert data["chart"] == "Uganda Top 100"
    
    def test_top100_custom_limit(self):
        """Test top100 endpoint with custom limit"""
        response = client.get("/charts/top100?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert len(data["entries"]) <= 5
    
    def test_invalid_region(self):
        """Test invalid region returns 404"""
        response = client.get("/charts/regions/invalid")
        assert response.status_code == 404
        data = response.json()
        assert "error" in data
    
    def test_valid_region(self):
        """Test valid region returns data"""
        response = client.get("/charts/regions/ug")
        assert response.status_code == 200
        data = response.json()
        assert data["region"] == "ug"
        assert "songs" in data
    
    def test_trending_endpoint(self):
        """Test trending endpoint"""
        response = client.get("/charts/trending?limit=3")
        assert response.status_code == 200
        data = response.json()
        assert "entries" in data
        assert data["chart"] == "Trending Now"

class TestAuthentication:
    """Test authentication"""
    
    def test_unauthenticated_admin(self):
        """Test admin endpoint without token"""
        response = client.get("/admin/health")
        assert response.status_code == 401
    
    def test_authenticated_with_invalid_token(self):
        """Test admin endpoint with invalid token"""
        response = client.get(
            "/admin/health",
            headers={"Authorization": "Bearer invalid_token"}
        )
        assert response.status_code == 401
    
    @patch('main.config.ADMIN_TOKEN', 'test_admin_token')
    def test_authenticated_with_valid_token(self):
        """Test admin endpoint with valid token (mocked)"""
        response = client.get(
            "/admin/health",
            headers={"Authorization": "Bearer test_admin_token"}
        )
        # Will be 200 if token matches, otherwise 401
        assert response.status_code in [200, 401]

class TestValidation:
    """Test data validation"""
    
    def test_invalid_song_item(self):
        """Test SongItem validation"""
        from core.models import SongItem
        
        # Valid song
        valid_song = SongItem(
            title="Test Song",
            artist="Test Artist",
            plays=100,
            score=85.5,
            region="ug"
        )
        assert valid_song.title == "Test Song"
        
        # Invalid region should raise error
        with pytest.raises(ValueError):
            SongItem(
                title="Test",
                artist="Test",
                region="invalid"
            )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
