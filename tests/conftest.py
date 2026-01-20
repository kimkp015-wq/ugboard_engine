"""
Test fixtures and configuration
"""
import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
from main import app

@pytest.fixture
def client():
    """Test client fixture"""
    return TestClient(app)

@pytest.fixture
def temp_data_dir():
    """Temporary data directory for tests"""
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()
        
        # Create minimal data files
        with open(data_dir / "songs.json", 'w') as f:
            json.dump([], f)
        
        with open(data_dir / "regions.json", 'w') as f:
            json.dump({
                "ug": {"name": "Uganda", "songs": []},
                "ke": {"name": "Kenya", "songs": []}
            }, f)
        
        with open(data_dir / "chart_history.json", 'w') as f:
            json.dump([], f)
        
        yield data_dir

@pytest.fixture
def sample_song_data():
    """Sample song data for testing"""
    return {
        "title": "Nalumansi",
        "artist": "Bobi Wine",
        "plays": 10000,
        "score": 95.5,
        "region": "ug",
        "station": "NTV Uganda"
    }

@pytest.fixture
def mock_database():
    """Mock database for testing"""
    mock_db = Mock()
    mock_db.songs = []
    mock_db.get_top_songs.return_value = []
    mock_db.get_trending_songs.return_value = []
    return mock_db
