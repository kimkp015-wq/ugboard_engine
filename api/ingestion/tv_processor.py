"""
tv_processor.py - Process and send TV data to UG Board Engine
"""

import json
import logging
import os
from typing import Dict, Optional
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

class TVIngestionProcessor:
    """Handle TV data processing and UG Board Engine integration"""
    
    def __init__(self, ugboard_url: Optional[str] = None):
        self.ugboard_url = ugboard_url or os.getenv("UGBOARD_API_URL", "http://localhost:8000")
        self.internal_token = os.getenv("INTERNAL_TOKEN", "1994199620002019866")
        
    def validate_tv_item(self, item: Dict) -> tuple[bool, str]:
        """Validate TV song item against UG Board rules"""
        from api.ingestion.tv import MusicRules  # Import your existing rules
        
        # Basic validation
        required_fields = ["title", "artist", "channel"]
        for field in required_fields:
            if field not in item:
                return False, f"Missing required field: {field}"
        
        # Artist validation using existing MusicRules
        artists = MusicRules.extract_artist_list(item["artist"])
        is_valid, error_msg = MusicRules.validate_artists(artists)
        
        if not is_valid:
            return False, f"Artist validation failed: {error_msg}"
        
        # Add artist metadata
        item["artist_metadata"] = {
            "artists_list": artists,
            "artist_types": [MusicRules.get_artist_type(a) for a in artists],
            "is_collaboration": len(artists) > 1,
            "has_ugandan_artist": any(MusicRules.is_ugandan_artist(a) for a in artists),
            "has_foreign_artist": any(not MusicRules.is_ugandan_artist(a) for a in artists)
        }
        
        # Ensure required fields for UG Board
        item.setdefault("id", f"tv_{int(datetime.now().timestamp())}")
        item.setdefault("score", item.get("score", 0.0))
        item.setdefault("plays", item.get("plays", 1))
        item.setdefault("change", "same")
        item.setdefault("genre", item.get("metadata", {}).get("genre", "afrobeat"))
        item.setdefault("region", "ug")
        item.setdefault("release_date", datetime.now().date().isoformat())
        
        return True, ""
    
    def prepare_payload(self, tv_item: Dict) -> Dict:
        """Prepare payload for UG Board ingestion"""
        # Validate the item
        is_valid, error_msg = self.validate_tv_item(tv_item)
        if not is_valid:
            raise ValueError(f"Invalid TV item: {error_msg}")
        
        # Create payload structure
        payload = {
            "items": [tv_item],
            "source": "tv_scraper",
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "scraper_version": "2.0.0",
                "detection_method": tv_item.get("metadata", {}).get("detection_method", "unknown"),
                "confidence": tv_item.get("metadata", {}).get("confidence", 0.0)
            }
        }
        
        return payload
    
    def send_to_ugboard(self, tv_item: Dict) -> bool:
        """Send TV data to UG Board Engine API"""
        try:
            # Prepare payload
            payload = self.prepare_payload(tv_item)
            
            # Send to UG Board
            response = requests.post(
                f"{self.ugboard_url}/ingest/tv",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": self.internal_token
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully ingested TV data: {result.get('message')}")
                return True
            else:
                logger.error(f"UG Board API error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error sending to UG Board: {e}")
            return False
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending to UG Board: {e}")
            return False
    
    def batch_send(self, tv_items: list) -> Dict:
        """Send multiple TV items in batch"""
        valid_items = []
        invalid_items = []
        
        for item in tv_items:
            try:
                payload = self.prepare_payload(item)
                valid_items.append(payload["items"][0])
            except ValueError as e:
                invalid_items.append({"item": item, "error": str(e)})
        
        if not valid_items:
            return {"success": False, "message": "No valid items to send"}
        
        # Create batch payload
        batch_payload = {
            "items": valid_items,
            "source": "tv_scraper_batch",
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": {
                "scraper_version": "2.0.0",
                "total_items": len(tv_items),
                "valid_items": len(valid_items),
                "invalid_items": len(invalid_items)
            }
        }
        
        try:
            response = requests.post(
                f"{self.ugboard_url}/ingest/tv",
                json=batch_payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": self.internal_token
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                result["invalid_items"] = invalid_items
                return {"success": True, **result}
            else:
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "error": response.text
                }
                
        except Exception as e:
            return {"success": False, "error": str(e)}
