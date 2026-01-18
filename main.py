# Add this import at the top if not already there
from typing import Dict, List

# Add this validation function (after MusicRules class)
def validate_song_item(item: Dict) -> Dict:
    """Validate song item with Ugandan music rules"""
    required = ["title", "artist"]
    for field in required:
        if field not in item:
            raise ValueError(f"Missing required field: {field}")
    
    # Extract artists and validate Ugandan music rules
    artists = MusicRules.extract_artist_list(item["artist"])
    is_valid, error_msg = MusicRules.validate_artists(artists)
    if not is_valid:
        raise ValueError(f"Artist validation failed: {error_msg}")
    
    # Add artist metadata
    item["artist_metadata"] = {
        "artists_list": artists,
        "artist_types": [MusicRules.get_artist_type(a) for a in artists],
        "is_collaboration": len(artists) > 1,
        "has_ugandan_artist": any(MusicRules.is_ugandan_artist(a) for a in artists),
        "has_foreign_artist": any(not MusicRules.is_ugandan_artist(a) for a in artists)
    }
    
    # Add defaults
    item.setdefault("id", str(uuid.uuid4())[:8])
    item.setdefault("score", 0.0)
    item.setdefault("plays", 0)
    item.setdefault("change", "same")
    item.setdefault("genre", "afrobeat")
    item.setdefault("region", "ug")
    item.setdefault("release_date", datetime.utcnow().date().isoformat())
    
    return item

def validate_ingestion_payload(payload: Dict) -> Dict:
    """Validate ingestion payload with Ugandan music rules"""
    if "items" not in payload:
        raise ValueError("Missing 'items' field")
    
    if not isinstance(payload["items"], list):
        raise ValueError("'items' must be a list")
    
    if len(payload["items"]) == 0:
        raise ValueError("'items' list cannot be empty")
    
    # Validate each song item
    for i, item in enumerate(payload["items"]):
        try:
            payload["items"][i] = validate_song_item(item)
        except ValueError as e:
            raise ValueError(f"Item {i} validation failed: {e}")
    
    payload.setdefault("timestamp", datetime.utcnow().isoformat())
    payload.setdefault("metadata", {})
    payload.setdefault("source", "unknown")
    
    return payload

# Add this endpoint (after the radio ingestion endpoint)
@app.post("/ingest/tv")
async def ingest_tv(
    payload: Dict = Body(...),
    x_internal_token: Optional[str] = Header(None)
):
    """Ingest TV data with Ugandan artist validation"""
    
    # Verify internal token
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    
    try:
        # Validate the entire payload
        validated = validate_ingestion_payload(payload)
        
        # TV-specific validation - require channel field
        for item in validated["items"]:
            if "channel" not in item:
                raise ValueError("TV items must include 'channel' field")
        
        # Ugandan music validation
        validation_report = {
            "total_items": len(validated["items"]),
            "valid_items": 0,
            "artist_breakdown": {
                "ugandan_only": 0,
                "collaborations": 0,
                "invalid_foreign": 0
            }
        }
        
        valid_items = []
        for i, item in enumerate(validated["items"]):
            artists = item.get("artist_metadata", {}).get("artists_list", [])
            has_ugandan = any(MusicRules.is_ugandan_artist(a) for a in artists)
            has_foreign = any(not MusicRules.is_ugandan_artist(a) for a in artists)
            
            if has_ugandan:
                validation_report["valid_items"] += 1
                valid_items.append(item)
                
                if has_foreign:
                    validation_report["artist_breakdown"]["collaborations"] += 1
                else:
                    validation_report["artist_breakdown"]["ugandan_only"] += 1
            else:
                validation_report["artist_breakdown"]["invalid_foreign"] += 1
        
        # Log the ingestion
        log_entry = data_store.log_ingestion("tv", len(valid_items), valid_items)
        
        return {
            "status": "success",
            "message": f"Ingested {len(valid_items)} TV items with Ugandan artist validation",
            "source": "tv",
            "items_processed": len(valid_items),
            "validation_passed": True,
            "validation_report": validation_report,
            "timestamp": datetime.utcnow().isoformat(),
            "log_entry": log_entry,
            "instance": INSTANCE_ID
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )
