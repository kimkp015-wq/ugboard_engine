# Alternative fingerprinting using Chromaprint (AcoustID)
import acoustid
import hashlib
import tempfile

class ChromaprintIdentifier:
    """Production-grade audio fingerprinting using Chromaprint"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ACOUSTID_API_KEY", "")
        
    def fingerprint_audio(self, audio_file: str) -> Optional[str]:
        """Generate fingerprint using Chromaprint"""
        try:
            # Generate fingerprint using ffmpeg + chromaprint
            duration, fp = acoustid.fingerprint_file(audio_file)
            
            # Create hash of fingerprint for database lookup
            fp_hash = hashlib.sha256(fp.encode()).hexdigest()
            
            return {
                "fingerprint": fp,
                "fingerprint_hash": fp_hash,
                "duration": duration,
                "source": "chromaprint"
            }
            
        except Exception as e:
            logger.error(f"Chromaprint fingerprinting failed: {e}")
            return None
    
    def identify_song(self, audio_file: str) -> Optional[Dict]:
        """Identify song using AcoustID API"""
        if not self.api_key:
            logger.warning("AcoustID API key not configured")
            return None
        
        try:
            # Send to AcoustID API
            results = acoustid.lookup(self.api_key, audio_file)
            
            if results and results.get("results"):
                best_match = results["results"][0]
                if best_match.get("score", 0) > 0.5:  # 50% confidence threshold
                    recordings = best_match.get("recordings", [])
                    if recordings:
                        recording = recordings[0]
                        return {
                            "song_title": recording.get("title", "Unknown"),
                            "artist": recording.get("artists", [{}])[0].get("name", "Unknown"),
                            "confidence": best_match.get("score", 0),
                            "duration": recording.get("duration", 0),
                            "source": "acoustid_api",
                            "metadata": {
                                "recording_id": recording.get("id"),
                                "release_id": recording.get("releases", [{}])[0].get("id", "")
                            }
                        }
                        
        except Exception as e:
            logger.error(f"AcoustID identification failed: {e}")
            
        return None
