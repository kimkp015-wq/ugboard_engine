# Replace the fingerprinting section in tv_scraper.py

class TVScraperEngine:
    def __init__(self, config_path: str = "config/tv_stations.yaml"):
        # ... existing code ...
        self.fingerprinter = UgandanMusicFingerprinter()
    
    def identify_song(self, audio_file: str) -> Optional[Dict[str, Any]]:
        """
        Identify song using custom fingerprinting
        """
        if not os.path.exists(audio_file):
            return None
        
        # Extract fingerprint
        fingerprint = self.fingerprinter.extract_fingerprint(audio_file)
        if not fingerprint:
            return None
        
        # Try to find match in database
        match = self.fingerprinter.find_match(fingerprint, threshold=0.6)
        
        if match:
            song_title, artist, confidence = match
            return {
                "song_title": song_title,
                "artist": artist,
                "confidence": confidence,
                "source": "custom_fingerprinting",
                "fingerprint_match": True
            }
        
        # Fallback to ACRCloud if available
        if self.acrcloud_available:
            return self._identify_acrcloud(audio_file)
        
        return None
    
    def store_reference_tracks(self, reference_dir: str = "data/reference_tracks"):
        """
        Store known Ugandan songs for matching
        Run this once to populate the database
        """
        import glob
        
        for audio_file in glob.glob(f"{reference_dir}/**/*.mp3", recursive=True):
            try:
                # Extract metadata from filename (format: Artist - Title.mp3)
                filename = Path(audio_file).stem
                if " - " in filename:
                    artist, song_title = filename.split(" - ", 1)
                else:
                    artist, song_title = "Unknown", filename
                
                # Extract fingerprint
                fingerprint = self.fingerprinter.extract_fingerprint(audio_file)
                if fingerprint:
                    # Store in database
                    self.fingerprinter.store_fingerprint(
                        fingerprint, song_title, artist
                    )
                    logger.info(f"Stored reference: {artist} - {song_title}")
                    
            except Exception as e:
                logger.error(f"Failed to process {audio_file}: {e}")
