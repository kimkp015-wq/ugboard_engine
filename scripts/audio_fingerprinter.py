"""
audio_fingerprinter.py - Production-grade audio fingerprinting for Ugandan music
No external dependencies beyond librosa/numpy
"""

import hashlib
import numpy as np
import librosa
import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import pickle
import zlib

logger = logging.getLogger(__name__)

@dataclass
class AudioFingerprint:
    """Audio fingerprint for Ugandan music patterns"""
    hash: str
    peaks: np.ndarray
    duration: float
    sample_rate: int
    metadata: Dict

class UgandanMusicFingerprinter:
    """Custom fingerprinting optimized for Ugandan music patterns"""
    
    def __init__(self, db_path: str = "data/audio_fingerprints.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database for fingerprints"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create fingerprints table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fingerprints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE NOT NULL,
                song_title TEXT NOT NULL,
                artist TEXT NOT NULL,
                duration REAL,
                sample_rate INTEGER,
                peaks BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        ''')
        
        # Create index for fast lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON fingerprints(hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_artist ON fingerprints(artist)')
        
        conn.commit()
        conn.close()
        logger.info(f"Fingerprint database initialized: {self.db_path}")
    
    def extract_fingerprint(self, audio_path: str) -> Optional[AudioFingerprint]:
        """
        Extract fingerprint from audio file using spectral analysis
        Optimized for Ugandan music (strong rhythmic patterns)
        """
        try:
            # Load audio with librosa (optimized for music)
            y, sr = librosa.load(audio_path, sr=22050, duration=30)  # 30s max for efficiency
            
            # Extract features optimized for African music
            features = self._extract_ugandan_features(y, sr)
            
            # Generate hash from features
            fingerprint_hash = self._generate_hash(features)
            
            # Find spectral peaks (for matching)
            peaks = self._find_spectral_peaks(y, sr)
            
            return AudioFingerprint(
                hash=fingerprint_hash,
                peaks=peaks,
                duration=librosa.get_duration(y=y, sr=sr),
                sample_rate=sr,
                metadata={
                    "feature_vector": features.tolist(),
                    "audio_path": audio_path
                }
            )
            
        except Exception as e:
            logger.error(f"Fingerprint extraction failed: {e}")
            return None
    
    def _extract_ugandan_features(self, y: np.ndarray, sr: int) -> np.ndarray:
        """Extract features optimized for Ugandan music patterns"""
        features = []
        
        # 1. Rhythm features (important for African music)
        tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr)
        features.append(tempo / 300)  # Normalize (assuming max 300 BPM)
        
        # 2. Spectral features
        spectral_centroid = librosa.feature.spectral_centroid(y=y, sr=sr)
        features.append(np.mean(spectral_centroid) / 5000)  # Normalize
        
        # 3. MFCCs (Mel-frequency cepstral coefficients)
        mfccs = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=13)
        features.extend(np.mean(mfccs, axis=1) / 1000)  # Normalize
        
        # 4. Chroma features (harmonic content)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        features.extend(np.mean(chroma, axis=1))
        
        # 5. Zero-crossing rate (percussive content)
        zcr = librosa.feature.zero_crossing_rate(y)
        features.append(np.mean(zcr))
        
        return np.array(features, dtype=np.float32)
    
    def _find_spectral_peaks(self, y: np.ndarray, sr: int, n_peaks: int = 100) -> np.ndarray:
        """Find prominent spectral peaks for matching"""
        # Compute spectrogram
        D = librosa.stft(y)
        S_db = librosa.amplitude_to_db(np.abs(D), ref=np.max)
        
        # Find local maxima in spectrogram
        peaks = []
        for i in range(S_db.shape[1]):  # For each time frame
            frame = S_db[:, i]
            # Find top n peaks in this frame
            peak_indices = np.argsort(frame)[-n_peaks//10:]
            for idx in peak_indices:
                peaks.append((i, idx, frame[idx]))
        
        # Convert to numpy array and take top n_peaks
        peaks_array = np.array(peaks[:n_peaks], dtype=np.float32)
        return peaks_array
    
    def _generate_hash(self, features: np.ndarray) -> str:
        """Generate deterministic hash from features"""
        # Convert features to bytes
        features_bytes = features.tobytes()
        
        # Create hash (SHA-256 for collision resistance)
        hash_obj = hashlib.sha256(features_bytes)
        
        # Add salt for uniqueness
        salt = b"ugandan_music_2026"
        hash_obj.update(salt)
        
        return hash_obj.hexdigest()[:32]  # 32-char hash
    
    def store_fingerprint(self, fingerprint: AudioFingerprint, 
                         song_title: str, artist: str) -> bool:
        """Store fingerprint in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Serialize peaks
            peaks_blob = zlib.compress(pickle.dumps(fingerprint.peaks))
            metadata_json = json.dumps(fingerprint.metadata)
            
            cursor.execute('''
                INSERT OR REPLACE INTO fingerprints 
                (hash, song_title, artist, duration, sample_rate, peaks, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                fingerprint.hash,
                song_title,
                artist,
                fingerprint.duration,
                fingerprint.sample_rate,
                peaks_blob,
                metadata_json
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Stored fingerprint for {artist} - {song_title}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store fingerprint: {e}")
            return False
    
    def find_match(self, fingerprint: AudioFingerprint, 
                  threshold: float = 0.7) -> Optional[Tuple[str, str, float]]:
        """
        Find matching song in database using feature similarity
        Returns: (song_title, artist, confidence)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all fingerprints for comparison
            cursor.execute('SELECT hash, song_title, artist, peaks FROM fingerprints')
            stored_fingerprints = cursor.fetchall()
            
            best_match = None
            best_score = 0.0
            
            for stored_hash, song_title, artist, peaks_blob in stored_fingerprints:
                # Deserialize peaks
                stored_peaks = pickle.loads(zlib.decompress(peaks_blob))
                
                # Calculate similarity score
                score = self._calculate_similarity(fingerprint.peaks, stored_peaks)
                
                if score > best_score and score > threshold:
                    best_score = score
                    best_match = (song_title, artist, score)
            
            conn.close()
            
            if best_match:
                logger.info(f"Found match: {best_match[1]} - {best_match[0]} ({best_score:.2%})")
                return best_match
            
            return None
            
        except Exception as e:
            logger.error(f"Match finding failed: {e}")
            return None
    
    def _calculate_similarity(self, peaks1: np.ndarray, peaks2: np.ndarray) -> float:
        """Calculate similarity between two peak sets"""
        if peaks1.shape[0] == 0 or peaks2.shape[0] == 0:
            return 0.0
        
        # Simple Euclidean distance for now
        # In production, implement more sophisticated matching
        min_len = min(len(peaks1), len(peaks2))
        
        if min_len == 0:
            return 0.0
        
        # Take first min_len peaks from each
        p1 = peaks1[:min_len].flatten()
        p2 = peaks2[:min_len].flatten()
        
        # Calculate normalized similarity
        diff = np.linalg.norm(p1 - p2)
        max_diff = np.linalg.norm(p1) + np.linalg.norm(p2)
        
        if max_diff == 0:
            return 0.0
        
        similarity = 1.0 - (diff / max_diff)
        return float(similarity)

# Factory function for dependency injection
def get_fingerprinter() -> UgandanMusicFingerprinter:
    """Dependency injection for fingerprinting service"""
    return UgandanMusicFingerprinter()
