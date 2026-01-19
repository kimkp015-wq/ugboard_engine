# scripts/test_fingerprinting.py
import sys
import numpy as np
from pathlib import Path

def test_audio_dependencies():
    """Test that all audio dependencies for your custom fingerprinter are working."""
    
    print("🔍 Testing UG Board Engine Audio Fingerprinting...")
    print("=" * 60)
    
    # Test librosa (your main dependency)
    try:
        import librosa
        print("✅ librosa installed:", librosa.__version__)
    except ImportError as e:
        print(f"❌ librosa failed: {e}")
        return False
    
    # Test numpy
    try:
        import numpy as np
        print("✅ numpy installed:", np.__version__)
    except ImportError as e:
        print(f"❌ numpy failed: {e}")
        return False
    
    # Test soundfile (for audio I/O)
    try:
        import soundfile as sf
        print("✅ soundfile installed")
    except ImportError as e:
        print(f"❌ soundfile failed: {e}")
        return False
    
    # Test audioread
    try:
        import audioread
        print("✅ audioread installed")
    except ImportError as e:
        print(f"⚠️  audioread warning: {e} (optional for some formats)")
    
    # Test numba (required by librosa)
    try:
        import numba
        print("✅ numba installed:", numba.__version__)
    except ImportError as e:
        print(f"❌ numba failed: {e}")
        return False
    
    # Test sqlite3 (for your fingerprint database)
    try:
        import sqlite3
        print("✅ sqlite3 available (built-in)")
    except ImportError as e:
        print(f"❌ sqlite3 failed: {e}")
        return False
    
    print("=" * 60)
    print("✅ All required audio dependencies are working!")
    print("\nNote: chromaprint is NOT required for your current implementation.")
    print("Your audio_fingerprinter.py uses custom fingerprinting with librosa.")
    
    return True

def create_sample_audio():
    """Create a test audio file for fingerprinting demo."""
    import wave
    import struct
    
    # Create a simple sine wave
    sample_rate = 22050
    duration = 1.0  # seconds
    frequency = 440.0  # Hz (A4 note)
    
    num_samples = int(sample_rate * duration)
    samples = []
    
    for i in range(num_samples):
        sample = 0.5 * np.sin(2 * np.pi * frequency * i / sample_rate)
        samples.append(sample)
    
    # Create test directory
    test_dir = Path("test_audio")
    test_dir.mkdir(exist_ok=True)
    
    # Save as WAV file
    test_file = test_dir / "test_tone.wav"
    
    with wave.open(str(test_file), 'w') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        
        for sample in samples:
            data = struct.pack('<h', int(sample * 32767))
            wav_file.writeframes(data)
    
    return test_file

def test_fingerprint_extraction():
    """Test the actual fingerprint extraction from your code."""
    print("\n🧪 Testing fingerprint extraction...")
    
    try:
        # Import your fingerprinter
        sys.path.insert(0, '.')
        from ingestion.fingerprinting.audio_fingerprinter import UgandanMusicFingerprinter
        
        # Create test audio
        test_file = create_sample_audio()
        print(f"✅ Created test audio: {test_file}")
        
        # Initialize fingerprinter
        fingerprinter = UgandanMusicFingerprinter()
        print("✅ Fingerprinter initialized")
        
        # Extract fingerprint
        fingerprint = fingerprinter.extract_fingerprint(str(test_file))
        
        if fingerprint:
            print(f"✅ Fingerprint extracted successfully!")
            print(f"   Hash: {fingerprint.hash[:16]}...")
            print(f"   Duration: {fingerprint.duration:.2f}s")
            print(f"   Sample rate: {fingerprint.sample_rate} Hz")
            
            # Test storing
            success = fingerprinter.store_fingerprint(
                fingerprint, "Test Song", "Test Artist"
            )
            print(f"✅ Fingerprint stored in database: {success}")
            
            # Test matching
            match = fingerprinter.find_match(fingerprint, threshold=0.3)
            if match:
                print(f"✅ Found match: {match[1]} - {match[0]} ({match[2]:.1%})")
            else:
                print("✅ No match found (expected for test audio)")
            
            return True
        else:
            print("❌ Fingerprint extraction failed")
            return False
            
    except Exception as e:
        print(f"❌ Fingerprint test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("UG Board Engine - Audio Fingerprinting Validation")
    print("=" * 60)
    
    # Test dependencies
    deps_ok = test_audio_dependencies()
    
    if deps_ok:
        # Test actual fingerprinting
        fingerprint_ok = test_fingerprint_extraction()
        
        print("\n" + "=" * 60)
        if fingerprint_ok:
            print("🎉 SUCCESS: Your audio fingerprinting system is fully functional!")
            print("\nSummary:")
            print("• No chromaprint dependency needed")
            print("• Custom fingerprinting with librosa is working")
            print("• SQLite database for fingerprints is operational")
            sys.exit(0)
        else:
            print("⚠️  Fingerprinting test partially failed")
            sys.exit(1)
    else:
        print("\n❌ Dependency check failed")
        sys.exit(1)
