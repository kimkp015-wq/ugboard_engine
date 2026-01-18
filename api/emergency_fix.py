#!/usr/bin/env python3
"""
Emergency fix for multipart error
"""
import os

# Fix requirements.txt
with open('requirements.txt', 'a') as f:
    f.write('\npython-multipart==0.0.9\n')

print("✅ Added python-multipart to requirements.txt")

# Fix radio.py if it exists
radio_path = 'api/ingestion/radio.py'
if os.path.exists(radio_path):
    with open(radio_path, 'r') as f:
        content = f.read()
    
    # Fix the problematic line
    content = content.replace('stations: list = None', 'scrape_request: dict = None')
    
    with open(radio_path, 'w') as f:
        f.write(content)
    
    print("✅ Fixed radio.py parameter type")

print("🚀 Run: git add . && git commit -m 'Fix multipart error' && git push")
