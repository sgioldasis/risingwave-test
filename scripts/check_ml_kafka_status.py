#!/usr/bin/env python3
"""Check ML Kafka Online status and debug predictions."""

import requests
import json

def check_status():
    """Check ML serving status."""
    print("=" * 60)
    print("ML Kafka Online Status Check")
    print("=" * 60)
    
    # Check health
    try:
        r = requests.get("http://localhost:8001/health", timeout=5)
        print(f"\n1. Health Check: {r.status_code}")
        print(json.dumps(r.json(), indent=2))
    except Exception as e:
        print(f"\n1. Health Check FAILED: {e}")
        return
    
    # Check models status
    try:
        r = requests.get("http://localhost:8001/models", timeout=5)
        print(f"\n2. Models Status: {r.status_code}")
        data = r.json()
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"\n2. Models Status FAILED: {e}")
    
    # Check predictions
    try:
        r = requests.get("http://localhost:8001/predict", timeout=5)
        print(f"\n3. Predictions: {r.status_code}")
        data = r.json()
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"\n3. Predictions FAILED: {e}")
    
    # Check online learning status
    try:
        r = requests.get("http://localhost:8001/online/status", timeout=5)
        print(f"\n4. Online Learning Status: {r.status_code}")
        data = r.json()
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"\n4. Online Learning Status FAILED: {e}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    check_status()
