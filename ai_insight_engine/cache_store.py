import os
import json

CACHE_DIR = "insight_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def cache_path(key):
    return os.path.join(CACHE_DIR, f"{key}.json")

def get_cache(key):
    path = cache_path(key)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None

def set_cache(key, value):
    path = cache_path(key)
    with open(path, "w") as f:
        json.dump(value, f, indent=2)