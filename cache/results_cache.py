import redis
import pickle
import hashlib

r = redis.Redis(host='localhost', port=6379, db=0)

def normalize_query(query: str) -> str:
    return " ".join(query.strip().lower().split())

def get_cache_key(query):
    return hashlib.md5(normalize_query(query).encode(), usedforsecurity=False).hexdigest()

def cache_query(query: str, df):
    key = get_cache_key(query)
    r.set(key, pickle.dumps(df), ex=3600)  # 1 hour expiry

def check_results_cache(query: str):
    key = get_cache_key(query)
    cached = r.get(key)
    if cached is not None:
        return pickle.loads(cached)
    return None

def clear_all_cache():
    r.flushdb()

def clear_query_cache(query: str):
    key = get_cache_key(query)
    r.delete(key)