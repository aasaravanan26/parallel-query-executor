import redis
import pickle
import hashlib
import pandas as pd
from session import session

# Redis Cache
# NOTE: Keeping it global as only single-thread is hitting Redis cache.
# If multi-thread, can create connection per thread.
r = redis.Redis(host='localhost', port=6379, db=0)

def normalize_query(sql_text: str) -> str:

    """ Normalizes SQL text: 
    trims spaces, converts to lowercase, and collapses multiple spaces into a single space.
    
    Args:
        sql_text (str): query string
    Returns:
        sql_text (str): normalized SQL text
    
    """
    return " ".join(sql_text.strip().lower().split())

def get_cache_key(sql_text):

    """ Given SQL text, get MD5 hash of normalized SQL text in hexadecimal string
    
    Args:
        sql_text (str): query string
    Returns:
        hash string (str) of 32 hex characters
    
    """
    return hashlib.md5(normalize_query(sql_text).encode(), usedforsecurity=False).hexdigest()

def cache_query(sql_text: str, df):

    """ Hash SQL text and store serialized DataFrame in Redis Cache with expiry time
    
    Args:
        sql_text (str): query string
        df (pandas.DataFrame): DataFrame
    Returns:
        None
    
    """

    key = get_cache_key(sql_text)
    r.set(key, pickle.dumps(df), ex=session.CACHE_EXPIRY_TIME)

def check_results_cache(sql_text: str):

    """ Hash SQL text, check in Redis Cache
        if cache hit, return DataFrame; else, return None
    
    Args:
        sql_text (str): query string
    Returns:
        df (pandas.DataFrame): DataFrame
        or None
    
    """

    key = get_cache_key(sql_text)
    cached = r.get(key)
    if cached is not None:
        df = pickle.loads(cached)
        if isinstance(df, pd.DataFrame):
            return df
    return None

def clear_all_cache():

    """ Clear entire Redis Cache
    
    Args:
        None
    Returns:
        None
    
    """

    r.flushdb()

def clear_query_cache(sql_text: str):

    """ Clear specified key based on SQL text in Redis Cache
    
    Args:
        sql_text (str): query string
    Returns:
        None
    
    """

    key = get_cache_key(sql_text)
    r.delete(key)