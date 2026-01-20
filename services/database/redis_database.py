"""
Redis-backed database with caching
"""
import json
import redis
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
from functools import wraps

from .json_database import JSONDatabase

def cache_response(ttl: int = 300):
    """Decorator to cache method responses in Redis"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, 'redis') or self.redis is None:
                return func(self, *args, **kwargs)
            
            # Create cache key from function name and arguments
            cache_key = f"{func.__name__}:{args}:{kwargs}"
            
            # Try to get from cache
            cached = self.redis.get(cache_key)
            if cached:
                return json.loads(cached)
            
            # Execute function and cache result
            result = func(self, *args, **kwargs)
            self.redis.setex(cache_key, ttl, json.dumps(result, default=str))
            return result
        return wrapper
    return decorator

class RedisDatabase(JSONDatabase):
    """
    Database with Redis caching layer
    
    Extends JSONDatabase to add Redis caching for frequent queries
    """
    
    def __init__(self, data_dir: Path, redis_url: Optional[str] = None):
        super().__init__(data_dir)
        
        # Initialize Redis connection
        self.redis = None
        if redis_url:
            try:
                self.redis = redis.from_url(redis_url)
                # Test connection
                self.redis.ping()
                print("✅ Redis connected successfully")
            except redis.ConnectionError as e:
                print(f"⚠️ Redis connection failed: {e}. Continuing without cache.")
                self.redis = None
    
    @cache_response(ttl=60)  # Cache for 1 minute
    def get_top_songs(self, limit: int = 100, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get top songs with Redis caching"""
        return super().get_top_songs(limit, region)
    
    @cache_response(ttl=30)  # Cache for 30 seconds (trending changes faster)
    def get_trending_songs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending songs with Redis caching"""
        return super().get_trending_songs(limit)
    
    def invalidate_cache(self, pattern: str = "*"):
        """Invalidate cache entries matching pattern"""
        if self.redis:
            keys = self.redis.keys(pattern)
            if keys:
                self.redis.delete(*keys)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        if not self.redis:
            return {"status": "not_configured"}
        
        try:
            info = self.redis.info()
            return {
                "status": "connected",
                "used_memory": info.get('used_memory_human', 'N/A'),
                "connected_clients": info.get('connected_clients', 0),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "hit_rate": (
                    info.get('keyspace_hits', 0) / 
                    max(1, info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0))
                ) if info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0) > 0 else 0
            }
        except redis.RedisError as e:
            return {"status": "error", "error": str(e)}

# Factory function to create appropriate database instance
def create_database(data_dir: Path, use_redis: bool = False, redis_url: Optional[str] = None):
    """Factory function to create database instance"""
    if use_redis and redis_url:
        return RedisDatabase(data_dir, redis_url)
    return JSONDatabase(data_dir)
