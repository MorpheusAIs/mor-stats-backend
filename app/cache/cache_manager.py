"""
Cache manager module that provides a centralized caching system.
Replaces the file-based caching with an in-memory TTL cache.
"""
import asyncio
import logging
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Type variable for function return type
T = TypeVar('T')

# Global cache instance with TTL (time-to-live) of 12 hours and max size of 1000 items
_cache = TTLCache(maxsize=1000, ttl=12 * 60 * 60)

# Last cache update time tracking
_last_cache_update_time: Optional[str] = None


def get_cache() -> Dict[str, Any]:
    """
    Get the current cache contents.
    
    Returns:
        Dict[str, Any]: The current cache contents
    """
    return dict(_cache)


def set_cache_item(key: str, value: Any, ttl: Optional[int] = None) -> None:
    """
    Set a cache item with an optional custom TTL.
    
    Args:
        key: The cache key
        value: The value to cache
        ttl: Optional TTL in seconds, defaults to the cache's default TTL
    """
    global _last_cache_update_time
    
    _cache[key] = value
    _last_cache_update_time = datetime.now().isoformat()
    logger.debug(f"Cache item set: {key}")


def get_cache_item(key: str, default: Any = None) -> Any:
    """
    Get a cache item by key.
    
    Args:
        key: The cache key
        default: Default value if key not found
        
    Returns:
        The cached value or default if not found
    """
    return _cache.get(key, default)


def delete_cache_item(key: str) -> None:
    """
    Delete a cache item by key.
    
    Args:
        key: The cache key to delete
    """
    if key in _cache:
        del _cache[key]
        logger.debug(f"Cache item deleted: {key}")


def clear_cache() -> None:
    """Clear the entire cache."""
    global _last_cache_update_time
    
    _cache.clear()
    _last_cache_update_time = datetime.now().isoformat()
    logger.info("Cache cleared")


def get_last_cache_update_time() -> Optional[str]:
    """
    Get the timestamp of the last cache update.
    
    Returns:
        Optional[str]: ISO format timestamp of the last update or None if never updated
    """
    return _last_cache_update_time


def cached(key: str, ttl: Optional[int] = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for caching function results.
    
    Args:
        key: The cache key to use
        ttl: Optional TTL in seconds, defaults to the cache's default TTL
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> T:
            # Check if result is in cache
            cached_result = get_cache_item(key)
            if cached_result is not None:
                logger.debug(f"Cache hit for key: {key}")
                return cached_result
            
            # If not in cache, call the function
            logger.debug(f"Cache miss for key: {key}")
            result = await func(*args, **kwargs)
            
            # Cache the result
            set_cache_item(key, result, ttl)
            return result
        
        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            # Check if result is in cache
            cached_result = get_cache_item(key)
            if cached_result is not None:
                logger.debug(f"Cache hit for key: {key}")
                return cached_result
            
            # If not in cache, call the function
            logger.debug(f"Cache miss for key: {key}")
            result = func(*args, **kwargs)
            
            # Cache the result
            set_cache_item(key, result, ttl)
            return result
        
        # Return the appropriate wrapper based on whether the function is async or not
        if asyncio.iscoroutinefunction(func):
            return cast(Callable[..., T], async_wrapper)
        return cast(Callable[..., T], sync_wrapper)
    
    return decorator


# Initialize the cache with the current time
_last_cache_update_time = datetime.now().isoformat()