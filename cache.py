"""
Query cache implementation for DSL interpreter.
"""
import hashlib
import time
from typing import Any, Dict, Optional
from functools import lru_cache

class QueryCache:
    def __init__(self, max_size: int = 100, ttl: int = 300):
        """
        Initialize query cache with max size and time-to-live (TTL) in seconds.
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._max_size = max_size
        self._ttl = ttl

    def _compute_key(self, query: str, context: Dict[str, Any]) -> str:
        """
        Compute cache key based on query and context.
        """
        # Sort context keys to ensure consistent hashing
        ctx_str = str(sorted(context.items()))
        data = f"{query}:{ctx_str}".encode('utf-8')
        return hashlib.sha256(data).hexdigest()

    def get(self, query: str, context: Dict[str, Any]) -> Optional[Any]:
        """
        Get cached results for query and context if they exist and are valid.
        """
        key = self._compute_key(query, context)
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry['timestamp'] <= self._ttl:
                return entry['result']
            del self._cache[key]
        return None

    def set(self, query: str, context: Dict[str, Any], result: Any) -> None:
        """
        Cache results for query and context.
        """
        key = self._compute_key(query, context)
        
        # Evict oldest entries if cache is full
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k]['timestamp'])
            del self._cache[oldest_key]
        
        self._cache[key] = {
            'result': result,
            'timestamp': time.time()
        }

@lru_cache(maxsize=1000)
def parse_dsl_query(query: str) -> Any:
    """
    Parse and validate DSL query. Results are cached to avoid repeated parsing.
    """
    # Add query parsing logic here
    pass

# Global cache instance
query_cache = QueryCache()