from pathlib import Path
from typing import Any
#import requests
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

class APIClient:
    # Switched to Gamma
    BASEURL = "https://gamma-api.polymarket.com"
    
    # Default cache directory (absolute path, anchored to package location)
    # This ensures cache is always at datacollection/data/cache regardless of where scripts run from
    DEFAULT_CACHE_DIR = Path(__file__).parent.parent / "data" / "cache"

    def __init__(self, rate_limit: float = 0.1, cache_dir: Path | str = DEFAULT_CACHE_DIR):
        """APIClient with deterministic caching.
        
        All caching is handled internally and automatically. Cache is organized by:
        - data/cache/fetch_closed_markets/<query_params_hash>/
        - data/cache/trades/<market_params_hash>/
        
        Args:
            rate_limit: Delay between API requests in seconds.
        """
        self.sleep = rate_limit
        self._closed_events_api = None
        self._trades_api = None
        
        self.cache_dir = Path(cache_dir)

    @property
    def closed_events(self):
        """Access closed events API methods.
        
        Lazy-loaded property that provides access to ClosedEventsAPI methods.
        Usage: client.closed_events.fetch_all(...)
        """
        if self._closed_events_api is None:
            from .closed_events import ClosedEventsAPI
            self._closed_events_api = ClosedEventsAPI(self)

        return self._closed_events_api 

    @property
    def trades(self):
        """Access trades API methods.

        Lazy-loaded property that provides access to TradesAPI methods.
        Usage: client.trades.fetch_prices(...)
        """
        if self._trades_api is None:
            from .trades import TradesAPI
            self._trades_api = TradesAPI(self)

        return self._trades_api

    def _get_cache_dir(self, endpoint_type: str) -> Path:
        """Get the cache directory for a specific endpoint type.
        
        Args:
            endpoint_type: Type of endpoint ('fetch_closed_markets', 'trades', etc.)
            
        Returns:
            Path to cache directory for this endpoint type.
        """
        cache_dir = self.cache_dir / endpoint_type
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _cache_path(self, endpoint_type: str, **params: Any) -> Path:
        """Return a cache subdirectory path for specific query params.

        Args:
            endpoint_type: Type of endpoint ('fetch_closed_markets', 'trades', etc.)
            **params: Query parameters that define the cached data.

        Returns:
            Path to cache subdirectory for these specific parameters.
            
        Uses a directory named after stable filtering parameters (excluding
        offset/limit) with readable key=value segments for easier inspection.
        """
        base_cache = self._get_cache_dir(endpoint_type)
        info = [f"{k}={v}" for k, v in sorted(params.items()) if v is not None]
        safe = "__".join(info)
        return base_cache / safe
