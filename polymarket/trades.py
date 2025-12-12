"""Trades / prices-history endpoint client.

This module implements a small wrapper around the public CLOB prices-history
endpoint documented in `mdfiles/TRADES_ENDPOINT.md`.

Endpoint URL: https://clob.polymarket.com/prices-history

Query parameters supported:
- market (required)
- startTs (optional, unix s)
- endTs (optional, unix s)
- interval (optional; mutually exclusive with startTs/endTs) one of 1m,1w,1d,6h,1h,max
- fidelity (optional, minutes)

Caching: responses are written to a cache path derived from the query params
using the APIClient._cache_path helper. This keeps behaviour consistent with
other modules in this package.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class TradesAPI:
    """API client for fetching price-history/trades from the CLOB endpoint."""

    VALID_INTERVALS = {"1m", "1w", "1d", "6h", "1h", "max"}

    def __init__(self, base_client):
        self._client = base_client

    def fetch_prices(
        self,
        market: str,
        startTs: Optional[int] = None,
        endTs: Optional[int] = None,
        interval: Optional[str] = None,
        fidelity: Optional[int] = None,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch price history for a market from the public CLOB endpoint.

        Args:
            market: Market identifier (required).
            startTs: Start timestamp in milliseconds (optional).
            endTs: End timestamp in milliseconds (optional).
            interval: Mutually exclusive with startTs/endTs. One of VALID_INTERVALS.
            fidelity: Resolution in minutes.
            use_cache: Whether to read/write cached response files.

        Returns:
            Parsed JSON response (list of datapoints) from the CLOB API.
        """
        if not market:
            raise ValueError("market parameter is required")

        if interval is not None and (startTs is not None or endTs is not None):
            raise ValueError("interval is mutually exclusive with startTs/endTs")

        if interval is not None and interval not in self.VALID_INTERVALS:
            raise ValueError(f"interval must be one of {sorted(self.VALID_INTERVALS)}")

        params: Dict[str, Any] = {"market": market}
        if startTs is not None:
            params["startTs"] = startTs
        if endTs is not None:
            params["endTs"] = endTs
        if interval is not None:
            params["interval"] = interval
        if fidelity is not None:
            params["fidelity"] = fidelity

        # Build cache path using stable params (exclude ephemeral fields if any)
        cache_path = self._client._cache_path('trades', market=market, interval=interval, fidelity=fidelity)
        cache_path.mkdir(parents=True, exist_ok=True)

        # Create a filename for this param set
        if interval is not None:
            fname = f"interval_{interval}__fidelity_{fidelity or 'auto'}.json"
        else:
            fname = f"start_{startTs or 'none'}__end_{endTs or 'none'}__fidelity_{fidelity or 'auto'}.json"

        path = cache_path / fname

        if use_cache and path.exists():
            try:
                logger.info(f"Loading cached prices-history from {path}")
                return __import__("json").loads(path.read_text(encoding="utf-8"))
            except Exception:
                logger.warning("Corrupted cache at %s, refetching", path)

        logger.info("Fetching prices-history from API for market=%s", market)

        url = "https://clob.polymarket.com/prices-history"

        logger.info(f"Fetching prices-history with params: {params}")
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
        except requests.RequestException as e:
            # Try to extract error message from response body
            error_detail = str(e)
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_body = e.response.json()
                    if 'error' in error_body:
                        error_detail = f"{e} - Server error: {error_body['error']}"
            except Exception:
                pass  # If we can't parse the error, just use the original
            
            logger.error("Request to prices-history failed: %s", error_detail)
            raise RuntimeError(error_detail) from e

        data = resp.json()
        logger.info(f"Received {len(data) if isinstance(data, list) else 'non-list'} datapoints from API")

        # Persist cache for reproducibility
        try:
            path.write_text(__import__("json").dumps(data, indent=2), encoding="utf-8")
        except Exception:
            logger.debug("Failed to write cache file %s", path)

        return data
