"""Closed events API endpoints for Polymarket Gamma API.

This module provides methods to fetch closed markets/events with resilient
pagination handling. It wraps the base APIClient to separate concerns.
"""
import json
import os
import time
import logging
import requests
from typing import Iterator, Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class ClosedEventsAPI:
    """API client for fetching closed Polymarket events."""

    def __init__(self, base_client):
        """Initialize with a base APIClient instance.
        
        Args:
            base_client: Instance of APIClient with core fetching logic.
        """
        self._client = base_client

    def _fetch_page(self, **params: Any) -> List[Dict[str, Any]]:
        """Fetch a single page from the /events endpoint.

        Includes lightweight caching. If the API intermittently returns fewer
        results than requested (e.g., limit=1000 returns ~400 due to load), we
        treat whatever comes back as authoritative for that slice and move the
        offset forward by the actual number of items received to avoid gaps.
        """
        page_params = {k: v for k, v in params.items() if k not in ['limit', 'offset']}
        path = self._client._cache_path('fetch_closed_markets', **page_params) / f"offset_{params.get('offset', 0)}.json"

        if path.exists():
            try:
                logger.info(f"Loading cached page from {path}")
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                logger.warning(f"Corrupted cache at {path}, refetching.")
        else:
            logger.info(f"No cache found at {path}, fetching from API.")

        url = f"{self._client.BASEURL}/events"
        try:
            response = requests.get(url, params=params, timeout=25)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Request failed for params={params}: {e}")
            raise

        data = response.json()

        path.parent.mkdir(parents=True, exist_ok=True)
        # Write with explicit flush and sync to ensure data is on disk
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())

        # Respect a minimal delay to avoid hammering endpoint
        time.sleep(self._client.sleep)
        return data

    def fetch_page_no_cache(self, **params: Any) -> List[Dict[str, Any]]:
        """Fetch a single /events page WITHOUT touching cache or progress.

        Used for lightweight previews; does not write files or update offsets.
        """
        url = f"{self._client.BASEURL}/events"
        clean = {k: v for k, v in params.items() if v is not None}
        try:
            response = requests.get(url, params=clean, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Preview request failed for params={clean}: {e}")
            return []
        return response.json()

    def _update_progress(self, total_fetched: int, **query_params: Any):
        """Update progress for a specific query parameter set.
        
        Args:
            total_fetched: The offset to save as progress
            query_params: The query parameters that uniquely identify this fetch operation
        """
        # Get the parameter-specific cache directory
        page_params = {k: v for k, v in query_params.items() if k not in ['offset', 'limit']}
        cache_path = self._client._cache_path('fetch_closed_markets', **page_params)
        progress_file = cache_path / "progress.json"
        
        progress = {"total_fetched": total_fetched, "last_offset": total_fetched}
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        # Write with explicit flush and sync to ensure progress is saved
        with open(progress_file, 'w') as f:
            json.dump(progress, f)
            f.flush()
            os.fsync(f.fileno())

    def _get_progress(self, **query_params: Any) -> int:
        """Get progress for a specific query parameter set.
        
        Args:
            query_params: The query parameters that uniquely identify this fetch operation
            
        Returns:
            The last offset saved, or 0 if no progress exists
        """
        # Get the parameter-specific cache directory
        page_params = {k: v for k, v in query_params.items() if k not in ['offset', 'limit']}
        cache_path = self._client._cache_path('fetch_closed_markets', **page_params)
        progress_file = cache_path / "progress.json"
        
        if progress_file.exists():
            return json.loads(progress_file.read_text()).get("last_offset", 0)
        return 0

    def iter_events(
        self,
        limit: int = 1000,
        offset: Optional[int] = None,
        closed: bool = True,
        start_date_min: Optional[str] = None,
        end_date_max: Optional[str] = None,
        tag_id: Optional[int] = None,
        ascending: bool = True,
        max_pages: Optional[int] = None,
        max_retries_short_page: int = 2,
        batch: bool = True,
        **extra_params: Any,
    ) -> Iterator[Dict[str, Any]] | Iterator[List[Dict[str, Any]]]:
        """Yield events with resilient pagination.

        Args:
            limit: Requested page size (Gamma supports up to 1000).
            offset: Starting offset; if None, resume from progress cache.
            closed: Filter for closed markets/events.
            start_date_min, end_date_max: ISO8601 strings with trailing 'Z'.
            tag_id: Optional category/tag filter.
            ascending: Sort order.
            max_pages: Safety cap to avoid infinite loops (None = no cap).
            max_retries_short_page: Attempts to refetch a short page before accepting it.
            batch: If True, yield entire pages as lists; if False, yield individual events.
            extra_params: Any other query params.
        """
        # Build base query parameters (excluding offset/limit for progress tracking)
        base_params = {
            "closed": str(closed).lower(),
            "ascending": str(ascending).lower(),
            "start_date_min": start_date_min,
            "end_date_max": end_date_max,
            "tag_id": tag_id,
            **extra_params,
        }

        query_params = {k: v for k, v in base_params.items() if v is not None}
        
        if offset is None:
            offset = self._get_progress(**query_params)
            logger.info(f"DEBUG: Resuming from offset={offset} based on progress")

        pages_fetched = 0
        while True:
            if max_pages is not None and pages_fetched >= max_pages:
                logger.info("Reached max_pages cap; stopping pagination.")
                break

            # Add pagination parameters
            params = {
                "limit": limit,
                "offset": offset,
                **query_params,
            }

            attempt = 0
            page: List[Dict[str, Any]] = []
            while attempt <= max_retries_short_page:
                page = self._fetch_page(**params)

                if len(page) == 0:
                    break  

                if len(page) < limit and attempt < max_retries_short_page:
                    logger.debug(
                        f"Short page (len={len(page)} < {limit}) at offset={offset}, retry {attempt+1}/{max_retries_short_page}."
                    )

                    time.sleep(self._client.sleep * (attempt + 1))
                    attempt += 1
                    continue

                break

            if not page:
                logger.info("Empty page received; pagination complete.")
                break

            actual_fetched = len(page)
            logger.debug(f"Fetched {actual_fetched} events at offset {offset} (limit={limit}).")
            
            # Yield entire page or individual events based on batch mode
            if batch:
                yield page
            else:
                yield from page
            
            self._update_progress(offset + actual_fetched, **query_params)
            pages_fetched += 1  
            offset += actual_fetched

            if actual_fetched == 0:
                break

    def fetch_all(
        self,
        start_date_min: Optional[str] = None,
        end_date_max: Optional[str] = None,
        tag_id: Optional[int] = None,
        limit: int = 1000,
        **extra_params: Any,
    ) -> List[Dict[str, Any]]:
        """Convenience wrapper returning a list of all closed events in the date span.

        Uses resilient pagination to avoid missing data when pages are short.
        """
        results: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        #check the cache directory if we have already fetched the requested data
        cache_path = self._client._cache_path(
            endpoint_type = 'events',
            closed=True,
            start_date_min=start_date_min,
            end_date_max=end_date_max,
            tag_id=tag_id,
            **extra_params,
        )

        # Build iteration parameters
        iter_params = {
            "limit": limit,
            "closed": True,
            "start_date_min": start_date_min,
            "end_date_max": end_date_max,
            "tag_id": tag_id,
            **extra_params,
        }

        for ev in self.iter_events(**iter_params):
            ev_id = str(ev.get("id"))
            if ev_id in seen_ids:
                continue

            seen_ids.add(ev_id)
            results.append(ev)

        logger.info(f"Total closed events fetched: {len(results)}")
        return results
