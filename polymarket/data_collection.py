"""High-level data collection utilities for Polymarket API.

This module provides the main public API for fetching Polymarket data with:
- Memory-safe streaming (cache mode) or in-memory loading (json mode)
- Guardrails to prevent accidentally loading 7GB datasets
- Support for closed events, price history, and category filtering

Public API:
    DataCollection.closed_events() - Fetch closed markets/events
    DataCollection.price_history() - Fetch price history for a market
    DataCollection.filter_by_categories() - Client-side tag filtering
    DataCollection.preview_size() - Estimate data size before pulling

Philosophy:
    - datacollection provides raw data fetching + simple filtering
    - IEOR4212 handles data transformation, calibration analysis, visualization
    - Users get full control over event structure interpretation

Target research: calibration of final market probabilities and price analysis.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Literal, Iterable, Dict, Any, List
from difflib import get_close_matches

from .api_client import APIClient

logger = logging.getLogger(__name__)


class DataCollection:
    # Guardrail thresholds
    # Require force for larger spans
    MAX_DAYS_WITHOUT_FORCE = 120          
    # Soft cap on number of events loaded into RAM
    MAX_EVENTS_JSON_DEFAULT = 200_000     

    @staticmethod
    def _to_iso(dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        
        iso = dt.isoformat()
        return iso + ("Z" if not iso.endswith("Z") else "")

    @classmethod
    def _validate_range(cls, start_iso: Optional[str], end_iso: Optional[str], force_large: bool) -> None:
        if not start_iso or not end_iso:
            return
        
        start = datetime.fromisoformat(start_iso.replace("Z", ""))
        end = datetime.fromisoformat(end_iso.replace("Z", ""))

        # Calculate span in days and enforce guardrail
        days = (end - start).days + 1
        if days > cls.MAX_DAYS_WITHOUT_FORCE and not force_large:
            raise ValueError(
                f"Requested range spans {days} days (> {cls.MAX_DAYS_WITHOUT_FORCE}). "
                "Pass force_large=True to confirm intentional large pull."
            )

    @classmethod
    def closed_events(
        cls,
        start_date_min: datetime,
        end_date_max: Optional[datetime] = None,
        tag_id: Optional[int] = None,
        limit: int = 1000,
        max_pages: Optional[int] = None,
        force_large: bool = False,
        mode: Literal["json", "cache", "both"] = "json",
        closed: Optional[bool] = True,
        **extra_params: Any,
    ) -> List[Dict[str, Any]]:
        """Fetch closed events (uses automatic caching).

        Args:
            start_date_min: Start date for closed events (required).
            end_date_max: End date for closed events (optional).
            tag_id: Optional filter for a category/tag.
            limit: Page size requested from API.
            max_pages: Optional cap on pagination iterations.

        Returns:
            List of event dictionaries.
        """

        start_iso = cls._to_iso(start_date_min)
        end_iso = cls._to_iso(end_date_max)

        cls._validate_range(start_iso, end_iso, force_large)

        # Create client and fetch (caching is handled automatically)
        client = APIClient()
        
        events: List[Dict[str, Any]] = []

        # Prepare iterator parameters
        iter_params = {
            "limit": limit,
            "offset": 0,  # Always start from 0 to get cached data
            "closed": str(closed).lower(),
            "start_date_min": start_iso,
            "end_date_max": end_iso,
            "tag_id": tag_id,
            "ascending": "true",
            "max_pages": max_pages,
            "batch": True,  # Get full pages at once for better performance
        }
        
        # Check for consolidated cache file (fast path)
        query_params = {k: v for k, v in iter_params.items() if k not in ['offset', 'limit', 'batch', 'max_pages']}
        cache_path = client._cache_path('fetch_closed_markets', **query_params)
        consolidated_file = cache_path / "consolidated.json"
        progress_file = cache_path / "progress.json"
        
        # If consolidated cache exists and fetch is complete, load it directly
        if consolidated_file.exists() and progress_file.exists():
            try:
                progress_data = json.loads(progress_file.read_text())
                is_complete = progress_data.get("is_complete", False)
                
                if is_complete:
                    logger.info(f"Loading from consolidated cache: {consolidated_file}")
                    events = json.loads(consolidated_file.read_text())
                    logger.info(f"Loaded {len(events)} events from consolidated cache")
                    return events
            except Exception as e:
                logger.warning(f"Failed to load consolidated cache: {e}. Falling back to normal iteration.")

        # Normal iteration path - fetch from individual offset files or API
        for page in client.closed_events.iter_events(**iter_params):
            if mode in ("json", "both"):
                events.extend(page)  # Extend with full page

            if len(events) >= cls.MAX_EVENTS_JSON_DEFAULT and not force_large and mode in ("json", "both"):
                raise ValueError(
                    f"Event accumulation exceeded {cls.MAX_EVENTS_JSON_DEFAULT}. "
                    "The date range is too large. Pass force_large=True to override."
                )
        
        # Save consolidated cache if we successfully fetched all events
        if events and mode in ("json", "both"):
            try:
                cache_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Saving consolidated cache with {len(events)} events to {consolidated_file}")
                
                # Write consolidated file with fsync
                with open(consolidated_file, 'w') as f:
                    json.dump(events, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                
                # Mark progress as complete
                if progress_file.exists():
                    progress_data = json.loads(progress_file.read_text())
                    progress_data["is_complete"] = True
                    with open(progress_file, 'w') as f:
                        json.dump(progress_data, f)
                        f.flush()
                        os.fsync(f.fileno())
                        
            except Exception as e:
                logger.warning(f"Failed to save consolidated cache: {e}")
        
        return events

    @staticmethod
    def _page_iter(client: APIClient, **params: Any) -> Iterable[List[Dict[str, Any]]]:
        """Internal helper yielding raw pages (list-of-events) without flattening."""
        # replicate pagination manually to capture page boundaries.
        limit = params.get("limit", 1000)
        buffer: List[Dict[str, Any]] = []

        # use client.closed_events.iter_events which yields individual events; regroup by limit.
        count = 0

        for ev in client.closed_events.iter_events(**params):
            buffer.append(ev)
            count += 1

            if count % limit == 0:
                yield buffer
                buffer = []

        if buffer:
            yield buffer


    @classmethod
    def price_history(
        cls,
        market: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        interval: Optional[str] = None,
        fidelity: Optional[int] = None,
        max_bars: Optional[int] = None,
        chunk_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch price history for a specific market from CLOB endpoint.
        
        This method supports 4 mutually exclusive modes for specifying time ranges:
        
        Mode 1: Using interval (now - interval to now)
            - interval: Time interval (1m, 1h, 6h, 1d, 1w, max)
            - Optional: fidelity (resolution in minutes)
            
        Mode 2: Explicit timestamp range
            - start_ts: Start timestamp in seconds
            - end_ts: End timestamp in seconds
            - fidelity (resolution in minutes)
            
        Mode 3: End-anchored with max bars (end_ts = now, calculate start from max_bars)
            - end_ts: End timestamp in seconds
            - max_bars: Maximum number of bars/datapoints to fetch
            - fidelity: Resolution in minutes (required for this mode)
            
        Mode 4: Start-anchored with max bars (start from market start, calculate end)
            - market_start_date: Market start date (datetime object)
            - max_bars: Maximum number of bars/datapoints to fetch
            - fidelity: Resolution in minutes (required for this mode)
        
        Args:
            market: Market ID or slug (required)
            start_ts: Start timestamp in seconds (Mode 2)
            end_ts: End timestamp in seconds (Mode 2, Mode 3)
            interval: Time interval string (Mode 1)
            fidelity: Resolution in minutes (optional for Mode 1-2, required for Mode 3-4)
            max_bars: Maximum number of bars (Mode 3-4)
            market_start_date: Market start date (Mode 4)
            chunk_days: Size of chunks in days for paginated requests (default 7)
            
        Returns:
            List of price datapoints
            
        Raises:
            ValueError: If parameters don't match one of the 4 mutually exclusive modes
            
        Examples:
            # Mode 1: Last week of price data
            prices = DataCollection.price_history(
                market="1394632640823764",
                interval="1w",
                fidelity=60
            )
            
            # Mode 2: Specific date range
            prices = DataCollection.price_history(
                market="1394632640823764",
                start_ts=1704067200,  # 2024-01-01 in seconds
                end_ts=1735689600,     # 2025-01-01 in seconds
                fidelity=1440  # daily
            )
            
            # Mode 3: Last N bars ending now (or at end_ts)
            prices = DataCollection.price_history(
                market="1394632640823764",
                end_ts=int(datetime.now().timestamp()),  # now
                max_bars=6000,
                fidelity=1  # 1-minute bars
            )
            
            # Mode 4: First N bars starting from market open
            prices = DataCollection.price_history(
                market="1394632640823764",
                market_start_date=datetime(2024, 1, 1),
                max_bars=6000,
                fidelity=60  # hourly bars
            )
        """
        from datetime import datetime, timezone
        
        client = APIClient()
        
        # Validate mutually exclusive modes
        mode_indicators = [
            interval is not None,  # Mode 1
            (start_ts is not None and end_ts is not None and max_bars is None and fidelity is not None),  # Mode 2
            (end_ts is not None and max_bars is not None and start_ts is None and fidelity is not None),  # Mode 3
            (start_ts is not None and max_bars is not None and end_ts is None and fidelity is not None),  # Mode 4
        ]
        
        active_modes = sum(mode_indicators)
        
        if active_modes == 0:
            raise ValueError(
                "Must specify one of the following modes:\n"
                "  1. interval (e.g., '1w', '1d')\n"
                "  2. start_ts + end_ts\n"
                "  3. end_ts + max_bars + fidelity\n"
                "  4. market_start_date + max_bars + fidelity"
            )
        
        if active_modes > 1:
            raise ValueError(
                "Parameters are mutually exclusive. Choose only one mode:\n"
                "  1. interval\n"
                "  2. start_ts + end_ts\n"
                "  3. end_ts + max_bars + fidelity\n"
                "  4. market_start_date + max_bars + fidelity"
            )
        
        # Mode 3: Calculate start_ts from end_ts, max_bars, and fidelity
        if end_ts is not None and max_bars is not None:
            if fidelity is None:
                raise ValueError("Mode 3 (end_ts + max_bars) requires fidelity to be specified")
            
            # Calculate time range: max_bars * fidelity minutes
            time_range_minutes = (max_bars - 1) * fidelity
            start_ts = end_ts - int(time_range_minutes * 60)  # Convert minutes to seconds
        
        # Mode 4: Calculate end_ts from market_start_date, max_bars, and fidelity
        if start_ts is not None and max_bars is not None:
            if fidelity is None:
                raise ValueError("Mode 4 (start_ts + max_bars) requires fidelity to be specified")
            
            # Convert start_ts to timestamp if it's a datetime object
            if isinstance(start_ts, datetime):
                start_ts = int(start_ts.timestamp())
            # Calculate end time: start + (max_bars * fidelity) minutes
            time_range_minutes = (max_bars - 1) * fidelity
            end_ts = start_ts + int(time_range_minutes * 60)  # Convert minutes to seconds

        if interval is not None or start_ts is None or end_ts is None:
            logger.info(f"Fetching price history for market {market} (single call mode)")
            response = client.trades.fetch_prices(
                market=market,
                startTs=start_ts,
                endTs=end_ts,
                interval=interval,
                fidelity=fidelity,
                use_cache=True,
            )
            return response.get('history', [])
        
        # Chunk large time ranges to avoid API limits
        all_prices = []
        chunk_seconds = chunk_days * 24 * 3600
        current_start = start_ts
        
        logger.info(f"Fetching price history for market {market} in {chunk_days}-day chunks")
        
        while current_start < end_ts:
            current_end = min(current_start + chunk_seconds, end_ts)
            
            try:
                response = client.trades.fetch_prices(
                    market=market,
                    startTs=current_start,
                    endTs=current_end,
                    interval=None,
                    fidelity=fidelity,
                    use_cache=True,
                )
                
                chunk_data = response.get('history', []) if isinstance(response, dict) else response
                all_prices.extend(chunk_data)
                
                logger.debug(
                    f"Fetched {len(chunk_data)} points for [{current_start}, {current_end})"
                )
                
            except Exception as e:
                logger.warning(
                    f"Failed to fetch chunk [{current_start}, {current_end}): {e}"
                )
                # Continue to next chunk
            
            current_start = current_end
        
        # Deduplicate and sort
        if all_prices:
            seen_timestamps = set()
            unique_prices = []
            
            for point in all_prices:
                t = point.get('t')
                if t is not None and t not in seen_timestamps:
                    seen_timestamps.add(t)
                    unique_prices.append(point)
            
            unique_prices.sort(key=lambda x: x['t'])
            
            logger.info(
                f"Price history fetch complete: {len(unique_prices)} unique points "
                f"(from {len(all_prices)} total fetched)"
            )
            
            return unique_prices
        
        return []
        return response.get('history', [])
    
    @classmethod
    def price_history_hft(
        cls,
        market: str,
        start_ts: int,
        end_ts: int,
        fidelity_seconds: int = 10,
        chunk_minutes: int = 24 * 60,
        force_use_api: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch high-frequency price history with sub-minute resolution.
        
        Args:
            market: Market ID (CLOB token ID)
            start_ts: Start timestamp in seconds (unix epoch)
            end_ts: End timestamp in seconds (unix epoch)
            fidelity_seconds: Effective sampling interval in seconds (1-60)
                Lower values = more API calls but higher resolution
            chunk_minutes: Time window per API call to respect rate limits
            force_use_api: If True, bypass cache and force API fetch
                
        Returns:
            List of price points sorted by timestamp, deduplicated
            Format: [{"t": unix_timestamp, "p": price}, ...]
        """
        if not (1 <= fidelity_seconds <= 60):
            raise ValueError("fidelity_seconds must be between 1 and 60")
        
        if end_ts <= start_ts:
            raise ValueError("end_ts must be greater than start_ts")
        
        client = APIClient()
        
        # Build cache path for the HFT result
        cache_path = client._cache_path(
            'hft_prices',
            market=market,
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity_seconds=fidelity_seconds,
            chunk_minutes=chunk_minutes
        )
        cache_path.mkdir(parents=True, exist_ok=True)
        cache_file = cache_path / "prices.json"
        
        # Check cache first unless force_use_api
        if not force_use_api and cache_file.exists():
            try:
                logger.info(f"Loading HFT prices from cache: {cache_file}")
                cached_data = json.loads(cache_file.read_text(encoding="utf-8"))
                logger.info(f"Loaded {len(cached_data)} cached price points")
                return cached_data
            except Exception as e:
                logger.warning(f"Failed to load cache at {cache_file}: {e}. Refetching.")
        
        all_prices = []

        num_offsets = 60 // fidelity_seconds
        
        logger.info(
            f"Fetching HFT data for market {market}: {num_offsets} offsets, "
            f"effective resolution={fidelity_seconds}s"
        )

        for offset_idx in range(num_offsets):
            offset_seconds = offset_idx * fidelity_seconds
            
            current_start = start_ts + offset_seconds
            
            while current_start < end_ts:
                current_end = min(current_start + (chunk_minutes * 60), end_ts)
                
                try:
                    response = client.trades.fetch_prices(
                        market=market,
                        startTs=current_start,
                        endTs=current_end,
                        fidelity=1, 
                        use_cache=True,
                    )
                    
                    chunk_data = response.get('history', []) if isinstance(response, dict) else response
                    all_prices.extend(chunk_data)
                    
                    logger.debug(
                        f"Offset {offset_seconds}s: fetched {len(chunk_data)} points "
                        f"for [{current_start}, {current_end})"
                    )
                    
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch offset={offset_seconds}s, "
                        f"range=[{current_start}, {current_end}): {e}"
                    )
                
                current_start = current_end
        
        if all_prices:
            seen_timestamps = set()
            unique_prices = []
            
            for point in all_prices:
                t = point.get('t')
                if t is not None and t not in seen_timestamps:
                    seen_timestamps.add(t)
                    unique_prices.append(point)
            
            # Sort by timestamp
            unique_prices.sort(key=lambda x: x['t'])
            
            logger.info(
                f"HFT fetch complete: {len(unique_prices)} unique points "
                f"(from {len(all_prices)} total fetched)"
            )
            
            # Save to cache
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(unique_prices, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                logger.info(f"Saved HFT prices to cache: {cache_file}")
            except Exception as e:
                logger.warning(f"Failed to save cache at {cache_file}: {e}")
            
            return unique_prices
        
        return []
        
    
    @classmethod
    def filter_by_categories(
        cls,
        events: List[Dict[str, Any]],
        categories: List[str],
        match_field: Literal["id", "label", "slug"] = "id",
        case_sensitive: bool = False,
    ) -> List[Dict[str, Any]]:
        """Filter events by category tags (client-side).
        
        Filters events based on their tags field. Each event can have multiple tags,
        and an event is included if ANY of its tags match ANY of the requested categories.
        
        Args:
            events: List of event dicts from closed_events()
            categories: List of category values to match against
            match_field: Which tag field to match on ("id", "label", or "slug")
            case_sensitive: Whether to do case-sensitive matching (only for label/slug)
            
        Returns:
            Filtered list of events that have at least one matching tag
            
        Examples:
            # Filter by tag ID (most reliable, server uses these)
            events = DataCollection.filter_by_categories(
                events, 
                ["1", "64"],  # Sports, Esports
                match_field="id", 
            )
            
            # Filter by human-readable label
            events = DataCollection.filter_by_categories(
                events,
                ["Sports", "Politics"],
                match_field="label",
                case_sensitive=False
            )
            
            # Filter by slug (URL-friendly)
            events = DataCollection.filter_by_categories(
                events,
                ["sports", "esports"],
                match_field="slug"
            )
        """
        if not categories:
            return events
        
        # Normalize categories for comparison
        if not case_sensitive and match_field in ("label", "slug"):
            normalized_cats = {str(cat).lower() for cat in categories}
        else:
            normalized_cats = {str(cat) for cat in categories}
        
        filtered = []
        for event in events:
            tags = event.get("tags", [])
            if not tags:
                continue
            
            for tag in tags:
                tag_value = tag.get(match_field)
                if tag_value is None:
                    continue
                
                # Normalize for comparison
                if not case_sensitive and match_field in ("label", "slug"):
                    tag_value = str(tag_value).lower()
                else:
                    tag_value = str(tag_value)
                
                if tag_value in normalized_cats:
                    filtered.append(event)
                    break
        
        return filtered

    @staticmethod
    def getClobTokenId(market: Dict[str, Any]) -> Optional[List[str]]:
        """Extract and clean CLOB token IDs from a market dictionary.
        
        Args:
            market: Market dictionary containing clobTokenIds field
            
        Returns:
            List of cleaned CLOB token IDs, or None if not found
            
        Example:
            for market in event.get('markets', []):
                token_ids = DataCollection.getClobTokenId(market)
                if token_ids:
                    # Use first token for binary markets
                    prices = DataCollection.price_history(market=token_ids[0], interval="1w")
        """
        clobTokenId = market.get('clobTokenIds')
        if not clobTokenId:
            return None
        
        if isinstance(clobTokenId, list):
            return [str(token).strip() for token in clobTokenId if token]
        
        if isinstance(clobTokenId, str):
            # Remove quotes and brackets
            clobTokenId = clobTokenId.replace('"', '').replace("'", "").strip("[]")
            
            # Split by comma if multiple tokens
            if ',' in clobTokenId:
                return [token.strip() for token in clobTokenId.split(',') if token.strip()]
 
            return [clobTokenId.strip()] if clobTokenId.strip() else None
        
        return None

    @staticmethod
    def _parse_date(value: Any) -> Optional[datetime]:
        """Parse various date formats into datetime objects.
        Args:
            value: Date value in various formats
    
        Returns:
            datetime object or None if parsing fails
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                return None
        
        if isinstance(value, (int, float)):
            try:
                if value > 2200 * 365 * 24 * 3600:
                    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                else:
                    return datetime.fromtimestamp(value, tz=timezone.utc)
            except (ValueError, OSError):
                return None
        
        return None

    @classmethod
    def get_field(
        cls,
        data: Dict[str, Any],
        field_name: str,
        fuzzy: bool = True,
        parse_dates: bool = True,
        parse_json: bool = True,
        return_all: bool = False,
        default: Any = None,
    ) -> Any:
        """Extract a field from event/market dict with fuzzy matching and automatic cleaning.
        
        Handles API inconsistencies:
        - Multiple matches (volumes, dates, liquidities) - returns all or first
        
        Args:
            data: Event or market dictionary
            field_name: Field name to extract (can be fuzzy, e.g., "start date")
            fuzzy: Use fuzzy matching for field names (default: True)
            parse_dates: Automatically parse date fields (default: True)
            parse_json: Automatically parse JSON string fields (default: True)
            return_all: If multiple fields match, return all as dict (default: False, returns first)
            default: Default value if field not found
            
        Returns:
            Field value (cleaned/parsed), or dict of all matches if return_all=True
        """
        search_name = field_name.lower().replace(' ', '').replace('_', '')
        
        matches = {}
        for key, value in data.items():
            key_normalized = key.lower().replace('_', '')
            
            # Exact match (after normalization)
            if key_normalized == search_name:
                matches[key] = value
            # Fuzzy match in case of partial match
            elif fuzzy and search_name in key_normalized:
                matches[key] = value
        
        # No matches found
        if not matches:
            if fuzzy:
                close_matches = get_close_matches(
                    field_name,
                    data.keys(),
                    n=5,
                    cutoff=0.7
                )

                for match in close_matches:
                    matches[match] = data[match]
            
            if not matches:
                return default
        
        cleaned_matches = {}
        for key, value in matches.items():
            if parse_json and isinstance(value, str) and value.startswith('['):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
            
            # Parse dates
            if parse_dates and ('date' in key.lower() or 'time' in key.lower()):
                parsed = cls._parse_date(value)
                if parsed is not None:
                    value = parsed
            
            cleaned_matches[key] = value
        
        # Return based on return_all flag
        if return_all:
            return cleaned_matches if cleaned_matches else default
        else:
            sorted_keys = sorted(cleaned_matches.keys())
            return cleaned_matches[sorted_keys[0]]

    @classmethod
    def extract_fields(
        cls,
        data: Dict[str, Any],
        fields: List[str],
        fuzzy: bool = True,
        parse_dates: bool = True,
        parse_json: bool = True,
    ) -> Dict[str, Any]:
        """Extract multiple fields from event/market dict with automatic cleaning.
        
        Batch version of get_field() for extracting multiple fields at once.
        For fields with multiple matches (dates, volumes), returns first match.
        
        Args:
            data: Event or market dictionary
            fields: List of field names to extract
            fuzzy: Use fuzzy matching for field names
            parse_dates: Automatically parse date fields
            parse_json: Automatically parse JSON string fields
            
        Returns:
            Dictionary mapping field names to extracted values
        """
        result = {}

        field_querry = {
            'data': data,
            'fuzzy': fuzzy,
            'parse_dates': parse_dates,
            'parse_json': parse_json,
            'return_all': False,
            'default': None
        }

        for field in fields:
            value = cls.get_field(field_name=field, **field_querry)
            result[field] = value
        
        return result


    @classmethod
    def get_dates_from_event(cls, event: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
        return cls.extract_fields(event, ['start_date', 'end_date'])
    
    @classmethod
    def get_dates_from_market(cls, market: Dict[str, Any]) -> Dict[str, Optional[datetime]]:
        return cls.extract_fields(market, ['start_date', 'end_date'])


