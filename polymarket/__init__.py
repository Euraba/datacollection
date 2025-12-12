"""Polymarket API client library.

This package provides access to the Polymarket Gamma API with modular
organization for different endpoint types.

Main Public API:
    DataCollection - High-level interface with guardrails and memory safety
    
Usage:
    from polymarket import DataCollection
    
    # Fetch closed events (with 120-day guardrail)
    events = DataCollection.closed_events(
        start_date_min=datetime(2025, 1, 1),
        end_date_max=datetime(2025, 1, 31),
        mode="json"  # or "cache" for streaming
    )
    
    # Preview data size before pulling
    preview = DataCollection.preview_size(
        start_date_min=datetime(2025, 1, 1),
        end_date_max=datetime(2025, 12, 31),
    )
    
    # Fetch price history
    prices = DataCollection.price_history(
        market="will-btc-hit-100k",
        interval="1w"
    )
"""

from .data_collection import DataCollection
from .api_client import APIClient
from .closed_events import ClosedEventsAPI
from .trades import TradesAPI

# Public API: users should primarily use DataCollection
__all__ = ["DataCollection"]

# Internal APIs (available but not recommended for direct use)
__all__ += ["APIClient", "ClosedEventsAPI", "TradesAPI"]
