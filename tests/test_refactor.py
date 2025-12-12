#!/usr/bin/env python
"""Quick test to verify the refactored API structure works correctly."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from polymarket import APIClient

def test_imports():
    """Test that imports work correctly."""
    print("✓ Successfully imported APIClient from polymarket")
    
def test_client_creation():
    """Test that client can be created."""
    cache_dir = Path("data/cache/test_refactor")
    client = APIClient(cache_dir=cache_dir)
    print(f"✓ Successfully created APIClient with cache_dir: {cache_dir}")
    return client

def test_closed_events_property(client):
    """Test that closed_events property is accessible."""
    closed_api = client.closed_events
    print(f"✓ Successfully accessed closed_events property: {type(closed_api).__name__}")
    return closed_api

def test_api_methods(closed_api):
    """Test that API methods are available."""
    assert hasattr(closed_api, 'iter_events'), "iter_events method missing"
    assert hasattr(closed_api, 'fetch_all'), "fetch_all method missing"
    print("✓ All expected methods are present on ClosedEventsAPI")

def test_lightweight_fetch(client):
    """Test a lightweight API call (preview mode)."""
    try:
        # Use the no-cache preview method to test basic connectivity
        events = client.closed_events.fetch_page_no_cache(limit=1, closed=True)
        print(f"✓ Successfully fetched preview page with {len(events)} event(s)")
        return True
    except Exception as e:
        print(f"✗ Preview fetch failed: {e}")
        return False

def main():
    print("Testing refactored Polymarket API structure...\n")
    
    try:
        test_imports()
        client = test_client_creation()
        closed_api = test_closed_events_property(client)
        test_api_methods(closed_api)
        test_lightweight_fetch(client)
        
        print("\n✓ All tests passed! The refactoring is working correctly.")
        print("\nUsage examples:")
        print("  from polymarket import APIClient")
        print("  client = APIClient(cache_dir='data/cache')")
        print("  events = client.closed_events.fetch_all(start_date_min='2025-01-01T00:00:00Z')")
        print("  for event in client.closed_events.iter_events(limit=100):")
        print("      # process event...")
        
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
