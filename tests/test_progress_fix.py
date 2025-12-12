#!/usr/bin/env python
"""Test to verify that different query parameters get separate progress tracking."""

import sys
from pathlib import Path
import json
import shutil

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from polymarket import APIClient

def test_separate_progress_tracking():
    """Test that different query parameters maintain separate progress files."""
    
    # Create a temporary test cache directory
    test_cache = Path("data/cache/test_progress_fix")
    if test_cache.exists():
        shutil.rmtree(test_cache)
    
    print("Testing separate progress tracking for different query parameters...\n")
    
    # Create client
    client = APIClient(cache_dir=test_cache)
    
    # Simulate two different queries
    params1 = {
        "closed": "true",
        "ascending": "true",
        "start_date_min": "2025-11-16T00:00:00Z",
        "end_date_max": "2025-11-17T23:59:59Z",
    }
    
    params2 = {
        "closed": "true",
        "ascending": "true", 
        "start_date_min": "2025-11-17T00:00:00Z",
        "end_date_max": "2025-11-18T23:59:59Z",
    }
    
    # Update progress for first query
    client.closed_events._update_progress(100, **params1)
    progress1 = client.closed_events._get_progress(**params1)
    print(f"✓ Query 1 (Nov 16-17): Set progress to 100, retrieved {progress1}")
    
    # Update progress for second query
    client.closed_events._update_progress(200, **params2)
    progress2 = client.closed_events._get_progress(**params2)
    print(f"✓ Query 2 (Nov 17-18): Set progress to 200, retrieved {progress2}")
    
    # Verify they're different
    assert progress1 == 100, f"Expected progress1=100, got {progress1}"
    assert progress2 == 200, f"Expected progress2=200, got {progress2}"
    print(f"✓ Progress tracking is independent: {progress1} != {progress2}")
    
    # Verify separate progress files exist
    progress_files = list(test_cache.rglob("progress.json"))
    print(f"\n✓ Found {len(progress_files)} progress.json files:")
    for pf in progress_files:
        content = json.loads(pf.read_text())
        relative = pf.relative_to(test_cache)
        print(f"  - {relative}: offset={content['last_offset']}")
    
    assert len(progress_files) == 2, f"Expected 2 progress files, found {len(progress_files)}"
    
    # Verify no progress.json at root
    root_progress = test_cache / "progress.json"
    assert not root_progress.exists(), "Root progress.json should not exist!"
    print(f"✓ No root-level progress.json (bug is fixed!)")
    
    # Cleanup
    shutil.rmtree(test_cache)
    
    print("\n✅ All tests passed! Each query parameter set has its own progress tracking.")
    return 0

if __name__ == "__main__":
    try:
        exit(test_separate_progress_tracking())
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
