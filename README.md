# Polymarket Data Collection

A Python toolkit for fetching Polymarket data (closed markets/events and price history) with automatic caching, resilient pagination, and resume support.

## Installation

Clone this repository and install in editable mode:

```bash
git clone https://github.com/Euraba/datacollection datacollection
mkdir my_project && cd my_project
pip install -e ../datacollection
touch main.py && echo 'from polymarket.data_collection import DataCollection' >> main.py
```

That's it! All dependencies will be installed automatically. The package uses automatic caching to speed up subsequent requests.

## Quick Start

```python
from polymarket.data_collection import DataCollection
from datetime import datetime

# Fetch closed events
events = DataCollection.closed_events(
    start_date_min=datetime(2025, 1, 1),
    end_date_max=datetime(2025, 1, 31),
)

# Get price history
prices = DataCollection.price_history(
    market="0x1234...",
    interval="1w",
    fidelity=60  # hourly resolution
)
```

## Key Features

- **Automatic caching**: All API responses cached automatically - no configuration needed
- **Centralized cache**: Single cache location (`data/cache/`) included in repository
- **Resilient pagination**: Handles API inconsistencies and resumes from interruptions
- **Smart field extraction**: Fuzzy matching handles inconsistent API field names
- **Multiple price modes**: Simple intervals or complex timestamp ranges with chunking
- **High-frequency trading data**: Sub-minute resolution support(not yet done)

---

## Fetching Closed Events

The primary way to fetch closed markets/events from Polymarket's Gamma API.

### Basic Usage

```python
from polymarket.data_collection import DataCollection
from datetime import datetime

# Fetch events for a date range
events = DataCollection.closed_events(
    start_date_min=datetime(2025, 1, 1),
    end_date_max=datetime(2025, 12, 31),
    limit=1000  # page size (default: 1000)
)

print(f"Fetched {len(events)} events")
```

### Filter by Category

```python
# Fetch only specific category (e.g., politics)
events = DataCollection.closed_events(
    start_date_min=datetime(2025, 1, 1),
    end_date_max=datetime(2025, 12, 31),
    tag_id=12  # category/tag ID
)
```

### Working with Large Datasets

```python
# For very large date ranges, use force_large to override safety limits
events = DataCollection.closed_events(
    start_date_min=datetime(2020, 1, 1),
    end_date_max=datetime(2025, 12, 31),
    force_large=True  # bypass 100k event safety check
)
```

### Key Parameters

- `start_date_min` (datetime, required): Start date for filtering
- `end_date_max` (datetime, optional): End date (defaults to now)
- `tag_id` (int, optional): Filter by category/tag ID
- `limit` (int, default=1000): Page size for API requests
- `max_pages` (int, optional): Limit number of pages to fetch
- `force_large` (bool, default=False): Override 100k event safety limit
- `closed` (bool, default=True): Filter for closed markets

---

## Fetching Price History

Fetch historical price data from Polymarket's CLOB API. Supports multiple modes for different use cases.

### Mode 1: Simple Time Interval (Recommended)

```python
# Last week of data
prices = DataCollection.price_history(
    market="0x1234...",
    interval="1w",
    fidelity=60  # hourly resolution (minutes)
)

# Available intervals: '1m', '1h', '6h', '1d', '1w', 'max'
```

### Mode 2: Specific Date Range

```python
from datetime import datetime

# Fetch specific time window
prices = DataCollection.price_history(
    market="0x1234...",
    start_ts=int(datetime(2024, 1, 1).timestamp()),
    end_ts=int(datetime(2024, 12, 31).timestamp()),
    fidelity=1440  # daily resolution (1440 minutes = 24 hours)
)
```

### Mode 3: Last N Bars

```python
# Get last 6000 bars ending now
prices = DataCollection.price_history(
    market="0x1234...",
    end_ts=int(datetime.now().timestamp()),
    max_bars=6000,
    fidelity=1  # 1-minute bars
)
```

### Mode 4: First N Bars from Market Start

```python
# Get first 10000 hourly bars from market open
prices = DataCollection.price_history(
    market="0x1234...",
    start_ts=int(datetime(2024, 1, 1).timestamp()),
    max_bars=10000,
    fidelity=60  # hourly bars
)
```

### High-Frequency Trading Data (Sub-Minute) (Not yet implemented, must use onchain data)

```python
# Fetch 10-second resolution price data
prices = DataCollection.price_history_hft(
    market="0x1234...",
    start_ts=int(datetime(2024, 6, 1).timestamp()),
    end_ts=int(datetime(2024, 6, 2).timestamp()),
    fidelity_seconds=10,  # 10-second intervals
    chunk_minutes=1440    # fetch in daily chunks
)
```

### Key Parameters

- `market` (str, required): Market ID (CLOB token ID)
- `interval` (str, optional): Simple time interval (`'1m'`, `'1h'`, `'6h'`, `'1d'`, `'1w'`, `'max'`)
- `start_ts` (int, optional): Start timestamp in seconds
- `end_ts` (int, optional): End timestamp in seconds
- `fidelity` (int, optional): Resolution in minutes
- `max_bars` (int, optional): Maximum number of datapoints to fetch
- `chunk_days` (int, default=7): Size of chunks for paginated requests

**Note**: Modes are mutually exclusive. Choose one:
1. `interval` + `fidelity`
2. `start_ts` + `end_ts` + `fidelity`
3. `end_ts` + `max_bars` + `fidelity`
4. `start_ts` + `max_bars` + `fidelity`

---

## Field Extraction Utilities

Polymarket's API has inconsistent field naming. These utilities provide fuzzy matching and type conversion.

### Basic Field Extraction

```python
from polymarket.data_collection import DataCollection

# Single field with fuzzy matching
start_date = DataCollection.get_field(event, "start date")
# Matches: startDate, start_date, startTime, start_ts, etc.
# Returns: datetime object (automatically parsed)

title = DataCollection.get_field(event, "title")
volume = DataCollection.get_field(event, "volume")
```

### Extract Multiple Fields

```python
# Get multiple fields at once
fields = DataCollection.extract_fields(
    event,
    ["id", "title", "start date", "end date", "volume", "liquidity"]
)

# Returns dict:
# {
#     "id": "22213",
#     "title": "Will GDP be negative in Q3?",
#     "start date": datetime(2025, 4, 7, 18, 26, 34),
#     "end date": datetime(2025, 10, 30, 12, 0, 0),
#     "volume": 673144.59,
#     "liquidity": 14753.85
# }
```

### Get All Matching Fields

```python
# Return all fields matching "volume"
all_volumes = DataCollection.get_field(event, "volume", return_all=True)
# Returns: {"volume": 673144.59, "volume24hr": 227.20, ...}

# Get all date-related fields
all_dates = DataCollection.get_field(event, "date", return_all=True)
# Returns: {"startDate": datetime(...), "endDate": datetime(...), ...}
```

### Market-Specific Fields

```python
# Extract outcomes (handles JSON strings)
outcomes = DataCollection.get_field(market, "outcomes")
# Returns: ["Yes", "No"] (not the string '["Yes", "No"]')

# Extract outcome prices
prices = DataCollection.get_field(market, "outcomePrices")
# Returns: ["0.014", "0.986"]

# Get CLOB token IDs
token_ids = DataCollection.getClobTokenId(market)
# Returns: ["7973401553...", "1105054585..."]
```

### Features

- **Fuzzy matching**: Handles variations like `startDate`, `start_date`, `start_time`
- **Auto type conversion**: Dates, numbers, JSON strings parsed automatically
- **Date parsing**: Handles ISO strings, Unix timestamps (seconds/milliseconds), datetime objects
- **JSON parsing**: Automatically parses stringified arrays/objects
- **List cleaning**: Removes brackets, quotes, and splits comma-separated values

---

## Cache System

All API responses are automatically cached to avoid redundant requests.

### Cache Location

```
datacollection/data/cache/
```

This path is **absolute and centralized** - all scripts share the same cache regardless of where they run from.

### Cache Structure

```
data/cache/
├── fetch_closed_markets/
│   └── ascending=true__closed=true__start_date_min=...Z__end_date_max=...Z/
│       ├── offset_0.json           # First page (up to 1000 events)
│       ├── offset_1000.json        # Second page
│       ├── consolidated.json       # All events combined
│       └── progress.json           # Resume tracking metadata
└── trades/
    └── market=0x...__interval=1d__fidelity=60/
        └── interval_1d__fidelity_60.json
```

### Cache Features

- **Automatic**: No configuration needed - works out of the box
- **Persistent**: Files synced to disk immediately (survives crashes)
- **Resumable**: Interrupted fetches continue from last complete page
- **Shared**: All scripts use the same cache
- **Deterministic**: Cache paths derived from query parameters
- **Consolidated**: Large fetches saved as single files for faster loading

### Using the Cache

```python
# Cache is used automatically - no code changes needed
events = DataCollection.closed_events(
    start_date_min=datetime(2025, 1, 1),
    end_date_max=datetime(2025, 1, 31)
)
# First call: fetches from API and caches
# Subsequent calls: loads instantly from cache
```

---

## Advanced Usage

### Filter Events by Categories

```python
# Filter events client-side by category names
filtered = DataCollection.filter_by_categories(
    events,
    categories=["Politics", "Economics", "Sports"]
)
```

### Low-Level API Access (Advanced)

For direct API control, use `APIClient`:

```python
from polymarket import APIClient

client = APIClient()

# Iterate through events one by one
for event in client.closed_events.iter_events(
    start_date_min="2025-01-01T00:00:00Z",
    end_date_max="2025-01-31T23:59:59Z",
    limit=1000
):
    print(event['id'], event['title'])

# Or iterate in batches (faster)
for page in client.closed_events.iter_events(
    start_date_min="2025-01-01T00:00:00Z",
    limit=1000,
    batch=True  # yields pages of up to 1000 events
):
    print(f"Processing {len(page)} events")
```

---

## Examples

### Example 1: Analyze Volume by Category

```python
from polymarket.data_collection import DataCollection
from datetime import datetime
from collections import defaultdict

# Fetch all events from 2025
events = DataCollection.closed_events(
    start_date_min=datetime(2025, 1, 1),
    end_date_max=datetime(2025, 12, 31)
)

# Group by category
volume_by_category = defaultdict(float)
for event in events:
    fields = DataCollection.extract_fields(event, ["tags", "volume"])
    tags = fields.get("tags", [])
    volume = fields.get("volume", 0)
    
    for tag in tags:
        volume_by_category[tag] += volume

# Print top categories
for tag, vol in sorted(volume_by_category.items(), key=lambda x: -x[1])[:10]:
    print(f"{tag}: ${vol:,.0f}")
```

### Example 2: Build Price DataFrame

```python
from polymarket.data_collection import DataCollection
import pandas as pd
from datetime import datetime

# Fetch hourly prices for last week
prices = DataCollection.price_history(
    market="0x1234...",
    interval="1w",
    fidelity=60
)

# Convert to DataFrame
df = pd.DataFrame(prices)
df['timestamp'] = pd.to_datetime(df['t'], unit='s')
df = df[['timestamp', 'p']].rename(columns={'p': 'price'})

print(df.head())
```

### Example 3: Monitor Recent Markets

```python
from polymarket.data_collection import DataCollection
from datetime import datetime, timedelta

# Get markets that closed in the last 24 hours
yesterday = datetime.now() - timedelta(days=1)
events = DataCollection.closed_events(
    start_date_min=yesterday,
    end_date_max=datetime.now()
)

for event in events:
    fields = DataCollection.extract_fields(
        event,
        ["title", "volume", "end date"]
    )
    print(f"{fields['title']}: ${fields['volume']:,.0f}")
```

---

## Requirements

- Python 3.8+
- requests
- pandas

All dependencies are automatically installed with `pip install -e .`

---

## Project Structure

```
datacollection/
├── polymarket/
│   ├── __init__.py
│   ├── api_client.py         # Low-level API wrapper
│   ├── closed_events.py      # Closed events endpoint
│   ├── trades.py             # Price history endpoint
│   ├── data_collection.py    # High-level public API
│   └── filters.py            # Field extraction utilities
├── data/
│   └── cache/                # Automatic cache storage
├── tests/
├── setup.py
├── requirements.txt
└── README.md
```
- ✅ **File sync**: Explicit `fsync()` prevents cache corruption
- ✅ **Separation of concerns**: `DataCollection` (high-level) vs `APIClient` (low-level)
- ✅ **Always returns data**: `closed_events()` always starts from offset=0, reading cached data
- ✅ **Field extraction utilities**: `get_field()` and `extract_fields()` with fuzzy matching
- ✅ **Automatic cleaning**: Date parsing, JSON parsing, type cleaning for API inconsistencies
- ✅ **Token ID extraction**: `getClobTokenId()` handles all clobTokenIds formats

## Next Steps
- Add adaptive backoff for rate limiting


