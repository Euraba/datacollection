import sys
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from polymarket.data_collection import DataCollection

print("Starting data fetch for closed events...")

start_date = datetime(2025, 11, 1, 0, 0, 0)
end_date = datetime(2025, 12, 6, 0, 0, 0)

print(f"Date range: {start_date} to {end_date}")

curr_time = datetime.now()
# Fetch closed events data - now always returns a list
closed_events = DataCollection.closed_events(
    start_date_min=start_date,
    end_date_max=end_date,
    #tag_id=84,
    closed=True,
    limit=1000,
    #related_tags=True,
    force_large=True,  # Bypass date range guardrail
)

end_time = datetime.now()
print(f"{(end_time - curr_time)} ms")

print(f"Fetched {len(closed_events)} closed events.")
if not closed_events:
    print("No events returned.")

print("\n" + "="*70)
print("First 5 events:")
print("="*70)
for event in closed_events[:5]:  # Print first 5 events as a sample
    print(f"Event ID: {event['id']}, Title: {event['title']}")

print("\n" + "="*70)
print("Fetching price history for first 5 events...")
print("="*70)

long_duration_markets = []
long_duration = timedelta(days=7)

for i, event in enumerate(closed_events, 1):
    markets = event.get('markets', [])
    for market in markets:
        dates = DataCollection.extract_fields(market, ['startDate', 'endDate'])
        start_date = dates.get('startDate')
        end_date = dates.get('endDate')

        #print(type(start_date), type(end_date))

        if start_date and end_date:
            duration = end_date - start_date
            if duration >= long_duration:
                long_duration_markets.append(event)
                if len(long_duration_markets) == 5:
                    break
    
print(f"Found {len(long_duration_markets)} markets with duration >= {long_duration.days} days.")


for i, event in enumerate(long_duration_markets[:1], 1):
    print(f"\n[{i}/5] Event: {event['title']}")
    
    # Get markets from the event
    markets = event.get('markets', [])
    if not markets:
        print("  No markets found in event")
        continue
    
    for market in markets[:1]:
        clobTokenIds = DataCollection.get_field(market, 'clobTokenId')
        if not clobTokenIds:
            print("  No clobTokenId found in market")
            continue

        clobTokenId = clobTokenIds[0]

        print(f"  Fetching prices for Market ID: {clobTokenId}")
        print(f"  Market Question: {market.get('question')}")

        # Get outcomes
        outcomes = DataCollection.get_field(market, 'outcomes')

        print(f"  Outcomes: {outcomes}")

        # Get start and end date in iso format as strings
        dates = DataCollection.extract_fields(market, ['startDate', 'endDate'])
        end_date = dates.get('endDate')
        start_date = dates.get('startDate')

        #end_date_str = market.get('endDate')
        #start_date_str = market.get('startDate')

        # Parse ISO format strings to datetime objects 
        #end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')) if end_date_str else None
        #start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')) if start_date_str else None

        print(f"  Start Date: {start_date}, End Date: {end_date}")

        # Example parameters for different modes
        max_bars = 20
        minutes = 120
        fidelity_val_seconds = 30
        fidelity_val_minutes = 1

        try:
            curr_time = datetime.now(timezone.utc)
            
            # MODE 3 EXAMPLE: Get last N bars ending at market end_date
            # This fetches the last 'minutes' worth of data with specified fidelity
            print(f"  Testing hft price history mode")
            start_ts = int(start_date.timestamp())  # Start at market open
            end_ts = int(end_date.timestamp())  # End at market close
            
            prices_seconds = []
            prices_seconds = DataCollection.price_history_hft(
                market=clobTokenId,
                end_ts=end_ts,
                start_ts=start_ts,
                fidelity_seconds=fidelity_val_seconds,
            )

            prices_minute = DataCollection.price_history(
                market=clobTokenId,
                end_ts=end_ts,
                start_ts=start_ts,
                #interval="max",
                fidelity=fidelity_val_minutes,
            )
            
            end_time = datetime.now(timezone.utc)
            print(f"received {len(prices_minute)} for minutes and {len(prices_seconds)} for seconds prices in {(end_time - curr_time)}")
            
            if prices_minute:
                print(f"Fetched {len(prices_minute)} price datapoints")
                if len(prices_minute) > 0:
                    for price in prices_minute[:min(3, len(prices_minute))]:
                        print(price)
            else:
                print(f"No price data returned")
            
            print("\n" + "-"*40 + "\n")

            if prices_seconds:
                print(f"Fetched {len(prices_seconds)} price datapoints")
                if len(prices_seconds) > 0:
                    for price in prices_seconds[:min(3, len(prices_seconds))]:
                        print(price)
            else:
                print(f"No price data returned")

            print ("\n" + "="*70)
                
        except Exception as e:
            print(f"Error fetching prices: {e}")


