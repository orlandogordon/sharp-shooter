#!/usr/bin/env python3
"""
Historical NFL Data Collector for Sharp-Shooter
Collects historical odds data from The Odds API for 2024 season + playoffs + 2025 Week 1
Uses efficient batching and raw data storage for analysis
"""

import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

class HistoricalDataCollector:
    """Collects historical NFL betting data using The Odds API historical endpoints"""
    
    def __init__(self):
        """Initialize the historical data collector"""
        Config.validate_config()
        
        self.api_key = Config.ODDS_API_KEY
        self.base_url = Config.ODDS_API_BASE_URL
        self.sport = 'americanfootball_nfl'
        self.regions = 'us'  # US bookmakers
        self.markets = 'h2h,spreads,totals'  # Core betting markets
        
        # Rate limiting - historical endpoints have higher costs
        self.requests_made = 0
        self.max_requests_per_minute = 100  # Conservative for paid endpoints
        self.request_timestamps = []
        
        # Session for connection pooling
        self.session = requests.Session()
        
        # Raw data storage paths
        self.raw_data_dir = Path('raw_data/historical')
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🏈 Initialized Historical Data Collector")
        print(f"📊 Markets: {self.markets}")
        print(f"🌎 Regions: {self.regions}")
        print(f"💾 Raw data directory: {self.raw_data_dir}")
    
    def get_nfl_season_date_ranges(self) -> List[Tuple[str, datetime, datetime]]:
        """Get date ranges for 2024 season + playoffs + 2025 Week 1"""
        date_ranges = []
        
        # 2024 NFL Regular Season (Weeks 1-18)
        # Season started September 5, 2024 (Thursday)
        season_2024_start = datetime(2024, 9, 5, 0, 0, 0, tzinfo=timezone.utc)
        
        # Regular season: 18 weeks
        for week in range(1, 19):
            week_start = season_2024_start + timedelta(weeks=week-1)
            week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
            date_ranges.append((f"2024_Week_{week}", week_start, week_end))
        
        # 2024 NFL Playoffs
        # Wild Card Weekend (Week 19): January 13-15, 2025
        wild_card_start = datetime(2025, 1, 13, 0, 0, 0, tzinfo=timezone.utc)
        wild_card_end = datetime(2025, 1, 15, 23, 59, 59, tzinfo=timezone.utc)
        date_ranges.append(("2024_Wild_Card", wild_card_start, wild_card_end))
        
        # Divisional Round (Week 20): January 18-19, 2025
        divisional_start = datetime(2025, 1, 18, 0, 0, 0, tzinfo=timezone.utc)
        divisional_end = datetime(2025, 1, 19, 23, 59, 59, tzinfo=timezone.utc)
        date_ranges.append(("2024_Divisional", divisional_start, divisional_end))
        
        # Conference Championships (Week 21): January 26, 2025
        conference_start = datetime(2025, 1, 26, 0, 0, 0, tzinfo=timezone.utc)
        conference_end = datetime(2025, 1, 26, 23, 59, 59, tzinfo=timezone.utc)
        date_ranges.append(("2024_Conference", conference_start, conference_end))
        
        # Super Bowl LIX (Week 22): February 9, 2025
        superbowl_start = datetime(2025, 2, 9, 0, 0, 0, tzinfo=timezone.utc)
        superbowl_end = datetime(2025, 2, 9, 23, 59, 59, tzinfo=timezone.utc)
        date_ranges.append(("2024_Super_Bowl", superbowl_start, superbowl_end))
        
        # 2025 NFL Season Week 1
        # Season starts September 4, 2025 (Thursday)
        season_2025_start = datetime(2025, 9, 4, 0, 0, 0, tzinfo=timezone.utc)
        week_1_end = season_2025_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        date_ranges.append(("2025_Week_1", season_2025_start, week_1_end))
        
        return date_ranges
    
    def get_historical_events(self, date: datetime, period_name: str, 
                            start_date: datetime, end_date: datetime) -> Optional[List[Dict]]:
        """Get historical events for a specific date, filtered by date range"""
        endpoint = f"{self.base_url}/historical/sports/{self.sport}/events"
        
        params = {
            'api_key': self.api_key,
            'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'dateFormat': 'iso'
        }
        
        try:
            print(f"🔍 Fetching events for {period_name} ({date.strftime('%Y-%m-%d')})")
            response = self._make_request(endpoint, params)
            
            if not response:
                return None
            
            data = response.json()
            all_events = data.get('data', [])
            
            if not all_events:
                print(f"⚠️  No events found for {period_name}")
                return []
            
            # Filter events to only include games within our target date range
            filtered_events = []
            for event in all_events:
                commence_time_str = event.get('commence_time', '')
                if not commence_time_str:
                    continue
                    
                try:
                    # Parse commence time
                    commence_time = datetime.fromisoformat(commence_time_str.replace('Z', '+00:00'))
                    
                    # Check if game is within our target period
                    if start_date <= commence_time <= end_date:
                        filtered_events.append(event)
                    else:
                        # Debug: show games outside range
                        print(f"🚫 Skipping game outside range: {event.get('away_team', 'Unknown')} @ {event.get('home_team', 'Unknown')} on {commence_time.strftime('%Y-%m-%d')}")
                        
                except Exception as e:
                    print(f"⚠️  Could not parse commence time for event: {e}")
                    continue
            
            print(f"✅ Found {len(filtered_events)} events in date range for {period_name} (filtered from {len(all_events)} total)")
            
            if filtered_events:
                # Save filtered events data
                filtered_data = {
                    'timestamp': data.get('timestamp'),
                    'data': filtered_events
                }
                self._save_raw_data(filtered_data, f"events_{period_name}_{date.strftime('%Y%m%d')}.json")
                
            return filtered_events
                
        except Exception as e:
            print(f"❌ Error fetching events for {period_name}: {e}")
            return None
    
    def get_historical_event_odds(self, event_id: str, event_date: datetime, period_name: str, 
                                home_team: str, away_team: str, commence_time: datetime) -> Optional[Dict]:
        """Get historical odds for a specific event"""
        endpoint = f"{self.base_url}/historical/sports/{self.sport}/events/{event_id}/odds"
        
        params = {
            'api_key': self.api_key,
            'date': event_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'regions': self.regions,
            'markets': self.markets,
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        }
        
        try:
            print(f"📊 Fetching odds for {away_team} @ {home_team} ({event_id[:8]}...) [Game: {commence_time.strftime('%m/%d %H:%M')}, Odds: {event_date.strftime('%m/%d %H:%M')}]")
            response = self._make_request(endpoint, params)
            
            if not response:
                return None
            
            data = response.json()
            
            if data:
                # Save raw odds data
                game_identifier = f"{away_team}_{home_team}".replace(' ', '_')
                self._save_raw_data(data, f"odds_{period_name}_{game_identifier}_{event_id[:8]}.json")
                return data
            else:
                print(f"⚠️  No odds found for {away_team} @ {home_team}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching odds for {away_team} @ {home_team}: {e}")
            return None
    
    def collect_historical_data_for_period(self, period_name: str, start_date: datetime, 
                                         end_date: datetime) -> Dict:
        """Collect all historical data for a specific period"""
        print(f"\n🏈 ===== COLLECTING {period_name.upper()} =====")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        results = {
            'period': period_name,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'events_collected': 0,
            'odds_collected': 0,
            'errors': []
        }
        
        # Strategy: Get events from middle of the period to maximize likelihood of finding games
        mid_date = start_date + (end_date - start_date) / 2
        
        # Get events for this period
        events = self.get_historical_events(mid_date, period_name, start_date, end_date)
        if not events:
            results['errors'].append(f"No events found for {period_name}")
            return results
        
        results['events_collected'] = len(events)
        
        # Collect odds for each event (limit for testing)
        max_games_for_test = getattr(self, 'max_games_limit', None)
        if max_games_for_test:
            events = events[:max_games_for_test]
            print(f"🧪 Test mode: Limited to first {len(events)} games")
        
        odds_collected = 0
        for event in events:
            event_id = event.get('id')
            if not event_id:
                continue
                
            home_team = event.get('home_team', 'Unknown')
            away_team = event.get('away_team', 'Unknown')
            commence_time_str = event.get('commence_time', '')
            
            # Parse commence time to use as odds collection timestamp
            try:
                commence_time = datetime.fromisoformat(commence_time_str.replace('Z', '+00:00'))
                now_utc = datetime.now(timezone.utc)
                
                if commence_time < now_utc:
                    # Past game: collect odds from ~2 hours before game start (closing lines)
                    odds_time = commence_time - timedelta(hours=2)
                    print(f"📈 Past game, collecting closing lines from {odds_time.strftime('%m/%d %H:%M')} UTC")
                else:
                    # Future game: collect current available odds (but not too far in future)
                    # Try to get odds from yesterday to avoid issues with future dates
                    odds_time = min(now_utc - timedelta(hours=1), commence_time - timedelta(days=1))
                    print(f"🔮 Future game, collecting available odds from {odds_time.strftime('%m/%d %H:%M')} UTC")
                    
            except Exception as e:
                # Fallback to mid-period date
                odds_time = mid_date
                commence_time = mid_date
                print(f"⚠️  Could not parse commence time, using fallback: {e}")
            
            # Get odds for this event
            odds_data = self.get_historical_event_odds(
                event_id, odds_time, period_name, home_team, away_team, commence_time
            )
            
            if odds_data:
                odds_collected += 1
            else:
                print(f"⚠️  No odds collected for {away_team} @ {home_team}")
            
            # Rate limiting - be conservative with historical endpoints
            time.sleep(1)  # 1 second between odds requests
        
        results['odds_collected'] = odds_collected
        
        print(f"✅ {period_name} Complete: {results['events_collected']} events, {results['odds_collected']} odds collected")
        return results
    
    def collect_full_historical_dataset(self) -> Dict:
        """Collect complete historical dataset for 2024 season + playoffs + 2025 Week 1"""
        print(f"🚀 Starting Full Historical Data Collection")
        print(f"📋 Target: 2024 NFL Season + Playoffs + 2025 Week 1")
        
        start_time = datetime.now()
        date_ranges = self.get_nfl_season_date_ranges()
        
        collection_results = {
            'collection_start': start_time.isoformat(),
            'total_periods': len(date_ranges),
            'periods_completed': 0,
            'total_events': 0,
            'total_odds': 0,
            'period_results': [],
            'errors': []
        }
        
        print(f"📊 Total periods to collect: {len(date_ranges)}")
        
        for i, (period_name, start_date, end_date) in enumerate(date_ranges, 1):
            print(f"\n📈 Progress: {i}/{len(date_ranges)} periods")
            
            try:
                period_result = self.collect_historical_data_for_period(
                    period_name, start_date, end_date
                )
                
                collection_results['period_results'].append(period_result)
                collection_results['periods_completed'] += 1
                collection_results['total_events'] += period_result['events_collected']
                collection_results['total_odds'] += period_result['odds_collected']
                
                # Add any period-specific errors
                collection_results['errors'].extend(period_result['errors'])
                
            except Exception as e:
                error_msg = f"Failed to collect {period_name}: {e}"
                print(f"❌ {error_msg}")
                collection_results['errors'].append(error_msg)
                continue
            
            # Progress update
            print(f"📊 Running totals: {collection_results['total_events']} events, {collection_results['total_odds']} odds")
        
        # Final summary
        end_time = datetime.now()
        collection_results['collection_end'] = end_time.isoformat()
        collection_results['duration_minutes'] = (end_time - start_time).total_seconds() / 60
        
        # Save collection summary
        self._save_raw_data(collection_results, f"collection_summary_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        
        return collection_results
    
    def _make_request(self, endpoint: str, params: Dict) -> Optional[requests.Response]:
        """Make API request with rate limiting and error handling"""
        
        # Check rate limiting
        if not self._check_rate_limit():
            print("⏱️  Rate limit reached, waiting 60 seconds...")
            time.sleep(60)
        
        try:
            response = self.session.get(endpoint, params=params, timeout=30)
            
            # Track request
            self._track_request()
            
            # Check for errors
            response.raise_for_status()
            
            # Check API usage
            remaining = response.headers.get('x-requests-remaining')
            if remaining:
                print(f"📊 API requests remaining: {remaining}")
            
            return response
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                print(f"⚠️  Historical data not available (404): {e}")
            else:
                print(f"❌ HTTP Error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return None
    
    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits"""
        now = time.time()
        
        # Remove timestamps older than 1 minute
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        # Check if we can make another request
        return len(self.request_timestamps) < self.max_requests_per_minute
    
    def _track_request(self):
        """Track a request for rate limiting"""
        self.request_timestamps.append(time.time())
        self.requests_made += 1
    
    def _save_raw_data(self, data: Dict, filename: str):
        """Save raw API response data with organized directory structure"""
        filepath = self.raw_data_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"💾 Saved raw data: {filename}")
    
    def enable_test_mode(self, max_games: int = 4):
        """Enable test mode to limit number of games collected"""
        self.max_games_limit = max_games
        print(f"🧪 Test mode enabled: Will collect data for max {max_games} games")
    
    def disable_test_mode(self):
        """Disable test mode"""
        self.max_games_limit = None
        print(f"🏈 Test mode disabled: Will collect data for all games")
    
    def get_usage_stats(self) -> Dict:
        """Get API usage statistics"""
        return {
            'requests_made': self.requests_made,
            'requests_in_last_minute': len(self.request_timestamps)
        }

def main():
    """Main execution function"""
    print("🏈 ===== NFL HISTORICAL DATA COLLECTOR =====")
    print("📋 Target: 2024 Season + Playoffs + 2025 Week 1")
    print("💰 Note: This will use significant API credits (historical data)")
    
    # Confirm before proceeding
    response = input("\n⚠️  This operation will consume many API credits. Continue? (y/N): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled by user")
        return
    
    # Initialize collector
    collector = HistoricalDataCollector()
    
    # Start collection
    try:
        results = collector.collect_full_historical_dataset()
        
        # Print final summary
        print(f"\n🎉 ===== COLLECTION COMPLETE =====")
        print(f"⏱️  Duration: {results['duration_minutes']:.1f} minutes")
        print(f"📊 Periods completed: {results['periods_completed']}/{results['total_periods']}")
        print(f"🏈 Total events: {results['total_events']}")
        print(f"📈 Total odds collected: {results['total_odds']}")
        
        if results['errors']:
            print(f"\n⚠️  Errors encountered: {len(results['errors'])}")
            for error in results['errors'][:5]:  # Show first 5 errors
                print(f"   • {error}")
            if len(results['errors']) > 5:
                print(f"   • ... and {len(results['errors']) - 5} more")
        
        # Usage stats
        stats = collector.get_usage_stats()
        print(f"\n📊 API Usage:")
        print(f"Total requests made: {stats['requests_made']}")
        print(f"Raw data files saved to: {collector.raw_data_dir}")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Collection interrupted by user")
    except Exception as e:
        print(f"\n❌ Collection failed: {e}")

if __name__ == "__main__":
    main()