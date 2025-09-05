#!/usr/bin/env python3
"""
The Odds API client for NFL betting data collection
Handles authentication, rate limiting, and data fetching
"""

import requests
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
import json
import os
import sys

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

class OddsAPIClient:
    """Client for The Odds API with rate limiting and error handling"""
    
    def __init__(self):
        """Initialize the API client"""
        Config.validate_config()
        
        self.api_key = Config.ODDS_API_KEY
        self.base_url = Config.ODDS_API_BASE_URL
        self.regions = Config.ODDS_API_REGIONS
        self.markets = Config.ODDS_API_MARKETS
        
        # Rate limiting
        self.requests_made = 0
        self.max_requests_per_minute = 500  # The Odds API limit
        self.request_timestamps = []
        
        # Session for connection pooling
        self.session = requests.Session()
        
        print(f"🏈 Initialized Odds API Client")
        print(f"📊 Markets: {self.markets}")
        print(f"🌎 Regions: {self.regions}")
    
    def get_nfl_games(self) -> Optional[List[Dict]]:
        """Get current NFL games and odds"""
        endpoint = f"{self.base_url}/sports/americanfootball_nfl/odds"
        
        params = {
            'api_key': self.api_key,
            'regions': self.regions,
            'markets': self.markets,
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        }
        
        try:
            response = self._make_request(endpoint, params)
            
            if response:
                games = response.json()
                print(f"✅ Retrieved {len(games)} NFL games")
                return games
            else:
                print("❌ Failed to retrieve NFL games")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching NFL games: {e}")
            return None
    
    def get_player_props(self, event_id: str) -> Optional[List[Dict]]:
        """Get player props for a specific game"""
        # Player props markets (US markets only) - using correct API market names
        prop_markets = [
            'player_pass_yds',        # Passing Yards
            'player_pass_tds',        # Passing Touchdowns
            'player_rush_yds',        # Rushing Yards
            'player_receptions',      # Receptions
            'player_reception_yds',   # Reception Yards
            'player_anytime_td'       # Anytime Touchdown Scorer
        ]
        
        all_props = []
        
        for market in prop_markets:
            try:
                props = self._fetch_single_prop_market(event_id, market)
                if props:
                    all_props.extend(props)
                    print(f"✅ {market}: {len(props)} props")
                else:
                    print(f"⚠️  {market}: No props available")
                    
                # Small delay between requests
                time.sleep(0.2)
                
            except Exception as e:
                print(f"❌ Error fetching {market}: {e}")
                continue
        
        if all_props:
            print(f"✅ Total player props retrieved: {len(all_props)}")
            return all_props
        else:
            print("⚠️  No player props available for this game")
            return None
    
    def _fetch_single_prop_market(self, event_id: str, market: str) -> Optional[List[Dict]]:
        """Fetch a single prop market with proper error handling"""
        endpoint = f"{self.base_url}/sports/americanfootball_nfl/events/{event_id}/odds"
        
        params = {
            'api_key': self.api_key,
            'regions': self.regions,
            'markets': market,
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        }
        
        response = self._make_request(endpoint, params)
        
        if not response:
            return None
            
        try:
            # Parse response
            response_data = response.json()
            
            # Handle different response formats
            if isinstance(response_data, str):
                print(f"⚠️  API returned string response for {market}: {response_data}")
                return None
            
            if isinstance(response_data, list):
                props_data = response_data
            elif isinstance(response_data, dict):
                # Sometimes wrapped in an object
                props_data = response_data.get('data', [response_data])
            else:
                print(f"⚠️  Unexpected response format for {market}: {type(response_data)}")
                return None
            
            # Process the props data
            return self._process_player_props(props_data, market)
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error for {market}: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error processing {market}: {e}")
            return None
    
    def _process_player_props(self, props_data: List[Dict], market_type: str) -> List[Dict]:
        """Process raw player props data into standardized format"""
        processed_props = []
        
        if not props_data:
            return processed_props
            
        for game in props_data:
            if not isinstance(game, dict):
                continue
                
            # Skip if no bookmakers data
            bookmakers = game.get('bookmakers', [])
            if not bookmakers:
                continue
                
            for bookmaker in bookmakers:
                if not isinstance(bookmaker, dict):
                    continue
                    
                markets = bookmaker.get('markets', [])
                if not markets:
                    continue
                    
                for market in markets:
                    if not isinstance(market, dict):
                        continue
                        
                    # Skip if not the market we're looking for
                    if market.get('key') != market_type:
                        continue
                    
                    outcomes = market.get('outcomes', [])
                    for outcome in outcomes:
                        if not isinstance(outcome, dict):
                            continue
                            
                        # Extract player name from description or name
                        player_name = ''
                        if 'description' in outcome:
                            player_name = outcome['description'].replace(' Over', '').replace(' Under', '').strip()
                        elif 'name' in outcome:
                            # Sometimes player name is in the outcome name
                            name_parts = outcome['name'].split(' ')
                            if len(name_parts) >= 2:
                                player_name = ' '.join(name_parts[:-1])  # Everything except last word (Over/Under)
                        
                        # Skip if we can't extract player name
                        if not player_name:
                            continue
                        
                        # Determine line type
                        line_type = 'Unknown'
                        if 'Over' in outcome.get('name', ''):
                            line_type = 'Over'
                        elif 'Under' in outcome.get('name', ''):
                            line_type = 'Under'
                        
                        prop = {
                            'game_id': game.get('id', ''),
                            'commence_time': game.get('commence_time', ''),
                            'home_team': game.get('home_team', ''),
                            'away_team': game.get('away_team', ''),
                            'bookmaker': bookmaker.get('title', ''),
                            'market_type': self._market_display_name(market_type),
                            'player_name': player_name,
                            'line_type': line_type,
                            'line_value': outcome.get('point'),
                            'odds': outcome.get('price'),
                            'collected_at': datetime.now(timezone.utc).isoformat()
                        }
                        processed_props.append(prop)
        
        return processed_props
    
    def _market_display_name(self, market_key: str) -> str:
        """Convert API market key to display name"""
        market_names = {
            'player_pass_yds': 'Passing Yards',
            'player_pass_tds': 'Passing TDs',
            'player_rush_yds': 'Rushing Yards',
            'player_receptions': 'Receptions',
            'player_reception_yds': 'Receiving Yards',
            'player_anytime_td': 'Anytime TD'
        }
        return market_names.get(market_key, market_key)
    
    def _make_request(self, endpoint: str, params: Dict) -> Optional[requests.Response]:
        """Make API request with rate limiting and error handling"""
        
        # Check rate limiting
        if not self._check_rate_limit():
            print("⏱️  Rate limit reached, waiting...")
            time.sleep(60)  # Wait 1 minute
        
        try:
            print(f"🌐 Making request to: {endpoint}")
            
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
    
    def get_usage_stats(self) -> Dict:
        """Get API usage statistics"""
        return {
            'requests_made': self.requests_made,
            'requests_in_last_minute': len(self.request_timestamps)
        }
    
    def save_raw_data(self, data: Dict, filename: str):
        """Save raw API response data for debugging"""
        os.makedirs('raw_data', exist_ok=True)
        
        filepath = os.path.join('raw_data', filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Saved raw data to: {filepath}")

def main():
    """Test the Odds API client"""
    client = OddsAPIClient()
    
    print("🧪 Testing NFL games retrieval...")
    games = client.get_nfl_games()
    
    if games:
        print(f"✅ Success! Retrieved {len(games)} games")
        
        # Save sample data
        client.save_raw_data(games, f'nfl_games_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        
        # Test player props for first few games
        if games:
            print(f"\n🧪 Testing player props for multiple games...")
            
            # Try first 3 games to find one with props
            for i, game in enumerate(games[:3]):
                game_id = game['id']
                home_team = game['home_team']
                away_team = game['away_team']
                
                print(f"\n🏈 Game {i+1}: {away_team} @ {home_team} (ID: {game_id[:8]}...)")
                
                props = client.get_player_props(game_id)
                if props:
                    print(f"✅ Success! Retrieved {len(props)} player props")
                    
                    # Show sample props
                    print("📊 Sample props:")
                    for prop in props[:3]:  # Show first 3
                        print(f"   {prop['player_name']} - {prop['market_type']} {prop['line_type']} {prop['line_value']} ({prop['odds']}) - {prop['bookmaker']}")
                    
                    client.save_raw_data(props, f'player_props_{game_id[:8]}.json')
                    break  # Found props, stop testing
                else:
                    print("⚠️  No player props available for this game")
            
            print(f"\nℹ️  Player props may be limited because:")
            print(f"   • Props are often released closer to game time")
            print(f"   • Not all games have props available")
            print(f"   • US market focus may limit availability")
    
    # Print usage stats
    stats = client.get_usage_stats()
    print(f"\n📊 Usage Statistics:")
    print(f"Total requests made: {stats['requests_made']}")
    print(f"Requests in last minute: {stats['requests_in_last_minute']}")

if __name__ == "__main__":
    main()