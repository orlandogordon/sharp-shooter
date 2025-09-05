#!/usr/bin/env python3
"""
4-snapshot data collection system for NFL betting data
Collects data on Tuesday, Thursday, Saturday, and Sunday AM schedule
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import time

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from odds_api_client import OddsAPIClient

class NFLDataCollector:
    """Collects NFL betting data in 4 snapshots throughout the week"""
    
    def __init__(self):
        """Initialize the data collector"""
        self.api_client = OddsAPIClient()
        self.snapshot_schedule = Config.SNAPSHOT_SCHEDULE
        
        # Create data storage directory
        os.makedirs('collected_data', exist_ok=True)
        
        print(f"🗓️  4-Snapshot Collection Schedule:")
        for snapshot_num, description in self.snapshot_schedule.items():
            print(f"   Snapshot {snapshot_num}: {description}")
    
    def collect_weekly_data(self, week_number: int, force_snapshot: Optional[int] = None) -> Dict:
        """
        Collect data for a specific week
        
        Args:
            week_number: NFL week number (1-18)
            force_snapshot: Force collection as specific snapshot (1-4), ignore schedule
            
        Returns:
            Dict with collection results
        """
        # Determine which snapshot we're collecting
        snapshot_num = force_snapshot if force_snapshot else self._determine_current_snapshot()
        
        if not snapshot_num:
            return {
                'success': False,
                'error': 'Not a scheduled collection time',
                'next_collection': self._get_next_collection_time()
            }
        
        snapshot_desc = self.snapshot_schedule[snapshot_num]
        print(f"📸 Collecting Snapshot {snapshot_num}: {snapshot_desc}")
        print(f"🏈 Week {week_number}")
        
        collection_timestamp = datetime.now(timezone.utc).isoformat()
        
        # Collect game lines data
        print("\n📊 Collecting game lines...")
        games_data = self.api_client.get_nfl_games()
        
        if not games_data:
            return {
                'success': False,
                'error': 'Failed to retrieve game lines data',
                'snapshot': snapshot_num
            }
        
        # Process game lines into snapshot format
        processed_games = self._process_games_for_snapshot(games_data, snapshot_num, collection_timestamp)
        
        # Collect player props for each game
        print("\n🎯 Collecting player props...")
        all_props = []
        props_collected = 0
        
        # Limit to first 5 games for initial testing to preserve API calls
        for i, game in enumerate(games_data[:5]):
            game_id = game['id']
            home_team = game['home_team']
            away_team = game['away_team']
            
            print(f"🏈 Game {i+1}/5: {away_team} @ {home_team}")
            
            props = self.api_client.get_player_props(game_id)
            if props:
                processed_props = self._process_props_for_snapshot(props, snapshot_num, collection_timestamp)
                all_props.extend(processed_props)
                props_collected += len(processed_props)
                print(f"   ✅ {len(processed_props)} props collected")
            else:
                print(f"   ⚠️  No props available")
            
            # Small delay between games
            time.sleep(0.5)
        
        # Save collected data
        data_package = {
            'week': week_number,
            'snapshot': snapshot_num,
            'snapshot_description': snapshot_desc,
            'collection_timestamp': collection_timestamp,
            'games_count': len(processed_games),
            'props_count': len(all_props),
            'games_data': processed_games,
            'props_data': all_props
        }
        
        # Save to file
        filename = f'week_{week_number}_snapshot_{snapshot_num}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        filepath = os.path.join('collected_data', filename)
        
        with open(filepath, 'w') as f:
            json.dump(data_package, f, indent=2)
        
        # Get API usage stats
        api_stats = self.api_client.get_usage_stats()
        
        results = {
            'success': True,
            'week': week_number,
            'snapshot': snapshot_num,
            'snapshot_description': snapshot_desc,
            'collection_timestamp': collection_timestamp,
            'games_collected': len(processed_games),
            'props_collected': len(all_props),
            'api_requests_made': api_stats['requests_made'],
            'data_file': filepath
        }
        
        print(f"\n✅ Collection Complete!")
        print(f"📊 Games: {len(processed_games)}, Props: {len(all_props)}")
        print(f"🌐 API requests used: {api_stats['requests_made']}")
        print(f"💾 Data saved to: {filename}")
        
        return results
    
    def _process_games_for_snapshot(self, games_data: List[Dict], snapshot_num: int, timestamp: str) -> List[Dict]:
        """Process raw games data into snapshot format for Google Sheets"""
        processed_games = []
        
        for game in games_data:
            # Generate consistent Game_ID
            game_id = self._generate_game_id(game)
            
            base_game = {
                'game_id': game_id,
                'date': game.get('commence_time', ''),
                'home_team': game.get('home_team', ''),
                'away_team': game.get('away_team', '')
            }
            
            # Process each bookmaker's odds
            if not game.get('bookmakers'):
                continue
                
            for bookmaker in game['bookmakers']:
                bookmaker_name = bookmaker.get('title', '')
                
                if not bookmaker.get('markets'):
                    continue
                
                # Initialize snapshot data structure
                game_snapshot = base_game.copy()
                game_snapshot['bookmaker'] = bookmaker_name
                
                # Initialize all snapshot columns
                for i in range(1, 5):
                    # Spread columns
                    game_snapshot[f'spread_line_{i}'] = ''
                    game_snapshot[f'spread_odds_home_{i}'] = ''
                    game_snapshot[f'spread_odds_away_{i}'] = ''
                    game_snapshot[f'collected_date_{i}'] = ''
                    
                    # Total columns
                    game_snapshot[f'total_line_{i}'] = ''
                    game_snapshot[f'total_over_odds_{i}'] = ''
                    game_snapshot[f'total_under_odds_{i}'] = ''
                    
                    # Moneyline columns
                    game_snapshot[f'ml_home_{i}'] = ''
                    game_snapshot[f'ml_away_{i}'] = ''
                
                # Fill current snapshot data
                for market in bookmaker['markets']:
                    market_key = market.get('key', '')
                    outcomes = market.get('outcomes', [])
                    
                    if market_key == 'spreads' and outcomes:
                        # Process spread data
                        for outcome in outcomes:
                            team = outcome.get('name', '')
                            point = outcome.get('point', '')
                            price = outcome.get('price', '')
                            
                            if team == game['home_team']:
                                game_snapshot[f'spread_line_{snapshot_num}'] = point
                                game_snapshot[f'spread_odds_home_{snapshot_num}'] = price
                            elif team == game['away_team']:
                                game_snapshot[f'spread_odds_away_{snapshot_num}'] = price
                        
                        game_snapshot[f'collected_date_{snapshot_num}'] = timestamp
                    
                    elif market_key == 'totals' and outcomes:
                        # Process totals data
                        for outcome in outcomes:
                            name = outcome.get('name', '')
                            point = outcome.get('point', '')
                            price = outcome.get('price', '')
                            
                            if 'Over' in name:
                                game_snapshot[f'total_line_{snapshot_num}'] = point
                                game_snapshot[f'total_over_odds_{snapshot_num}'] = price
                            elif 'Under' in name:
                                game_snapshot[f'total_under_odds_{snapshot_num}'] = price
                    
                    elif market_key == 'h2h' and outcomes:
                        # Process moneyline data
                        for outcome in outcomes:
                            team = outcome.get('name', '')
                            price = outcome.get('price', '')
                            
                            if team == game['home_team']:
                                game_snapshot[f'ml_home_{snapshot_num}'] = price
                            elif team == game['away_team']:
                                game_snapshot[f'ml_away_{snapshot_num}'] = price
                
                processed_games.append(game_snapshot)
        
        return processed_games
    
    def _process_props_for_snapshot(self, props_data: List[Dict], snapshot_num: int, timestamp: str) -> List[Dict]:
        """Process raw props data into snapshot format for Google Sheets"""
        processed_props = []
        
        # Group props by player and market type
        props_by_player_market = {}
        
        for prop in props_data:
            key = f"{prop['player_name']}_{prop['market_type']}_{prop['bookmaker']}"
            if key not in props_by_player_market:
                props_by_player_market[key] = {
                    'game_id': prop['game_id'],
                    'player_name': prop['player_name'],
                    'position': self._extract_position_from_market(prop['market_type']),
                    'team': self._determine_player_team(prop, prop['home_team'], prop['away_team']),
                    'market_type': prop['market_type'],
                    'bookmaker': prop['bookmaker'],
                    'data_source': 'odds_api',
                    'over_line': None,
                    'over_odds': None,
                    'under_line': None,
                    'under_odds': None
                }
            
            # Add line data
            if prop['line_type'] == 'Over':
                props_by_player_market[key]['over_line'] = prop['line_value']
                props_by_player_market[key]['over_odds'] = prop['odds']
            elif prop['line_type'] == 'Under':
                props_by_player_market[key]['under_line'] = prop['line_value']
                props_by_player_market[key]['under_odds'] = prop['odds']
        
        # Convert to snapshot format
        for prop_data in props_by_player_market.values():
            prop_snapshot = {
                'game_id': prop_data['game_id'],
                'player_name': prop_data['player_name'],
                'position': prop_data['position'],
                'team': prop_data['team'],
                'market_type': prop_data['market_type'],
                'bookmaker': prop_data['bookmaker'],
                'data_source': prop_data['data_source']
            }
            
            # Initialize all snapshot columns
            for i in range(1, 5):
                prop_snapshot[f'over_line_{i}'] = ''
                prop_snapshot[f'over_odds_{i}'] = ''
                prop_snapshot[f'under_line_{i}'] = ''
                prop_snapshot[f'under_odds_{i}'] = ''
                prop_snapshot[f'collected_date_{i}'] = ''
            
            # Fill current snapshot data
            prop_snapshot[f'over_line_{snapshot_num}'] = prop_data['over_line'] or ''
            prop_snapshot[f'over_odds_{snapshot_num}'] = prop_data['over_odds'] or ''
            prop_snapshot[f'under_line_{snapshot_num}'] = prop_data['under_line'] or ''
            prop_snapshot[f'under_odds_{snapshot_num}'] = prop_data['under_odds'] or ''
            prop_snapshot[f'collected_date_{snapshot_num}'] = timestamp
            
            # Add reference data placeholders
            prop_snapshot['season_over_rate'] = 'N/A'
            prop_snapshot['season_attempts'] = 'N/A'
            prop_snapshot['vs_defense_rate'] = 'N/A'
            prop_snapshot['recent_form_3g'] = 'N/A'
            prop_snapshot['home_away_split'] = 'N/A'
            
            processed_props.append(prop_snapshot)
        
        return processed_props
    
    def _generate_game_id(self, game: Dict) -> str:
        """Generate consistent Game_ID from game data"""
        date = game.get('commence_time', '')[:10]  # YYYY-MM-DD
        home = game.get('home_team', '').replace(' ', '')[:4].upper()
        away = game.get('away_team', '').replace(' ', '')[:4].upper()
        
        return f"NFL_2025_{date}_{away}_{home}"
    
    def _extract_position_from_market(self, market_type: str) -> str:
        """Extract player position from market type"""
        if 'pass' in market_type.lower():
            return 'QB'
        elif 'rush' in market_type.lower():
            return 'RB/QB'
        elif 'receiv' in market_type.lower():
            return 'WR/TE/RB'
        else:
            return 'Unknown'
    
    def _determine_player_team(self, prop: Dict, home_team: str, away_team: str) -> str:
        """Determine which team a player belongs to (simplified logic)"""
        # This is a placeholder - in reality you'd need a player-team mapping
        return 'TBD'
    
    def _determine_current_snapshot(self) -> Optional[int]:
        """Determine which snapshot should be collected based on current date/time"""
        now = datetime.now()
        day_of_week = now.weekday()  # 0=Monday, 6=Sunday
        hour = now.hour
        
        print(f"📅 Current time: {now.strftime('%A %Y-%m-%d %H:%M:%S')}")
        
        # Smart schedule mapping based on day and time
        if day_of_week == 1:  # Tuesday
            if hour >= 10:  # After 10 AM Tuesday
                return 1
        elif day_of_week == 3:  # Thursday
            if hour >= 10:  # After 10 AM Thursday
                return 2
        elif day_of_week == 5:  # Saturday
            if hour >= 10:  # After 10 AM Saturday
                return 3
        elif day_of_week == 6:  # Sunday
            if hour >= 8 and hour < 13:  # Sunday 8 AM - 1 PM
                return 4
        
        # Handle edge cases - if we're outside normal times
        if day_of_week >= 1:  # Tuesday or later in the week
            # Determine what snapshot we should be on based on how far through week
            if day_of_week == 1:  # Tuesday
                return 1
            elif day_of_week >= 2 and day_of_week < 4:  # Wed-Thu
                return 2  
            elif day_of_week >= 4 and day_of_week < 6:  # Thu-Sat
                return 3
            elif day_of_week == 6:  # Sunday
                return 4
        
        # If it's Monday or very early Tuesday, suggest waiting
        return None
    
    def _get_next_collection_time(self) -> str:
        """Get next scheduled collection time"""
        now = datetime.now()
        day_of_week = now.weekday()
        
        if day_of_week < 1:  # Before Tuesday
            return "Next Tuesday (Snapshot 1)"
        elif day_of_week < 3:  # Before Thursday
            return "Next Thursday (Snapshot 2)"
        elif day_of_week < 5:  # Before Saturday
            return "Next Saturday (Snapshot 3)"
        elif day_of_week < 6:  # Before Sunday
            return "Next Sunday (Snapshot 4)"
        else:
            return "Next Tuesday (Snapshot 1 - New Week)"

def main():
    """Test the data collection system"""
    collector = NFLDataCollector()
    
    print("🧪 Testing data collection system...")
    print("📅 Current day:", datetime.now().strftime("%A"))
    
    # Force collect as Snapshot 1 for testing
    print("\n🧪 Forcing collection as Snapshot 1 (Tuesday - Opening Lines)")
    
    results = collector.collect_weekly_data(week_number=1, force_snapshot=1)
    
    if results['success']:
        print(f"\n🎉 Collection Test Successful!")
        print(f"📊 Results Summary:")
        print(f"   Week: {results['week']}")
        print(f"   Snapshot: {results['snapshot']} - {results['snapshot_description']}")
        print(f"   Games collected: {results['games_collected']}")
        print(f"   Props collected: {results['props_collected']}")
        print(f"   API requests used: {results['api_requests_made']}")
        print(f"   Data file: {os.path.basename(results['data_file'])}")
        
        print(f"\n✅ 4-snapshot collection system is working!")
        print(f"📁 Check the 'collected_data' folder for output files")
    else:
        print(f"❌ Collection failed: {results.get('error')}")

if __name__ == "__main__":
    main()