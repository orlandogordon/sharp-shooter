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
        all_games_data = self.api_client.get_nfl_games(week_number)
        
        if not all_games_data:
            return {
                'success': False,
                'error': 'Failed to retrieve game lines data',
                'snapshot': snapshot_num
            }
        
        # Filter games based on snapshot type
        if snapshot_num == 1:
            # Snapshot 1 (Tuesday): Opening lines for ALL games
            games_data = all_games_data
            print(f"📊 Opening lines: collecting all {len(games_data)} games for the week")
        else:
            # Snapshots 2-6 (Day of Event): Final lines for TODAY'S games only
            games_data = self._filter_games_for_today(all_games_data)
            if not games_data:
                print("⚠️  No games found for today - checking game schedule")
                print("📅 Available games this week:")
                for game in all_games_data[:5]:  # Show first 5 games for debugging
                    game_date = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
                    print(f"   {game['away_team']} @ {game['home_team']} - {game_date.strftime('%A %m/%d %H:%M')}")
                return {
                    'success': False,
                    'error': 'No games scheduled for today - cannot collect final lines',
                    'snapshot': snapshot_num
                }
            print(f"📊 Final lines: collecting {len(games_data)} games for today")
        
        # Process game lines into snapshot format
        processed_games = self._process_games_for_snapshot(games_data, snapshot_num, collection_timestamp)
        
        # Collect player props based on snapshot strategy
        all_props = []
        anytime_td_props = []
        props_collected = 0
        td_props_collected = 0
        
        # Check if we should collect props for this snapshot
        should_collect_props = self._should_collect_props_for_snapshot(snapshot_num)
        
        if should_collect_props:
            print("\n🎯 Collecting player props...")
            
            # For props, we always use today's games (already filtered in game lines logic for snapshots 2-6)
            props_games = games_data if snapshot_num > 1 else []
            
            if not props_games:
                print("⚠️  No games for player props collection")
                return {
                    'success': False, 
                    'error': 'No games available for player props',
                    'snapshot': snapshot_num
                }
            
            print(f"🎯 Collecting props for {len(props_games)} games")
            
            for i, game in enumerate(props_games):
                game_id = game['id']
                home_team = game['home_team']
                away_team = game['away_team']
                game_date = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
                
                print(f"🏈 Game {i+1}/{len(props_games)}: {away_team} @ {home_team} ({game_date.strftime('%A %H:%M')})")
                
                props = self.api_client.get_player_props(game_id)
                if props:
                    # Separate regular props from anytime TD props
                    regular_props, td_props = self._separate_anytime_td_props(props)
                    
                    # Process regular props
                    if regular_props:
                        processed_props = self._process_props_for_snapshot(regular_props, snapshot_num, collection_timestamp)
                        all_props.extend(processed_props)
                        props_collected += len(processed_props)
                        print(f"   ✅ {len(processed_props)} regular props collected")
                    
                    # Process anytime TD props separately
                    if td_props:
                        processed_td_props = self._process_anytime_td_props_for_snapshot(td_props, snapshot_num, collection_timestamp)
                        anytime_td_props.extend(processed_td_props)
                        td_props_collected += len(processed_td_props)
                        print(f"   🏆 {len(processed_td_props)} anytime TD props collected")
                else:
                    print(f"   ⚠️  No props available")
                
                # Small delay between games
                time.sleep(0.5)
        else:
            print(f"\n⏭️  Skipping player props collection for {snapshot_desc}")
            print("   Player props are only collected on day of event (final pre-game snapshots)")
        
        # Save collected data
        data_package = {
            'week': week_number,
            'snapshot': snapshot_num,
            'snapshot_description': snapshot_desc,
            'collection_timestamp': collection_timestamp,
            'games_count': len(processed_games),
            'props_count': len(all_props),
            'anytime_td_props_count': len(anytime_td_props),
            'games_data': processed_games,
            'props_data': all_props,
            'anytime_td_props_data': anytime_td_props
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
            'anytime_td_props_collected': len(anytime_td_props),
            'api_requests_made': api_stats['requests_made'],
            'data_file': filepath
        }
        
        print(f"\n✅ Collection Complete!")
        print(f"📊 Games: {len(processed_games)}, Props: {len(all_props)}, TD Props: {len(anytime_td_props)}")
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
                
                # Initialize 2-snapshot data structure
                game_snapshot = base_game.copy()
                game_snapshot['bookmaker'] = bookmaker_name
                
                # Initialize opening and final snapshot columns
                # Opening snapshot (Snapshot 1 - Tuesday)
                game_snapshot['opening_spread_line'] = ''
                game_snapshot['opening_spread_home_odds'] = ''
                game_snapshot['opening_spread_away_odds'] = ''
                game_snapshot['opening_collected_date'] = ''
                game_snapshot['opening_total_line'] = ''
                game_snapshot['opening_total_over_odds'] = ''
                game_snapshot['opening_total_under_odds'] = ''
                game_snapshot['opening_ml_home'] = ''
                game_snapshot['opening_ml_away'] = ''
                
                # Final snapshot (Day of Event)
                game_snapshot['final_spread_line'] = ''
                game_snapshot['final_spread_home_odds'] = ''
                game_snapshot['final_spread_away_odds'] = ''
                game_snapshot['final_collected_date'] = ''
                game_snapshot['final_total_line'] = ''
                game_snapshot['final_total_over_odds'] = ''
                game_snapshot['final_total_under_odds'] = ''
                game_snapshot['final_ml_home'] = ''
                game_snapshot['final_ml_away'] = ''
                
                # Determine snapshot type (opening vs final)
                snapshot_type = 'opening' if snapshot_num == 1 else 'final'
                
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
                                game_snapshot[f'{snapshot_type}_spread_line'] = point
                                game_snapshot[f'{snapshot_type}_spread_home_odds'] = price
                            elif team == game['away_team']:
                                game_snapshot[f'{snapshot_type}_spread_away_odds'] = price
                        
                        game_snapshot[f'{snapshot_type}_collected_date'] = timestamp
                    
                    elif market_key == 'totals' and outcomes:
                        # Process totals data
                        for outcome in outcomes:
                            name = outcome.get('name', '')
                            point = outcome.get('point', '')
                            price = outcome.get('price', '')
                            
                            if 'Over' in name:
                                game_snapshot[f'{snapshot_type}_total_line'] = point
                                game_snapshot[f'{snapshot_type}_total_over_odds'] = price
                            elif 'Under' in name:
                                game_snapshot[f'{snapshot_type}_total_under_odds'] = price
                    
                    elif market_key == 'h2h' and outcomes:
                        # Process moneyline data
                        for outcome in outcomes:
                            team = outcome.get('name', '')
                            price = outcome.get('price', '')
                            
                            if team == game['home_team']:
                                game_snapshot[f'{snapshot_type}_ml_home'] = price
                            elif team == game['away_team']:
                                game_snapshot[f'{snapshot_type}_ml_away'] = price
                
                processed_games.append(game_snapshot)
        
        return processed_games
    
    def _process_props_for_snapshot(self, props_data: List[Dict], snapshot_num: int, timestamp: str) -> List[Dict]:
        """Process raw props data into simplified single snapshot format for Google Sheets"""
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
        
        # Convert to simplified single snapshot format
        for prop_data in props_by_player_market.values():
            prop_snapshot = {
                'game_id': prop_data['game_id'],
                'player_name': prop_data['player_name'],
                'position': prop_data['position'],
                'team': prop_data['team'],
                'market_type': prop_data['market_type'],
                'bookmaker': prop_data['bookmaker'],
                'data_source': prop_data['data_source'],
                # Single snapshot data (day of event only)
                'over_line': prop_data['over_line'] or '',
                'over_odds': prop_data['over_odds'] or '',
                'under_line': prop_data['under_line'] or '',
                'under_odds': prop_data['under_odds'] or '',
                'collected_date': timestamp,
                # Reference data placeholders
                'season_over_rate': 'N/A',
                'season_attempts': 'N/A',
                'vs_defense_rate': 'N/A',
                'recent_form_3g': 'N/A',
                'home_away_split': 'N/A'
            }
            
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
    
    def _should_collect_props_for_snapshot(self, snapshot_num: int) -> bool:
        """Determine if player props should be collected for this snapshot"""
        # Snapshot 1 (Tuesday opening lines) = NO props
        # Snapshots 2-6 (day of event) = YES props
        return snapshot_num > 1
    
    def _separate_anytime_td_props(self, props: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Separate anytime TD props from regular player props"""
        regular_props = []
        anytime_td_props = []
        
        for prop in props:
            if prop.get('market_type') == 'Anytime TD':
                anytime_td_props.append(prop)
            else:
                regular_props.append(prop)
        
        return regular_props, anytime_td_props
    
    def _process_anytime_td_props_for_snapshot(self, td_props: List[Dict], snapshot_num: int, timestamp: str) -> List[Dict]:
        """Process anytime TD props into simplified format for dedicated sheet"""
        processed_td_props = []
        
        # Group TD props by player and bookmaker
        props_by_player_bookmaker = {}
        
        for prop in td_props:
            key = f"{prop['player_name']}_{prop['bookmaker']}"
            if key not in props_by_player_bookmaker:
                props_by_player_bookmaker[key] = {
                    'game_id': prop['game_id'],
                    'player_name': prop['player_name'],
                    'team': self._determine_player_team(prop, prop['home_team'], prop['away_team']),
                    'bookmaker': prop['bookmaker'],
                    'data_source': 'odds_api',
                    'odds': None
                }
            
            # Anytime TD props are Yes/No markets - we want the "Yes" odds
            if prop['line_type'] == 'Over':  # "Over" represents "Yes" for TD scoring
                props_by_player_bookmaker[key]['odds'] = prop['odds']
        
        # Convert to sheet format (simplified - only current odds, no snapshots)
        for prop_data in props_by_player_bookmaker.values():
            td_prop = {
                'game_id': prop_data['game_id'],
                'player_name': prop_data['player_name'],
                'team': prop_data['team'],
                'bookmaker': prop_data['bookmaker'],
                'data_source': prop_data['data_source'],
                'anytime_td_odds': prop_data['odds'] or '',
                'collected_date': timestamp,
                # Reference data placeholders
                'season_tds': 'N/A',
                'red_zone_targets': 'N/A',
                'goal_line_carries': 'N/A',
                'recent_td_rate': 'N/A'
            }
            
            processed_td_props.append(td_prop)
        
        return processed_td_props
    
    def _filter_games_for_today(self, games_data: List[Dict]) -> List[Dict]:
        """Filter games to only those happening today (event-based collection with timezone handling)"""
        now = datetime.now()
        today = now.date()
        current_day_name = now.strftime('%A').lower()
        todays_games = []
        
        print(f"🕐 Current time: {now.strftime('%A %Y-%m-%d %H:%M:%S')} (looking for {current_day_name} games)")
        
        for game in games_data:
            try:
                # Parse game commence time
                commence_time_str = game.get('commence_time', '')
                if not commence_time_str:
                    continue
                    
                # Handle ISO format with Z timezone (UTC)
                if commence_time_str.endswith('Z'):
                    game_datetime_utc = datetime.fromisoformat(commence_time_str.replace('Z', '+00:00'))
                else:
                    game_datetime_utc = datetime.fromisoformat(commence_time_str)
                
                # Convert UTC to local time for proper day detection
                # Thursday Night Football often starts around 8:15 PM ET (00:15 UTC Friday)
                # We need to check if this is conceptually a "Thursday" game
                game_datetime_local = game_datetime_utc.replace(tzinfo=None)  # Remove timezone for local comparison
                game_date_utc = game_datetime_utc.date()
                
                # Special handling for Thursday Night Football
                # If it's Thursday and we find a game early Friday morning (00:00-06:00 UTC), it's likely TNF
                if current_day_name == 'thursday':
                    # Check for games that are technically Friday but early morning (TNF in UTC)
                    if game_date_utc == today or (game_date_utc == today + timedelta(days=1) and game_datetime_utc.hour < 6):
                        print(f"🏈 Found Thursday Night Football: {game['away_team']} @ {game['home_team']} at {game_datetime_utc.strftime('%A %H:%M UTC')}")
                        todays_games.append(game)
                # For other days, use standard date matching
                elif game_date_utc == today:
                    print(f"🏈 Found {current_day_name} game: {game['away_team']} @ {game['home_team']} at {game_datetime_utc.strftime('%A %H:%M UTC')}")
                    todays_games.append(game)
                    
            except (ValueError, TypeError) as e:
                print(f"⚠️  Error parsing game time for {game.get('away_team', '')} @ {game.get('home_team', '')}: {e}")
                continue
        
        if todays_games:
            print(f"✅ Found {len(todays_games)} games for {current_day_name}")
        else:
            print(f"⚠️  No games found for {current_day_name}")
        
        return todays_games
    
    def _determine_current_snapshot(self) -> Optional[int]:
        """Determine which snapshot should be collected based on current date/time and actual game schedule"""
        now = datetime.now()
        day_of_week = now.weekday()  # 0=Monday, 6=Sunday
        hour = now.hour
        day_name = now.strftime('%A').lower()
        
        print(f"📅 Current time: {now.strftime('%A %Y-%m-%d %H:%M:%S')}")
        
        # Event-based collection logic
        if day_of_week == 1:  # Tuesday
            if hour >= 10:  # After 10 AM Tuesday
                print("📊 Tuesday opening lines collection window")
                return 1
        elif day_of_week == 3:  # Thursday  
            if hour >= 15:  # After 3 PM Thursday (2-3 hours before TNF kickoff)
                print("🏈 Thursday Night Football collection window")
                return 2
        elif day_of_week == 4:  # Friday
            if hour >= 15:  # After 3 PM Friday (for rare Friday games)
                print("🏈 Friday games collection window (if any)")
                return 3
        elif day_of_week == 5:  # Saturday
            if hour >= 10:  # After 10 AM Saturday (for Saturday games)  
                print("🏈 Saturday games collection window")
                return 4
        elif day_of_week == 6:  # Sunday
            if hour >= 8 and hour < 13:  # Sunday 8 AM - 1 PM (before 1 PM kickoffs)
                print("🏈 Sunday games collection window")
                return 5
        elif day_of_week == 0:  # Monday
            if hour >= 15:  # After 3 PM Monday (before MNF kickoff)
                print("🏈 Monday Night Football collection window")
                return 6
        
        # Fallback logic for edge cases
        print("⏰ Outside optimal collection windows, using fallback logic...")
        
        if day_of_week >= 1:  # Tuesday or later in the week
            if day_of_week == 1:  # Tuesday
                return 1
            elif day_of_week == 2:  # Wednesday
                return 2  # Prepare for Thursday
            elif day_of_week == 3:  # Thursday
                return 2
            elif day_of_week == 4:  # Friday  
                return 3
            elif day_of_week == 5:  # Saturday
                return 4
            elif day_of_week == 6:  # Sunday
                return 5
            elif day_of_week == 0:  # Monday
                return 6
        
        # If it's very early Tuesday or late Monday, suggest waiting
        print("⏸️  Not a scheduled collection time")
        return None
    
    def _get_next_collection_time(self) -> str:
        """Get next scheduled collection time"""
        now = datetime.now()
        day_of_week = now.weekday()
        hour = now.hour
        
        # Event-based next collection logic
        if day_of_week < 1 or (day_of_week == 1 and hour < 10):  # Before Tuesday 10 AM
            return "Next Tuesday 10 AM+ (Snapshot 1 - Opening Lines)"
        elif day_of_week < 3 or (day_of_week == 3 and hour < 15):  # Before Thursday 3 PM
            return "Next Thursday 3 PM+ (Snapshot 2 - Thursday Night Football)"
        elif day_of_week < 4 or (day_of_week == 4 and hour < 15):  # Before Friday 3 PM
            return "Next Friday 3 PM+ (Snapshot 3 - Friday Games if any)"
        elif day_of_week < 5 or (day_of_week == 5 and hour < 10):  # Before Saturday 10 AM
            return "Next Saturday 10 AM+ (Snapshot 4 - Saturday Games)"
        elif day_of_week < 6 or (day_of_week == 6 and hour < 8):  # Before Sunday 8 AM
            return "Next Sunday 8 AM-1 PM (Snapshot 5 - Sunday Games)"
        elif day_of_week == 6 and hour >= 13:  # After Sunday 1 PM
            return "Next Monday 3 PM+ (Snapshot 6 - Monday Night Football)"
        elif day_of_week == 0 and hour < 15:  # Before Monday 3 PM
            return "Next Monday 3 PM+ (Snapshot 6 - Monday Night Football)"
        else:
            return "Next Tuesday 10 AM+ (Snapshot 1 - New Week Opening Lines)"

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