#!/usr/bin/env python3
"""
Historical Data Processor for Sharp-Shooter
Converts raw historical API data into format compatible with existing sheets writer
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pathlib import Path

class HistoricalDataProcessor:
    """Processes raw historical data into sheets-compatible format"""
    
    def __init__(self):
        self.raw_data_dir = Path('raw_data/historical')
    
    def process_week_data(self, week_name: str, start_date: datetime, end_date: datetime) -> Dict:
        """
        Process historical data for a week into sheets-compatible format
        
        Args:
            week_name: Name of the week (e.g., "2024_Week_1")
            start_date: Start of week date range
            end_date: End of week date range
            
        Returns:
            Dict in format expected by sheets writer
        """
        print(f"🔄 Processing historical data for {week_name}")
        
        # Find raw data files for this week
        event_files = list(self.raw_data_dir.glob(f"events_{week_name}_*.json"))
        odds_files = list(self.raw_data_dir.glob(f"odds_{week_name}_*.json"))
        
        if not event_files:
            raise ValueError(f"No event files found for {week_name}")
        
        print(f"📁 Found {len(event_files)} event files and {len(odds_files)} odds files")
        
        # Load and process events
        events = {}
        for event_file in event_files:
            with open(event_file, 'r') as f:
                event_data = json.load(f)
            
            for event in event_data.get('data', []):
                game_id = event.get('id')
                if game_id:
                    events[game_id] = event
        
        # Load and process odds
        odds_map = {}
        for odds_file in odds_files:
            with open(odds_file, 'r') as f:
                odds_data = json.load(f)
            
            # Extract game ID from filename (format: odds_2024_Week_1_TeamA_TeamB_12345678.json)
            filename = odds_file.stem
            parts = filename.split('_')
            if len(parts) >= 6:
                game_id_part = parts[-1]  # Last part is game ID prefix
                
                # Find matching event by ID prefix
                for event_id in events.keys():
                    if event_id.startswith(game_id_part):
                        odds_map[event_id] = odds_data
                        break
        
        print(f"✅ Matched {len(odds_map)} odds files to events")
        
        # Convert to sheets format
        processed_data = self._convert_to_sheets_format(events, odds_map, week_name)
        
        return processed_data
    
    def _convert_to_sheets_format(self, events: Dict, odds_map: Dict, week_name: str) -> Dict:
        """Convert raw data to sheets writer format"""
        
        # Extract week number from week_name (e.g., "2024_Week_1" -> 1)
        week_num = 1
        if 'Week_' in week_name:
            try:
                week_num = int(week_name.split('Week_')[1])
            except:
                pass
        
        games_data = []
        props_data = []
        anytime_td_props_data = []
        
        for game_id, event in events.items():
            odds_data = odds_map.get(game_id)
            
            # Process game lines
            game_line = self._process_game_lines(event, odds_data)
            if game_line:
                games_data.append(game_line)
            
            # Process player props if odds available
            if odds_data:
                props, td_props = self._process_player_props(event, odds_data)
                props_data.extend(props)
                anytime_td_props_data.extend(td_props)
        
        # Build final data structure in sheets writer format
        collection_data = {
            'week': week_num,
            'snapshot': 1,  # Historical data is single snapshot
            'snapshot_description': f'Historical Data - {week_name}',
            'collection_time': datetime.now(timezone.utc).isoformat(),
            'games_count': len(games_data),
            'props_count': len(props_data),
            'anytime_td_props_count': len(anytime_td_props_data),
            'games_data': games_data,
            'props_data': props_data,
            'anytime_td_props_data': anytime_td_props_data,
            'metadata': {
                'source': 'historical_api',
                'week_name': week_name,
                'processing_time': datetime.now(timezone.utc).isoformat()
            }
        }
        
        print(f"📊 Processed data summary:")
        print(f"   Games: {len(games_data)}")
        print(f"   Player Props: {len(props_data)}")
        print(f"   Anytime TD Props: {len(anytime_td_props_data)}")
        
        return collection_data
    
    def _process_game_lines(self, event: Dict, odds_data: Dict) -> Optional[Dict]:
        """Process game line data"""
        game_line = {
            'game_id': event.get('id', ''),
            'commence_time': event.get('commence_time', ''),
            'home_team': event.get('home_team', ''),
            'away_team': event.get('away_team', ''),
            'sport_title': event.get('sport_title', ''),
            'bookmakers': []
        }
        
        if not odds_data:
            return game_line
        
        # Process bookmaker odds data
        bookmakers = odds_data.get('bookmakers', [])
        for bookmaker in bookmakers:
            bookmaker_data = {
                'title': bookmaker.get('title', ''),
                'markets': {}
            }
            
            markets = bookmaker.get('markets', [])
            for market in markets:
                market_key = market.get('key', '')
                market_data = {
                    'key': market_key,
                    'outcomes': market.get('outcomes', [])
                }
                bookmaker_data['markets'][market_key] = market_data
            
            game_line['bookmakers'].append(bookmaker_data)
        
        return game_line
    
    def _process_player_props(self, event: Dict, odds_data: Dict) -> tuple:
        """Process player props data"""
        regular_props = []
        anytime_td_props = []
        
        if not odds_data:
            return regular_props, anytime_td_props
        
        game_id = event.get('id', '')
        home_team = event.get('home_team', '')
        away_team = event.get('away_team', '')
        commence_time = event.get('commence_time', '')
        
        bookmakers = odds_data.get('bookmakers', [])
        for bookmaker in bookmakers:
            bookmaker_title = bookmaker.get('title', '')
            
            markets = bookmaker.get('markets', [])
            for market in markets:
                market_key = market.get('key', '')
                outcomes = market.get('outcomes', [])
                
                for outcome in outcomes:
                    player_name = outcome.get('description', outcome.get('name', ''))
                    # Clean player name
                    player_name = player_name.replace(' Over', '').replace(' Under', '').strip()
                    
                    if not player_name:
                        continue
                    
                    prop_data = {
                        'game_id': game_id,
                        'player_name': player_name,
                        'position': '',  # Not available in historical data
                        'team': '',      # Not available in historical data
                        'market_type': self._market_display_name(market_key),
                        'bookmaker': bookmaker_title,
                        'line_value': outcome.get('point'),
                        'odds': outcome.get('price'),
                        'line_type': self._determine_line_type(outcome, market_key),
                        'commence_time': commence_time,
                        'home_team': home_team,
                        'away_team': away_team,
                        'collected_at': datetime.now(timezone.utc).isoformat()
                    }
                    
                    # Separate anytime TD props
                    if market_key == 'player_anytime_td':
                        anytime_td_props.append(prop_data)
                    else:
                        regular_props.append(prop_data)
        
        return regular_props, anytime_td_props
    
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
    
    def _determine_line_type(self, outcome: Dict, market_key: str) -> str:
        """Determine if outcome is Over/Under"""
        outcome_name = outcome.get('name', '')
        
        if 'Over' in outcome_name:
            return 'Over'
        elif 'Under' in outcome_name:
            return 'Under'
        elif market_key == 'player_anytime_td':
            return 'Over'  # Anytime TD is always "Yes/Over"
        
        return 'Unknown'
    
    def save_processed_data(self, data: Dict, week_name: str) -> str:
        """Save processed data to file"""
        output_dir = Path('processed_data')
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"processed_{week_name}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"💾 Saved processed data: {filepath}")
        return str(filepath)

def main():
    """Test the historical data processor"""
    processor = HistoricalDataProcessor()
    
    # Test with available data
    print("🧪 Testing Historical Data Processor")
    print("Looking for available historical data files...")
    
    # Find available week data
    raw_data_dir = Path('raw_data/historical')
    if not raw_data_dir.exists():
        print("❌ No historical data directory found")
        return
    
    event_files = list(raw_data_dir.glob("events_*.json"))
    if not event_files:
        print("❌ No event files found")
        return
    
    # Extract week names from files
    weeks = set()
    for event_file in event_files:
        filename = event_file.stem
        # Format: events_2024_Week_1_20240905
        parts = filename.split('_')
        if len(parts) >= 4:
            week_name = f"{parts[1]}_{parts[2]}_{parts[3]}"
            weeks.add(week_name)
    
    print(f"📅 Found data for weeks: {list(weeks)}")
    
    if weeks:
        # Test with first available week
        test_week = list(weeks)[0]
        print(f"🧪 Testing with week: {test_week}")
        
        try:
            # Mock date range (not used for file processing)
            from datetime import datetime, timezone
            start_date = datetime(2024, 9, 5, tzinfo=timezone.utc)
            end_date = datetime(2024, 9, 11, tzinfo=timezone.utc)
            
            processed_data = processor.process_week_data(test_week, start_date, end_date)
            
            # Save processed data
            output_file = processor.save_processed_data(processed_data, test_week)
            
            print(f"✅ Processing complete! Output: {output_file}")
            
        except Exception as e:
            print(f"❌ Processing failed: {e}")

if __name__ == "__main__":
    main()