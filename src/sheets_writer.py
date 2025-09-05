#!/usr/bin/env python3
"""
Google Sheets writer for NFL betting collected data
Writes 4-snapshot data to Google Sheets in proper format
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from googleapiclient.errors import HttpError

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from oauth_auth import GoogleOAuthClient

class NFLSheetsWriter:
    """Writes collected NFL betting data to Google Sheets"""
    
    def __init__(self, spreadsheet_id: str):
        """Initialize the sheets writer"""
        self.spreadsheet_id = spreadsheet_id
        
        # Set up OAuth authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        oauth_client = GoogleOAuthClient(scopes)
        self.sheets_service = oauth_client.get_sheets_service()
        
        print(f"📊 Initialized Sheets Writer (OAuth)")
        print(f"📄 Target Spreadsheet: {spreadsheet_id}")
        
    def write_collection_data(self, data_file_path: str) -> Dict:
        """
        Write collected data from JSON file to Google Sheets
        
        Args:
            data_file_path: Path to the collected data JSON file
            
        Returns:
            Dict with write results
        """
        try:
            # Load collected data
            with open(data_file_path, 'r') as f:
                data = json.load(f)
            
            print(f"📖 Loading data from: {os.path.basename(data_file_path)}")
            print(f"📊 Data summary:")
            print(f"   Week: {data['week']}")
            print(f"   Snapshot: {data['snapshot']} - {data['snapshot_description']}")
            print(f"   Games: {data['games_count']}")
            print(f"   Props: {data['props_count']}")
            
            results = {
                'success': True,
                'week': data['week'],
                'snapshot': data['snapshot'],
                'games_written': 0,
                'props_written': 0,
                'errors': []
            }
            
            # Write game lines data
            if data.get('games_data'):
                print(f"\n📊 Writing game lines data...")
                games_result = self._write_game_lines(data['games_data'], data['snapshot'])
                results['games_written'] = games_result['rows_written']
                if games_result['errors']:
                    results['errors'].extend(games_result['errors'])
            
            # Write player props data
            if data.get('props_data'):
                print(f"🎯 Writing player props data...")
                props_result = self._write_player_props(data['props_data'], data['snapshot'])
                results['props_written'] = props_result['rows_written']
                if props_result['errors']:
                    results['errors'].extend(props_result['errors'])
            
            # Update overview tab
            print(f"📋 Updating overview tab...")
            self._update_overview_tab(data)
            
            print(f"\n✅ Write operation complete!")
            print(f"📊 Games written: {results['games_written']}")
            print(f"🎯 Props written: {results['props_written']}")
            
            if results['errors']:
                print(f"⚠️  Errors encountered: {len(results['errors'])}")
                for error in results['errors']:
                    print(f"   • {error}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error writing to sheets: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _write_game_lines(self, games_data: List[Dict], snapshot_num: int) -> Dict:
        """Write game lines data to the Game_Lines sheet"""
        sheet_name = Config.TAB_NAMES['game_lines']
        
        try:
            # Get existing data to find where to append/update
            existing_data = self._get_existing_sheet_data(sheet_name)
            
            # Convert games data to sheet format
            sheet_rows = []
            for game in games_data:
                row = self._game_to_sheet_row(game)
                sheet_rows.append(row)
            
            if not sheet_rows:
                return {'rows_written': 0, 'errors': []}
            
            # Determine write range
            start_row = len(existing_data) + 1  # Start after existing data
            end_row = start_row + len(sheet_rows) - 1
            
            range_name = f"{sheet_name}!A{start_row}:AZ{end_row}"
            
            # Write data
            body = {
                'values': sheet_rows
            }
            
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            rows_written = result.get('updatedRows', 0)
            print(f"   ✅ {rows_written} game lines written to rows {start_row}-{end_row}")
            
            return {
                'rows_written': rows_written,
                'errors': []
            }
            
        except HttpError as error:
            error_msg = f"Failed to write game lines: {error}"
            print(f"   ❌ {error_msg}")
            return {
                'rows_written': 0,
                'errors': [error_msg]
            }
    
    def _write_player_props(self, props_data: List[Dict], snapshot_num: int) -> Dict:
        """Write player props data to the Player_Props sheet"""
        sheet_name = Config.TAB_NAMES['player_props']
        
        try:
            # Get existing data
            existing_data = self._get_existing_sheet_data(sheet_name)
            
            # Convert props data to sheet format
            sheet_rows = []
            for prop in props_data:
                row = self._prop_to_sheet_row(prop)
                sheet_rows.append(row)
            
            if not sheet_rows:
                return {'rows_written': 0, 'errors': []}
            
            # Determine write range
            start_row = len(existing_data) + 1
            end_row = start_row + len(sheet_rows) - 1
            
            range_name = f"{sheet_name}!A{start_row}:AZ{end_row}"
            
            # Write data
            body = {
                'values': sheet_rows
            }
            
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            rows_written = result.get('updatedRows', 0)
            print(f"   ✅ {rows_written} player props written to rows {start_row}-{end_row}")
            
            return {
                'rows_written': rows_written,
                'errors': []
            }
            
        except HttpError as error:
            error_msg = f"Failed to write player props: {error}"
            print(f"   ❌ {error_msg}")
            return {
                'rows_written': 0,
                'errors': [error_msg]
            }
    
    def _game_to_sheet_row(self, game: Dict) -> List[Any]:
        """Convert game data to sheet row format"""
        # Order matches the Game_Lines sheet headers
        return [
            game.get('game_id', ''),
            game.get('date', ''),
            game.get('home_team', ''),
            game.get('away_team', ''),
            game.get('bookmaker', ''),
            # Spread snapshots (4 sets of 4 columns each)
            game.get('spread_line_1', ''), game.get('spread_odds_home_1', ''), game.get('spread_odds_away_1', ''), game.get('collected_date_1', ''),
            game.get('spread_line_2', ''), game.get('spread_odds_home_2', ''), game.get('spread_odds_away_2', ''), game.get('collected_date_2', ''),
            game.get('spread_line_3', ''), game.get('spread_odds_home_3', ''), game.get('spread_odds_away_3', ''), game.get('collected_date_3', ''),
            game.get('spread_line_4', ''), game.get('spread_odds_home_4', ''), game.get('spread_odds_away_4', ''), game.get('collected_date_4', ''),
            # Total snapshots (4 sets of 3 columns each)
            game.get('total_line_1', ''), game.get('total_over_odds_1', ''), game.get('total_under_odds_1', ''),
            game.get('total_line_2', ''), game.get('total_over_odds_2', ''), game.get('total_under_odds_2', ''),
            game.get('total_line_3', ''), game.get('total_over_odds_3', ''), game.get('total_under_odds_3', ''),
            game.get('total_line_4', ''), game.get('total_over_odds_4', ''), game.get('total_under_odds_4', ''),
            # Moneyline snapshots (4 sets of 2 columns each)
            game.get('ml_home_1', ''), game.get('ml_away_1', ''),
            game.get('ml_home_2', ''), game.get('ml_away_2', ''),
            game.get('ml_home_3', ''), game.get('ml_away_3', ''),
            game.get('ml_home_4', ''), game.get('ml_away_4', '')
        ]
    
    def _prop_to_sheet_row(self, prop: Dict) -> List[Any]:
        """Convert prop data to sheet row format"""
        # Order matches the Player_Props sheet headers
        return [
            prop.get('game_id', ''),
            prop.get('player_name', ''),
            prop.get('position', ''),
            prop.get('team', ''),
            prop.get('market_type', ''),
            prop.get('bookmaker', ''),
            prop.get('data_source', ''),
            # Snapshot 1
            prop.get('over_line_1', ''), prop.get('over_odds_1', ''), prop.get('under_line_1', ''), prop.get('under_odds_1', ''), prop.get('collected_date_1', ''),
            # Snapshot 2
            prop.get('over_line_2', ''), prop.get('over_odds_2', ''), prop.get('under_line_2', ''), prop.get('under_odds_2', ''), prop.get('collected_date_2', ''),
            # Snapshot 3
            prop.get('over_line_3', ''), prop.get('over_odds_3', ''), prop.get('under_line_3', ''), prop.get('under_odds_3', ''), prop.get('collected_date_3', ''),
            # Snapshot 4
            prop.get('over_line_4', ''), prop.get('over_odds_4', ''), prop.get('under_line_4', ''), prop.get('under_odds_4', ''), prop.get('collected_date_4', ''),
            # Reference data placeholders
            prop.get('season_over_rate', 'N/A'),
            prop.get('season_attempts', 'N/A'),
            prop.get('vs_defense_rate', 'N/A'),
            prop.get('recent_form_3g', 'N/A'),
            prop.get('home_away_split', 'N/A')
        ]
    
    def _get_existing_sheet_data(self, sheet_name: str) -> List[List[Any]]:
        """Get existing data from a sheet"""
        try:
            range_name = f"{sheet_name}!A:Z"
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            return result.get('values', [])
            
        except HttpError as error:
            print(f"⚠️  Warning: Could not read existing data from {sheet_name}: {error}")
            return []
    
    def _update_overview_tab(self, data: Dict):
        """Update the Overview tab with collection status"""
        sheet_name = Config.TAB_NAMES['overview']
        
        try:
            # Update collection status for this snapshot
            snapshot_num = data['snapshot']
            status_text = f"Collected - {data['collection_timestamp']}"
            
            # Row mapping for snapshot status (based on template structure)
            status_rows = {
                1: 11,  # Row 11: Snapshot 1 status
                2: 12,  # Row 12: Snapshot 2 status  
                3: 13,  # Row 13: Snapshot 3 status
                4: 14   # Row 14: Snapshot 4 status
            }
            
            if snapshot_num in status_rows:
                range_name = f"{sheet_name}!B{status_rows[snapshot_num]}"
                
                body = {
                    'values': [[status_text]]
                }
                
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                print(f"   ✅ Updated overview: Snapshot {snapshot_num} status")
            
            # Update summary counts
            summary_updates = [
                (5, f"B5", str(data['games_count'])),  # Total Games
                (6, f"B6", "0"),  # Total Picks (placeholder)
            ]
            
            for row, range_cell, value in summary_updates:
                try:
                    body = {'values': [[value]]}
                    self.sheets_service.spreadsheets().values().update(
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{sheet_name}!{range_cell}",
                        valueInputOption='RAW',
                        body=body
                    ).execute()
                except:
                    continue  # Skip if update fails
                    
        except HttpError as error:
            print(f"⚠️  Warning: Could not update overview tab: {error}")

def main():
    """Test the sheets writer"""
    # Load template ID
    try:
        with open('template_id.txt', 'r') as f:
            template_id = f.read().strip()
    except:
        template_id = '1OS6zUYgz_SRsDJREXEexgFv_qlptQh8SVWGDnqdVZOw'  # Fallback to your template
    
    writer = NFLSheetsWriter(template_id)
    
    # Find the most recent collected data file
    collected_data_dir = 'collected_data'
    if not os.path.exists(collected_data_dir):
        print(f"❌ No collected data directory found. Run data_collector.py first.")
        return
    
    data_files = [f for f in os.listdir(collected_data_dir) if f.endswith('.json')]
    if not data_files:
        print(f"❌ No data files found in {collected_data_dir}. Run data_collector.py first.")
        return
    
    # Use most recent file
    most_recent_file = sorted(data_files)[-1]
    data_file_path = os.path.join(collected_data_dir, most_recent_file)
    
    print(f"🧪 Testing sheets writer with: {most_recent_file}")
    
    # Write data to sheets
    results = writer.write_collection_data(data_file_path)
    
    if results['success']:
        print(f"\n🎉 Sheets write test successful!")
        print(f"📊 Results:")
        print(f"   Week: {results['week']}")
        print(f"   Snapshot: {results['snapshot']}")  
        print(f"   Games written: {results['games_written']}")
        print(f"   Props written: {results['props_written']}")
        print(f"\n🔗 Check your Google Sheet: https://docs.google.com/spreadsheets/d/{template_id}")
    else:
        print(f"❌ Sheets write failed: {results.get('error')}")

if __name__ == "__main__":
    main()