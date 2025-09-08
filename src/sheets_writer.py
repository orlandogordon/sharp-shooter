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
        self.api_requests_made = 0  # Track total API requests
        self.start_time = None  # Track when we started
        
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
            try:
                self._update_overview_tab(data)
            except Exception as e:
                print(f"⚠️  Warning: Failed to update overview tab: {e}")
                results['errors'].append(f"Overview update failed: {e}")
            
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
            # First, expand sheet if needed
            self._ensure_sheet_size(sheet_name, len(games_data) + 100)  # Add buffer rows
            
            if snapshot_num == 1:
                # Snapshot 1: Create new rows
                return self._write_game_lines_snapshot_1(games_data, sheet_name)
            else:
                # Snapshots 2-4: Update existing rows with additional snapshot data
                return self._update_game_lines_snapshots(games_data, snapshot_num, sheet_name)
            
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
            # First, expand sheet if needed  
            self._ensure_sheet_size(sheet_name, len(props_data) + 100)  # Add buffer rows
            
            if snapshot_num == 1:
                # Snapshot 1: Create new rows
                return self._write_props_snapshot_1(props_data, sheet_name)
            else:
                # Snapshots 2-4: Update existing rows with additional snapshot data
                return self._update_props_snapshots(props_data, snapshot_num, sheet_name)
            
        except HttpError as error:
            error_msg = f"Failed to write player props: {error}"
            print(f"   ❌ {error_msg}")
            return {
                'rows_written': 0,
                'errors': [error_msg]
            }
    
    def _ensure_sheet_size(self, sheet_name: str, min_rows: int):
        """Ensure sheet has enough rows to accommodate data"""
        try:
            # Get sheet properties
            sheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            # Find the sheet
            target_sheet = None
            for sheet in sheet_metadata['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    target_sheet = sheet
                    break
            
            if not target_sheet:
                return
            
            current_rows = target_sheet['properties']['gridProperties']['rowCount']
            current_cols = target_sheet['properties']['gridProperties']['columnCount']
            
            # Check if we need to expand
            needs_expansion = False
            new_rows = current_rows
            new_cols = current_cols
            
            if min_rows > current_rows:
                new_rows = min_rows + 100  # Add buffer
                needs_expansion = True
                
            if new_cols < 52:  # Ensure we have enough columns (AZ = 52)
                new_cols = 52
                needs_expansion = True
            
            if needs_expansion:
                print(f"   📏 Expanding {sheet_name}: {current_rows}→{new_rows} rows, {current_cols}→{new_cols} cols")
                
                requests = [{
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': target_sheet['properties']['sheetId'],
                            'gridProperties': {
                                'rowCount': new_rows,
                                'columnCount': new_cols
                            }
                        },
                        'fields': 'gridProperties.rowCount,gridProperties.columnCount'
                    }
                }]
                
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                
        except Exception as e:
            print(f"   ⚠️  Warning: Could not expand sheet {sheet_name}: {e}")
    
    def _write_game_lines_snapshot_1(self, games_data: List[Dict], sheet_name: str) -> Dict:
        """Write game lines for snapshot 1 (create new rows)"""
        existing_data = self._get_existing_sheet_data(sheet_name)
        
        # Convert games data to sheet format
        sheet_rows = []
        for game in games_data:
            row = self._game_to_sheet_row(game)
            sheet_rows.append(row)
        
        if not sheet_rows:
            return {'rows_written': 0, 'errors': []}
        
        # Determine write range (append after existing data)
        start_row = len(existing_data) + 1
        end_row = start_row + len(sheet_rows) - 1
        
        range_name = f"{sheet_name}!A{start_row}:AZ{end_row}"
        
        body = {'values': sheet_rows}
        
        result = self.sheets_service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        rows_written = result.get('updatedRows', 0)
        print(f"   ✅ {rows_written} game lines written to rows {start_row}-{end_row}")
        
        return {'rows_written': rows_written, 'errors': []}
    
    def _update_game_lines_snapshots(self, games_data: List[Dict], snapshot_num: int, sheet_name: str) -> Dict:
        """Update existing game lines with additional snapshot data AND add new games"""
        try:
            # Get existing data to find the rows to update
            existing_data = self._get_existing_sheet_data(sheet_name)
            
            # Build a map of existing games by game_id
            game_row_map = {}
            if len(existing_data) > 1:  # Has data beyond headers
                for row_idx, row_data in enumerate(existing_data[1:], start=2):  # Skip header, start from row 2
                    if row_data and len(row_data) > 0:
                        game_id = row_data[0] if len(row_data) > 0 else ''
                        if game_id:
                            game_row_map[game_id] = row_idx
            
            # Separate games into existing (to update) and new (to add)
            existing_games = []
            new_games = []
            
            for game in games_data:
                game_id = game.get('game_id', '')
                if game_id and game_id in game_row_map:
                    existing_games.append((game, game_id, game_row_map[game_id]))
                elif game_id:  # Valid game_id but not found in existing data
                    new_games.append(game)
            
            updates_made = 0
            errors = []
            
            print(f"   📊 Processing: {len(existing_games)} updates, {len(new_games)} new games")
            
            # Part 1: Update existing games with snapshot data (batch updates)
            if existing_games:
                try:
                    updates_made = self._batch_update_games(existing_games, snapshot_num, sheet_name)
                    print(f"   ✅ Updated {updates_made} existing games using batch API")
                    
                except Exception as e:
                    errors.append(f"Failed to batch update existing games: {e}")
                    
                    # Fallback to individual updates
                    for game, game_id, row_num in existing_games:
                        try:
                            update_ranges = self._get_game_snapshot_columns(snapshot_num)
                            
                            for range_name, range_type in update_ranges:
                                try:
                                    full_range = f"{sheet_name}!{range_name}{row_num}"
                                    snapshot_values = self._extract_game_snapshot_values(game, snapshot_num, range_type)
                                    
                                    if snapshot_values and any(val for val in snapshot_values):
                                        body = {'values': [snapshot_values]}
                                        
                                        self.sheets_service.spreadsheets().values().update(
                                            spreadsheetId=self.spreadsheet_id,
                                            range=full_range,
                                            valueInputOption='RAW',
                                            body=body
                                        ).execute()
                                        
                                except Exception as e:
                                    errors.append(f"Failed to update {game_id} {range_type} row {row_num}: {e}")
                            
                            updates_made += 1
                            
                        except Exception as e:
                            errors.append(f"Failed to update game {game_id}: {e}")
            
            # Part 2: Add new games as complete rows (using batch API to avoid quota limits)
            new_rows_added = 0
            if new_games:
                print(f"   ➕ Adding {len(new_games)} new games that appeared in snapshot {snapshot_num}")
                
                # Convert new games to sheet rows
                new_sheet_rows = []
                for game in new_games:
                    row = self._game_to_sheet_row(game)
                    new_sheet_rows.append(row)
                
                if new_sheet_rows:
                    # Use batchUpdate to reduce API calls
                    try:
                        new_rows_added = self._batch_add_rows(sheet_name, new_sheet_rows, len(existing_data) + 1)
                        print(f"   ✅ Added {new_rows_added} new game rows using batch API")
                        
                        # Expand table formatting to include new rows
                        if new_rows_added > 0:
                            self._expand_table_formatting(sheet_name, new_rows_added, 'game_lines')
                        
                    except Exception as e:
                        errors.append(f"Failed to add new games: {e}")
            
            total_written = updates_made + new_rows_added
            print(f"   ✅ Snapshot {snapshot_num}: Updated {updates_made} existing + Added {new_rows_added} new = {total_written} total")
            
            return {
                'rows_written': total_written,
                'errors': errors
            }
            
        except Exception as e:
            error_msg = f"Failed to update game lines snapshot {snapshot_num}: {e}"
            print(f"   ❌ {error_msg}")
            return {
                'rows_written': 0,
                'errors': [error_msg]
            }
    
    def _write_props_snapshot_1(self, props_data: List[Dict], sheet_name: str) -> Dict:
        """Write player props for snapshot 1 (create new rows)"""
        existing_data = self._get_existing_sheet_data(sheet_name)
        
        # Convert props data to sheet format
        sheet_rows = []
        for prop in props_data:
            row = self._prop_to_sheet_row(prop)
            sheet_rows.append(row)
        
        if not sheet_rows:
            return {'rows_written': 0, 'errors': []}
        
        # Determine write range (append after existing data)
        start_row = len(existing_data) + 1
        end_row = start_row + len(sheet_rows) - 1
        
        range_name = f"{sheet_name}!A{start_row}:AZ{end_row}"
        
        body = {'values': sheet_rows}
        
        result = self.sheets_service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        rows_written = result.get('updatedRows', 0)
        print(f"   ✅ {rows_written} player props written to rows {start_row}-{end_row}")
        
        return {'rows_written': rows_written, 'errors': []}
    
    def _update_props_snapshots(self, props_data: List[Dict], snapshot_num: int, sheet_name: str) -> Dict:
        """Update existing props with additional snapshot data AND add new props"""
        try:
            # Get existing data to find the rows to update
            existing_data = self._get_existing_sheet_data(sheet_name)
            
            # Build a map of existing props by unique key (player_name + market_type + game_id)
            prop_row_map = {}
            if len(existing_data) > 1:  # Has data beyond headers
                for row_idx, row_data in enumerate(existing_data[1:], start=2):  # Skip header, start from row 2
                    if row_data and len(row_data) >= 5:  # Need at least game_id, player, position, team, market
                        game_id = row_data[0] if len(row_data) > 0 else ''
                        player_name = row_data[1] if len(row_data) > 1 else ''
                        market_type = row_data[4] if len(row_data) > 4 else ''
                        
                        # Create unique key for matching
                        prop_key = f"{game_id}|{player_name}|{market_type}"
                        if prop_key:
                            prop_row_map[prop_key] = row_idx
            
            # Separate props into existing (to update) and new (to add)
            existing_props = []
            new_props = []
            
            for prop in props_data:
                game_id = prop.get('game_id', '')
                player_name = prop.get('player_name', '')
                market_type = prop.get('market_type', '')
                prop_key = f"{game_id}|{player_name}|{market_type}"
                
                if prop_key and prop_key in prop_row_map:
                    existing_props.append((prop, prop_key, prop_row_map[prop_key]))
                elif prop_key:  # Valid prop key but not found in existing data
                    new_props.append(prop)
            
            updates_made = 0
            errors = []
            
            print(f"   📊 Processing: {len(existing_props)} updates, {len(new_props)} new props")
            
            # Part 1: Update existing props with snapshot data (batch updates)
            if existing_props:
                try:
                    updates_made = self._batch_update_props(existing_props, snapshot_num, sheet_name)
                    print(f"   ✅ Updated {updates_made} existing props using batch API")
                    
                except Exception as e:
                    errors.append(f"Failed to batch update existing props: {e}")
                    
                    # Fallback to individual updates
                    for prop, prop_key, row_num in existing_props:
                        try:
                            snapshot_start_col = 7 + (snapshot_num - 1) * 5
                            snapshot_end_col = snapshot_start_col + 4
                            
                            start_col_letter = self._col_num_to_letter(snapshot_start_col + 1)
                            end_col_letter = self._col_num_to_letter(snapshot_end_col + 1)
                            
                            range_name = f"{sheet_name}!{start_col_letter}{row_num}:{end_col_letter}{row_num}"
                            
                            snapshot_values = [
                                prop.get(f'over_line_{snapshot_num}', ''),
                                prop.get(f'over_odds_{snapshot_num}', ''),
                                prop.get(f'under_line_{snapshot_num}', ''),
                                prop.get(f'under_odds_{snapshot_num}', ''),
                                prop.get(f'collected_date_{snapshot_num}', '')
                            ]
                            
                            body = {'values': [snapshot_values]}
                            
                            self.sheets_service.spreadsheets().values().update(
                                spreadsheetId=self.spreadsheet_id,
                                range=range_name,
                                valueInputOption='RAW',
                                body=body
                            ).execute()
                            
                            updates_made += 1
                            
                        except Exception as e:
                            errors.append(f"Failed to update {prop_key} row {row_num}: {e}")
            
            # Part 2: Add new props as complete rows (using batch API to avoid quota limits)
            new_rows_added = 0
            if new_props:
                print(f"   ➕ Adding {len(new_props)} new props that appeared in snapshot {snapshot_num}")
                
                # Convert new props to sheet rows
                new_sheet_rows = []
                for prop in new_props:
                    row = self._prop_to_sheet_row(prop)
                    new_sheet_rows.append(row)
                
                if new_sheet_rows:
                    # Use batchUpdate with multiple ranges to reduce API calls
                    try:
                        new_rows_added = self._batch_add_rows(sheet_name, new_sheet_rows, len(existing_data) + 1)
                        print(f"   ✅ Added {new_rows_added} new prop rows using batch API")
                        
                        # Expand table formatting to include new rows
                        if new_rows_added > 0:
                            self._expand_table_formatting(sheet_name, new_rows_added, 'player_props')
                        
                    except Exception as e:
                        errors.append(f"Failed to add new props: {e}")
            
            total_written = updates_made + new_rows_added
            print(f"   ✅ Snapshot {snapshot_num}: Updated {updates_made} existing + Added {new_rows_added} new = {total_written} total")
            
            return {
                'rows_written': total_written,
                'errors': errors
            }
            
        except Exception as e:
            error_msg = f"Failed to update props snapshot {snapshot_num}: {e}"
            print(f"   ❌ {error_msg}")
            return {
                'rows_written': 0,
                'errors': [error_msg]
            }
    
    def _col_num_to_letter(self, col_num):
        """Convert column number to Excel letter (1=A, 2=B, etc.)"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result
    
    def _get_game_snapshot_columns(self, snapshot_num: int):
        """Get column ranges for game snapshot updates"""
        # Game lines structure from _game_to_sheet_row:
        # Columns 1-5: game_id, date, home_team, away_team, bookmaker
        # Columns 6-21: Spread snapshots (4 sets of 4 columns each)
        # Columns 22-33: Total snapshots (4 sets of 3 columns each)  
        # Columns 34-41: Moneyline snapshots (4 sets of 2 columns each)
        
        ranges = []
        
        # Spread snapshot columns (4 columns per snapshot)
        spread_start = 6 + (snapshot_num - 1) * 4
        spread_end = spread_start + 3
        spread_range = f"{self._col_num_to_letter(spread_start)}:{self._col_num_to_letter(spread_end)}"
        ranges.append((spread_range, 'spread'))
        
        # Total snapshot columns (3 columns per snapshot)
        total_start = 22 + (snapshot_num - 1) * 3
        total_end = total_start + 2
        total_range = f"{self._col_num_to_letter(total_start)}:{self._col_num_to_letter(total_end)}"
        ranges.append((total_range, 'total'))
        
        # Moneyline snapshot columns (2 columns per snapshot)
        ml_start = 34 + (snapshot_num - 1) * 2
        ml_end = ml_start + 1
        ml_range = f"{self._col_num_to_letter(ml_start)}:{self._col_num_to_letter(ml_end)}"
        ranges.append((ml_range, 'moneyline'))
        
        return ranges
    
    def _extract_game_snapshot_values(self, game: Dict, snapshot_num: int, range_type: str):
        """Extract values for a specific game snapshot range"""
        if range_type == 'spread':
            return [
                game.get(f'spread_line_{snapshot_num}', ''),
                game.get(f'spread_odds_home_{snapshot_num}', ''),
                game.get(f'spread_odds_away_{snapshot_num}', ''),
                game.get(f'collected_date_{snapshot_num}', '')
            ]
        elif range_type == 'total':
            return [
                game.get(f'total_line_{snapshot_num}', ''),
                game.get(f'total_over_odds_{snapshot_num}', ''),
                game.get(f'total_under_odds_{snapshot_num}', '')
            ]
        elif range_type == 'moneyline':
            return [
                game.get(f'ml_home_{snapshot_num}', ''),
                game.get(f'ml_away_{snapshot_num}', '')
            ]
        return []
    
    def _batch_add_rows(self, sheet_name: str, rows: List[List], start_row: int) -> int:
        """Add multiple rows using batch API to minimize quota usage"""
        if not rows:
            return 0
            
        # Split into chunks to avoid hitting API limits (max 100 rows per chunk)
        chunk_size = 100
        total_rows_added = 0
        
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            chunk_start_row = start_row + i
            chunk_end_row = chunk_start_row + len(chunk) - 1
            
            range_name = f"{sheet_name}!A{chunk_start_row}:AZ{chunk_end_row}"
            
            try:
                # Use values().update for batch writing
                body = {'values': chunk}
                
                result = self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                chunk_rows_added = result.get('updatedRows', 0)
                total_rows_added += chunk_rows_added
                
                print(f"     ✅ Batch {i//chunk_size + 1}: Added {chunk_rows_added} rows (rows {chunk_start_row}-{chunk_end_row})")
                
                # Smart rate limiting
                self._smart_rate_limit()
                    
            except Exception as e:
                print(f"     ❌ Failed to add batch {i//chunk_size + 1}: {e}")
                # Continue with next chunk even if one fails
                continue
                
        return total_rows_added
    
    def _batch_update_props(self, existing_props: List[tuple], snapshot_num: int, sheet_name: str) -> int:
        """Update existing props using batch API to minimize quota usage"""
        if not existing_props:
            return 0
            
        # Build batch update requests using batchUpdate API
        requests = []
        
        # Calculate snapshot columns
        snapshot_start_col = 7 + (snapshot_num - 1) * 5  # 0-indexed
        
        for prop, prop_key, row_num in existing_props:
            # Extract snapshot values
            snapshot_values = [
                prop.get(f'over_line_{snapshot_num}', ''),
                prop.get(f'over_odds_{snapshot_num}', ''),
                prop.get(f'under_line_{snapshot_num}', ''),
                prop.get(f'under_odds_{snapshot_num}', ''),
                prop.get(f'collected_date_{snapshot_num}', '')
            ]
            
            # Only update if there's actual data
            if any(val for val in snapshot_values):
                requests.append({
                    'range': f"{sheet_name}!{self._col_num_to_letter(snapshot_start_col + 1)}{row_num}:{self._col_num_to_letter(snapshot_start_col + 5)}{row_num}",
                    'values': [snapshot_values]
                })
        
        if not requests:
            return 0
            
        # Execute batch update (split into chunks of 20 to avoid API limits)
        chunk_size = 20
        total_updates = 0
        
        for i in range(0, len(requests), chunk_size):
            chunk = requests[i:i + chunk_size]
            
            try:
                body = {
                    'valueInputOption': 'RAW',
                    'data': chunk
                }
                
                result = self.sheets_service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                
                total_updates += len(chunk)
                print(f"     ✅ Batch update {i//chunk_size + 1}: Updated {len(chunk)} prop rows")
                
                # Smart rate limiting
                self._smart_rate_limit()
                    
            except Exception as e:
                print(f"     ❌ Failed batch update {i//chunk_size + 1}: {e}")
                continue
                
        return total_updates
    
    def _smart_rate_limit(self):
        """Smart rate limiting to stay under 60 requests/minute"""
        import time
        
        if self.start_time is None:
            self.start_time = time.time()
        
        self.api_requests_made += 1
        
        # Calculate how long we should wait to maintain ~50 requests/minute (safe buffer)
        elapsed_time = time.time() - self.start_time
        target_rate = 50  # requests per minute (safe buffer under 60)
        expected_time = (self.api_requests_made / target_rate) * 60  # Expected time for this many requests
        
        if elapsed_time < expected_time:
            wait_time = expected_time - elapsed_time
            if wait_time > 0:
                print(f"     ⏱️  Rate limiting: waiting {wait_time:.1f}s (request {self.api_requests_made})")
                time.sleep(wait_time)
    
    def _batch_update_games(self, existing_games: List[tuple], snapshot_num: int, sheet_name: str) -> int:
        """Update existing games using batch API to minimize quota usage"""
        if not existing_games:
            return 0
            
        # Build batch update requests using batchUpdate API
        requests = []
        
        for game, game_id, row_num in existing_games:
            # Get all update ranges for this game (spread, totals, moneyline)
            update_ranges = self._get_game_snapshot_columns(snapshot_num)
            
            for range_name, range_type in update_ranges:
                snapshot_values = self._extract_game_snapshot_values(game, snapshot_num, range_type)
                
                # Only update if there's actual data
                if snapshot_values and any(val for val in snapshot_values):
                    full_range = f"{sheet_name}!{range_name}{row_num}"
                    requests.append({
                        'range': full_range,
                        'values': [snapshot_values]
                    })
        
        if not requests:
            return 0
            
        # Execute batch update (split into chunks of 20 to avoid API limits)
        chunk_size = 20
        total_updates = 0
        
        for i in range(0, len(requests), chunk_size):
            chunk = requests[i:i + chunk_size]
            
            try:
                body = {
                    'valueInputOption': 'RAW',
                    'data': chunk
                }
                
                result = self.sheets_service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                
                total_updates += len(chunk)
                print(f"     ✅ Batch update {i//chunk_size + 1}: Updated {len(chunk)} game ranges")
                
                # Smart rate limiting
                self._smart_rate_limit()
                    
            except Exception as e:
                print(f"     ❌ Failed batch update {i//chunk_size + 1}: {e}")
                continue
                
        # Return number of games updated (not individual ranges)
        return len(existing_games)
    
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
    
    def _expand_table_formatting(self, sheet_name: str, new_rows_added: int, table_type: str):
        """Expand table formatting (autofilter, banding) to include newly added rows"""
        if new_rows_added <= 0:
            return
            
        try:
            print(f"     🎨 Expanding table formatting for {new_rows_added} new rows in {sheet_name}")
            
            # Get sheet metadata to find the sheet ID
            spreadsheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_id = None
            for sheet in spreadsheet_metadata['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
                    
            if sheet_id is None:
                print(f"     ⚠️  Could not find sheet ID for {sheet_name}")
                return
            
            # Get current data range to determine new end row
            current_data = self._get_existing_sheet_data(sheet_name)
            current_row_count = len(current_data) if current_data else 1
            
            # Determine column counts based on table type
            column_counts = {
                'game_lines': 35,      
                'player_props': 30,    
                'my_picks': 17,        
                'results': 10,         
                'futures': 17          
            }
            max_cols = column_counts.get(table_type, 26)
            
            requests = []
            
            # 1. Update autofilter range to include new rows
            requests.append({
                'setBasicFilter': {
                    'filter': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': current_row_count + new_rows_added,
                            'startColumnIndex': 0,
                            'endColumnIndex': max_cols
                        }
                    }
                }
            })
            
            # 2. Clear existing banded ranges (we need to replace them, not extend them)
            sheet_props = None
            for sheet in spreadsheet_metadata['sheets']:
                if sheet['properties']['sheetId'] == sheet_id:
                    sheet_props = sheet['properties']
                    break
            
            # Get current banded ranges to delete them first
            existing_banded_ranges = []
            if sheet_props and 'bandedRanges' in sheet_props:
                existing_banded_ranges = sheet_props['bandedRanges']
                for banded_range in existing_banded_ranges:
                    requests.append({
                        'deleteBanding': {
                            'bandedRangeId': banded_range['bandedRangeId']
                        }
                    })
            
            # 3. Add new banded range that includes all rows (including new ones)
            requests.append({
                'addBanding': {
                    'bandedRange': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,  # Skip header
                            'endRowIndex': current_row_count + new_rows_added,
                            'startColumnIndex': 0,
                            'endColumnIndex': max_cols
                        },
                        'rowProperties': {
                            'headerColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
                            'firstBandColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
                            'secondBandColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                        }
                    }
                }
            })
            
            # Execute operations in two batches: first delete existing banded ranges, then add new formatting
            
            # First batch: Delete existing banded ranges and update autofilter
            delete_requests = [req for req in requests if 'deleteBanding' in req or 'setBasicFilter' in req]
            if delete_requests:
                body = {'requests': delete_requests}
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                self._smart_rate_limit()
            
            # Second batch: Add new banded ranges
            add_requests = [req for req in requests if 'addBanding' in req]
            if add_requests:
                body = {'requests': add_requests}
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                self._smart_rate_limit()
            
            print(f"     ✅ Extended table formatting to include {new_rows_added} new rows")
                
        except Exception as e:
            print(f"     ⚠️  Warning: Could not expand table formatting: {e}")
            # Don't fail the entire operation if formatting fails
    
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
            raise  # Re-raise so outer handler can catch it
        except Exception as e:
            print(f"⚠️  Warning: Overview tab update failed: {e}")
            raise

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