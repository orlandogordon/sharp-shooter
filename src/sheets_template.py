#!/usr/bin/env python3
"""
Google Sheets template creator for NFL Betting Data Workflow
Creates the master template with proper tab structure and schemas
"""

import os
import sys
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config

class SheetsTemplateCreator:
    """Creates Google Sheets templates for NFL betting data tracking"""
    
    def __init__(self):
        """Initialize with Google Sheets API credentials"""
        Config.validate_config()
        
        # Set up credentials
        credentials = Credentials.from_service_account_file(
            Config.GOOGLE_CREDENTIALS_FILE,
            scopes=Config.GOOGLE_SCOPES
        )
        
        # Build services
        self.sheets_service = build('sheets', 'v4', credentials=credentials)
        self.drive_service = build('drive', 'v3', credentials=credentials)
        
    def create_master_template(self):
        """Create the master template spreadsheet"""
        try:
            # Create new spreadsheet
            spreadsheet_body = {
                'properties': {
                    'title': 'NFL_Betting_Template_Master',
                    'locale': 'en_US',
                    'timeZone': 'America/New_York'
                }
            }
            
            spreadsheet = self.sheets_service.spreadsheets().create(
                body=spreadsheet_body
            ).execute()
            
            spreadsheet_id = spreadsheet['spreadsheetId']
            print(f"Created master template: {spreadsheet_id}")
            
            # Move to Drive folder
            self._move_to_folder(spreadsheet_id)
            
            # Create all tabs
            self._create_all_tabs(spreadsheet_id)
            
            # Set up schemas for each tab
            self._setup_overview_tab(spreadsheet_id)
            self._setup_game_lines_tab(spreadsheet_id)
            self._setup_player_props_tab(spreadsheet_id)
            self._setup_my_picks_tab(spreadsheet_id)
            self._setup_results_tab(spreadsheet_id)
            
            print(f"✅ Master template created successfully!")
            print(f"📄 Spreadsheet ID: {spreadsheet_id}")
            print(f"🔗 URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            return spreadsheet_id
            
        except HttpError as error:
            print(f"❌ Error creating template: {error}")
            return None
    
    def _move_to_folder(self, spreadsheet_id):
        """Move spreadsheet to the specified Drive folder"""
        try:
            # Find the folder by name
            folder_query = f"name='{Config.GOOGLE_DRIVE_FOLDER_ID}' and mimeType='application/vnd.google-apps.folder'"
            folder_results = self.drive_service.files().list(q=folder_query).execute()
            folders = folder_results.get('files', [])
            
            if not folders:
                print(f"⚠️  Warning: Folder '{Config.GOOGLE_DRIVE_FOLDER_ID}' not found")
                return
                
            folder_id = folders[0]['id']
            
            # Move file to folder
            file = self.drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            
            self.drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            print(f"📁 Moved template to folder: {Config.GOOGLE_DRIVE_FOLDER_ID}")
            
        except HttpError as error:
            print(f"⚠️  Warning: Could not move to folder: {error}")
    
    def _create_all_tabs(self, spreadsheet_id):
        """Create all required tabs"""
        requests = []
        
        # Delete default Sheet1 and create our tabs
        requests.append({
            'deleteSheet': {
                'sheetId': 0  # Default sheet ID
            }
        })
        
        # Create all tabs
        sheet_id = 0
        for tab_key, tab_name in Config.TAB_NAMES.items():
            requests.append({
                'addSheet': {
                    'properties': {
                        'sheetId': sheet_id,
                        'title': tab_name,
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 26
                        }
                    }
                }
            })
            sheet_id += 1
        
        # Execute batch update
        body = {'requests': requests}
        self.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
        print(f"📋 Created {len(Config.TAB_NAMES)} tabs")
    
    def _setup_overview_tab(self, spreadsheet_id):
        """Set up the Overview dashboard tab"""
        tab_name = Config.TAB_NAMES['overview']
        
        # Overview headers and layout
        values = [
            ['NFL Betting Data - Weekly Overview', '', '', '', '', ''],
            ['Created:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '', '', '', ''],
            ['', '', '', '', '', ''],
            ['Week Summary', '', '', '', '', ''],
            ['Total Games:', '', '', '', '', ''],
            ['Total Picks Made:', '', '', '', '', ''],
            ['Current ROI:', '', '', '', '', ''],
            ['Win Rate:', '', '', '', '', ''],
            ['', '', '', '', '', ''],
            ['Data Collection Status', '', '', '', '', ''],
            ['Snapshot 1 (Tuesday):', 'Not Collected', '', '', '', ''],
            ['Snapshot 2 (Thursday):', 'Not Collected', '', '', '', ''],
            ['Snapshot 3 (Saturday):', 'Not Collected', '', '', '', ''],
            ['Snapshot 4 (Sunday AM):', 'Not Collected', '', '', '', ''],
            ['', '', '', '', '', ''],
            ['Quick Stats', '', '', '', '', ''],
            ['Games with Line Movement:', '', '', '', '', ''],
            ['Average Line Movement:', '', '', '', '', ''],
            ['Most Confident Pick:', '', '', '', '', ''],
            ['Best Performing Tag:', '', '', '', '', '']
        ]
        
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
        # Format the overview tab
        self._format_overview_tab(spreadsheet_id, 0)
        
    def _setup_game_lines_tab(self, spreadsheet_id):
        """Set up the Game Lines tab with 4-snapshot schema"""
        tab_name = Config.TAB_NAMES['game_lines']
        
        # Headers for game lines with 4 snapshots
        headers = [
            'Game_ID', 'Date', 'Home_Team', 'Away_Team', 'Bookmaker',
            # Spread snapshots
            'Spread_Line_1', 'Spread_Odds_Home_1', 'Spread_Odds_Away_1', 'Collected_Date_1',
            'Spread_Line_2', 'Spread_Odds_Home_2', 'Spread_Odds_Away_2', 'Collected_Date_2',
            'Spread_Line_3', 'Spread_Odds_Home_3', 'Spread_Odds_Away_3', 'Collected_Date_3',
            'Spread_Line_4', 'Spread_Odds_Home_4', 'Spread_Odds_Away_4', 'Collected_Date_4',
            # Total snapshots
            'Total_Line_1', 'Total_Over_Odds_1', 'Total_Under_Odds_1',
            'Total_Line_2', 'Total_Over_Odds_2', 'Total_Under_Odds_2',
            'Total_Line_3', 'Total_Over_Odds_3', 'Total_Under_Odds_3',
            'Total_Line_4', 'Total_Over_Odds_4', 'Total_Under_Odds_4',
            # Moneyline snapshots
            'ML_Home_1', 'ML_Away_1',
            'ML_Home_2', 'ML_Away_2',
            'ML_Home_3', 'ML_Away_3',
            'ML_Home_4', 'ML_Away_4'
        ]
        
        # Add sample data row
        sample_data = [
            'NFL_2024_W1_001', '2024-09-08', 'Chiefs', 'Ravens', 'DraftKings',
            # Spread snapshots
            '-3.0', '-110', '-110', '2024-09-03 10:00:00',
            '-2.5', '-110', '-110', '2024-09-05 14:00:00', 
            '-3.0', '-108', '-112', '2024-09-07 18:00:00',
            '-3.5', '-110', '-110', '2024-09-08 10:00:00',
            # Total snapshots
            '47.5', '-110', '-110',
            '48.0', '-110', '-110',
            '47.5', '-108', '-112', 
            '47.0', '-110', '-110',
            # Moneyline snapshots
            '-150', '+130',
            '-145', '+125',
            '-155', '+135',
            '-160', '+140'
        ]
        
        values = [headers, sample_data]
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
    def _setup_player_props_tab(self, spreadsheet_id):
        """Set up the Player Props tab with 4-snapshot schema and reference data placeholders"""
        tab_name = Config.TAB_NAMES['player_props']
        
        headers = [
            'Game_ID', 'Player_Name', 'Position', 'Team', 'Market_Type', 'Bookmaker', 'Data_Source',
            # Snapshot 1
            'Over_Line_1', 'Over_Odds_1', 'Under_Line_1', 'Under_Odds_1', 'Collected_Date_1',
            # Snapshot 2  
            'Over_Line_2', 'Over_Odds_2', 'Under_Line_2', 'Under_Odds_2', 'Collected_Date_2',
            # Snapshot 3
            'Over_Line_3', 'Over_Odds_3', 'Under_Line_3', 'Under_Odds_3', 'Collected_Date_3',
            # Snapshot 4
            'Over_Line_4', 'Over_Odds_4', 'Under_Line_4', 'Under_Odds_4', 'Collected_Date_4',
            # Reference data placeholders (initially N/A)
            'Season_Over_Rate', 'Season_Attempts', 'Vs_Defense_Rate', 'Recent_Form_3G', 'Home_Away_Split'
        ]
        
        # Sample data
        sample_data = [
            'NFL_2024_W1_001', 'Patrick Mahomes', 'QB', 'Chiefs', 'Passing Yards', 'DraftKings', 'odds_api',
            # Snapshot 1
            '274.5', '-110', '-110', '2024-09-03 10:00:00',
            # Snapshot 2
            '276.5', '-110', '-110', '2024-09-05 14:00:00',
            # Snapshot 3  
            '274.5', '-108', '-112', '2024-09-07 18:00:00',
            # Snapshot 4
            '275.5', '-110', '-110', '2024-09-08 10:00:00',
            # Reference data (N/A for now)
            'N/A', 'N/A', 'N/A', 'N/A', 'N/A'
        ]
        
        values = [headers, sample_data]
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
    def _setup_my_picks_tab(self, spreadsheet_id):
        """Set up the My Picks tab with enhanced tracking"""
        tab_name = Config.TAB_NAMES['my_picks']
        
        headers = [
            'Pick_ID', 'Game_ID', 'Player_Name', 'Bet_Type', 'Selection', 'Line_Value', 'Odds',
            'Stake', 'Confidence', 'Pick_Type', 'Tags', 'Comments', 'Reasoning',
            'Date_Placed', 'Date_Resolved', 'Result', 'Profit_Loss'
        ]
        
        # Sample data
        sample_data = [
            'PICK_001', 'NFL_2024_W1_001', 'Patrick Mahomes', 'Player Prop', 'Over 274.5 Passing Yards', 
            '274.5', '-110', '100', '8', 'manual', 'weather_play,home_opener', 
            'Chiefs at home in opener', 'Mahomes historically performs well in home openers with good weather',
            '2024-09-08 12:00:00', '', '', ''
        ]
        
        values = [headers, sample_data]
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
    def _setup_results_tab(self, spreadsheet_id):
        """Set up the Results tab for outcome tracking"""
        tab_name = Config.TAB_NAMES['results']
        
        headers = [
            'Game_ID', 'Date', 'Home_Team', 'Away_Team', 'Home_Score', 'Away_Score',
            'Spread_Result', 'Total_Result', 'Game_Status', 'Last_Updated'
        ]
        
        # Sample data
        sample_data = [
            'NFL_2024_W1_001', '2024-09-08', 'Chiefs', 'Ravens', '', '', 
            '', '', 'Scheduled', '2024-09-03 10:00:00'
        ]
        
        values = [headers, sample_data]
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
    def _format_overview_tab(self, spreadsheet_id, sheet_id):
        """Format the overview tab for better presentation"""
        requests = [
            # Format title row
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 6
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                            'textFormat': {
                                'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                                'fontSize': 14,
                                'bold': True
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                }
            },
            # Format section headers
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 3,
                        'endRowIndex': 4,
                        'startColumnIndex': 0,
                        'endColumnIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {
                                'bold': True,
                                'fontSize': 12
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat)'
                }
            }
        ]
        
        body = {'requests': requests}
        self.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
    
    def _write_to_sheet(self, spreadsheet_id, sheet_name, range_start, values):
        """Write values to a specific sheet and range"""
        try:
            range_name = f"{sheet_name}!{range_start}"
            
            body = {
                'values': values
            }
            
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            print(f"📝 Updated {sheet_name}: {result.get('updatedCells', 0)} cells")
            
        except HttpError as error:
            print(f"❌ Error writing to {sheet_name}: {error}")

def main():
    """Create the master template"""
    creator = SheetsTemplateCreator()
    spreadsheet_id = creator.create_master_template()
    
    if spreadsheet_id:
        print(f"\n🎉 Template creation complete!")
        print(f"Save this Spreadsheet ID for cloning: {spreadsheet_id}")

if __name__ == "__main__":
    main()