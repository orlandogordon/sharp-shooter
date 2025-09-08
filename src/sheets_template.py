#!/usr/bin/env python3
"""
Google Sheets template creator for NFL Betting Data Workflow
Creates the master template with proper tab structure and schemas
"""

import os
import sys
from datetime import datetime
from googleapiclient.errors import HttpError

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from oauth_auth import GoogleOAuthClient

class SheetsTemplateCreator:
    """Creates Google Sheets templates for NFL betting data tracking"""
    
    def __init__(self):
        """Initialize with OAuth authentication"""
        Config.validate_config()
        
        # Set up OAuth authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        oauth_client = GoogleOAuthClient(scopes)
        self.sheets_service = oauth_client.get_sheets_service()
        self.drive_service = oauth_client.get_drive_service()
        
        print("🔐 Initialized with OAuth authentication")
        
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
            self._setup_futures_tab(spreadsheet_id)
            
            # Apply formatting to all tabs
            self._apply_all_formatting(spreadsheet_id)
            
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
            # Use the folder ID directly (not search by name)
            folder_id = Config.GOOGLE_DRIVE_FOLDER_ID
            
            # Verify folder exists by getting its metadata
            folder = self.drive_service.files().get(fileId=folder_id).execute()
            folder_name = folder.get('name', 'Unknown')
            
            # Move file to folder
            file = self.drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            
            self.drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            print(f"📁 Moved template to folder: {folder_name} ({folder_id})")
            
        except HttpError as error:
            print(f"⚠️  Warning: Could not move to folder: {error}")
    
    def _create_all_tabs(self, spreadsheet_id):
        """Create all required tabs"""
        # First, create all our tabs
        requests = []
        sheet_id = 1  # Start from 1 since Sheet1 (ID=0) already exists
        
        for tab_key, tab_name in Config.TAB_NAMES.items():
            requests.append({
                'addSheet': {
                    'properties': {
                        'sheetId': sheet_id,
                        'title': tab_name,
                        'gridProperties': {
                            'rowCount': 1000,
                            'columnCount': 52  # More columns for snapshot data
                        }
                    }
                }
            })
            sheet_id += 1
        
        # Execute batch update to create new tabs
        if requests:
            body = {'requests': requests}
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            print(f"📋 Created {len(Config.TAB_NAMES)} new tabs")
        
        # Now delete the default Sheet1 (now that we have other tabs)
        delete_request = {
            'requests': [{
                'deleteSheet': {
                    'sheetId': 0  # Default Sheet1 ID
                }
            }]
        }
        
        try:
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=delete_request
            ).execute()
            print(f"🗑️  Removed default Sheet1")
            
        except HttpError as e:
            print(f"⚠️  Warning: Could not remove default sheet: {e}")
    
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
        
        # Note: Overview formatting is now handled by _apply_all_formatting()
        
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
        
    def _setup_futures_tab(self, spreadsheet_id):
        """Set up the Season Futures tab for manual entry of season-long bets"""
        tab_name = Config.TAB_NAMES['futures']
        
        headers = [
            'Bet_ID', 'Date_Placed', 'Market_Type', 'Selection', 'Odds_Placed', 'Current_Odds',
            'Stake', 'Potential_Payout', 'Confidence', 'Tags', 'Comments', 'Reasoning',
            'Status', 'Result', 'Profit_Loss', 'Date_Resolved', 'Last_Updated'
        ]
        
        # Sample futures bets for reference
        sample_data = [
            [
                'FUTURES_001', '2024-08-15', 'Super Bowl Winner', 'Kansas City Chiefs', '+1000', '+900',
                '100', '1000', '8', 'championship,value_bet', 'Strong value at +1000 preseason',
                'Dynasty team with Mahomes', 'Open', '', '', '', '2024-08-15'
            ],
            [
                'FUTURES_002', '2024-08-20', 'NFL MVP', 'Josh Allen', '+700', '+650', 
                '50', '350', '7', 'mvp,bills', 'Bills should have strong season',
                'Top QB on playoff team', 'Open', '', '', '', '2024-08-20'
            ],
            [
                'FUTURES_003', '2024-08-25', 'Team Win Total', 'Detroit Lions Over 10.5', '-110', '-120',
                '75', '68.18', '6', 'win_total,nfc', 'Lions improved significantly',
                'Strong offense and coaching', 'Open', '', '', '', '2024-08-25'
            ],
            [
                'FUTURES_004', '2024-09-01', 'DPOY', 'Micah Parsons', '+450', '+400',
                '25', '112.50', '5', 'dpoy,cowboys', 'Elite pass rusher',
                'Should get plenty of sacks', 'Open', '', '', '', '2024-09-01'
            ],
            [
                'FUTURES_005', '2024-08-10', 'OPOY', 'Christian McCaffrey', '+800', '+750',
                '40', '320', '7', 'opoy,49ers', 'Best offensive weapon in league',
                'Healthy CMC is unstoppable', 'Open', '', '', '', '2024-08-10'
            ]
        ]
        
        values = [headers] + sample_data
        self._write_to_sheet(spreadsheet_id, tab_name, 'A1', values)
        
        print(f"📊 Created Season Futures tab with sample futures bets")
        
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
    
    def _apply_all_formatting(self, spreadsheet_id):
        """Apply comprehensive formatting to all tabs"""
        print(f"🎨 Applying formatting and filters to all tabs...")
        
        # Get sheet metadata to find sheet IDs
        spreadsheet_metadata = self.sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        # Create sheet ID mapping
        sheet_id_map = {}
        for sheet in spreadsheet_metadata['sheets']:
            title = sheet['properties']['title']
            sheet_id = sheet['properties']['sheetId']
            sheet_id_map[title] = sheet_id
        
        requests = []
        
        # Apply formatting to each tab
        for tab_key, tab_name in Config.TAB_NAMES.items():
            if tab_name in sheet_id_map:
                sheet_id = sheet_id_map[tab_name]
                
                # Add filters and sorting for data tabs
                if tab_key in ['game_lines', 'player_props', 'my_picks', 'results', 'futures']:
                    requests.extend(self._get_data_tab_formatting(sheet_id, tab_key))
                elif tab_key == 'overview':
                    requests.extend(self._get_overview_formatting(sheet_id))
        
        # Apply all formatting in batch
        if requests:
            body = {'requests': requests}
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            print(f"✅ Applied formatting to {len(Config.TAB_NAMES)} tabs")
        
    def _get_data_tab_formatting(self, sheet_id, tab_key):
        """Get formatting requests for data tabs"""
        requests = []
        
        # Determine number of columns based on tab type
        column_counts = {
            'game_lines': 35,      # Game lines has many snapshot columns
            'player_props': 30,    # Player props has snapshot columns  
            'my_picks': 17,        # My picks has standard columns
            'results': 10,         # Results is simpler
            'futures': 17          # Futures has comprehensive tracking
        }
        
        max_cols = column_counts.get(tab_key, 26)
        
        # 1. Add autofilter for sorting/filtering
        requests.append({
            'setBasicFilter': {
                'filter': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1000,  # Large range for data
                        'startColumnIndex': 0,
                        'endColumnIndex': max_cols
                    }
                }
            }
        })
        
        # 2. Header row formatting - Professional blue header
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': max_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'fontSize': 11,
                            'bold': True
                        },
                        'horizontalAlignment': 'CENTER',
                        'borders': {
                            'bottom': {'style': 'SOLID', 'width': 2, 'color': {'red': 0.1, 'green': 0.2, 'blue': 0.6}}
                        }
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,borders)'
            }
        })
        
        # 3. Freeze header row
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })
        
        # 4. Alternate row colors for data
        requests.append({
            'addBanding': {
                'bandedRange': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 1,  # Skip header
                        'endRowIndex': 1000,
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
        
        # 5. Column-specific formatting based on tab type
        if tab_key == 'my_picks' or tab_key == 'futures':
            # Color confidence levels (assuming confidence is in column I for my_picks, column I for futures)
            confidence_col = 8  # Column I (0-indexed)
            requests.extend(self._get_confidence_color_formatting(sheet_id, confidence_col))
            
        if tab_key == 'futures':
            # Color status column (column M)
            status_col = 12  # Column M (0-indexed) 
            requests.extend(self._get_status_color_formatting(sheet_id, status_col))
        
        return requests
    
    def _get_overview_formatting(self, sheet_id):
        """Get formatting requests for overview tab"""
        requests = []
        
        # Main title formatting
        requests.append({
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
                        'backgroundColor': {'red': 0.1, 'green': 0.3, 'blue': 0.7},
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'fontSize': 16,
                            'bold': True
                        },
                        'horizontalAlignment': 'CENTER'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
            }
        })
        
        # Section headers formatting (rows 4, 10, 16)
        section_rows = [3, 9, 15]  # 0-indexed
        for row in section_rows:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': row,
                        'endRowIndex': row + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': 6
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': 0.8, 'green': 0.9, 'blue': 1.0},
                            'textFormat': {
                                'fontSize': 12,
                                'bold': True,
                                'foregroundColor': {'red': 0.1, 'green': 0.2, 'blue': 0.6}
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                }
            })
        
        return requests
    
    def _get_confidence_color_formatting(self, sheet_id, column_index):
        """Add conditional formatting for confidence levels"""
        return [{
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 1000,
                        'startColumnIndex': column_index,
                        'endColumnIndex': column_index + 1
                    }],
                    'gradientRule': {
                        'minpoint': {
                            'color': {'red': 1.0, 'green': 0.8, 'blue': 0.8},
                            'type': 'NUMBER',
                            'value': '1'
                        },
                        'maxpoint': {
                            'color': {'red': 0.8, 'green': 1.0, 'blue': 0.8},
                            'type': 'NUMBER', 
                            'value': '10'
                        }
                    }
                },
                'index': 0
            }
        }]
    
    def _get_status_color_formatting(self, sheet_id, column_index):
        """Add conditional formatting for status column"""
        requests = []
        
        # Green for "Won"
        requests.append({
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 1000,
                        'startColumnIndex': column_index,
                        'endColumnIndex': column_index + 1
                    }],
                    'booleanRule': {
                        'condition': {
                            'type': 'TEXT_CONTAINS',
                            'values': [{'userEnteredValue': 'Won'}]
                        },
                        'format': {
                            'backgroundColor': {'red': 0.8, 'green': 1.0, 'blue': 0.8}
                        }
                    }
                },
                'index': 0
            }
        })
        
        # Red for "Lost" 
        requests.append({
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 1,
                        'endRowIndex': 1000,
                        'startColumnIndex': column_index,
                        'endColumnIndex': column_index + 1
                    }],
                    'booleanRule': {
                        'condition': {
                            'type': 'TEXT_CONTAINS',
                            'values': [{'userEnteredValue': 'Lost'}]
                        },
                        'format': {
                            'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8}
                        }
                    }
                },
                'index': 1
            }
        })
        
        return requests
    
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