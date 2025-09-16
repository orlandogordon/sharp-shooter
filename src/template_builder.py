#!/usr/bin/env python3
"""
Google Sheets Template Builder for NFL Betting Data Workflow
Creates a new master template with all tabs and proper structure
"""

import os
import sys
from typing import Dict, List, Any
from googleapiclient.errors import HttpError

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from oauth_auth import GoogleOAuthClient

class NFLTemplateBuilder:
    """Builds the NFL betting data master template"""
    
    def __init__(self):
        """Initialize the template builder"""
        Config.validate_config()
        
        # Set up OAuth authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        self.oauth_client = GoogleOAuthClient(scopes)
        self.sheets_service = self.oauth_client.get_sheets_service()
        self.drive_service = self.oauth_client.get_drive_service()
        
        print(f"🏗️  NFL Template Builder Initialized")
        
    def build_template(self) -> str:
        """Build complete NFL betting template and return spreadsheet ID"""
        print(f"\n🚀 Building NFL Betting Data Master Template...")
        
        try:
            # Step 1: Create new spreadsheet
            spreadsheet_id = self._create_base_spreadsheet()
            print(f"✅ Created base spreadsheet: {spreadsheet_id}")
            
            # Step 2: Build all tabs
            self._build_overview_tab(spreadsheet_id)
            self._build_game_lines_tab(spreadsheet_id)
            self._build_player_props_tab(spreadsheet_id)
            self._build_anytime_td_props_tab(spreadsheet_id)
            self._build_my_picks_tab(spreadsheet_id)
            self._build_results_tab(spreadsheet_id)
            self._build_season_futures_tab(spreadsheet_id)
            
            # Step 3: Delete default "Sheet1"
            self._delete_default_sheet(spreadsheet_id)
            
            # Step 4: Apply formatting
            self._apply_template_formatting(spreadsheet_id)
            
            # Step 5: Move to Sharp-Shooter folder
            self._move_to_sharp_shooter_folder(spreadsheet_id)
            
            print(f"\n🎉 Template Build Complete!")
            print(f"📄 Template ID: {spreadsheet_id}")
            print(f"🔗 Template URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            
            return spreadsheet_id
            
        except Exception as e:
            print(f"❌ Template build failed: {e}")
            raise
    
    def _create_base_spreadsheet(self) -> str:
        """Create base spreadsheet"""
        spreadsheet = {
            'properties': {
                'title': 'NFL_Betting_Master_Template_2025',
                'locale': 'en_US',
                'timeZone': 'America/New_York'
            }
        }
        
        result = self.sheets_service.spreadsheets().create(body=spreadsheet).execute()
        return result['spreadsheetId']
    
    def _build_overview_tab(self, spreadsheet_id: str):
        """Build Overview tab with dashboard structure"""
        print("📊 Building Overview tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Overview', 0)
        
        # Headers and structure
        overview_data = [
            ['NFL BETTING DATA WORKFLOW', '', '', ''],
            ['', '', '', ''],
            ['Week Information:', '', '', ''],
            ['Week Number:', '', '', ''],
            ['Total Games:', '', '', ''],
            ['Total Picks:', '', '', ''],
            ['', '', '', ''],
            ['Collection Status:', '', '', ''],
            ['', '', '', ''],
            ['Snapshot 1 (Tuesday Opening):', 'Not Collected', '', ''],
            ['Snapshot 2 (Thursday TNF):', 'Not Collected', '', ''],
            ['Snapshot 3 (Friday Games):', 'Not Collected', '', ''],
            ['Snapshot 4 (Saturday Games):', 'Not Collected', '', ''],
            ['Snapshot 5 (Sunday Games):', 'Not Collected', '', ''],
            ['Snapshot 6 (Monday MNF):', 'Not Collected', '', ''],
            ['', '', '', ''],
            ['Data Summary:', '', '', ''],
            ['Games Collected:', '0', '', ''],
            ['Player Props Collected:', '0', '', ''],
            ['Anytime TD Props Collected:', '0', '', ''],
            ['', '', '', ''],
            ['Last Updated:', '', '', '']
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Overview', overview_data)
    
    def _build_game_lines_tab(self, spreadsheet_id: str):
        """Build Game_Lines tab with 2-snapshot structure (opening + final)"""
        print("🏈 Building Game_Lines tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Game_Lines', 1)
        
        # Headers for 2-snapshot structure
        headers = [
            'Game_ID', 'Date', 'Home_Team', 'Away_Team', 'Bookmaker',
            # Opening Snapshot (Snapshot 1 - Tuesday)
            'Opening_Spread_Line', 'Opening_Spread_Home_Odds', 'Opening_Spread_Away_Odds', 'Opening_Collected_Date',
            'Opening_Total_Line', 'Opening_Total_Over_Odds', 'Opening_Total_Under_Odds',
            'Opening_ML_Home', 'Opening_ML_Away',
            # Final Snapshot (Day of Event)
            'Final_Spread_Line', 'Final_Spread_Home_Odds', 'Final_Spread_Away_Odds', 'Final_Collected_Date',
            'Final_Total_Line', 'Final_Total_Over_Odds', 'Final_Total_Under_Odds',
            'Final_ML_Home', 'Final_ML_Away'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['NFL_2025_2025-09-05_CHIK_RAMS', '2025-09-05', 'Los Angeles Rams', 'Chicago Bears', 'DraftKings',
             '', '', '', '', '', '', '', '', '',  # Opening snapshot
             '', '', '', '', '', '', '', '', '']   # Final snapshot
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Game_Lines', sample_data)
    
    def _build_player_props_tab(self, spreadsheet_id: str):
        """Build Player_Props tab with single snapshot structure"""
        print("🎯 Building Player_Props tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Player_Props', 2)
        
        # Headers for single snapshot structure (day of event only)
        headers = [
            'Game_ID', 'Player_Name', 'Position', 'Team', 'Market_Type', 'Bookmaker', 'Data_Source',
            # Single Snapshot (Day of Event)
            'Over_Line', 'Over_Odds', 'Under_Line', 'Under_Odds', 'Collected_Date',
            # Reference Data Placeholders
            'Season_Over_Rate', 'Season_Attempts', 'Vs_Defense_Rate', 'Recent_Form_3G', 'Home_Away_Split'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['NFL_2025_2025-09-05_CHIK_RAMS', 'Justin Herbert', 'QB', 'TBD', 'Passing Yards', 'DraftKings', 'odds_api',
             '275.5', '-110', '275.5', '-110', '',  # Single snapshot
             'N/A', 'N/A', 'N/A', 'N/A', 'N/A']     # Reference data
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Player_Props', sample_data)
    
    def _build_anytime_td_props_tab(self, spreadsheet_id: str):
        """Build Anytime_TD_Props tab with simplified structure"""
        print("🏆 Building Anytime_TD_Props tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Anytime_TD_Props', 3)
        
        # Headers for simplified anytime TD structure
        headers = [
            'Game_ID', 'Player_Name', 'Team', 'Bookmaker', 'Data_Source',
            'Anytime_TD_Odds', 'Collected_Date',
            # Reference Data Placeholders
            'Season_TDs', 'Red_Zone_Targets', 'Goal_Line_Carries', 'Recent_TD_Rate'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['NFL_2025_2025-09-05_CHIK_RAMS', 'Cooper Kupp', 'TBD', 'DraftKings', 'odds_api',
             '+450', '',  # TD odds
             'N/A', 'N/A', 'N/A', 'N/A']  # Reference data
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Anytime_TD_Props', sample_data)
    
    def _build_my_picks_tab(self, spreadsheet_id: str):
        """Build My_Picks tab for manual pick entry"""
        print("📝 Building My_Picks tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'My_Picks', 4)
        
        # Headers for enhanced pick tracking
        headers = [
            'Pick_ID', 'Game_ID', 'Bet_Type', 'Selection', 'Line_Value', 'Odds', 'Stake',
            'Confidence', 'Pick_Type', 'Tags', 'Comments', 'Reasoning',
            'Result', 'Profit_Loss', 'Date_Placed', 'Date_Resolved', 'Bookmaker'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['PICK_001', 'NFL_2025_2025-09-05_CHIK_RAMS', 'Spread', 'Los Angeles Rams -3.5', '-3.5', '-110', '100',
             '8', 'manual', 'home_favorite,revenge_game', 'Strong home opener', 'Rams looked great in preseason',
             '', '', '2025-09-05', '', 'DraftKings']
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'My_Picks', sample_data)
    
    def _build_results_tab(self, spreadsheet_id: str):
        """Build Results tab for outcome tracking"""
        print("📊 Building Results tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Results', 5)
        
        # Headers for results tracking
        headers = [
            'Game_ID', 'Date', 'Home_Team', 'Away_Team', 'Home_Score', 'Away_Score',
            'Final_Spread', 'Final_Total', 'Winner', 'Updated_Date'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['NFL_2025_2025-09-05_CHIK_RAMS', '2025-09-05', 'Los Angeles Rams', 'Chicago Bears', '', '',
             '', '', '', '']
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Results', sample_data)
    
    def _build_season_futures_tab(self, spreadsheet_id: str):
        """Build Season_Futures tab for long-term bets"""
        print("🔮 Building Season_Futures tab...")
        
        # Create the sheet
        self._add_sheet(spreadsheet_id, 'Season_Futures', 6)
        
        # Headers for futures tracking
        headers = [
            'Bet_ID', 'Bet_Type', 'Selection', 'Odds', 'Stake', 'Bookmaker',
            'Date_Placed', 'Confidence', 'Tags', 'Comments', 'Reasoning',
            'Status', 'Result', 'Profit_Loss', 'Date_Resolved', 'Notes', 'Current_Odds'
        ]
        
        # Sample row for reference
        sample_data = [
            headers,
            ['FUT_001', 'Super Bowl Winner', 'Los Angeles Rams', '+1200', '50', 'DraftKings',
             '2025-08-01', '6', 'super_bowl,nfc_west', 'Good value on defending champs', 'Strong roster continuity',
             'Active', '', '', '', '', '+1400']
        ]
        
        self._write_data_to_sheet(spreadsheet_id, 'Season_Futures', sample_data)
    
    def _add_sheet(self, spreadsheet_id: str, sheet_name: str, sheet_index: int):
        """Add a new sheet to the spreadsheet"""
        requests = [{
            'addSheet': {
                'properties': {
                    'title': sheet_name,
                    'index': sheet_index,
                    'gridProperties': {
                        'rowCount': 1000,
                        'columnCount': 26
                    }
                }
            }
        }]
        
        self.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
    
    def _write_data_to_sheet(self, spreadsheet_id: str, sheet_name: str, data: List[List[Any]]):
        """Write data to a specific sheet"""
        range_name = f"{sheet_name}!A1:Z{len(data)}"
        
        body = {'values': data}
        
        self.sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
    
    def _delete_default_sheet(self, spreadsheet_id: str):
        """Delete the default 'Sheet1' that comes with new spreadsheets"""
        try:
            # Get sheet metadata to find Sheet1's ID
            spreadsheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            for sheet in spreadsheet_metadata['sheets']:
                if sheet['properties']['title'] == 'Sheet1':
                    sheet1_id = sheet['properties']['sheetId']
                    
                    requests = [{
                        'deleteSheet': {
                            'sheetId': sheet1_id
                        }
                    }]
                    
                    self.sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id,
                        body={'requests': requests}
                    ).execute()
                    
                    print("🗑️  Deleted default Sheet1")
                    break
        except Exception as e:
            print(f"⚠️  Could not delete Sheet1: {e}")
    
    def _apply_template_formatting(self, spreadsheet_id: str):
        """Apply formatting to the template"""
        print("🎨 Applying template formatting...")
        
        try:
            # Get all sheets
            spreadsheet_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            requests = []
            
            for sheet in spreadsheet_metadata['sheets']:
                sheet_id = sheet['properties']['sheetId']
                sheet_title = sheet['properties']['title']
                
                # Header row formatting (bold, background color) - ONLY row 1 (index 0)
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,  # Only header row
                            'startColumnIndex': 0,
                            'endColumnIndex': 26
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.8},
                                'textFormat': {
                                    'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                })
                
                # Clear any formatting on row 2 (first data row) to ensure no blue background
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': 2,  # Only row 2 (first data row)
                            'startColumnIndex': 0,
                            'endColumnIndex': 26
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},  # White background
                                'textFormat': {
                                    'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},  # Black text
                                    'bold': False
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                })
                
                # Freeze header row
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
                
                # Auto-resize columns
                requests.append({
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': 26
                        }
                    }
                })
            
            # Apply all formatting requests
            if requests:
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()
                
        except Exception as e:
            print(f"⚠️  Could not apply formatting: {e}")
    
    def _move_to_sharp_shooter_folder(self, spreadsheet_id: str):
        """Move template to Sharp-Shooter folder"""
        try:
            folder_id = Config.GOOGLE_DRIVE_FOLDER_ID
            if not folder_id:
                print("⚠️  No Sharp-Shooter folder ID configured")
                return
            
            # Get current parents
            current_file = self.drive_service.files().get(
                fileId=spreadsheet_id,
                fields='parents'
            ).execute()
            
            current_parents = ",".join(current_file.get('parents', []))
            
            # Move to Sharp-Shooter folder
            self.drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=current_parents,
                fields='id, parents'
            ).execute()
            
            print(f"📁 Moved template to Sharp-Shooter folder")
            
        except Exception as e:
            print(f"⚠️  Could not move to folder: {e}")
    
    def update_env_file(self, template_id: str):
        """Update .env file with new template ID"""
        try:
            env_path = '.env'
            if not os.path.exists(env_path):
                print(f"⚠️  .env file not found at {env_path}")
                return
            
            # Read current .env file
            with open(env_path, 'r') as f:
                lines = f.readlines()
            
            # Update MASTER_TEMPLATE_ID
            updated = False
            for i, line in enumerate(lines):
                if line.startswith('MASTER_TEMPLATE_ID='):
                    lines[i] = f'MASTER_TEMPLATE_ID={template_id}\n'
                    updated = True
                    break
            
            # Add if not found
            if not updated:
                lines.append(f'MASTER_TEMPLATE_ID={template_id}\n')
            
            # Write back to .env file
            with open(env_path, 'w') as f:
                f.writelines(lines)
            
            print(f"✅ Updated .env file with new template ID")
            
        except Exception as e:
            print(f"⚠️  Could not update .env file: {e}")

def main():
    """Build NFL betting template"""
    builder = NFLTemplateBuilder()
    
    try:
        # Build the template
        template_id = builder.build_template()
        
        # Update .env file
        builder.update_env_file(template_id)
        
        print(f"\n🎉 SUCCESS! New NFL Betting Template Created")
        print(f"📄 Template ID: {template_id}")
        print(f"🔗 Template URL: https://docs.google.com/spreadsheets/d/{template_id}")
        print(f"📁 Location: Sharp-Shooter Google Drive folder")
        print(f"✅ .env file updated with new template ID")
        
        print(f"\n📋 Template Structure:")
        print(f"   • Overview - Dashboard with collection status")
        print(f"   • Game_Lines - 2 snapshots (opening + final)")
        print(f"   • Player_Props - Single snapshot (day of event)")
        print(f"   • Anytime_TD_Props - Simplified TD tracking")
        print(f"   • My_Picks - Enhanced pick tracking")
        print(f"   • Results - Game outcome tracking")
        print(f"   • Season_Futures - Long-term bets")
        
        print(f"\n🚀 Ready to use with weekly_workflow.py!")
        
    except Exception as e:
        print(f"❌ Template build failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())