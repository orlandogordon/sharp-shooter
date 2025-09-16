#!/usr/bin/env python3
"""
Complete weekly NFL betting data workflow
1. Clone master template for new week
2. Collect data in 4-snapshot format  
3. Write data to new weekly file
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Optional, List
from googleapiclient.errors import HttpError

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from data_collector import NFLDataCollector
from sheets_writer import NFLSheetsWriter
from oauth_auth import GoogleOAuthClient

class WeeklyWorkflow:
    """Complete weekly NFL betting data workflow"""
    
    def __init__(self, master_template_id: str):
        """Initialize the workflow"""
        Config.validate_config()
        
        self.master_template_id = master_template_id
        
        # Set up OAuth authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        self.oauth_client = GoogleOAuthClient(scopes)
        self.drive_service = self.oauth_client.get_drive_service()
        
        # Initialize components
        self.data_collector = NFLDataCollector()
        
        print(f"🏈 Weekly NFL Betting Workflow Initialized (OAuth)")
        print(f"📄 Master Template: {master_template_id}")
    
    def execute_weekly_collection(self, week_number: int, snapshot_num: Optional[int] = None) -> Dict:
        """
        Execute complete weekly collection workflow
        
        Args:
            week_number: NFL week number (1-18)
            snapshot_num: Force specific snapshot (1-4), or auto-detect
            
        Returns:
            Dict with workflow results
        """
        print(f"🚀 Starting Weekly Collection Workflow")
        print(f"📅 Week: {week_number}")
        
        # Step 1: Determine snapshot number if not provided
        if snapshot_num is None:
            snapshot_num = self.data_collector._determine_current_snapshot()
            if snapshot_num is None:
                return {
                    'success': False,
                    'error': 'Not a scheduled collection time',
                    'next_collection': self.data_collector._get_next_collection_time()
                }
        
        print(f"📸 Target snapshot: {snapshot_num} - {self.data_collector.snapshot_schedule[snapshot_num]}")
        
        # Step 2: Create or find weekly spreadsheet
        print(f"\n📋 Step 2: Setting up weekly spreadsheet...")
        weekly_spreadsheet_id = self._get_or_create_weekly_spreadsheet(week_number)
        
        if not weekly_spreadsheet_id:
            return {
                'success': False,
                'error': 'Failed to create weekly spreadsheet',
                'step': 'spreadsheet_creation'
            }
        
        # Step 3: Check if snapshot already exists
        existing_snapshots = self._check_existing_snapshots(weekly_spreadsheet_id)
        if snapshot_num in existing_snapshots:
            print(f"\n⚠️  Snapshot {snapshot_num} already collected - skipping data collection")
            return {
                'success': False,
                'error': f'Snapshot {snapshot_num} already exists',
                'snapshot': snapshot_num,
                'existing_snapshots': existing_snapshots
            }
        
        # Step 4: Collect data
        print(f"\n📊 Step 4: Collecting betting data...")
        collection_result = self.data_collector.collect_weekly_data(week_number, snapshot_num)
        
        if not collection_result['success']:
            return {
                'success': False,
                'error': collection_result.get('error'),
                'step': 'data_collection'
            }
        
        # Step 5: Write data to weekly spreadsheet
        print(f"\n✍️  Step 5: Writing data to weekly spreadsheet...")
        writer = NFLSheetsWriter(weekly_spreadsheet_id)
        write_result = writer.write_collection_data(collection_result['data_file'])
        
        if not write_result['success']:
            return {
                'success': False,
                'error': write_result.get('error'),
                'step': 'data_writing'
            }
        
        # Success!
        results = {
            'success': True,
            'week': week_number,
            'snapshot': collection_result['snapshot'],
            'snapshot_description': collection_result['snapshot_description'],
            'weekly_spreadsheet_id': weekly_spreadsheet_id,
            'games_collected': collection_result['games_collected'],
            'props_collected': collection_result['props_collected'],
            'anytime_td_props_collected': collection_result.get('anytime_td_props_collected', 0),
            'games_written': write_result['games_written'],
            'props_written': write_result['props_written'],
            'anytime_td_props_written': write_result.get('anytime_td_props_written', 0),
            'api_requests_used': collection_result['api_requests_made'],
            'collection_timestamp': collection_result['collection_timestamp']
        }
        
        print(f"\n🎉 Weekly Collection Workflow Complete!")
        print(f"📊 Summary:")
        print(f"   Week: {results['week']}")
        print(f"   Snapshot: {results['snapshot']} - {results['snapshot_description']}")
        print(f"   Games: {results['games_collected']} collected → {results['games_written']} written")
        print(f"   Props: {results['props_collected']} collected → {results['props_written']} written")
        print(f"   TD Props: {results['anytime_td_props_collected']} collected → {results['anytime_td_props_written']} written") 
        print(f"   API requests used: {results['api_requests_used']}")
        print(f"🔗 Weekly file: https://docs.google.com/spreadsheets/d/{weekly_spreadsheet_id}")
        
        return results
    
    def _get_or_create_weekly_spreadsheet(self, week_number: int) -> Optional[str]:
        """Get existing or create new weekly spreadsheet"""
        weekly_filename = f"Week_{week_number}_NFL_Betting_2025"
        
        # First, check if weekly file already exists
        try:
            query = f"name='{weekly_filename}' and mimeType='application/vnd.google-apps.spreadsheet'"
            results = self.drive_service.files().list(q=query).execute()
            files = results.get('files', [])
            
            if files:
                spreadsheet_id = files[0]['id']
                print(f"   ✅ Found existing weekly file: {spreadsheet_id}")
                
                # Check which snapshots already exist
                existing_snapshots = self._check_existing_snapshots(spreadsheet_id)
                if existing_snapshots:
                    print(f"   📊 Existing snapshots: {', '.join(map(str, existing_snapshots))}")
                
                return spreadsheet_id
            
        except HttpError as error:
            print(f"   ⚠️  Warning: Could not search for existing file: {error}")
        
        # Create new weekly file by cloning master template
        print(f"   📋 Creating new weekly file: {weekly_filename}")
        
        try:
            # Clone the master template
            copy_request_body = {
                'name': weekly_filename
            }
            
            copied_file = self.drive_service.files().copy(
                fileId=self.master_template_id,
                body=copy_request_body
            ).execute()
            
            weekly_spreadsheet_id = copied_file['id']
            print(f"   ✅ Created weekly file: {weekly_spreadsheet_id}")
            
            # Move to the same folder as master template
            self._move_to_same_folder(weekly_spreadsheet_id, self.master_template_id)
            
            # Remove dummy data from cloned template
            self._remove_dummy_data(weekly_spreadsheet_id)
            
            return weekly_spreadsheet_id
            
        except HttpError as error:
            print(f"   ❌ Failed to create weekly file: {error}")
            return None
    
    def _check_existing_snapshots(self, spreadsheet_id: str) -> List[int]:
        """Check which snapshots already exist in the weekly file"""
        try:
            sheets_service = self.oauth_client.get_sheets_service()
            
            # Check Overview tab for snapshot status (supports 6 snapshots)
            range_name = "Overview!B10:B15"
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            existing_snapshots = []
            
            for i, row in enumerate(values):
                if row and len(row) > 0:
                    status = row[0]
                    if status and status != 'Not Collected' and 'Collected' in status:
                        existing_snapshots.append(i + 1)  # Snapshot numbers are 1-based
            
            return existing_snapshots
            
        except Exception as e:
            print(f"   ⚠️  Warning: Could not check existing snapshots: {e}")
            return []
    
    def _remove_dummy_data(self, spreadsheet_id: str):
        """Remove dummy data from freshly cloned template"""
        try:
            sheets_service = self.oauth_client.get_sheets_service()
            
            print(f"   🧹 Removing dummy data from cloned template...")
            
            # Clear sample data from Game_Lines (row 2)
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Game_Lines!A2:AZ2",
                body={}
            ).execute()
            
            # Clear sample data from Player_Props (row 2)  
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Player_Props!A2:AZ2",
                body={}
            ).execute()
            
            # Clear sample data from Anytime_TD_Props (row 2)
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Anytime_TD_Props!A2:K2",
                body={}
            ).execute()
            
            # Clear sample data from My_Picks (row 2)
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="My_Picks!A2:Q2",
                body={}
            ).execute()
            
            # Clear sample data from Results (row 2)
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Results!A2:J2",
                body={}
            ).execute()
            
            # Clear sample data from Season_Futures (row 2)
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range="Season_Futures!A2:Q2",
                body={}
            ).execute()
            
            print(f"   ✅ Removed dummy data - template is clean")
            
        except Exception as e:
            print(f"   ⚠️  Warning: Could not remove dummy data: {e}")
    
    def _move_to_same_folder(self, file_id: str, reference_file_id: str):
        """Move file to same folder as reference file"""
        try:
            # Get reference file's parent folder
            reference_file = self.drive_service.files().get(
                fileId=reference_file_id, 
                fields='parents'
            ).execute()
            
            reference_parents = reference_file.get('parents', [])
            if not reference_parents:
                return
            
            target_folder_id = reference_parents[0]
            
            # Get current file's parents
            current_file = self.drive_service.files().get(
                fileId=file_id,
                fields='parents'
            ).execute()
            
            current_parents = ",".join(current_file.get('parents', []))
            
            # Move file
            self.drive_service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=current_parents,
                fields='id, parents'
            ).execute()
            
            print(f"   📁 Moved to same folder as master template")
            
        except HttpError as error:
            print(f"   ⚠️  Warning: Could not move to folder: {error}")
    
    def get_weekly_file_url(self, week_number: int) -> Optional[str]:
        """Get URL for weekly file"""
        weekly_filename = f"Week_{week_number}_NFL_Betting_2025"
        
        try:
            query = f"name='{weekly_filename}' and mimeType='application/vnd.google-apps.spreadsheet'"
            results = self.drive_service.files().list(q=query).execute()
            files = results.get('files', [])
            
            if files:
                file_id = files[0]['id']
                return f"https://docs.google.com/spreadsheets/d/{file_id}"
            
        except:
            pass
            
        return None
    
    def list_weekly_files(self) -> List[Dict]:
        """List all weekly NFL betting files"""
        try:
            query = "name contains 'Week_' and name contains 'NFL_Betting_2025' and mimeType='application/vnd.google-apps.spreadsheet'"
            results = self.drive_service.files().list(q=query).execute()
            files = results.get('files', [])
            
            weekly_files = []
            for file in files:
                weekly_files.append({
                    'name': file['name'],
                    'id': file['id'],
                    'url': f"https://docs.google.com/spreadsheets/d/{file['id']}"
                })
            
            return weekly_files
            
        except HttpError as error:
            print(f"❌ Error listing weekly files: {error}")
            return []

def main():
    """Test the complete weekly workflow"""
    # Get master template ID from environment
    master_template_id = Config.MASTER_TEMPLATE_ID
    
    if not master_template_id:
        print("❌ Error: MASTER_TEMPLATE_ID not found in .env file")
        return
    
    # Initialize workflow
    workflow = WeeklyWorkflow(master_template_id)
    
    print(f"🧪 Testing complete weekly workflow...")
    print("📅 Using automatic snapshot detection based on current date/time")
    
    # Execute workflow for current week, auto-detect snapshot
    current_week = Config.get_current_week()
    print(f"🏈 Executing workflow for Week {current_week} (auto-detected)")
    
    results = workflow.execute_weekly_collection(week_number=current_week)
    
    if results['success']:
        print(f"\n✅ Complete workflow test successful!")
        print(f"🏈 Week {results['week']} NFL betting data collected and processed")
        print(f"📊 Snapshot {results['snapshot']}: {results['snapshot_description']}")
        print(f"📈 Data Summary:")
        print(f"   • {results['games_written']} game lines written")
        print(f"   • {results['props_written']} player props written")
        print(f"   • {results['api_requests_used']} API requests used")
        
        # Show weekly file URL
        weekly_url = workflow.get_weekly_file_url(current_week)
        if weekly_url:
            print(f"\n🔗 Weekly File: {weekly_url}")
        
        print(f"\n🎯 The complete NFL betting data workflow is operational!")
        
    else:
        error_msg = results.get('error', 'Unknown error')
        
        if 'already exists' in error_msg:
            print(f"\n✅ Snapshot already collected - no action needed!")
            print(f"📊 Snapshot {results['snapshot']} was previously collected")
            
            # Show existing snapshots
            if 'existing_snapshots' in results:
                print(f"📋 Existing snapshots: {', '.join(map(str, results['existing_snapshots']))}")
            
            # Show weekly file URL
            weekly_url = workflow.get_weekly_file_url(current_week)
            if weekly_url:
                print(f"🔗 Weekly File: {weekly_url}")
                
        else:
            step = results.get('step', 'unknown')
            print(f"❌ Workflow failed at step: {step}")
            print(f"Error: {error_msg}")

if __name__ == "__main__":
    main()