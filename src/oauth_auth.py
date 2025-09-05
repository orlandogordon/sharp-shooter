#!/usr/bin/env python3
"""
OAuth authentication for Google APIs
Uses personal Google account instead of service account
"""

import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from typing import Any

class GoogleOAuthClient:
    """OAuth client for Google APIs using personal account"""
    
    def __init__(self, scopes: list):
        """Initialize OAuth client"""
        self.scopes = scopes
        self.credentials = None
        self.token_file = 'token.pickle'
        
    def authenticate(self) -> Any:
        """Authenticate and return credentials"""
        # Check if we have saved credentials
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                self.credentials = pickle.load(token)
        
        # If no valid credentials, get new ones
        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                print("🔄 Refreshing expired OAuth token...")
                self.credentials.refresh(Request())
            else:
                print("🔐 Starting OAuth authentication flow...")
                print("📝 A browser window will open for Google authentication")
                
                # Create OAuth flow
                flow = InstalledAppFlow.from_client_config(
                    self._get_client_config(),
                    self.scopes
                )
                
                # Run local server for OAuth callback
                self.credentials = flow.run_local_server(port=0)
                print("✅ OAuth authentication successful!")
            
            # Save credentials for next time
            with open(self.token_file, 'wb') as token:
                pickle.dump(self.credentials, token)
                print("💾 OAuth token saved for future use")
        
        return self.credentials
    
    def _get_client_config(self) -> dict:
        """Get OAuth client configuration from environment"""
        from dotenv import load_dotenv
        load_dotenv()
        
        client_id = os.getenv('GOOGLE_CLIENT_ID')
        client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
        
        if not client_id or not client_secret:
            raise ValueError("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET in .env file")
        
        return {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
            }
        }
    
    def get_drive_service(self):
        """Get authenticated Google Drive service"""
        credentials = self.authenticate()
        return build('drive', 'v3', credentials=credentials)
    
    def get_sheets_service(self):
        """Get authenticated Google Sheets service"""
        credentials = self.authenticate()
        return build('sheets', 'v4', credentials=credentials)

def test_oauth_auth():
    """Test OAuth authentication"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    print("🧪 Testing OAuth authentication...")
    
    try:
        oauth_client = GoogleOAuthClient(scopes)
        
        # Test Drive access
        print("\n📁 Testing Google Drive access...")
        drive_service = oauth_client.get_drive_service()
        
        # List files in Drive
        results = drive_service.files().list(pageSize=5).execute()
        files = results.get('files', [])
        
        print(f"✅ Successfully accessed Google Drive")
        print(f"📊 Found {len(files)} files (showing first 5):")
        for file in files:
            print(f"   • {file['name']} ({file['mimeType'][:30]}...)")
        
        # Test Sheets access
        print("\n📊 Testing Google Sheets access...")
        sheets_service = oauth_client.get_sheets_service()
        
        # Try to access master template
        template_id = '1OS6zUYgz_SRsDJREXEexgFv_qlptQh8SVWGDnqdVZOw'
        try:
            sheet_info = sheets_service.spreadsheets().get(spreadsheetId=template_id).execute()
            print(f"✅ Successfully accessed master template: {sheet_info['properties']['title']}")
        except:
            print("⚠️  Could not access master template (this is expected if it was shared with service account only)")
        
        print(f"\n🎉 OAuth authentication working perfectly!")
        print(f"🔐 Token saved to: {oauth_client.token_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ OAuth authentication failed: {e}")
        return False

if __name__ == "__main__":
    test_oauth_auth()