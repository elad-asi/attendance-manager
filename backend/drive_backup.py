"""
Google Drive Backup Module
Handles uploading and downloading database backups to/from Google Drive
Uses OAuth with stored refresh token for admin-authorized backups
"""
import os
import io
import json
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Backup folder name in Google Drive
BACKUP_FOLDER_NAME = 'AttendanceManager_Backups'
DATABASE_FILE = 'data/attendance.db'
TOKEN_FILE = 'data/drive_token.json'

# OAuth Scopes for Drive access
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Google OAuth Client ID (same as used for Sheets)
GOOGLE_CLIENT_ID = '651831609522-bvrgmop9hmdghlrn2tqm1hv0dmkhu933.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')


def get_stored_credentials():
    """Load stored OAuth credentials from file or environment"""
    # Try environment variable first (for production/Render)
    token_json = os.environ.get('GOOGLE_DRIVE_TOKEN')
    if token_json:
        try:
            token_data = json.loads(token_json)
            creds = Credentials(
                token=token_data.get('access_token'),
                refresh_token=token_data.get('refresh_token'),
                token_uri='https://oauth2.googleapis.com/token',
                client_id=GOOGLE_CLIENT_ID,
                client_secret=GOOGLE_CLIENT_SECRET,
                scopes=SCOPES
            )
            # Refresh if expired
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            return creds
        except Exception as e:
            print(f"Error loading token from env: {e}")
            return None

    # Try loading from file (for local development)
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            creds = Credentials(
                token=token_data.get('access_token'),
                refresh_token=token_data.get('refresh_token'),
                token_uri='https://oauth2.googleapis.com/token',
                client_id=GOOGLE_CLIENT_ID,
                client_secret=GOOGLE_CLIENT_SECRET,
                scopes=SCOPES
            )
            # Refresh if expired
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Save refreshed token
                save_credentials(creds)
            return creds
        except Exception as e:
            print(f"Error loading token from file: {e}")
            return None

    return None


def save_credentials(creds):
    """Save OAuth credentials to file"""
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    token_data = {
        'access_token': creds.token,
        'refresh_token': creds.refresh_token,
        'token_uri': creds.token_uri,
        'client_id': creds.client_id,
        'scopes': list(creds.scopes) if creds.scopes else SCOPES
    }
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f)


def authorize_drive_backup(auth_code):
    """
    Exchange authorization code for credentials and store them.
    This should be called once by the admin to authorize Drive backups.
    """
    from google_auth_oauthlib.flow import Flow

    # Create flow from client secrets
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["postmessage"]
            }
        },
        scopes=SCOPES
    )
    flow.redirect_uri = 'postmessage'

    try:
        # Exchange auth code for tokens
        flow.fetch_token(code=auth_code)
        creds = flow.credentials

        # Save credentials
        save_credentials(creds)

        return {'success': True, 'message': 'Drive backup authorized successfully'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def get_drive_service():
    """Create a Google Drive service instance using stored credentials"""
    creds = get_stored_credentials()
    if not creds:
        return None
    return build('drive', 'v3', credentials=creds)


def is_drive_configured():
    """Check if Google Drive backup is properly configured"""
    return get_stored_credentials() is not None


def get_or_create_backup_folder(service):
    """Get or create the backup folder in Google Drive"""
    # Search for existing folder
    query = f"name='{BACKUP_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()
    files = results.get('files', [])

    if files:
        return files[0]['id']

    # Create folder if not found
    folder_metadata = {
        'name': BACKUP_FOLDER_NAME,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']


def upload_backup_to_drive():
    """Upload current database to Google Drive"""
    if not os.path.exists(DATABASE_FILE):
        return {'success': False, 'error': 'Database file not found'}

    service = get_drive_service()
    if not service:
        return {'success': False, 'error': 'Google Drive not configured. Admin needs to authorize Drive backup first.'}

    try:
        folder_id = get_or_create_backup_folder(service)

        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f'attendance_backup_{timestamp}.db'

        # Upload file
        file_metadata = {
            'name': backup_name,
            'parents': [folder_id]
        }
        media = MediaFileUpload(DATABASE_FILE, mimetype='application/x-sqlite3')

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, createdTime, size'
        ).execute()

        return {
            'success': True,
            'file': {
                'id': file['id'],
                'name': file['name'],
                'createdTime': file.get('createdTime'),
                'size': file.get('size')
            }
        }
    except HttpError as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def list_drive_backups():
    """List all backups from Google Drive"""
    service = get_drive_service()
    if not service:
        return {'success': False, 'error': 'Google Drive not configured', 'backups': []}

    try:
        folder_id = get_or_create_backup_folder(service)

        # List files in backup folder
        query = f"'{folder_id}' in parents and trashed=false and name contains 'attendance_backup_'"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, createdTime, size)',
            orderBy='createdTime desc'
        ).execute()

        files = results.get('files', [])

        # Parse timestamps from filenames
        backups = []
        for f in files:
            # Extract timestamp from filename
            name = f['name']
            timestamp_str = name.replace('attendance_backup_', '').replace('.db', '')
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                backups.append({
                    'id': f['id'],
                    'filename': f['name'],
                    'timestamp': timestamp.isoformat(),
                    'size': int(f.get('size', 0)),
                    'source': 'drive'
                })
            except ValueError:
                # If timestamp parsing fails, use createdTime
                backups.append({
                    'id': f['id'],
                    'filename': f['name'],
                    'timestamp': f.get('createdTime', ''),
                    'size': int(f.get('size', 0)),
                    'source': 'drive'
                })

        return {'success': True, 'backups': backups}
    except HttpError as e:
        return {'success': False, 'error': str(e), 'backups': []}
    except Exception as e:
        return {'success': False, 'error': str(e), 'backups': []}


def download_backup_from_drive(file_id):
    """Download a backup file from Google Drive"""
    service = get_drive_service()
    if not service:
        return {'success': False, 'error': 'Google Drive not configured'}

    try:
        # Get file metadata first
        file_metadata = service.files().get(fileId=file_id, fields='name').execute()

        # Download file content
        request = service.files().get_media(fileId=file_id)
        content = io.BytesIO()
        downloader = MediaIoBaseDownload(content, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        content.seek(0)
        return {
            'success': True,
            'filename': file_metadata['name'],
            'content': content.read()
        }
    except HttpError as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def restore_from_drive(file_id):
    """Restore database from a Google Drive backup"""
    import database as db

    # First, create a local backup of current state
    db.create_backup()

    # Download the backup from Drive
    result = download_backup_from_drive(file_id)
    if not result['success']:
        return result

    try:
        # Write the downloaded content to the database file
        with open(DATABASE_FILE, 'wb') as f:
            f.write(result['content'])

        return {
            'success': True,
            'message': f"Restored from {result['filename']}"
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}


def delete_drive_backup(file_id):
    """Delete a backup from Google Drive"""
    service = get_drive_service()
    if not service:
        return {'success': False, 'error': 'Google Drive not configured'}

    try:
        service.files().delete(fileId=file_id).execute()
        return {'success': True}
    except HttpError as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}
