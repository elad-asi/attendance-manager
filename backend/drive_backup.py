"""
Google Drive Backup Module
Handles uploading and downloading database backups to/from Google Drive
"""
import os
import io
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Backup folder name in Google Drive
BACKUP_FOLDER_NAME = 'AttendanceManager_Backups'
DATABASE_FILE = 'data/attendance.db'


def get_drive_service(access_token):
    """Create a Google Drive service instance"""
    creds = Credentials(token=access_token)
    return build('drive', 'v3', credentials=creds)


def get_or_create_backup_folder(service):
    """Get or create the backup folder in Google Drive"""
    # Search for existing folder
    query = f"name='{BACKUP_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = results.get('files', [])

    if files:
        return files[0]['id']

    # Create folder if not exists
    folder_metadata = {
        'name': BACKUP_FOLDER_NAME,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = service.files().create(body=folder_metadata, fields='id').execute()
    return folder['id']


def upload_backup_to_drive(access_token):
    """Upload current database to Google Drive"""
    if not os.path.exists(DATABASE_FILE):
        return {'success': False, 'error': 'Database file not found'}

    try:
        service = get_drive_service(access_token)
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


def list_drive_backups(access_token):
    """List all backups from Google Drive"""
    try:
        service = get_drive_service(access_token)
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


def download_backup_from_drive(access_token, file_id):
    """Download a backup file from Google Drive"""
    try:
        service = get_drive_service(access_token)

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


def restore_from_drive(access_token, file_id):
    """Restore database from a Google Drive backup"""
    import shutil
    import database as db

    # First, create a local backup of current state
    db.create_backup()

    # Download the backup from Drive
    result = download_backup_from_drive(access_token, file_id)
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


def delete_drive_backup(access_token, file_id):
    """Delete a backup from Google Drive"""
    try:
        service = get_drive_service(access_token)
        service.files().delete(fileId=file_id).execute()
        return {'success': True}
    except HttpError as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:
        return {'success': False, 'error': str(e)}
