"""
Dropbox Backup Module
Handles uploading and downloading database backups to/from Dropbox
Uses a simple access token for authentication
"""
import os
import json
import requests
from datetime import datetime

# Backup folder path in Dropbox (empty for App folder root)
BACKUP_FOLDER = ''
DATABASE_FILE = 'data/attendance.db'
TOKEN_FILE = 'data/dropbox_token.json'

# Dropbox API endpoints
DROPBOX_API_URL = 'https://api.dropboxapi.com/2'
DROPBOX_CONTENT_URL = 'https://content.dropboxapi.com/2'


def get_access_token():
    """Get Dropbox access token from environment or file"""
    # Try environment variable first (for production/Render)
    token = os.environ.get('DROPBOX_ACCESS_TOKEN')
    if token:
        return token

    # Try loading from file (for local development)
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                return data.get('access_token')
        except Exception as e:
            print(f"Error loading Dropbox token: {e}")

    return None


def save_access_token(token):
    """Save Dropbox access token to file for local development"""
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, 'w') as f:
        json.dump({'access_token': token}, f)


def is_dropbox_configured():
    """Check if Dropbox backup is properly configured"""
    return get_access_token() is not None


def _get_headers():
    """Get authorization headers for Dropbox API"""
    token = get_access_token()
    return {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }


def upload_backup_to_dropbox():
    """Upload current database to Dropbox"""
    if not os.path.exists(DATABASE_FILE):
        return {'success': False, 'error': 'Database file not found'}

    token = get_access_token()
    if not token:
        return {'success': False, 'error': 'Dropbox not configured. Set DROPBOX_ACCESS_TOKEN environment variable.'}

    try:
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f'attendance_backup_{timestamp}.db'
        dropbox_path = f'/{backup_name}'  # Root of app folder

        # Read database file
        with open(DATABASE_FILE, 'rb') as f:
            file_content = f.read()

        # Upload to Dropbox
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/octet-stream',
            'Dropbox-API-Arg': json.dumps({
                'path': dropbox_path,
                'mode': 'add',
                'autorename': True,
                'mute': False
            })
        }

        response = requests.post(
            f'{DROPBOX_CONTENT_URL}/files/upload',
            headers=headers,
            data=file_content
        )

        if response.status_code == 200:
            result = response.json()
            return {
                'success': True,
                'file': {
                    'id': result.get('id'),
                    'name': result.get('name'),
                    'path': result.get('path_display'),
                    'size': result.get('size')
                }
            }
        else:
            try:
                error_data = response.json()
                error_msg = error_data.get('error_summary', str(error_data))
            except:
                error_msg = f'HTTP {response.status_code}: {response.text}'
            return {'success': False, 'error': error_msg}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def list_dropbox_backups():
    """List all backups from Dropbox"""
    token = get_access_token()
    if not token:
        return {'success': False, 'error': 'Dropbox not configured', 'backups': []}

    try:
        headers = _get_headers()

        # List files in backup folder
        response = requests.post(
            f'{DROPBOX_API_URL}/files/list_folder',
            headers=headers,
            json={
                'path': BACKUP_FOLDER,
                'recursive': False,
                'include_deleted': False
            }
        )

        if response.status_code == 200:
            result = response.json()
            entries = result.get('entries', [])

            backups = []
            for entry in entries:
                if entry.get('.tag') == 'file' and entry.get('name', '').startswith('attendance_backup_'):
                    name = entry['name']
                    # Extract timestamp from filename
                    timestamp_str = name.replace('attendance_backup_', '').replace('.db', '')
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                        backups.append({
                            'id': entry.get('id'),
                            'filename': name,
                            'path': entry.get('path_display'),
                            'timestamp': timestamp.isoformat(),
                            'size': entry.get('size', 0),
                            'source': 'dropbox'
                        })
                    except ValueError:
                        # Use server_modified if timestamp parsing fails
                        backups.append({
                            'id': entry.get('id'),
                            'filename': name,
                            'path': entry.get('path_display'),
                            'timestamp': entry.get('server_modified', ''),
                            'size': entry.get('size', 0),
                            'source': 'dropbox'
                        })

            # Sort by timestamp descending
            backups.sort(key=lambda x: x['timestamp'], reverse=True)
            return {'success': True, 'backups': backups}

        elif response.status_code == 409:
            # Folder doesn't exist yet - that's ok, just no backups
            error = response.json().get('error', {})
            if 'path' in error and 'not_found' in str(error):
                return {'success': True, 'backups': []}
            return {'success': False, 'error': response.json().get('error_summary', 'Unknown error'), 'backups': []}
        else:
            return {'success': False, 'error': response.json().get('error_summary', response.text), 'backups': []}

    except Exception as e:
        return {'success': False, 'error': str(e), 'backups': []}


def download_backup_from_dropbox(file_path):
    """Download a backup file from Dropbox"""
    token = get_access_token()
    if not token:
        return {'success': False, 'error': 'Dropbox not configured'}

    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Dropbox-API-Arg': json.dumps({'path': file_path})
        }

        response = requests.post(
            f'{DROPBOX_CONTENT_URL}/files/download',
            headers=headers
        )

        if response.status_code == 200:
            # Get filename from response header
            api_result = json.loads(response.headers.get('Dropbox-API-Result', '{}'))
            return {
                'success': True,
                'filename': api_result.get('name', 'backup.db'),
                'content': response.content
            }
        else:
            return {'success': False, 'error': response.text}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def restore_from_dropbox(file_path):
    """Restore database from a Dropbox backup"""
    import database as db

    # First, create a local backup of current state
    db.create_backup()

    # Download the backup from Dropbox
    result = download_backup_from_dropbox(file_path)
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


def delete_dropbox_backup(file_path):
    """Delete a backup from Dropbox"""
    token = get_access_token()
    if not token:
        return {'success': False, 'error': 'Dropbox not configured'}

    try:
        headers = _get_headers()

        response = requests.post(
            f'{DROPBOX_API_URL}/files/delete_v2',
            headers=headers,
            json={'path': file_path}
        )

        if response.status_code == 200:
            return {'success': True}
        else:
            return {'success': False, 'error': response.json().get('error_summary', response.text)}

    except Exception as e:
        return {'success': False, 'error': str(e)}
