"""
Cloud Backup Module using JSONBin.io
Free JSON storage service - no OAuth required, just an API key
Free tier: 10,000 requests/month, 100KB per bin
Uses zlib compression to fit larger databases
"""
import os
import json
import base64
import zlib
import requests
from datetime import datetime

# Configuration
DATABASE_FILE = 'data/attendance.db'
TOKEN_FILE = 'data/cloud_token.json'
INDEX_FILE = 'data/cloud_index.json'

# JSONBin.io API
JSONBIN_API_URL = 'https://api.jsonbin.io/v3'

# Master index bin ID (stores list of all backup bin IDs)
# Set JSONBIN_INDEX_ID env var or it will be created on first upload
INDEX_BIN_ID_FILE = 'data/cloud_index_bin_id.txt'


def get_api_key():
    """Get JSONBin API key from environment or file"""
    # Try environment variable first (for production/Render)
    key = os.environ.get('JSONBIN_API_KEY')
    if key:
        return key

    # Try loading from file (for local development)
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                data = json.load(f)
                return data.get('api_key')
        except Exception as e:
            print(f"Error loading cloud token: {e}")

    return None


def save_api_key(api_key):
    """Save API key to file for local development"""
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, 'w') as f:
        json.dump({'api_key': api_key}, f)


def is_cloud_configured():
    """Check if cloud backup is properly configured"""
    return get_api_key() is not None


def _get_headers():
    """Get authorization headers for JSONBin API"""
    api_key = get_api_key()
    return {
        'X-Master-Key': api_key,
        'Content-Type': 'application/json'
    }


def _get_index_bin_id():
    """Get the master index bin ID from env or file"""
    # Try environment variable first
    bin_id = os.environ.get('JSONBIN_INDEX_ID')
    if bin_id:
        return bin_id

    # Try loading from file
    if os.path.exists(INDEX_BIN_ID_FILE):
        try:
            with open(INDEX_BIN_ID_FILE, 'r') as f:
                return f.read().strip()
        except:
            pass

    return None


def _save_index_bin_id(bin_id):
    """Save the index bin ID to file"""
    os.makedirs(os.path.dirname(INDEX_BIN_ID_FILE), exist_ok=True)
    with open(INDEX_BIN_ID_FILE, 'w') as f:
        f.write(bin_id)


def _load_cloud_index():
    """Load the backup index from JSONBin"""
    index_bin_id = _get_index_bin_id()
    if not index_bin_id:
        return {'backups': []}

    try:
        headers = _get_headers()
        response = requests.get(
            f'{JSONBIN_API_URL}/b/{index_bin_id}/latest',
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            return result.get('record', {'backups': []})
        else:
            return {'backups': []}
    except:
        return {'backups': []}


def _save_cloud_index(index):
    """Save the backup index to JSONBin"""
    headers = _get_headers()
    index_bin_id = _get_index_bin_id()

    if index_bin_id:
        # Update existing index bin
        response = requests.put(
            f'{JSONBIN_API_URL}/b/{index_bin_id}',
            headers=headers,
            json=index
        )
        return response.status_code == 200
    else:
        # Create new index bin
        headers['X-Bin-Name'] = 'attendance_backup_index'
        response = requests.post(
            f'{JSONBIN_API_URL}/b',
            headers=headers,
            json=index
        )
        if response.status_code in [200, 201]:
            result = response.json()
            new_bin_id = result.get('metadata', {}).get('id')
            if new_bin_id:
                _save_index_bin_id(new_bin_id)
                print(f"Created new index bin: {new_bin_id}")
                print(f"Add JSONBIN_INDEX_ID={new_bin_id} to Render environment variables!")
                return True
        return False


def _load_backup_index():
    """Load the index of all backups (from cloud or local cache)"""
    # Try cloud first
    cloud_index = _load_cloud_index()
    if cloud_index.get('backups'):
        # Cache locally
        _save_backup_index(cloud_index)
        return cloud_index

    # Fall back to local cache
    if os.path.exists(INDEX_FILE):
        try:
            with open(INDEX_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {'backups': []}


def _save_backup_index(index):
    """Save the backup index locally and to cloud"""
    os.makedirs(os.path.dirname(INDEX_FILE), exist_ok=True)
    with open(INDEX_FILE, 'w') as f:
        json.dump(index, f, indent=2)

    # Also save to cloud
    _save_cloud_index(index)


def _get_sheets_info():
    """Get list of sheets from the database"""
    import sqlite3
    sheets = []
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT id, name FROM sheets')
        for row in cursor.fetchall():
            sheets.append({
                'id': row['id'],
                'name': row['name']
            })
        conn.close()
    except:
        pass
    return sheets


def upload_backup_to_cloud():
    """Upload current database to JSONBin as base64-encoded JSON"""
    if not os.path.exists(DATABASE_FILE):
        return {'success': False, 'error': 'Database file not found'}

    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured. Set JSONBIN_API_KEY.'}

    try:
        # Get sheets info before backup
        sheets_info = _get_sheets_info()

        # Read and compress database file
        with open(DATABASE_FILE, 'rb') as f:
            db_content = f.read()

        # Compress with zlib for smaller size
        compressed = zlib.compress(db_content, 9)

        # Create backup data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f'attendance_backup_{timestamp}'

        backup_data = {
            'name': backup_name,
            'timestamp': datetime.now().isoformat(),
            'size': len(db_content),
            'compressed': True,
            'sheets': sheets_info,  # Include sheets info in backup
            'data': base64.b64encode(compressed).decode('utf-8')
        }

        # Upload to JSONBin
        headers = _get_headers()
        headers['X-Bin-Name'] = backup_name

        response = requests.post(
            f'{JSONBIN_API_URL}/b',
            headers=headers,
            json=backup_data
        )

        if response.status_code in [200, 201]:
            result = response.json()
            bin_id = result.get('metadata', {}).get('id')

            # Update local index
            index = _load_backup_index()
            index['backups'].append({
                'id': bin_id,
                'name': backup_name,
                'timestamp': backup_data['timestamp'],
                'size': backup_data['size'],
                'sheets': sheets_info  # Include sheets info in index
            })
            _save_backup_index(index)

            return {
                'success': True,
                'file': {
                    'id': bin_id,
                    'name': backup_name,
                    'size': backup_data['size']
                }
            }
        else:
            try:
                error_msg = response.json().get('message', response.text)
            except:
                error_msg = f'HTTP {response.status_code}: {response.text}'
            return {'success': False, 'error': error_msg}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def list_cloud_backups(filter_sheet_id=None):
    """List backups from the cloud index, optionally filtered by sheet_id

    Args:
        filter_sheet_id: If provided, only return backups that contain this sheet_id
    """
    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured', 'backups': []}

    try:
        # Load index from cloud (or local cache)
        index = _load_backup_index()
        backups = []

        for backup in index.get('backups', []):
            # Filter by sheet_id if provided
            if filter_sheet_id is not None:
                backup_sheets = backup.get('sheets', [])
                # Check if this backup contains the requested sheet
                sheet_ids = [s.get('id') for s in backup_sheets]
                if filter_sheet_id not in sheet_ids:
                    continue  # Skip backups that don't include this sheet

            backups.append({
                'id': backup['id'],
                'filename': backup['name'],
                'path': backup['id'],  # Use ID as path for consistency
                'timestamp': backup['timestamp'],
                'size': backup.get('size', 0),
                'sheets': backup.get('sheets', []),  # Include sheets info
                'source': 'cloud'
            })

        # Sort by timestamp descending
        backups.sort(key=lambda x: x['timestamp'], reverse=True)
        return {'success': True, 'backups': backups}

    except Exception as e:
        return {'success': False, 'error': str(e), 'backups': []}


def download_backup_from_cloud(bin_id):
    """Download a backup from JSONBin"""
    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured'}

    try:
        headers = _get_headers()

        response = requests.get(
            f'{JSONBIN_API_URL}/b/{bin_id}/latest',
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            record = result.get('record', {})

            # Decode base64 data
            encoded_data = base64.b64decode(record.get('data', ''))

            # Decompress if compressed
            if record.get('compressed', False):
                db_content = zlib.decompress(encoded_data)
            else:
                db_content = encoded_data

            return {
                'success': True,
                'filename': record.get('name', 'backup.db'),
                'content': db_content
            }
        else:
            try:
                error_msg = response.json().get('message', response.text)
            except:
                error_msg = f'HTTP {response.status_code}: {response.text}'
            return {'success': False, 'error': error_msg}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def restore_from_cloud(bin_id):
    """Restore database from a cloud backup"""
    import database as db

    # First, create a local backup of current state
    db.create_backup()

    # Download the backup from cloud
    result = download_backup_from_cloud(bin_id)
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


def compare_with_cloud(bin_id, sheet_id=None):
    """Compare current database with a cloud backup and return differences"""
    import tempfile
    import sqlite3

    # Download the backup
    result = download_backup_from_cloud(bin_id)
    if not result['success']:
        return result

    try:
        # Write backup to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
            tmp.write(result['content'])
            tmp_path = tmp.name

        # Connect to both databases
        current_conn = sqlite3.connect(DATABASE_FILE)
        current_conn.row_factory = sqlite3.Row
        backup_conn = sqlite3.connect(tmp_path)
        backup_conn.row_factory = sqlite3.Row

        differences = {
            'attendance_changes': [],
            'members_added': [],
            'members_removed': [],
            'summary': {}
        }

        # Get sheets to compare
        if sheet_id:
            sheet_ids = [sheet_id]
        else:
            # Get all sheet IDs from current DB
            cursor = current_conn.cursor()
            cursor.execute('SELECT id FROM sheets')
            sheet_ids = [row['id'] for row in cursor.fetchall()]

        for sid in sheet_ids:
            # Compare attendance data
            current_cursor = current_conn.cursor()
            backup_cursor = backup_conn.cursor()

            # Get current attendance
            current_cursor.execute('''
                SELECT a.ma, a.date, a.status, t.first_name, t.last_name
                FROM attendance a
                LEFT JOIN team_members t ON a.sheet_id = t.sheet_id AND a.ma = t.ma
                WHERE a.sheet_id = ?
            ''', (sid,))
            current_attendance = {(row['ma'], row['date']): {
                'status': row['status'],
                'name': f"{row['first_name'] or ''} {row['last_name'] or ''}".strip()
            } for row in current_cursor.fetchall()}

            # Get backup attendance
            try:
                backup_cursor.execute('''
                    SELECT a.ma, a.date, a.status, t.first_name, t.last_name
                    FROM attendance a
                    LEFT JOIN team_members t ON a.sheet_id = t.sheet_id AND a.ma = t.ma
                    WHERE a.sheet_id = ?
                ''', (sid,))
                backup_attendance = {(row['ma'], row['date']): {
                    'status': row['status'],
                    'name': f"{row['first_name'] or ''} {row['last_name'] or ''}".strip()
                } for row in backup_cursor.fetchall()}
            except:
                backup_attendance = {}

            # Find differences
            all_keys = set(current_attendance.keys()) | set(backup_attendance.keys())

            for key in all_keys:
                ma, date = key
                current_data = current_attendance.get(key)
                backup_data = backup_attendance.get(key)

                if current_data and backup_data:
                    if current_data['status'] != backup_data['status']:
                        differences['attendance_changes'].append({
                            'ma': ma,
                            'date': date,
                            'name': current_data['name'] or backup_data['name'],
                            'current_status': current_data['status'],
                            'backup_status': backup_data['status']
                        })
                elif current_data and not backup_data:
                    differences['attendance_changes'].append({
                        'ma': ma,
                        'date': date,
                        'name': current_data['name'],
                        'current_status': current_data['status'],
                        'backup_status': None,
                        'type': 'added'
                    })
                elif backup_data and not current_data:
                    differences['attendance_changes'].append({
                        'ma': ma,
                        'date': date,
                        'name': backup_data['name'],
                        'current_status': None,
                        'backup_status': backup_data['status'],
                        'type': 'removed'
                    })

            # Compare team members
            current_cursor.execute('SELECT ma, first_name, last_name FROM team_members WHERE sheet_id = ?', (sid,))
            current_members = {row['ma']: f"{row['first_name']} {row['last_name']}" for row in current_cursor.fetchall()}

            try:
                backup_cursor.execute('SELECT ma, first_name, last_name FROM team_members WHERE sheet_id = ?', (sid,))
                backup_members = {row['ma']: f"{row['first_name']} {row['last_name']}" for row in backup_cursor.fetchall()}
            except:
                backup_members = {}

            # Members in current but not backup
            for ma in set(current_members.keys()) - set(backup_members.keys()):
                differences['members_added'].append({'ma': ma, 'name': current_members[ma]})

            # Members in backup but not current
            for ma in set(backup_members.keys()) - set(current_members.keys()):
                differences['members_removed'].append({'ma': ma, 'name': backup_members[ma]})

        # Summary
        differences['summary'] = {
            'attendance_changed': len([d for d in differences['attendance_changes'] if 'type' not in d]),
            'attendance_added': len([d for d in differences['attendance_changes'] if d.get('type') == 'added']),
            'attendance_removed': len([d for d in differences['attendance_changes'] if d.get('type') == 'removed']),
            'members_added': len(differences['members_added']),
            'members_removed': len(differences['members_removed'])
        }

        current_conn.close()
        backup_conn.close()

        # Clean up temp file
        os.remove(tmp_path)

        return {
            'success': True,
            'differences': differences,
            'has_changes': any([
                differences['summary']['attendance_changed'],
                differences['summary']['attendance_added'],
                differences['summary']['attendance_removed'],
                differences['summary']['members_added'],
                differences['summary']['members_removed']
            ])
        }

    except Exception as e:
        return {'success': False, 'error': str(e)}


def delete_cloud_backup(bin_id):
    """Delete a backup from JSONBin and local index"""
    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured'}

    try:
        headers = _get_headers()

        response = requests.delete(
            f'{JSONBIN_API_URL}/b/{bin_id}',
            headers=headers
        )

        # Remove from local index regardless of API response
        index = _load_backup_index()
        index['backups'] = [b for b in index['backups'] if b['id'] != bin_id]
        _save_backup_index(index)

        if response.status_code == 200:
            return {'success': True}
        else:
            # Still return success since we removed from index
            return {'success': True}

    except Exception as e:
        return {'success': False, 'error': str(e)}
