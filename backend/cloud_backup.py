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
import hashlib
import requests
from datetime import datetime

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configuration (paths relative to script directory)
DATABASE_FILE = os.path.join(SCRIPT_DIR, 'data/attendance.db')
TOKEN_FILE = os.path.join(SCRIPT_DIR, 'data/cloud_token.json')
INDEX_FILE = os.path.join(SCRIPT_DIR, 'data/cloud_index.json')

# JSONBin.io API
JSONBIN_API_URL = 'https://api.jsonbin.io/v3'

# Global master index bin ID - maps spreadsheet_id to index_bin_id
# This must be shared across all machines (set via JSONBIN_MASTER_INDEX_ID env var)
MASTER_INDEX_BIN_ID_FILE = os.path.join(SCRIPT_DIR, 'data/master_index_bin_id.txt')

# Local cache of spreadsheet-specific index bin IDs
INDEX_BIN_CACHE_FILE = os.path.join(SCRIPT_DIR, 'data/index_bin_cache.json')

# Legacy: old index bin ID file (for backwards compatibility)
LEGACY_INDEX_BIN_ID_FILE = os.path.join(SCRIPT_DIR, 'data/cloud_index_bin_id.txt')

# Maximum number of backups to keep in cloud
MAX_CLOUD_BACKUPS = 5

# Hardcoded fallback index bin ID for cross-machine sync
# This is used when no local cache exists and no env var is set
HARDCODED_INDEX_BIN_ID = '693bbf3a43b1c97be9e89bd6'

# Hardcoded API key for JSONBin.io (fallback if env var not set)
HARDCODED_API_KEY = '$2a$10$ZzyHz/Jk0nNhtikSlY7TpunjyhHL6mF6YwDl141GW4yjhMDiN6Rqa'


def _compute_db_hash():
    """Compute SHA256 hash of the database file content"""
    if not os.path.exists(DATABASE_FILE):
        return None
    with open(DATABASE_FILE, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


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

    # Fallback to hardcoded API key
    return HARDCODED_API_KEY


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


def _get_master_index_bin_id():
    """Get the global master index bin ID from env or file"""
    # Try environment variable first (required for cross-machine sync)
    bin_id = os.environ.get('JSONBIN_MASTER_INDEX_ID')
    if bin_id:
        return bin_id

    # Try loading from file (local fallback)
    if os.path.exists(MASTER_INDEX_BIN_ID_FILE):
        try:
            with open(MASTER_INDEX_BIN_ID_FILE, 'r') as f:
                return f.read().strip()
        except:
            pass

    return None


def _save_master_index_bin_id(bin_id):
    """Save the master index bin ID to file"""
    os.makedirs(os.path.dirname(MASTER_INDEX_BIN_ID_FILE), exist_ok=True)
    with open(MASTER_INDEX_BIN_ID_FILE, 'w') as f:
        f.write(bin_id)


def _load_index_bin_cache():
    """Load local cache of spreadsheet_id -> index_bin_id mappings"""
    if os.path.exists(INDEX_BIN_CACHE_FILE):
        try:
            with open(INDEX_BIN_CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}


def _save_index_bin_cache(cache):
    """Save local cache of spreadsheet_id -> index_bin_id mappings"""
    os.makedirs(os.path.dirname(INDEX_BIN_CACHE_FILE), exist_ok=True)
    with open(INDEX_BIN_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def _load_master_index():
    """Load the global master index from JSONBin (maps spreadsheet_id -> index_bin_id)"""
    master_bin_id = _get_master_index_bin_id()
    if not master_bin_id:
        return {'spreadsheets': {}}

    try:
        headers = _get_headers()
        response = requests.get(
            f'{JSONBIN_API_URL}/b/{master_bin_id}/latest',
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            return result.get('record', {'spreadsheets': {}})
        else:
            return {'spreadsheets': {}}
    except:
        return {'spreadsheets': {}}


def _save_master_index(master_index):
    """Save the global master index to JSONBin"""
    headers = _get_headers()
    master_bin_id = _get_master_index_bin_id()

    if master_bin_id:
        # Update existing master index bin
        response = requests.put(
            f'{JSONBIN_API_URL}/b/{master_bin_id}',
            headers=headers,
            json=master_index
        )
        return response.status_code == 200
    else:
        # Create new master index bin
        headers['X-Bin-Name'] = 'attendance_master_index'
        response = requests.post(
            f'{JSONBIN_API_URL}/b',
            headers=headers,
            json=master_index
        )
        if response.status_code in [200, 201]:
            result = response.json()
            new_bin_id = result.get('metadata', {}).get('id')
            if new_bin_id:
                _save_master_index_bin_id(new_bin_id)
                print(f"Created new master index bin: {new_bin_id}")
                print(f"IMPORTANT: Set JSONBIN_MASTER_INDEX_ID={new_bin_id} on all machines!")
                return True
        return False


def _get_legacy_index_bin_id():
    """Get the legacy index bin ID from old file (backwards compatibility)"""
    if os.path.exists(LEGACY_INDEX_BIN_ID_FILE):
        try:
            with open(LEGACY_INDEX_BIN_ID_FILE, 'r') as f:
                return f.read().strip()
        except:
            pass
    return None


def _get_index_bin_id_for_spreadsheet(spreadsheet_id):
    """Get the index bin ID for a specific spreadsheet_id from master index"""
    # First check local cache
    cache = _load_index_bin_cache()
    if spreadsheet_id in cache:
        return cache[spreadsheet_id]

    # Check for direct index bin ID from environment variable (for cross-machine sync)
    # This allows Render to directly access the index bin without needing the master index
    direct_index_bin_id = os.environ.get('JSONBIN_INDEX_BIN_ID')
    if direct_index_bin_id:
        print(f"Using direct index bin ID from env: {direct_index_bin_id[:8]}...")
        # Cache it locally
        cache[spreadsheet_id] = direct_index_bin_id
        _save_index_bin_cache(cache)
        return direct_index_bin_id

    # Load from master index in cloud
    master_index = _load_master_index()
    index_bin_id = master_index.get('spreadsheets', {}).get(spreadsheet_id)

    # If not found in master index, try legacy index (backwards compatibility)
    if not index_bin_id:
        legacy_id = _get_legacy_index_bin_id()
        if legacy_id:
            # Use legacy index for this spreadsheet
            index_bin_id = legacy_id
            # Cache it locally
            cache[spreadsheet_id] = index_bin_id
            _save_index_bin_cache(cache)
            print(f"Using legacy index bin {legacy_id} for spreadsheet {spreadsheet_id[:8]}")

    # Cache locally if found
    if index_bin_id and spreadsheet_id not in cache:
        cache[spreadsheet_id] = index_bin_id
        _save_index_bin_cache(cache)

    return index_bin_id


def _register_index_bin_for_spreadsheet(spreadsheet_id, index_bin_id):
    """Register a new index bin ID for a spreadsheet in the master index"""
    master_index = _load_master_index()
    master_index['spreadsheets'][spreadsheet_id] = index_bin_id
    _save_master_index(master_index)

    # Update local cache
    cache = _load_index_bin_cache()
    cache[spreadsheet_id] = index_bin_id
    _save_index_bin_cache(cache)


def _load_cloud_index_for_spreadsheet(spreadsheet_id):
    """Load the backup index for a specific spreadsheet from JSONBin"""
    index_bin_id = _get_index_bin_id_for_spreadsheet(spreadsheet_id)
    if not index_bin_id:
        return {'backups': [], 'spreadsheet_id': spreadsheet_id}

    try:
        headers = _get_headers()
        response = requests.get(
            f'{JSONBIN_API_URL}/b/{index_bin_id}/latest',
            headers=headers
        )

        if response.status_code == 200:
            result = response.json()
            index = result.get('record', {'backups': []})
            index['spreadsheet_id'] = spreadsheet_id
            return index
        else:
            return {'backups': [], 'spreadsheet_id': spreadsheet_id}
    except:
        return {'backups': [], 'spreadsheet_id': spreadsheet_id}


def _save_cloud_index_for_spreadsheet(spreadsheet_id, index):
    """Save the backup index for a specific spreadsheet to JSONBin"""
    headers = _get_headers()
    index_bin_id = _get_index_bin_id_for_spreadsheet(spreadsheet_id)

    if index_bin_id:
        # Update existing index bin
        response = requests.put(
            f'{JSONBIN_API_URL}/b/{index_bin_id}',
            headers=headers,
            json=index
        )
        return response.status_code == 200
    else:
        # Create new index bin for this spreadsheet
        headers['X-Bin-Name'] = f'attendance_index_{spreadsheet_id[:8]}'
        response = requests.post(
            f'{JSONBIN_API_URL}/b',
            headers=headers,
            json=index
        )
        if response.status_code in [200, 201]:
            result = response.json()
            new_bin_id = result.get('metadata', {}).get('id')
            if new_bin_id:
                # Register this index bin in the master index
                _register_index_bin_for_spreadsheet(spreadsheet_id, new_bin_id)
                print(f"Created new index bin for spreadsheet {spreadsheet_id[:8]}: {new_bin_id}")
                return True
        return False


def _load_backup_index_for_spreadsheet(spreadsheet_id):
    """Load the index of backups for a specific spreadsheet (from cloud or local cache)"""
    # Try cloud first
    cloud_index = _load_cloud_index_for_spreadsheet(spreadsheet_id)
    if cloud_index.get('backups'):
        return cloud_index

    return {'backups': [], 'spreadsheet_id': spreadsheet_id}


def _load_cloud_index_direct(index_bin_id):
    """Load a cloud index directly by bin ID (for use when spreadsheet_id is unknown)"""
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


def _save_backup_index_for_spreadsheet(spreadsheet_id, index):
    """Save the backup index for a specific spreadsheet to cloud"""
    index['spreadsheet_id'] = spreadsheet_id
    _save_cloud_index_for_spreadsheet(spreadsheet_id, index)


def _load_backup_index():
    """Load the combined index of all backups for all spreadsheets in this database"""
    spreadsheet_ids = _get_all_spreadsheet_ids()
    if not spreadsheet_ids:
        return {'backups': []}

    all_backups = []
    for spreadsheet_id in spreadsheet_ids:
        index = _load_backup_index_for_spreadsheet(spreadsheet_id)
        all_backups.extend(index.get('backups', []))

    # Sort by timestamp descending
    all_backups.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
    return {'backups': all_backups}


def _cleanup_old_backups(index):
    """Keep backups according to retention policy:
    - Last 3 backups from today
    - Last backup from each of the past 5 days (closest to 23:00)
    Deletes older backups from JSONBin to save space.
    """
    backups = index.get('backups', [])
    if not backups:
        return index

    # Sort by timestamp descending (newest first)
    backups.sort(key=lambda x: x['timestamp'], reverse=True)

    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')

    # Group backups by date
    by_date = {}
    for backup in backups:
        date = backup['timestamp'][:10]  # Extract YYYY-MM-DD
        if date not in by_date:
            by_date[date] = []
        by_date[date].append(backup)

    backups_to_keep = []
    backups_to_delete = []

    # Get sorted dates (newest first)
    sorted_dates = sorted(by_date.keys(), reverse=True)

    for i, date in enumerate(sorted_dates):
        day_backups = by_date[date]
        # Sort day's backups by timestamp descending
        day_backups.sort(key=lambda x: x['timestamp'], reverse=True)

        if date == today:
            # Today: keep last 3 backups
            backups_to_keep.extend(day_backups[:3])
            backups_to_delete.extend(day_backups[3:])
        elif i <= 5:  # Past 5 days (i=0 is today, so i=1 to i=5 are past 5 days)
            # Past days: keep only the backup closest to 23:00
            # Find backup closest to 23:00 (end of day)
            best_backup = None
            best_diff = float('inf')
            for backup in day_backups:
                # Extract time from timestamp
                time_str = backup['timestamp'][11:19]  # HH:MM:SS
                hours, minutes, seconds = map(int, time_str.split(':'))
                # Calculate difference from 23:00:00
                backup_minutes = hours * 60 + minutes
                target_minutes = 23 * 60  # 23:00
                diff = abs(target_minutes - backup_minutes)
                if diff < best_diff:
                    best_diff = diff
                    best_backup = backup

            if best_backup:
                backups_to_keep.append(best_backup)
                backups_to_delete.extend([b for b in day_backups if b != best_backup])
            else:
                backups_to_delete.extend(day_backups)
        else:
            # Older than 5 days: delete all
            backups_to_delete.extend(day_backups)

    # Delete old backups from JSONBin
    headers = _get_headers()
    for backup in backups_to_delete:
        try:
            requests.delete(
                f'{JSONBIN_API_URL}/b/{backup["id"]}',
                headers=headers
            )
            print(f"Deleted old backup: {backup['name']}")
        except Exception as e:
            print(f"Failed to delete backup {backup['id']}: {e}")

    index['backups'] = backups_to_keep
    return index


def _get_sheets_info():
    """Get list of sheets from the database including spreadsheet_id"""
    import sqlite3
    sheets = []
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT id, spreadsheet_id, sheet_name FROM sheets')
        for row in cursor.fetchall():
            sheets.append({
                'id': row['id'],
                'spreadsheet_id': row['spreadsheet_id'],
                'name': row['sheet_name']
            })
        conn.close()
    except Exception as e:
        print(f"Error getting sheets info: {e}")
    return sheets


def _get_all_spreadsheet_ids():
    """Get all unique spreadsheet IDs from the database"""
    import sqlite3
    spreadsheet_ids = set()
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT spreadsheet_id FROM sheets')
        for row in cursor.fetchall():
            spreadsheet_ids.add(row[0])
        conn.close()
    except:
        pass
    return list(spreadsheet_ids)


def upload_backup_to_cloud(source='manual'):
    """Upload current database to JSONBin as base64-encoded JSON.
    Skips upload if data hasn't changed since last backup.
    Saves backup reference to each spreadsheet's index for cross-machine sync.

    Args:
        source: 'manual' for user-initiated backups, 'auto' for hourly auto-backups
    """
    if not os.path.exists(DATABASE_FILE):
        return {'success': False, 'error': 'Database file not found'}

    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured. Set JSONBIN_API_KEY.'}

    try:
        # Compute hash of current database
        current_hash = _compute_db_hash()

        # Get sheets info before backup
        sheets_info = _get_sheets_info()
        spreadsheet_ids = _get_all_spreadsheet_ids()

        if not spreadsheet_ids:
            return {'success': False, 'error': 'No spreadsheets found in database'}

        # Check if data has changed since last backup (check first spreadsheet's index)
        index = _load_backup_index_for_spreadsheet(spreadsheet_ids[0])
        if index.get('backups'):
            last_backup = max(index['backups'], key=lambda x: x['timestamp'])
            if last_backup.get('hash') == current_hash:
                print('Backup skipped: data unchanged since last backup')
                return {
                    'success': True,
                    'skipped': True,
                    'message': 'No changes since last backup'
                }

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
            'sheets': sheets_info,
            'spreadsheet_ids': spreadsheet_ids,
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

            # Create backup entry for index
            backup_entry = {
                'id': bin_id,
                'name': backup_name,
                'timestamp': backup_data['timestamp'],
                'size': backup_data['size'],
                'sheets': sheets_info,
                'spreadsheet_ids': spreadsheet_ids,
                'hash': current_hash,
                'source': source  # 'manual' or 'auto'
            }

            # Save to each spreadsheet's index (for cross-machine sync)
            for spreadsheet_id in spreadsheet_ids:
                ss_index = _load_backup_index_for_spreadsheet(spreadsheet_id)
                ss_index['backups'].append(backup_entry)
                # Cleanup: Keep only the latest backup per day, max 5 days
                ss_index = _cleanup_old_backups(ss_index)
                _save_backup_index_for_spreadsheet(spreadsheet_id, ss_index)

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


def list_cloud_backups(filter_sheet_id=None, spreadsheet_id=None):
    """List backups from the cloud index, optionally filtered by sheet_id or spreadsheet_id

    Args:
        filter_sheet_id: If provided, only return backups that contain this sheet_id (internal DB id)
        spreadsheet_id: If provided, load index for this Google spreadsheet ID (for cross-machine sync)
    """
    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured', 'backups': []}

    try:
        # Load index - if spreadsheet_id provided, use it; otherwise load for all spreadsheets
        if spreadsheet_id:
            index = _load_backup_index_for_spreadsheet(spreadsheet_id)
        else:
            # Try env var first, then hardcoded fallback, then local index
            direct_index_bin_id = os.environ.get('JSONBIN_INDEX_BIN_ID') or HARDCODED_INDEX_BIN_ID
            if direct_index_bin_id:
                print(f"Using index bin ID: {direct_index_bin_id[:8]}...")
                index = _load_cloud_index_direct(direct_index_bin_id)
            else:
                index = _load_backup_index()

        backups = []

        for backup in index.get('backups', []):
            # Filter by sheet_id if provided
            if filter_sheet_id is not None:
                backup_sheets = backup.get('sheets', [])
                # If sheets list is empty, show the backup (it's a full backup)
                # Otherwise check if this backup contains the requested sheet
                if backup_sheets:  # Only filter if we have sheet info
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
                'source': backup.get('source', 'manual')  # Use actual source from backup
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
    """Delete a backup from JSONBin and all spreadsheet indexes"""
    api_key = get_api_key()
    if not api_key:
        return {'success': False, 'error': 'Cloud backup not configured'}

    try:
        headers = _get_headers()

        response = requests.delete(
            f'{JSONBIN_API_URL}/b/{bin_id}',
            headers=headers
        )

        # Remove from all spreadsheet indexes
        spreadsheet_ids = _get_all_spreadsheet_ids()
        for spreadsheet_id in spreadsheet_ids:
            ss_index = _load_backup_index_for_spreadsheet(spreadsheet_id)
            original_count = len(ss_index.get('backups', []))
            ss_index['backups'] = [b for b in ss_index.get('backups', []) if b['id'] != bin_id]
            if len(ss_index['backups']) < original_count:
                _save_backup_index_for_spreadsheet(spreadsheet_id, ss_index)

        if response.status_code == 200:
            return {'success': True}
        else:
            # Still return success since we removed from indexes
            return {'success': True}

    except Exception as e:
        return {'success': False, 'error': str(e)}
