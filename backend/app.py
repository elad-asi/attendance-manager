from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import json
import os
import threading
import time
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import database as db
import cloud_backup

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Version
BE_VERSION = '0.6.3'

# Active users tracking (email -> {sheet_id, last_seen})
active_users = {}
ACTIVE_USER_TIMEOUT_SECONDS = 30  # Consider user inactive after 30 seconds

# Auto-backup configuration
AUTO_BACKUP_INTERVAL_SECONDS = 60 * 60  # 1 hour
auto_backup_thread = None

# Configuration
GOOGLE_CLIENT_ID = '651831609522-bvrgmop9hmdghlrn2tqm1hv0dmkhu933.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.file'  # For backup storage
]

# Ensure data directory exists
os.makedirs('data', exist_ok=True)

# ============================================
# Static File Serving
# ============================================

@app.route('/')
def serve_frontend():
    """Serve the frontend index.html"""
    return send_from_directory('../frontend', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    """Serve static files from frontend directory"""
    return send_from_directory('../frontend', path)

# ============================================
# Sheet Management API
# ============================================

@app.route('/api/sheets', methods=['GET'])
def get_all_sheets():
    """Get list of all loaded sheets"""
    sheets = db.get_all_sheets()
    return jsonify(sheets)

@app.route('/api/sheets/<int:sheet_id>', methods=['GET'])
def get_sheet(sheet_id):
    """Get a specific sheet with all its data"""
    sheet = db.get_sheet_by_id(sheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404

    team_members = db.get_team_members(sheet_id)
    attendance_data = db.get_attendance(sheet_id)

    return jsonify({
        'sheet': sheet,
        'teamMembers': team_members,
        'attendanceData': attendance_data
    })

@app.route('/api/sheets/<int:sheet_id>', methods=['DELETE'])
def delete_sheet(sheet_id):
    """Delete a sheet and all its data"""
    db.delete_sheet(sheet_id)
    return jsonify({'success': True})

@app.route('/api/sheets/load', methods=['POST'])
def load_or_create_sheet():
    """Load existing sheet or create new one, save team members"""
    req = request.json
    spreadsheet_id = req.get('spreadsheetId')
    sheet_name = req.get('sheetName')
    gdud = req.get('gdud', '')
    pluga = req.get('pluga', '')
    members = req.get('members', [])

    if not spreadsheet_id or not sheet_name:
        return jsonify({'error': 'Missing spreadsheetId or sheetName'}), 400

    # Get or create sheet
    sheet_id = db.get_or_create_sheet(spreadsheet_id, sheet_name, gdud, pluga)

    # Save team members
    db.save_team_members(sheet_id, members)

    # Get existing attendance data
    attendance_data = db.get_attendance(sheet_id)
    sheet = db.get_sheet_by_id(sheet_id)

    return jsonify({
        'success': True,
        'sheetId': sheet_id,
        'sheet': sheet,
        'teamMembers': members,
        'attendanceData': attendance_data
    })

# ============================================
# Team Members API (Sheet-based)
# ============================================

@app.route('/api/sheets/<int:sheet_id>/team-members', methods=['GET'])
def get_sheet_team_members(sheet_id):
    """Get all team members for a sheet"""
    members = db.get_team_members(sheet_id)
    return jsonify(members)

@app.route('/api/sheets/<int:sheet_id>/team-members', methods=['POST'])
def save_sheet_team_members(sheet_id):
    """Save team members for a sheet"""
    members = request.json.get('members', [])
    db.save_team_members(sheet_id, members)
    return jsonify({'success': True, 'count': len(members)})

# ============================================
# Attendance API (Sheet-based)
# ============================================

@app.route('/api/sheets/<int:sheet_id>/attendance', methods=['GET'])
def get_sheet_attendance(sheet_id):
    """Get all attendance data for a sheet"""
    attendance = db.get_attendance(sheet_id)
    return jsonify(attendance)

@app.route('/api/sheets/<int:sheet_id>/attendance', methods=['POST'])
def update_sheet_attendance(sheet_id):
    """Update attendance for a specific member and date"""
    req = request.json
    ma = req.get('ma')
    date = req.get('date')
    status = req.get('status')

    if not all([ma, date, status]):
        return jsonify({'error': 'Missing ma, date, or status'}), 400

    db.update_attendance(sheet_id, ma, date, status)
    return jsonify({'success': True})

# ============================================
# Date Range API (Sheet-based)
# ============================================

@app.route('/api/sheets/<int:sheet_id>/date-range', methods=['GET'])
def get_sheet_date_range(sheet_id):
    """Get the date range for a sheet"""
    sheet = db.get_sheet_by_id(sheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404
    return jsonify({
        'startDate': sheet.get('start_date', '2025-12-21'),
        'endDate': sheet.get('end_date', '2026-02-01')
    })

@app.route('/api/sheets/<int:sheet_id>/date-range', methods=['POST'])
def set_sheet_date_range(sheet_id):
    """Set the date range for a sheet"""
    req = request.json
    start_date = req.get('startDate')
    end_date = req.get('endDate')

    if start_date and end_date:
        db.update_sheet_dates(sheet_id, start_date, end_date)

    return jsonify({'success': True})

# ============================================
# Export API (Sheet-based)
# ============================================

@app.route('/api/sheets/<int:sheet_id>/export', methods=['GET'])
def export_sheet_data(sheet_id):
    """Export all data for a sheet"""
    sheet = db.get_sheet_by_id(sheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404

    team_members = db.get_team_members(sheet_id)
    attendance_data = db.get_attendance(sheet_id)

    return jsonify({
        'sheet': sheet,
        'teamMembers': team_members,
        'attendanceData': attendance_data,
        'startDate': sheet.get('start_date'),
        'endDate': sheet.get('end_date')
    })

# ============================================
# Google Sheets API (Proxy)
# ============================================

@app.route('/api/sheets/fetch', methods=['POST'])
def fetch_sheets():
    """Fetch sheet names from a Google Spreadsheet"""
    req = request.json
    access_token = req.get('accessToken')
    spreadsheet_id = req.get('spreadsheetId')

    if not access_token or not spreadsheet_id:
        return jsonify({'error': 'Missing accessToken or spreadsheetId'}), 400

    try:
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)

        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = [{'title': s['properties']['title'], 'sheetId': s['properties']['sheetId']}
                  for s in spreadsheet.get('sheets', [])]

        return jsonify({
            'success': True,
            'title': spreadsheet.get('properties', {}).get('title', ''),
            'sheets': sheets
        })
    except HttpError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sheets/data', methods=['POST'])
def get_sheet_data():
    """Get data from a specific sheet"""
    req = request.json
    access_token = req.get('accessToken')
    spreadsheet_id = req.get('spreadsheetId')
    sheet_name = req.get('sheetName')

    if not all([access_token, spreadsheet_id, sheet_name]):
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)

        # Get sheet data (A:AZ covers columns A through AZ = 52 columns)
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:AZ"
        ).execute()

        values = result.get('values', [])

        if not values:
            return jsonify({'error': 'Sheet is empty'}), 400

        # Parse the data - first row is headers
        headers = values[0]
        rows = values[1:]

        # Find column indices (Hebrew headers)
        header_map = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            h_clean = h.strip()
            h_lower = h_clean.lower()

            # First name detection
            if 'שם פרטי' in h_clean or h_lower == 'first name':
                header_map['firstName'] = i
            # Last name detection
            elif 'שם משפחה' in h_clean or h_lower == 'last name':
                header_map['lastName'] = i
            # ID detection - מספר אישי, מ.א, מא
            elif 'מספר אישי' in h_clean or 'מ.א' in h_clean or 'מא' in h_clean or h_lower == 'id':
                header_map['ma'] = i
            # Department/מחלקה detection
            elif 'מחלקה' in h_clean or h_lower == 'department':
                header_map['mahlaka'] = i
            # Military profession/מקצוע צבאי detection
            elif 'מקצוע צבאי' in h_clean or 'מקצוע' in h_clean:
                header_map['miktzoaTzvai'] = i

        # Parse rows into team members
        members = []
        for row in rows:
            if len(row) == 0:
                continue

            # Get values safely using header_map
            def get_value(field, default_idx=None):
                idx = header_map.get(field, default_idx)
                if idx is not None and len(row) > idx:
                    val = row[idx]
                    # Treat "?" as empty - do not invent values
                    if val == '?' or val.strip() == '?':
                        return ''
                    return val
                return ''

            member = {
                'firstName': get_value('firstName', 0),
                'lastName': get_value('lastName', 1),
                'ma': get_value('ma'),  # No default - must be found in headers
                'mahlaka': get_value('mahlaka'),  # No default - must be found in headers
                'miktzoaTzvai': get_value('miktzoaTzvai')  # Military profession
            }

            # Only add if we have at least a name or ma
            if member['firstName'] or member['lastName'] or member['ma']:
                members.append(member)

        # Also return raw data for custom mapping
        # Get sample values for each column (first non-empty value)
        sample_values = []
        for col_idx in range(len(headers)):
            sample = ''
            for row in rows[:5]:  # Check first 5 rows for sample
                if len(row) > col_idx and row[col_idx] and row[col_idx].strip():
                    sample = row[col_idx].strip()
                    break
            sample_values.append(sample)

        return jsonify({
            'success': True,
            'members': members,
            'headers': headers,
            'headerMap': header_map,
            'sampleValues': sample_values,
            'rawRows': rows[:3]  # Return first 3 rows for preview
        })

    except HttpError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sheets/parse-with-mapping', methods=['POST'])
def parse_sheet_with_mapping():
    """Parse sheet data using custom column mapping from user"""
    req = request.json
    access_token = req.get('accessToken')
    spreadsheet_id = req.get('spreadsheetId')
    sheet_name = req.get('sheetName')
    column_mapping = req.get('columnMapping', {})

    if not all([access_token, spreadsheet_id, sheet_name]):
        return jsonify({'error': 'Missing required parameters'}), 400

    try:
        creds = Credentials(token=access_token)
        service = build('sheets', 'v4', credentials=creds)

        # Get all sheet data
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:AZ"
        ).execute()

        values = result.get('values', [])

        if not values:
            return jsonify({'error': 'Sheet is empty'}), 400

        # First row is headers, rest is data
        headers = values[0]
        rows = values[1:]

        # Parse rows using the custom column mapping
        members = []
        for row in rows:
            if len(row) == 0:
                continue

            def get_value(field):
                idx = column_mapping.get(field)
                # Skip if marked as 'skip' or not mapped
                if idx == 'skip' or idx is None:
                    return ''
                if isinstance(idx, int) and len(row) > idx:
                    val = row[idx]
                    # Treat "?" as empty
                    if val == '?' or (isinstance(val, str) and val.strip() == '?'):
                        return ''
                    return str(val).strip() if val else ''
                return ''

            member = {
                'firstName': get_value('firstName'),
                'lastName': get_value('lastName'),
                'ma': get_value('ma'),
                'mahlaka': get_value('mahlaka'),
                'miktzoaTzvai': get_value('miktzoaTzvai')
            }

            # Only add if we have at least firstName or lastName or ma
            if member['firstName'] or member['lastName'] or member['ma']:
                members.append(member)

        return jsonify({
            'success': True,
            'members': members,
            'count': len(members)
        })

    except HttpError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================
# Backup API
# ============================================

@app.route('/api/backups', methods=['GET'])
def get_backups():
    """List all available backups"""
    backups = db.list_backups()
    return jsonify({'backups': backups})

@app.route('/api/backups/create', methods=['POST'])
def create_backup():
    """Manually create a backup"""
    backup_file = db.create_backup()
    if backup_file:
        return jsonify({'success': True, 'backup': backup_file})
    return jsonify({'error': 'Failed to create backup'}), 500

@app.route('/api/backups/restore', methods=['POST'])
def restore_backup():
    """Restore from a backup file"""
    req = request.json
    filename = req.get('filename')

    if not filename:
        return jsonify({'error': 'Missing filename'}), 400

    success, message = db.restore_backup(filename)
    if success:
        return jsonify({'success': True, 'message': message})
    return jsonify({'error': message}), 400

# ============================================
# Health Check & Version
# ============================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

@app.route('/api/version', methods=['GET'])
def get_version():
    """Get backend version"""
    return jsonify({'version': BE_VERSION})

# ============================================
# Active Users Tracking
# ============================================

def cleanup_inactive_users():
    """Remove users who haven't been seen in the timeout period"""
    global active_users
    now = time.time()
    active_users = {
        email: data for email, data in active_users.items()
        if now - data['last_seen'] < ACTIVE_USER_TIMEOUT_SECONDS
    }

@app.route('/api/sheets/<int:sheet_id>/heartbeat', methods=['POST'])
def heartbeat(sheet_id):
    """Register user activity on a sheet and return current data + active users"""
    req = request.json or {}
    user_email = req.get('email', 'Anonymous')

    # Update active users
    active_users[user_email] = {
        'sheet_id': sheet_id,
        'last_seen': time.time()
    }

    # Cleanup inactive users
    cleanup_inactive_users()

    # Get list of other active users on this sheet
    other_users = [
        email for email, data in active_users.items()
        if data['sheet_id'] == sheet_id and email != user_email
    ]

    # Get current data from database
    sheet = db.get_sheet_by_id(sheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404

    team_members = db.get_team_members(sheet_id)
    attendance_data = db.get_attendance(sheet_id)

    return jsonify({
        'success': True,
        'sheet': sheet,
        'teamMembers': team_members,
        'attendanceData': attendance_data,
        'activeUsers': other_users
    })

@app.route('/api/sheets/<int:sheet_id>/active-users', methods=['GET'])
def get_active_users(sheet_id):
    """Get list of active users on a sheet"""
    cleanup_inactive_users()

    users_on_sheet = [
        email for email, data in active_users.items()
        if data['sheet_id'] == sheet_id
    ]

    return jsonify({
        'activeUsers': users_on_sheet
    })

# ============================================
# Backup Management
# ============================================

@app.route('/api/backups', methods=['GET'])
def list_backups_api():
    """List all available backups"""
    backups = db.list_backups()
    return jsonify({'backups': backups})

@app.route('/api/backups/compare/<filename>', methods=['GET'])
def compare_backup(filename):
    """Compare a backup with current data and return differences"""
    import sqlite3

    backup_path = os.path.join(db.BACKUP_DIR, filename)
    if not os.path.exists(backup_path):
        return jsonify({'error': 'Backup not found'}), 404

    # Get current attendance data
    current_conn = db.get_db_connection()
    current_cursor = current_conn.cursor()
    current_cursor.execute('''
        SELECT a.sheet_id, a.ma, a.date, a.status, t.first_name, t.last_name
        FROM attendance a
        LEFT JOIN team_members t ON a.sheet_id = t.sheet_id AND a.ma = t.ma
    ''')
    current_data = {f"{row['sheet_id']}_{row['ma']}_{row['date']}": {
        'status': row['status'],
        'firstName': row['first_name'] or '',
        'lastName': row['last_name'] or '',
        'ma': row['ma'],
        'date': row['date'],
        'sheet_id': row['sheet_id']
    } for row in current_cursor.fetchall()}
    current_conn.close()

    # Get backup attendance data
    backup_conn = sqlite3.connect(backup_path)
    backup_conn.row_factory = sqlite3.Row
    backup_cursor = backup_conn.cursor()

    # Check if team_members table exists in backup
    backup_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_members'")
    has_team_members = backup_cursor.fetchone() is not None

    if has_team_members:
        backup_cursor.execute('''
            SELECT a.sheet_id, a.ma, a.date, a.status, t.first_name, t.last_name
            FROM attendance a
            LEFT JOIN team_members t ON a.sheet_id = t.sheet_id AND a.ma = t.ma
        ''')
    else:
        backup_cursor.execute('SELECT sheet_id, ma, date, status FROM attendance')

    backup_data = {}
    for row in backup_cursor.fetchall():
        key = f"{row['sheet_id']}_{row['ma']}_{row['date']}"
        backup_data[key] = {
            'status': row['status'],
            'firstName': row['first_name'] if has_team_members and row['first_name'] else '',
            'lastName': row['last_name'] if has_team_members and row['last_name'] else '',
            'ma': row['ma'],
            'date': row['date'],
            'sheet_id': row['sheet_id']
        }
    backup_conn.close()

    # Find differences
    differences = []

    # Check for changes and items only in current
    all_keys = set(current_data.keys()) | set(backup_data.keys())
    for key in all_keys:
        current = current_data.get(key)
        backup = backup_data.get(key)

        if current and backup:
            if current['status'] != backup['status']:
                differences.append({
                    'type': 'changed',
                    'ma': current['ma'],
                    'firstName': current['firstName'],
                    'lastName': current['lastName'],
                    'date': current['date'],
                    'currentStatus': current['status'],
                    'backupStatus': backup['status']
                })
        elif current and not backup:
            differences.append({
                'type': 'added',
                'ma': current['ma'],
                'firstName': current['firstName'],
                'lastName': current['lastName'],
                'date': current['date'],
                'currentStatus': current['status'],
                'backupStatus': None
            })
        elif backup and not current:
            differences.append({
                'type': 'removed',
                'ma': backup['ma'],
                'firstName': backup['firstName'],
                'lastName': backup['lastName'],
                'date': backup['date'],
                'currentStatus': None,
                'backupStatus': backup['status']
            })

    # Sort by date, then by name
    differences.sort(key=lambda x: (x['date'], x['lastName'], x['firstName']))

    return jsonify({
        'filename': filename,
        'totalDifferences': len(differences),
        'differences': differences
    })

@app.route('/api/backups/restore/<filename>', methods=['POST'])
def restore_backup_api(filename):
    """Restore database from a backup"""
    success, message = db.restore_backup(filename)
    if success:
        return jsonify({'success': True, 'message': message})
    else:
        return jsonify({'success': False, 'error': message}), 400

# ============================================
# Cloud Backup API (JSONBin.io)
# ============================================

@app.route('/api/cloud-backups', methods=['GET'])
def list_cloud_backups():
    """List backups from cloud, optionally filtered by sheet_id or spreadsheet_id

    Query params:
        sheet_id: Internal DB sheet ID to filter backups
        spreadsheet_id: Google spreadsheet ID for cross-machine sync
    """
    # Get optional filters from query params
    sheet_id = request.args.get('sheet_id', type=int)
    spreadsheet_id = request.args.get('spreadsheet_id', type=str)
    result = cloud_backup.list_cloud_backups(filter_sheet_id=sheet_id, spreadsheet_id=spreadsheet_id)
    return jsonify(result)

@app.route('/api/cloud-backups/upload', methods=['POST'])
def upload_to_cloud():
    """Upload current database to cloud"""
    result = cloud_backup.upload_backup_to_cloud()
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

@app.route('/api/cloud-backups/restore', methods=['POST'])
def restore_from_cloud():
    """Restore database from a cloud backup"""
    data = request.get_json()
    file_path = data.get('path')
    if not file_path:
        return jsonify({'success': False, 'error': 'File path required'}), 400
    result = cloud_backup.restore_from_cloud(file_path)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

@app.route('/api/cloud-backups/delete', methods=['POST'])
def delete_from_cloud():
    """Delete a backup from cloud"""
    data = request.get_json()
    file_path = data.get('path')
    if not file_path:
        return jsonify({'success': False, 'error': 'File path required'}), 400
    result = cloud_backup.delete_cloud_backup(file_path)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

@app.route('/api/cloud-backups/status', methods=['GET'])
def get_cloud_status():
    """Check if cloud backup is configured"""
    is_configured = cloud_backup.is_cloud_configured()
    return jsonify({
        'configured': is_configured,
        'message': 'Cloud backup is ready' if is_configured else 'Cloud backup not configured. Set JSONBIN_API_KEY environment variable.'
    })

@app.route('/api/cloud-backups/compare', methods=['POST'])
def compare_with_cloud_backup():
    """Compare current database with a cloud backup"""
    data = request.get_json()
    file_path = data.get('path')
    sheet_id = data.get('sheet_id')
    if not file_path:
        return jsonify({'success': False, 'error': 'File path required'}), 400
    result = cloud_backup.compare_with_cloud(file_path, sheet_id)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

# ============================================
# Auto-backup Background Thread
# ============================================

def auto_backup_worker():
    """Background thread that performs hourly backups if data has changed"""
    print("Auto-backup: Background worker started")
    while True:
        time.sleep(AUTO_BACKUP_INTERVAL_SECONDS)
        try:
            if cloud_backup.is_cloud_configured():
                print("Auto-backup: Running hourly backup check...")
                result = cloud_backup.upload_backup_to_cloud(source='auto')
                if result.get('success'):
                    if result.get('skipped'):
                        print("Auto-backup: Skipped - no changes since last backup")
                    else:
                        print(f"Auto-backup: Backup created successfully")
                else:
                    print(f"Auto-backup: Failed - {result.get('error')}")
            else:
                print("Auto-backup: Cloud not configured, skipping")
        except Exception as e:
            print(f"Auto-backup: Error - {e}")


def start_auto_backup():
    """Start the auto-backup background thread"""
    global auto_backup_thread
    if auto_backup_thread is None or not auto_backup_thread.is_alive():
        auto_backup_thread = threading.Thread(target=auto_backup_worker, daemon=True)
        auto_backup_thread.start()
        print("Auto-backup: Scheduled every hour")


# ============================================
# Main
# ============================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    print("Starting Attendance Manager Backend...")
    print(f"Server running on port {port}")

    # Start auto-backup in background (only in production or when not in reloader)
    if not debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        start_auto_backup()

    app.run(host='0.0.0.0', port=port, debug=debug)
