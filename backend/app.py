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
import local_cache as db  # Use local cache with Neon sync
import cloud_backup
import email_auth

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Version
BE_VERSION = '2.5.2'  # Fast Neon sync with persistent connection

# NOTE: Using local SQLite for fast reads/writes with periodic Neon sync

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

@app.route('/api/sheets/<spreadsheet_id>', methods=['GET'])
def get_sheet(spreadsheet_id):
    """Get a specific sheet with all its data - optimized single connection"""
    sheet, team_members, attendance_data = db.get_full_sheet_data(spreadsheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404

    return jsonify({
        'sheet': sheet,
        'teamMembers': team_members,
        'attendanceData': attendance_data
    })

@app.route('/api/sheets/<spreadsheet_id>', methods=['DELETE'])
def delete_sheet(spreadsheet_id):
    """Delete a sheet and all its data"""
    db.delete_sheet(spreadsheet_id)
    return jsonify({'success': True})

@app.route('/api/sheets/load', methods=['POST'])
def load_or_create_sheet():
    """Load existing sheet or create new one, save team members.

    Uses spreadsheet_id (Google Sheet ID) as the primary identifier.
    No internal IDs - the Google Sheet ID IS the identifier.
    """
    req = request.json
    spreadsheet_id = req.get('spreadsheetId')
    sheet_name = req.get('sheetName', '')
    gdud = req.get('gdud', '')
    pluga = req.get('pluga', '')
    spreadsheet_title = req.get('spreadsheetTitle', '')
    members = req.get('members', [])

    if not spreadsheet_id:
        return jsonify({'error': 'Missing spreadsheetId'}), 400

    # Get or create sheet - returns the same spreadsheet_id
    db.get_or_create_sheet(spreadsheet_id, sheet_name, gdud, pluga, spreadsheet_title)

    # Save team members
    db.save_team_members(spreadsheet_id, members)

    # Get existing attendance data
    attendance_data = db.get_attendance(spreadsheet_id)
    sheet = db.get_sheet_by_id(spreadsheet_id)

    return jsonify({
        'success': True,
        'spreadsheetId': spreadsheet_id,  # Return Google Sheet ID directly
        'sheet': sheet,
        'teamMembers': members,
        'attendanceData': attendance_data
    })

# ============================================
# Team Members API (Sheet-based)
# ============================================

@app.route('/api/sheets/<spreadsheet_id>/team-members', methods=['GET'])
def get_sheet_team_members(spreadsheet_id):
    """Get all team members for a sheet"""
    members = db.get_team_members(spreadsheet_id)
    return jsonify(members)

@app.route('/api/sheets/<spreadsheet_id>/team-members', methods=['POST'])
def save_sheet_team_members(spreadsheet_id):
    """Save team members for a sheet"""
    members = request.json.get('members', [])
    db.save_team_members(spreadsheet_id, members)
    return jsonify({'success': True, 'count': len(members)})

# ============================================
# Attendance API (Sheet-based)
# ============================================

@app.route('/api/sheets/<spreadsheet_id>/attendance', methods=['GET'])
def get_sheet_attendance(spreadsheet_id):
    """Get all attendance data for a sheet"""
    attendance = db.get_attendance(spreadsheet_id)
    return jsonify(attendance)

@app.route('/api/sheets/<spreadsheet_id>/attendance', methods=['POST'])
def update_sheet_attendance(spreadsheet_id):
    """Update attendance for a specific member and date"""
    req = request.json
    ma = req.get('ma')
    date = req.get('date')
    status = req.get('status')
    session_id = req.get('sessionId', '')  # Track who made the change

    if not all([ma, date, status]):
        return jsonify({'error': 'Missing ma, date, or status'}), 400

    db.update_attendance(spreadsheet_id, ma, date, status, session_id)
    return jsonify({'success': True, 'serverTimestamp': db.get_server_timestamp()})

@app.route('/api/sheets/<spreadsheet_id>/attendance/batch', methods=['POST'])
def update_sheet_attendance_batch(spreadsheet_id):
    """Update multiple attendance records in a single request"""
    req = request.json
    updates = req.get('updates', [])
    session_id = req.get('sessionId', '')

    if not updates:
        return jsonify({'error': 'No updates provided'}), 400

    db.update_attendance_batch(spreadsheet_id, updates, session_id)
    return jsonify({'success': True, 'serverTimestamp': db.get_server_timestamp()})

# ============================================
# Date Range API (Sheet-based)
# ============================================

@app.route('/api/sheets/<spreadsheet_id>/date-range', methods=['GET'])
def get_sheet_date_range(spreadsheet_id):
    """Get the date range for a sheet"""
    sheet = db.get_sheet_by_id(spreadsheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404
    return jsonify({
        'startDate': sheet.get('start_date', '2025-12-21'),
        'endDate': sheet.get('end_date', '2026-02-01')
    })

@app.route('/api/sheets/<spreadsheet_id>/date-range', methods=['POST'])
def set_sheet_date_range(spreadsheet_id):
    """Set the date range for a sheet"""
    req = request.json
    start_date = req.get('startDate')
    end_date = req.get('endDate')

    if start_date and end_date:
        db.update_sheet_dates(spreadsheet_id, start_date, end_date)

    return jsonify({'success': True})

# ============================================
# Export API (Sheet-based)
# ============================================

@app.route('/api/sheets/<spreadsheet_id>/export', methods=['GET'])
def export_sheet_data(spreadsheet_id):
    """Export all data for a sheet - optimized single connection"""
    sheet, team_members, attendance_data = db.get_full_sheet_data(spreadsheet_id)
    if not sheet:
        return jsonify({'error': 'Sheet not found'}), 404

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
            'allRows': rows  # Return ALL rows for frontend parsing
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

@app.route('/api/sync-status', methods=['GET'])
def get_sync_status():
    """Get pending sync count for UI indicator"""
    pending_count = db.get_pending_sync_count()
    return jsonify({
        'pendingCount': pending_count,
        'synced': pending_count == 0
    })

@app.route('/api/force-sync', methods=['POST'])
def force_sync():
    """Force immediate sync to Neon"""
    db.force_sync_now()
    return jsonify({'success': True, 'pendingCount': db.get_pending_sync_count()})

@app.route('/api/migrate', methods=['POST', 'GET'])
def run_migration():
    """Run database migrations - add updated_by_session column"""
    conn = db.get_db_connection()
    cursor = conn.cursor()

    # Check current columns
    cursor.execute("PRAGMA table_info(attendance)")
    columns = [col['name'] for col in cursor.fetchall()]

    result = {'columns_before': columns}

    if 'updated_by_session' not in columns:
        try:
            cursor.execute('ALTER TABLE attendance ADD COLUMN updated_by_session TEXT DEFAULT ""')
            conn.commit()
            result['migration'] = 'Added updated_by_session column'
        except Exception as e:
            result['migration_error'] = str(e)
    else:
        result['migration'] = 'Column already exists'

    # Check columns after
    cursor.execute("PRAGMA table_info(attendance)")
    columns_after = [col['name'] for col in cursor.fetchall()]
    result['columns_after'] = columns_after

    conn.close()
    return jsonify(result)

@app.route('/api/debug/sync-status', methods=['GET'])
def debug_sync_status():
    """Debug endpoint to check sync status"""
    import sqlite3
    conn = db.get_db_connection()
    cursor = conn.cursor()

    # Check if updated_by_session column exists
    cursor.execute("PRAGMA table_info(attendance)")
    columns = [col['name'] for col in cursor.fetchall()]
    has_session_col = 'updated_by_session' in columns

    # Get recent attendance records with session info
    recent = []
    try:
        cursor.execute('''
            SELECT ma, date, status, updated_at, updated_by_session
            FROM attendance
            ORDER BY updated_at DESC
            LIMIT 10
        ''')
        for row in cursor.fetchall():
            recent.append({
                'ma': row['ma'],
                'date': row['date'],
                'status': row['status'],
                'updated_at': row['updated_at'],
                'session': row['updated_by_session'] if 'updated_by_session' in row.keys() else 'N/A'
            })
    except Exception as e:
        recent = [{'error': str(e)}]

    conn.close()

    return jsonify({
        'columns': columns,
        'has_session_column': has_session_col,
        'recent_attendance': recent,
        'server_time': db.get_server_timestamp()
    })

@app.route('/api/debug/test-sync', methods=['GET'])
def debug_test_sync():
    """Test sync query with specific parameters"""
    spreadsheet_id = request.args.get('sheet', '1NfKW8Z52YNwfArSKVyAVENkUZogdKqXEFqONYVj9w0U')
    since = request.args.get('since', '2025-12-15T00:00:00')
    exclude_session = request.args.get('exclude', '')

    changes = db.get_attendance_changes_since(spreadsheet_id, since, exclude_session)

    return jsonify({
        'query_params': {
            'spreadsheet_id': spreadsheet_id,
            'since': since,
            'exclude_session': exclude_session
        },
        'changes_found': len(changes),
        'changes': changes
    })

# ============================================
# Email Authentication API
# ============================================

@app.route('/api/auth/request-code', methods=['POST'])
def request_auth_code():
    """Send verification code to email"""
    req = request.json or {}
    email = req.get('email', '').strip()

    if not email:
        return jsonify({'success': False, 'error': 'נא להזין כתובת מייל'}), 400

    # Basic email validation
    if '@' not in email or '.' not in email:
        return jsonify({'success': False, 'error': 'כתובת מייל לא תקינה'}), 400

    success, message = email_auth.request_verification_code(email)

    if success:
        return jsonify({'success': True, 'message': message})
    else:
        return jsonify({'success': False, 'error': message}), 500


@app.route('/api/auth/verify', methods=['POST'])
def verify_auth_code():
    """Verify code and create session"""
    req = request.json or {}
    email = req.get('email', '').strip()
    code = req.get('code', '').strip()

    if not email or not code:
        return jsonify({'success': False, 'error': 'נא להזין מייל וקוד'}), 400

    success, session_token, message = email_auth.verify_code(email, code)

    if success:
        return jsonify({
            'success': True,
            'sessionToken': session_token,
            'email': email,
            'message': message
        })
    else:
        return jsonify({'success': False, 'error': message}), 401


@app.route('/api/auth/validate', methods=['POST'])
def validate_auth_session():
    """Validate session token"""
    req = request.json or {}
    session_token = req.get('sessionToken', '')

    email = email_auth.validate_session(session_token)

    if email:
        return jsonify({'success': True, 'valid': True, 'email': email})
    else:
        return jsonify({'success': True, 'valid': False}), 200


@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    """Logout and invalidate session"""
    req = request.json or {}
    session_token = req.get('sessionToken', '')

    email_auth.logout(session_token)
    return jsonify({'success': True, 'message': 'התנתקת בהצלחה'})

# ============================================
# Active Users Tracking (Database-backed for multi-worker support)
# ============================================

@app.route('/api/sheets/<spreadsheet_id>/heartbeat', methods=['POST'])
def heartbeat(spreadsheet_id):
    """Register user activity and return only changes since last sync (from other users)"""
    req = request.json or {}
    user_email = req.get('email', 'Anonymous')
    session_id = req.get('sessionId', 'unknown')
    last_sync = req.get('lastSync', '')  # ISO timestamp of last sync
    client_data_version = req.get('dataVersion', 0)  # Client's last known data version

    # Update active users in database (shared across all workers)
    db.update_active_user(session_id, user_email, spreadsheet_id, time.time())

    # Get list of other active users on this sheet (exclude current session)
    other_users = db.get_active_users_for_sheet(spreadsheet_id, exclude_session=session_id)

    # Get current server timestamp and data version
    server_timestamp = db.get_server_timestamp()
    current_data_version = db.get_data_version()

    # Check if data version changed (backup was restored) - force full reload
    if client_data_version and client_data_version != current_data_version:
        print(f"[SYNC] Data version mismatch: client={client_data_version}, server={current_data_version} - forcing full reload")
        sheet, team_members, attendance_data = db.get_full_sheet_data(spreadsheet_id)
        if not sheet:
            return jsonify({'error': 'Sheet not found'}), 404

        return jsonify({
            'success': True,
            'mode': 'full',
            'reason': 'data_version_changed',
            'sheet': sheet,
            'teamMembers': team_members,
            'attendanceData': attendance_data,
            'serverTimestamp': server_timestamp,
            'dataVersion': current_data_version,
            'activeUsers': other_users
        })

    # If client has lastSync, only return changes from OTHER users since that time
    if last_sync:
        # Get only changes made by other users since last sync
        changes = db.get_attendance_changes_since(spreadsheet_id, last_sync, exclude_session_id=session_id)
        return jsonify({
            'success': True,
            'mode': 'incremental',
            'changes': changes,
            'serverTimestamp': server_timestamp,
            'dataVersion': current_data_version,
            'activeUsers': other_users
        })
    else:
        # First sync - return full data (optimized single connection)
        sheet, team_members, attendance_data = db.get_full_sheet_data(spreadsheet_id)
        if not sheet:
            return jsonify({'error': 'Sheet not found'}), 404

        return jsonify({
            'success': True,
            'mode': 'full',
            'sheet': sheet,
            'teamMembers': team_members,
            'attendanceData': attendance_data,
            'serverTimestamp': server_timestamp,
            'dataVersion': current_data_version,
            'activeUsers': other_users
        })

@app.route('/api/sheets/<spreadsheet_id>/active-users', methods=['GET'])
def get_active_users(spreadsheet_id):
    """Get list of active users on a sheet"""
    users_on_sheet = db.get_all_active_users_for_sheet(spreadsheet_id)

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
        SELECT a.spreadsheet_id, a.ma, a.date, a.status, t.first_name, t.last_name
        FROM attendance a
        LEFT JOIN team_members t ON a.spreadsheet_id = t.spreadsheet_id AND a.ma = t.ma
    ''')
    current_data = {f"{row['spreadsheet_id']}_{row['ma']}_{row['date']}": {
        'status': row['status'],
        'firstName': row['first_name'] or '',
        'lastName': row['last_name'] or '',
        'ma': row['ma'],
        'date': row['date'],
        'spreadsheet_id': row['spreadsheet_id']
    } for row in current_cursor.fetchall()}
    current_conn.close()

    # Get backup attendance data
    backup_conn = sqlite3.connect(backup_path)
    backup_conn.row_factory = sqlite3.Row
    backup_cursor = backup_conn.cursor()

    # Check if team_members table exists in backup
    backup_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_members'")
    has_team_members = backup_cursor.fetchone() is not None

    # Check if backup uses new schema (spreadsheet_id) or old (sheet_id)
    backup_cursor.execute("PRAGMA table_info(attendance)")
    columns = [col[1] for col in backup_cursor.fetchall()]
    uses_new_schema = 'spreadsheet_id' in columns

    backup_data = {}
    if uses_new_schema:
        if has_team_members:
            backup_cursor.execute('''
                SELECT a.spreadsheet_id, a.ma, a.date, a.status, t.first_name, t.last_name
                FROM attendance a
                LEFT JOIN team_members t ON a.spreadsheet_id = t.spreadsheet_id AND a.ma = t.ma
            ''')
        else:
            backup_cursor.execute('SELECT spreadsheet_id, ma, date, status FROM attendance')

        for row in backup_cursor.fetchall():
            key = f"{row['spreadsheet_id']}_{row['ma']}_{row['date']}"
            backup_data[key] = {
                'status': row['status'],
                'firstName': row['first_name'] if has_team_members and 'first_name' in row.keys() else '',
                'lastName': row['last_name'] if has_team_members and 'last_name' in row.keys() else '',
                'ma': row['ma'],
                'date': row['date'],
                'spreadsheet_id': row['spreadsheet_id']
            }
    else:
        # Old schema - skip comparison (incompatible)
        backup_conn.close()
        return jsonify({
            'filename': filename,
            'totalDifferences': 0,
            'differences': [],
            'warning': 'Backup uses old schema - cannot compare'
        })

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
    """Upload current database to cloud with optional notes and user email"""
    data = request.get_json() or {}
    notes = data.get('notes', '')
    user_email = data.get('user_email', 'Anonymous')
    result = cloud_backup.upload_backup_to_cloud(source='manual', notes=notes, user_email=user_email)
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
    spreadsheet_id = data.get('spreadsheet_id')
    if not file_path:
        return jsonify({'success': False, 'error': 'File path required'}), 400
    result = cloud_backup.compare_with_cloud(file_path, spreadsheet_id)
    if result['success']:
        return jsonify(result)
    return jsonify(result), 400

@app.route('/api/cloud-backups/clear-all', methods=['POST'])
def clear_all_cloud_backups():
    """Clear all cloud backup references (reset index to empty)"""
    result = cloud_backup.clear_all_cloud_backups()
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
