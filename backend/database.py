import sqlite3
import os
import json
import shutil
import glob
from datetime import datetime

# Use absolute paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILE = os.path.join(SCRIPT_DIR, 'data', 'attendance.db')
BACKUP_DIR = os.path.join(SCRIPT_DIR, 'data', 'backups')
MAX_BACKUPS = 10

def get_db_mtime():
    """Get the last modified time of the database file"""
    if os.path.exists(DATABASE_FILE):
        return os.path.getmtime(DATABASE_FILE)
    return None

def validate_db_modified(before_mtime, operation_name):
    """Validate that the database was modified after an operation"""
    after_mtime = get_db_mtime()
    if before_mtime is not None and after_mtime is not None:
        if after_mtime <= before_mtime:
            print(f"WARNING: Database file was NOT modified after {operation_name}")
            print(f"  Before: {before_mtime}, After: {after_mtime}")
            print(f"  Database path: {DATABASE_FILE}")
            return False
        else:
            print(f"DB validated: {operation_name} - file modified at {datetime.fromtimestamp(after_mtime).isoformat()}")
    return True

def get_db_connection():
    """Get a database connection with row factory"""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def create_backup():
    """Create a backup of the database file, keeping only last MAX_BACKUPS"""
    if not os.path.exists(DATABASE_FILE):
        return None

    # Create backup directory if it doesn't exist
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # Generate backup filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(BACKUP_DIR, f'attendance_backup_{timestamp}.db')

    # Copy the database file
    shutil.copy2(DATABASE_FILE, backup_file)

    # Clean up old backups - keep only last MAX_BACKUPS
    cleanup_old_backups()

    return backup_file

def cleanup_old_backups():
    """Remove old backups, keeping only the last MAX_BACKUPS"""
    backup_pattern = os.path.join(BACKUP_DIR, 'attendance_backup_*.db')
    backups = sorted(glob.glob(backup_pattern), reverse=True)

    # Delete backups beyond MAX_BACKUPS
    for old_backup in backups[MAX_BACKUPS:]:
        try:
            os.remove(old_backup)
        except OSError:
            pass

def list_backups():
    """List all available backups with their timestamps"""
    backup_pattern = os.path.join(BACKUP_DIR, 'attendance_backup_*.db')
    backups = sorted(glob.glob(backup_pattern), reverse=True)

    backup_list = []
    for backup_path in backups:
        filename = os.path.basename(backup_path)
        # Extract timestamp from filename
        timestamp_str = filename.replace('attendance_backup_', '').replace('.db', '')
        try:
            timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            backup_list.append({
                'filename': filename,
                'path': backup_path,
                'timestamp': timestamp.isoformat(),
                'size': os.path.getsize(backup_path)
            })
        except ValueError:
            continue

    return backup_list

def restore_backup(backup_filename):
    """Restore database from a backup file"""
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    if not os.path.exists(backup_path):
        return False, f"Backup file not found: {backup_filename}"

    try:
        # Create a backup of current state before restore
        create_backup()

        # Copy backup file to database location
        shutil.copy2(backup_path, DATABASE_FILE)
        return True, f"Successfully restored from {backup_filename}"
    except Exception as e:
        return False, f"Restore failed: {str(e)}"

def init_database():
    """Initialize the database schema"""
    os.makedirs(os.path.join(SCRIPT_DIR, 'data'), exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    # Sheets table - each loaded Google Sheet is uniquely identified
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spreadsheet_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            gdud TEXT DEFAULT '',
            pluga TEXT DEFAULT '',
            start_date TEXT DEFAULT '2025-12-21',
            end_date TEXT DEFAULT '2026-02-01',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(spreadsheet_id, sheet_name, gdud, pluga)
        )
    ''')

    # Team members table - linked to a sheet
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_id INTEGER NOT NULL,
            first_name TEXT DEFAULT '',
            last_name TEXT DEFAULT '',
            ma TEXT DEFAULT '',
            gdud TEXT DEFAULT '',
            pluga TEXT DEFAULT '',
            mahlaka TEXT DEFAULT '',
            miktzoa_tzvai TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sheet_id) REFERENCES sheets(id) ON DELETE CASCADE
        )
    ''')

    # Attendance table - linked to team member and date
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_id INTEGER NOT NULL,
            ma TEXT NOT NULL,
            date TEXT NOT NULL,
            status TEXT DEFAULT 'unmarked',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sheet_id) REFERENCES sheets(id) ON DELETE CASCADE,
            UNIQUE(sheet_id, ma, date)
        )
    ''')

    # Active users table - for tracking who's online (shared across workers)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_users (
            session_id TEXT PRIMARY KEY,
            email TEXT DEFAULT 'Anonymous',
            sheet_id INTEGER NOT NULL,
            last_seen REAL NOT NULL,
            FOREIGN KEY (sheet_id) REFERENCES sheets(id) ON DELETE CASCADE
        )
    ''')

    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_sheet ON attendance(sheet_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_ma ON attendance(ma)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_members_sheet ON team_members(sheet_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_active_users_sheet ON active_users(sheet_id)')

    conn.commit()
    conn.close()

def get_or_create_sheet(spreadsheet_id, sheet_name, gdud='', pluga=''):
    """Get existing sheet or create a new one, returns sheet_id"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Try to find existing sheet
    cursor.execute('''
        SELECT id FROM sheets
        WHERE spreadsheet_id = ? AND sheet_name = ? AND gdud = ? AND pluga = ?
    ''', (spreadsheet_id, sheet_name, gdud, pluga))

    row = cursor.fetchone()
    if row:
        sheet_id = row['id']
    else:
        # Create new sheet
        cursor.execute('''
            INSERT INTO sheets (spreadsheet_id, sheet_name, gdud, pluga)
            VALUES (?, ?, ?, ?)
        ''', (spreadsheet_id, sheet_name, gdud, pluga))
        sheet_id = cursor.lastrowid
        conn.commit()

    conn.close()
    return sheet_id

def get_sheet_by_id(sheet_id):
    """Get sheet info by ID"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM sheets WHERE id = ?', (sheet_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def update_sheet_dates(sheet_id, start_date, end_date):
    """Update date range for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sheets SET start_date = ?, end_date = ?, updated_at = ?
        WHERE id = ?
    ''', (start_date, end_date, datetime.now().isoformat(), sheet_id))
    conn.commit()
    conn.close()

def save_team_members(sheet_id, members):
    """Save team members for a sheet (replaces existing)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete existing members for this sheet
    cursor.execute('DELETE FROM team_members WHERE sheet_id = ?', (sheet_id,))

    # Insert new members
    for member in members:
        cursor.execute('''
            INSERT INTO team_members (sheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sheet_id,
            member.get('firstName', ''),
            member.get('lastName', ''),
            member.get('ma', ''),
            member.get('gdud', ''),
            member.get('pluga', ''),
            member.get('mahlaka', ''),
            member.get('miktzoaTzvai', '')
        ))

    conn.commit()
    conn.close()

def get_team_members(sheet_id):
    """Get all team members for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai
        FROM team_members WHERE sheet_id = ?
    ''', (sheet_id,))
    rows = cursor.fetchall()
    conn.close()

    members = []
    for row in rows:
        members.append({
            'firstName': row['first_name'],
            'lastName': row['last_name'],
            'ma': row['ma'],
            'gdud': row['gdud'],
            'pluga': row['pluga'],
            'mahlaka': row['mahlaka'],
            'miktzoaTzvai': row['miktzoa_tzvai'] if 'miktzoa_tzvai' in row.keys() else ''
        })
    return members

def update_attendance(sheet_id, ma, date, status):
    """Update attendance for a specific member and date"""
    # Get timestamp before operation for validation
    before_mtime = get_db_mtime()

    # Create backup before making changes
    create_backup()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO attendance (sheet_id, ma, date, status, updated_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (sheet_id, ma, date, status, datetime.now().isoformat()))

    conn.commit()
    conn.close()

    # Validate DB was modified
    validate_db_modified(before_mtime, f"update_attendance(ma={ma}, date={date}, status={status})")

def get_attendance(sheet_id):
    """Get all attendance data for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ma, date, status FROM attendance WHERE sheet_id = ?
    ''', (sheet_id,))
    rows = cursor.fetchall()
    conn.close()

    # Convert to nested dict format: {ma: {date: status}}
    attendance_data = {}
    for row in rows:
        ma = row['ma']
        if ma not in attendance_data:
            attendance_data[ma] = {}
        attendance_data[ma][row['date']] = row['status']

    return attendance_data

def get_all_sheets():
    """Get list of all sheets"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, spreadsheet_id, sheet_name, gdud, pluga, start_date, end_date, created_at
        FROM sheets ORDER BY created_at DESC
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def delete_sheet(sheet_id):
    """Delete a sheet and all its data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM attendance WHERE sheet_id = ?', (sheet_id,))
    cursor.execute('DELETE FROM team_members WHERE sheet_id = ?', (sheet_id,))
    cursor.execute('DELETE FROM sheets WHERE id = ?', (sheet_id,))
    conn.commit()
    conn.close()

# ============================================
# Active Users Functions (for multi-worker support)
# ============================================

ACTIVE_USER_TIMEOUT_SECONDS = 30  # Consider user inactive after 30 seconds

def update_active_user(session_id, email, sheet_id, last_seen):
    """Update or insert an active user session"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO active_users (session_id, email, sheet_id, last_seen)
        VALUES (?, ?, ?, ?)
    ''', (session_id, email, sheet_id, last_seen))
    conn.commit()
    conn.close()

def cleanup_inactive_users():
    """Remove users who haven't been seen recently"""
    import time
    cutoff = time.time() - ACTIVE_USER_TIMEOUT_SECONDS
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM active_users WHERE last_seen < ?', (cutoff,))
    conn.commit()
    conn.close()

def get_active_users_for_sheet(sheet_id, exclude_session=None):
    """Get list of active user emails for a sheet, optionally excluding a session"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = conn.cursor()

    if exclude_session:
        cursor.execute('''
            SELECT email FROM active_users
            WHERE sheet_id = ? AND session_id != ?
        ''', (sheet_id, exclude_session))
    else:
        cursor.execute('''
            SELECT email FROM active_users WHERE sheet_id = ?
        ''', (sheet_id,))

    rows = cursor.fetchall()
    conn.close()
    return [row['email'] for row in rows]

def get_all_active_users_for_sheet(sheet_id):
    """Get all active users for a sheet (including current session)"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT email FROM active_users WHERE sheet_id = ?
    ''', (sheet_id,))
    rows = cursor.fetchall()
    conn.close()
    return [row['email'] for row in rows]

# Initialize database when module is imported
init_database()
