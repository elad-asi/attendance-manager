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
            print(f"Deleted old backup: {os.path.basename(old_backup)}")
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
    """Initialize the database schema - using spreadsheet_id (Google Sheet ID) as primary key"""
    os.makedirs(os.path.join(SCRIPT_DIR, 'data'), exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()

    # Sheets table - spreadsheet_id (Google Sheet ID) is the PRIMARY KEY
    # This guarantees ONE and ONLY ONE entry per Google Sheet
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sheets (
            spreadsheet_id TEXT PRIMARY KEY,
            spreadsheet_title TEXT DEFAULT '',
            sheet_name TEXT DEFAULT '',
            gdud TEXT DEFAULT '',
            pluga TEXT DEFAULT '',
            start_date TEXT DEFAULT '2025-12-21',
            end_date TEXT DEFAULT '2026-02-01',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Team members table - linked directly to spreadsheet_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS team_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spreadsheet_id TEXT NOT NULL,
            first_name TEXT DEFAULT '',
            last_name TEXT DEFAULT '',
            ma TEXT DEFAULT '',
            gdud TEXT DEFAULT '',
            pluga TEXT DEFAULT '',
            mahlaka TEXT DEFAULT '',
            miktzoa_tzvai TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (spreadsheet_id) REFERENCES sheets(spreadsheet_id) ON DELETE CASCADE
        )
    ''')

    # Attendance table - linked directly to spreadsheet_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS attendance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spreadsheet_id TEXT NOT NULL,
            ma TEXT NOT NULL,
            date TEXT NOT NULL,
            status TEXT DEFAULT 'unmarked',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_by_session TEXT DEFAULT '',
            FOREIGN KEY (spreadsheet_id) REFERENCES sheets(spreadsheet_id) ON DELETE CASCADE,
            UNIQUE(spreadsheet_id, ma, date)
        )
    ''')

    # Add updated_by_session column if it doesn't exist (migration)
    try:
        cursor.execute('ALTER TABLE attendance ADD COLUMN updated_by_session TEXT DEFAULT ""')
        conn.commit()
        print("Migration: Added updated_by_session column to attendance table")
    except Exception as e:
        if 'duplicate column' in str(e).lower() or 'already exists' in str(e).lower():
            pass  # Column already exists
        else:
            print(f"Migration warning (may be harmless): {e}")

    # Active users table - linked directly to spreadsheet_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_users (
            session_id TEXT PRIMARY KEY,
            email TEXT DEFAULT 'Anonymous',
            spreadsheet_id TEXT NOT NULL,
            last_seen REAL NOT NULL,
            FOREIGN KEY (spreadsheet_id) REFERENCES sheets(spreadsheet_id) ON DELETE CASCADE
        )
    ''')

    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_spreadsheet ON attendance(spreadsheet_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_ma ON attendance(ma)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_members_spreadsheet ON team_members(spreadsheet_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_active_users_spreadsheet ON active_users(spreadsheet_id)')

    conn.commit()
    conn.close()

def migrate_old_data():
    """Migrate data from old schema (with sheet_id) to new schema (with spreadsheet_id)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if old tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sheets'")
    if not cursor.fetchone():
        conn.close()
        return

    # Check if old schema has 'id' column (old format)
    cursor.execute("PRAGMA table_info(sheets)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'id' in columns and 'sheet_id' in [col[1] for col in cursor.execute("PRAGMA table_info(attendance)").fetchall()]:
        print("Migrating old data to new schema...")

        # Get all sheets with their old IDs and spreadsheet_ids
        cursor.execute("SELECT id, spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga, start_date, end_date FROM sheets")
        old_sheets = cursor.fetchall()

        for old_sheet in old_sheets:
            old_id = old_sheet['id']
            spreadsheet_id = old_sheet['spreadsheet_id']

            # Update attendance records
            cursor.execute('''
                UPDATE attendance SET spreadsheet_id = ? WHERE sheet_id = ?
            ''', (spreadsheet_id, old_id))

            # Update team_members records
            cursor.execute('''
                UPDATE team_members SET spreadsheet_id = ? WHERE sheet_id = ?
            ''', (spreadsheet_id, old_id))

            # Update active_users records
            cursor.execute('''
                UPDATE active_users SET spreadsheet_id = ? WHERE sheet_id = ?
            ''', (spreadsheet_id, old_id))

        conn.commit()
        print("Migration completed")

    conn.close()

def get_or_create_sheet(spreadsheet_id, sheet_name='', gdud='', pluga='', spreadsheet_title=''):
    """Get existing sheet or create a new one using spreadsheet_id as the key.

    Returns spreadsheet_id (Google Sheet ID) - this IS the identifier.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if sheet exists
    cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
    row = cursor.fetchone()

    if row:
        # Update title if provided
        if spreadsheet_title:
            cursor.execute('''
                UPDATE sheets SET spreadsheet_title = ?, updated_at = ? WHERE spreadsheet_id = ?
            ''', (spreadsheet_title, datetime.now().isoformat(), spreadsheet_id))
            conn.commit()
    else:
        # Create new sheet entry
        cursor.execute('''
            INSERT INTO sheets (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga)
            VALUES (?, ?, ?, ?, ?)
        ''', (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga))
        conn.commit()

    conn.close()
    return spreadsheet_id  # Return the Google Sheet ID directly

def get_sheet_by_id(spreadsheet_id):
    """Get sheet info by spreadsheet_id (Google Sheet ID)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def update_sheet_dates(spreadsheet_id, start_date, end_date):
    """Update date range for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sheets SET start_date = ?, end_date = ?, updated_at = ?
        WHERE spreadsheet_id = ?
    ''', (start_date, end_date, datetime.now().isoformat(), spreadsheet_id))
    conn.commit()
    conn.close()

def save_team_members(spreadsheet_id, members):
    """Save team members for a sheet (replaces existing)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete existing members for this sheet
    cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = ?', (spreadsheet_id,))

    # Insert new members
    for member in members:
        cursor.execute('''
            INSERT INTO team_members (spreadsheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            spreadsheet_id,
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

def get_team_members(spreadsheet_id):
    """Get all team members for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai
        FROM team_members WHERE spreadsheet_id = ?
    ''', (spreadsheet_id,))
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

def update_attendance(spreadsheet_id, ma, date, status, session_id=''):
    """Update attendance for a specific member and date"""
    # Get timestamp before operation for validation
    before_mtime = get_db_mtime()

    # Create backup before making changes
    create_backup()

    conn = get_db_connection()
    cursor = conn.cursor()

    timestamp = datetime.now().isoformat()

    try:
        # Try with updated_by_session column
        cursor.execute('''
            INSERT OR REPLACE INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (spreadsheet_id, ma, date, status, timestamp, session_id))
        print(f"[SYNC DEBUG] Saved attendance: ma={ma}, date={date}, status={status}, session={session_id[:8] if session_id else 'none'}..., time={timestamp}")
    except Exception as e:
        # Fallback without updated_by_session if column doesn't exist
        print(f"Warning: Falling back to update without session_id: {e}")
        cursor.execute('''
            INSERT OR REPLACE INTO attendance (spreadsheet_id, ma, date, status, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (spreadsheet_id, ma, date, status, timestamp))

    conn.commit()
    conn.close()

    # Validate DB was modified
    validate_db_modified(before_mtime, f"update_attendance(ma={ma}, date={date}, status={status})")

def get_attendance(spreadsheet_id):
    """Get all attendance data for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ma, date, status FROM attendance WHERE spreadsheet_id = ?
    ''', (spreadsheet_id,))
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

def get_attendance_changes_since(spreadsheet_id, since_timestamp, exclude_session_id=''):
    """Get attendance changes since a timestamp, optionally excluding changes by a specific session"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if exclude_session_id:
            # Only return changes made by OTHER sessions (not empty/null session IDs - those are old data)
            # This prevents returning all old records that don't have session tracking yet
            cursor.execute('''
                SELECT ma, date, status, updated_at, updated_by_session FROM attendance
                WHERE spreadsheet_id = ? AND updated_at > ?
                AND updated_by_session IS NOT NULL
                AND updated_by_session != ''
                AND updated_by_session != ?
            ''', (spreadsheet_id, since_timestamp, exclude_session_id))
        else:
            cursor.execute('''
                SELECT ma, date, status, updated_at FROM attendance
                WHERE spreadsheet_id = ? AND updated_at > ?
            ''', (spreadsheet_id, since_timestamp))

        rows = cursor.fetchall()

        # Debug: log what we found
        print(f"[SYNC DEBUG] Query: since={since_timestamp}, exclude_session={exclude_session_id[:8] if exclude_session_id else 'none'}..., found={len(rows)} rows")

        conn.close()

        # Return as list of changes with metadata
        changes = []
        for row in rows:
            changes.append({
                'ma': row['ma'],
                'date': row['date'],
                'status': row['status'],
                'updated_at': row['updated_at']
            })

        return changes

    except Exception as e:
        # Fallback if updated_by_session column doesn't exist yet - return empty (no incremental updates)
        print(f"Warning: updated_by_session column may not exist, returning empty changes: {e}")
        conn.close()
        return []  # No incremental updates possible without the column

def get_server_timestamp():
    """Get current server timestamp in ISO format"""
    return datetime.now().isoformat()

def get_all_sheets():
    """Get list of all sheets"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga, start_date, end_date, created_at
        FROM sheets ORDER BY created_at DESC
    ''')
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def delete_sheet(spreadsheet_id):
    """Delete a sheet and all its data"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM attendance WHERE spreadsheet_id = ?', (spreadsheet_id,))
    cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = ?', (spreadsheet_id,))
    cursor.execute('DELETE FROM active_users WHERE spreadsheet_id = ?', (spreadsheet_id,))
    cursor.execute('DELETE FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
    conn.commit()
    conn.close()

# ============================================
# Active Users Functions (for multi-worker support)
# ============================================

ACTIVE_USER_TIMEOUT_SECONDS = 30  # Consider user inactive after 30 seconds

def update_active_user(session_id, email, spreadsheet_id, last_seen):
    """Update or insert an active user session"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO active_users (session_id, email, spreadsheet_id, last_seen)
        VALUES (?, ?, ?, ?)
    ''', (session_id, email, spreadsheet_id, last_seen))
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

def get_active_users_for_sheet(spreadsheet_id, exclude_session=None):
    """Get list of active user emails for a sheet, optionally excluding a session"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = conn.cursor()

    if exclude_session:
        cursor.execute('''
            SELECT email FROM active_users
            WHERE spreadsheet_id = ? AND session_id != ?
        ''', (spreadsheet_id, exclude_session))
    else:
        cursor.execute('''
            SELECT email FROM active_users WHERE spreadsheet_id = ?
        ''', (spreadsheet_id,))

    rows = cursor.fetchall()
    conn.close()
    return [row['email'] for row in rows]

def get_all_active_users_for_sheet(spreadsheet_id):
    """Get all active users for a sheet (including current session)"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT email FROM active_users WHERE spreadsheet_id = ?
    ''', (spreadsheet_id,))
    rows = cursor.fetchall()
    conn.close()
    return [row['email'] for row in rows]

# ============================================
# Backwards compatibility functions
# ============================================

def get_sheet_id_by_google_identifiers(spreadsheet_id, sheet_name='', gdud='', pluga=''):
    """Get sheet by Google spreadsheet_id - returns spreadsheet_id itself for backwards compatibility"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row['spreadsheet_id']
    return None

# Initialize database when module is imported
init_database()
print("Database initialized with spreadsheet_id as primary key")
