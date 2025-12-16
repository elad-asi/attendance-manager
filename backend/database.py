import os
import psycopg
from psycopg.rows import dict_row
from datetime import datetime

# Database connection URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:Asi1Mic0@db.ykzjmngikwutzqlmzxzv.supabase.co:5432/postgres')

# For backwards compatibility with backup functions
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPT_DIR, 'data', 'backups')
MAX_BACKUPS = 10

def get_db_connection():
    """Get a database connection"""
    conn = psycopg.connect(DATABASE_URL)
    return conn

def get_dict_cursor(conn):
    """Get a cursor that returns dicts"""
    return conn.cursor(row_factory=dict_row)

# These functions are no longer applicable for PostgreSQL but kept for compatibility
def get_db_mtime():
    """Not applicable for PostgreSQL - always returns None"""
    return None

def validate_db_modified(before_mtime, operation_name):
    """Not applicable for PostgreSQL - always returns True"""
    return True

def create_backup():
    """Local backups not supported with PostgreSQL - use cloud backups instead"""
    print("Note: Local backups not supported with PostgreSQL. Use cloud backups.")
    return None

def cleanup_old_backups():
    """Not applicable for PostgreSQL"""
    pass

def list_backups():
    """Local backups not supported with PostgreSQL"""
    return []

def restore_backup(backup_filename):
    """Local backups not supported with PostgreSQL - use cloud backups"""
    return False, "Local backups not supported with PostgreSQL. Use cloud backups instead."

def _post_restore_sync_setup():
    """After restoring a backup, increment data version to force full reload"""
    increment_data_version()
    print("Post-restore: Incremented data version to force full reload on all clients")

def get_data_version():
    """Get the current data version number from database"""
    try:
        conn = get_db_connection()
        cursor = get_dict_cursor(conn)
        cursor.execute("SELECT version FROM data_version WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row['version']
    except Exception as e:
        print(f"get_data_version error (may be first run): {e}")
    return 1

def increment_data_version():
    """Increment data version to force all clients to full reload"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO data_version (id, version) VALUES (1, 2)
            ON CONFLICT (id) DO UPDATE SET version = data_version.version + 1
            RETURNING version
        """)
        result = cursor.fetchone()
        new_version = result[0] if result else 2
        conn.commit()
        print(f"Data version incremented to: {new_version}")
        return new_version
    except Exception as e:
        print(f"increment_data_version error: {e}")
        conn.rollback()
        return 1
    finally:
        conn.close()

def init_database():
    """Initialize the database schema - using spreadsheet_id (Google Sheet ID) as primary key"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Data version table for sync
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_version (
            id INTEGER PRIMARY KEY,
            version INTEGER DEFAULT 1
        )
    ''')

    # Initialize data version if not exists
    cursor.execute('''
        INSERT INTO data_version (id, version) VALUES (1, 1)
        ON CONFLICT (id) DO NOTHING
    ''')

    # Sheets table - spreadsheet_id (Google Sheet ID) is the PRIMARY KEY
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
            id SERIAL PRIMARY KEY,
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
            id SERIAL PRIMARY KEY,
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
    print("PostgreSQL database initialized successfully")

def migrate_old_data():
    """Migration not needed for fresh PostgreSQL database"""
    pass

def get_or_create_sheet(spreadsheet_id, sheet_name='', gdud='', pluga='', spreadsheet_title=''):
    """Get existing sheet or create a new one using spreadsheet_id as the key."""
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)

    # Check if sheet exists
    cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = %s', (spreadsheet_id,))
    row = cursor.fetchone()

    if row:
        # Update title if provided
        if spreadsheet_title:
            cursor.execute('''
                UPDATE sheets SET spreadsheet_title = %s, updated_at = %s WHERE spreadsheet_id = %s
            ''', (spreadsheet_title, datetime.now().isoformat(), spreadsheet_id))
            conn.commit()
    else:
        # Create new sheet entry
        cursor.execute('''
            INSERT INTO sheets (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga)
            VALUES (%s, %s, %s, %s, %s)
        ''', (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga))
        conn.commit()

    conn.close()
    return spreadsheet_id

def get_sheet_by_id(spreadsheet_id):
    """Get sheet info by spreadsheet_id (Google Sheet ID)"""
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)
    cursor.execute('SELECT * FROM sheets WHERE spreadsheet_id = %s', (spreadsheet_id,))
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
        UPDATE sheets SET start_date = %s, end_date = %s, updated_at = %s
        WHERE spreadsheet_id = %s
    ''', (start_date, end_date, datetime.now().isoformat(), spreadsheet_id))
    conn.commit()
    conn.close()

def save_team_members(spreadsheet_id, members):
    """Save team members for a sheet (replaces existing)"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete existing members for this sheet
    cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = %s', (spreadsheet_id,))

    # Insert new members
    for member in members:
        cursor.execute('''
            INSERT INTO team_members (spreadsheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
    cursor = get_dict_cursor(conn)
    cursor.execute('''
        SELECT first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai
        FROM team_members WHERE spreadsheet_id = %s
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
            'miktzoaTzvai': row.get('miktzoa_tzvai', '')
        })
    return members

def update_attendance(spreadsheet_id, ma, date, status, session_id=''):
    """Update attendance for a specific member and date"""
    conn = get_db_connection()
    cursor = conn.cursor()

    timestamp = datetime.now().isoformat()

    # PostgreSQL upsert syntax
    cursor.execute('''
        INSERT INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (spreadsheet_id, ma, date)
        DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, updated_by_session = EXCLUDED.updated_by_session
    ''', (spreadsheet_id, ma, date, status, timestamp, session_id))

    print(f"[SYNC DEBUG] Saved attendance: ma={ma}, date={date}, status={status}, session={session_id[:8] if session_id else 'none'}..., time={timestamp}")

    conn.commit()
    conn.close()

def get_attendance(spreadsheet_id):
    """Get all attendance data for a sheet"""
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)
    cursor.execute('''
        SELECT ma, date, status FROM attendance WHERE spreadsheet_id = %s
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
    cursor = get_dict_cursor(conn)

    try:
        if exclude_session_id:
            cursor.execute('''
                SELECT ma, date, status, updated_at, updated_by_session FROM attendance
                WHERE spreadsheet_id = %s AND updated_at > %s
                AND updated_by_session IS NOT NULL
                AND updated_by_session != ''
                AND updated_by_session != %s
            ''', (spreadsheet_id, since_timestamp, exclude_session_id))
        else:
            cursor.execute('''
                SELECT ma, date, status, updated_at FROM attendance
                WHERE spreadsheet_id = %s AND updated_at > %s
            ''', (spreadsheet_id, since_timestamp))

        rows = cursor.fetchall()

        print(f"[SYNC DEBUG] Query: since={since_timestamp}, exclude_session={exclude_session_id[:8] if exclude_session_id else 'none'}..., found={len(rows)} rows")

        conn.close()

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
        print(f"Warning: get_attendance_changes_since error: {e}")
        conn.close()
        return []

def get_server_timestamp():
    """Get current server timestamp in ISO format"""
    return datetime.now().isoformat()

def get_all_sheets():
    """Get list of all sheets"""
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)
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
    cursor.execute('DELETE FROM attendance WHERE spreadsheet_id = %s', (spreadsheet_id,))
    cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = %s', (spreadsheet_id,))
    cursor.execute('DELETE FROM active_users WHERE spreadsheet_id = %s', (spreadsheet_id,))
    cursor.execute('DELETE FROM sheets WHERE spreadsheet_id = %s', (spreadsheet_id,))
    conn.commit()
    conn.close()

# ============================================
# Active Users Functions (for multi-worker support)
# ============================================

ACTIVE_USER_TIMEOUT_SECONDS = 30

def update_active_user(session_id, email, spreadsheet_id, last_seen):
    """Update or insert an active user session"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO active_users (session_id, email, spreadsheet_id, last_seen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (session_id) DO UPDATE SET email = EXCLUDED.email, spreadsheet_id = EXCLUDED.spreadsheet_id, last_seen = EXCLUDED.last_seen
    ''', (session_id, email, spreadsheet_id, last_seen))
    conn.commit()
    conn.close()

def cleanup_inactive_users():
    """Remove users who haven't been seen recently"""
    import time
    cutoff = time.time() - ACTIVE_USER_TIMEOUT_SECONDS
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM active_users WHERE last_seen < %s', (cutoff,))
    conn.commit()
    conn.close()

def get_active_users_for_sheet(spreadsheet_id, exclude_session=None):
    """Get list of active user emails for a sheet, optionally excluding a session"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)

    if exclude_session:
        cursor.execute('''
            SELECT email FROM active_users
            WHERE spreadsheet_id = %s AND session_id != %s
        ''', (spreadsheet_id, exclude_session))
    else:
        cursor.execute('''
            SELECT email FROM active_users WHERE spreadsheet_id = %s
        ''', (spreadsheet_id,))

    rows = cursor.fetchall()
    conn.close()
    return [row['email'] for row in rows]

def get_all_active_users_for_sheet(spreadsheet_id):
    """Get all active users for a sheet (including current session)"""
    cleanup_inactive_users()
    conn = get_db_connection()
    cursor = get_dict_cursor(conn)
    cursor.execute('''
        SELECT email FROM active_users WHERE spreadsheet_id = %s
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
    cursor = get_dict_cursor(conn)
    cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = %s', (spreadsheet_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row['spreadsheet_id']
    return None

# Initialize database when module is imported
init_database()
