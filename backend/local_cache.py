"""
Local SQLite Cache with Neon PostgreSQL Sync

This module provides a local SQLite cache for fast reads/writes,
with periodic background sync to Neon PostgreSQL.

Architecture:
- All reads/writes go to local SQLite (instant)
- Background thread syncs changes to Neon every N seconds
- On startup, pulls latest data from Neon to local cache
"""

import os
import sqlite3
import threading
import time
import psycopg
from psycopg.rows import dict_row
from datetime import datetime
from contextlib import contextmanager

# Configuration
SYNC_INTERVAL_SECONDS = 3  # Sync to Neon every 3 seconds
LOCAL_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'local_cache.db')
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://neondb_owner:npg_0h1CnwkqOjfi@ep-summer-rain-agc7n25e-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require')

# Track pending changes for sync
_pending_attendance = []  # List of (spreadsheet_id, ma, date, status, timestamp, session_id)
_pending_sheets = []  # List of (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga)
_pending_team_members = {}  # Dict of spreadsheet_id -> list of members
_pending_lock = threading.Lock()
_sync_thread = None
_sync_running = False

# Persistent Neon connection for sync thread (avoids 0.4s connection overhead per sync)
_neon_sync_conn = None
_neon_conn_lock = threading.Lock()

# ============================================
# Local SQLite Connection
# ============================================

def get_local_connection():
    """Get a connection to local SQLite cache"""
    os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(LOCAL_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# Alias for backwards compatibility with app.py
def get_db_connection():
    """Alias for get_local_connection - for backwards compatibility"""
    return get_local_connection()

@contextmanager
def local_db():
    """Context manager for local SQLite connection"""
    conn = get_local_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

# ============================================
# Neon PostgreSQL Connection
# ============================================

def get_neon_connection():
    """Get a new connection to Neon PostgreSQL"""
    return psycopg.connect(DATABASE_URL)

def get_neon_sync_connection():
    """Get or reuse persistent connection for sync thread (much faster)"""
    global _neon_sync_conn
    with _neon_conn_lock:
        if _neon_sync_conn is None or _neon_sync_conn.closed:
            print("[SYNC] Creating persistent Neon connection...")
            _neon_sync_conn = psycopg.connect(DATABASE_URL)
        return _neon_sync_conn

def get_neon_dict_cursor(conn):
    """Get a cursor that returns dicts"""
    return conn.cursor(row_factory=dict_row)

# ============================================
# Initialize Local Cache
# ============================================

def init_local_cache():
    """Initialize local SQLite schema"""
    with local_db() as conn:
        cursor = conn.cursor()

        # Data version for sync
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_version (
                id INTEGER PRIMARY KEY,
                version INTEGER DEFAULT 1
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO data_version (id, version) VALUES (1, 1)')

        # Sheets table
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

        # Team members table
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

        # Attendance table
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

        # Active users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS active_users (
                session_id TEXT PRIMARY KEY,
                email TEXT DEFAULT 'Anonymous',
                spreadsheet_id TEXT NOT NULL,
                last_seen REAL NOT NULL,
                FOREIGN KEY (spreadsheet_id) REFERENCES sheets(spreadsheet_id) ON DELETE CASCADE
            )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_spreadsheet ON attendance(spreadsheet_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_ma ON attendance(ma)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_members_spreadsheet ON team_members(spreadsheet_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_active_users_spreadsheet ON active_users(spreadsheet_id)')

    print("Local SQLite cache initialized")

# ============================================
# Pull from Neon (on startup)
# ============================================

def pull_from_neon():
    """Pull all data from Neon to local cache"""
    print("[SYNC] Pulling data from Neon to local cache...")
    try:
        neon_conn = get_neon_connection()
        neon_cursor = get_neon_dict_cursor(neon_conn)

        with local_db() as local_conn:
            local_cursor = local_conn.cursor()

            # Clear local cache
            local_cursor.execute('DELETE FROM attendance')
            local_cursor.execute('DELETE FROM team_members')
            local_cursor.execute('DELETE FROM active_users')
            local_cursor.execute('DELETE FROM sheets')

            # Pull sheets
            neon_cursor.execute('SELECT * FROM sheets')
            sheets = neon_cursor.fetchall()
            for sheet in sheets:
                local_cursor.execute('''
                    INSERT INTO sheets (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga, start_date, end_date, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sheet['spreadsheet_id'], sheet.get('spreadsheet_title', ''), sheet.get('sheet_name', ''),
                    sheet.get('gdud', ''), sheet.get('pluga', ''),
                    sheet.get('start_date', '2025-12-21'), sheet.get('end_date', '2026-02-01'),
                    sheet.get('created_at', ''), sheet.get('updated_at', '')
                ))

            # Pull team members
            neon_cursor.execute('SELECT * FROM team_members')
            members = neon_cursor.fetchall()
            for m in members:
                local_cursor.execute('''
                    INSERT INTO team_members (spreadsheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    m['spreadsheet_id'], m.get('first_name', ''), m.get('last_name', ''),
                    m.get('ma', ''), m.get('gdud', ''), m.get('pluga', ''),
                    m.get('mahlaka', ''), m.get('miktzoa_tzvai', ''), m.get('created_at', '')
                ))

            # Pull attendance
            neon_cursor.execute('SELECT * FROM attendance')
            attendance = neon_cursor.fetchall()
            for a in attendance:
                local_cursor.execute('''
                    INSERT INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    a['spreadsheet_id'], a['ma'], a['date'], a['status'],
                    a.get('updated_at', ''), a.get('updated_by_session', '')
                ))

            # Pull data version
            neon_cursor.execute('SELECT version FROM data_version WHERE id = 1')
            row = neon_cursor.fetchone()
            if row:
                local_cursor.execute('UPDATE data_version SET version = ? WHERE id = 1', (row['version'],))

        neon_conn.close()
        print(f"[SYNC] Pulled {len(sheets)} sheets, {len(members)} members, {len(attendance)} attendance records")
        return True
    except Exception as e:
        print(f"[SYNC ERROR] Failed to pull from Neon: {e}")
        return False

# ============================================
# Push to Neon (periodic sync)
# ============================================

def push_pending_to_neon():
    """Push all pending changes to Neon using persistent connection"""
    global _pending_attendance, _pending_sheets, _pending_team_members, _neon_sync_conn

    # Get all pending items
    with _pending_lock:
        pending_attendance = _pending_attendance.copy()
        _pending_attendance = []
        pending_sheets = _pending_sheets.copy()
        _pending_sheets = []
        pending_team_members = dict(_pending_team_members)
        _pending_team_members = {}

    if not pending_attendance and not pending_sheets and not pending_team_members:
        return

    try:
        # Use persistent connection (avoids 0.4s connection overhead)
        conn = get_neon_sync_connection()
        cursor = conn.cursor()

        # Sync sheets first
        if pending_sheets:
            for sheet_data in pending_sheets:
                cursor.execute('''
                    INSERT INTO sheets (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (spreadsheet_id) DO UPDATE SET
                        spreadsheet_title = EXCLUDED.spreadsheet_title,
                        updated_at = CURRENT_TIMESTAMP
                ''', sheet_data)
            print(f"[SYNC] Pushed {len(pending_sheets)} sheets to Neon")

        # Sync team members
        if pending_team_members:
            for spreadsheet_id, members in pending_team_members.items():
                cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = %s', (spreadsheet_id,))
                for member in members:
                    cursor.execute('''
                        INSERT INTO team_members (spreadsheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        spreadsheet_id,
                        member.get('firstName', ''), member.get('lastName', ''), member.get('ma', ''),
                        member.get('gdud', ''), member.get('pluga', ''),
                        member.get('mahlaka', ''), member.get('miktzoaTzvai', '')
                    ))
            print(f"[SYNC] Pushed team members for {len(pending_team_members)} sheets to Neon")

        # Sync attendance
        if pending_attendance:
            cursor.executemany('''
                INSERT INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (spreadsheet_id, ma, date)
                DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, updated_by_session = EXCLUDED.updated_by_session
            ''', pending_attendance)
            print(f"[SYNC] Pushed {len(pending_attendance)} attendance records to Neon")

        conn.commit()
        # Don't close - keep connection open for reuse
    except Exception as e:
        print(f"[SYNC ERROR] Failed to push to Neon: {e}")
        # Reset connection on error so it gets recreated
        with _neon_conn_lock:
            if _neon_sync_conn:
                try:
                    _neon_sync_conn.close()
                except:
                    pass
                _neon_sync_conn = None
        # Re-add failed items to pending queue
        with _pending_lock:
            _pending_attendance = pending_attendance + _pending_attendance
            _pending_sheets = pending_sheets + _pending_sheets
            for sid, members in pending_team_members.items():
                if sid not in _pending_team_members:
                    _pending_team_members[sid] = members

def _sync_loop():
    """Background sync loop"""
    global _sync_running
    while _sync_running:
        time.sleep(SYNC_INTERVAL_SECONDS)
        if _sync_running:
            push_pending_to_neon()

def start_sync_thread():
    """Start the background sync thread"""
    global _sync_thread, _sync_running
    if _sync_thread is None or not _sync_thread.is_alive():
        _sync_running = True
        _sync_thread = threading.Thread(target=_sync_loop, daemon=True)
        _sync_thread.start()
        print(f"[SYNC] Background sync started (every {SYNC_INTERVAL_SECONDS}s)")

def stop_sync_thread():
    """Stop the background sync thread"""
    global _sync_running, _neon_sync_conn
    _sync_running = False
    # Push any remaining changes
    push_pending_to_neon()
    # Close persistent connection
    with _neon_conn_lock:
        if _neon_sync_conn:
            try:
                _neon_sync_conn.close()
            except:
                pass
            _neon_sync_conn = None

# ============================================
# Database API (uses local cache)
# ============================================

def get_data_version():
    """Get the current data version number"""
    try:
        with local_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version FROM data_version WHERE id = 1")
            row = cursor.fetchone()
            if row:
                return row['version']
    except Exception as e:
        print(f"get_data_version error: {e}")
    return 1

def increment_data_version():
    """Increment data version"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE data_version SET version = version + 1 WHERE id = 1")
        cursor.execute("SELECT version FROM data_version WHERE id = 1")
        row = cursor.fetchone()
        new_version = row['version'] if row else 2

    # Also update Neon
    try:
        neon_conn = get_neon_connection()
        neon_cursor = neon_conn.cursor()
        neon_cursor.execute("""
            INSERT INTO data_version (id, version) VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET version = %s
        """, (new_version, new_version))
        neon_conn.commit()
        neon_conn.close()
    except Exception as e:
        print(f"increment_data_version Neon sync error: {e}")

    print(f"Data version incremented to: {new_version}")
    return new_version

def get_server_timestamp():
    """Get current server timestamp"""
    return datetime.now().isoformat()

def get_or_create_sheet(spreadsheet_id, sheet_name='', gdud='', pluga='', spreadsheet_title=''):
    """Get or create sheet in local cache (Neon sync happens in background)"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
        row = cursor.fetchone()

        if row:
            if spreadsheet_title:
                cursor.execute('UPDATE sheets SET spreadsheet_title = ?, updated_at = ? WHERE spreadsheet_id = ?',
                              (spreadsheet_title, datetime.now().isoformat(), spreadsheet_id))
        else:
            cursor.execute('''
                INSERT INTO sheets (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga)
                VALUES (?, ?, ?, ?, ?)
            ''', (spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga))

    # Queue for background Neon sync (no blocking!)
    with _pending_lock:
        _pending_sheets.append((spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga))

    return spreadsheet_id

def get_sheet_by_id(spreadsheet_id):
    """Get sheet info from local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None

def get_all_sheets():
    """Get all sheets from local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT spreadsheet_id, spreadsheet_title, sheet_name, gdud, pluga, start_date, end_date, created_at
            FROM sheets ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

def update_sheet_dates(spreadsheet_id, start_date, end_date):
    """Update date range in local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE sheets SET start_date = ?, end_date = ?, updated_at = ? WHERE spreadsheet_id = ?',
                      (start_date, end_date, datetime.now().isoformat(), spreadsheet_id))

    # Sync to Neon
    try:
        neon_conn = get_neon_connection()
        neon_cursor = neon_conn.cursor()
        neon_cursor.execute('UPDATE sheets SET start_date = %s, end_date = %s, updated_at = %s WHERE spreadsheet_id = %s',
                           (start_date, end_date, datetime.now().isoformat(), spreadsheet_id))
        neon_conn.commit()
        neon_conn.close()
    except Exception as e:
        print(f"update_sheet_dates Neon sync error: {e}")

def save_team_members(spreadsheet_id, members):
    """Save team members to local cache (Neon sync happens in background)"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = ?', (spreadsheet_id,))
        for member in members:
            cursor.execute('''
                INSERT INTO team_members (spreadsheet_id, first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                spreadsheet_id,
                member.get('firstName', ''), member.get('lastName', ''), member.get('ma', ''),
                member.get('gdud', ''), member.get('pluga', ''),
                member.get('mahlaka', ''), member.get('miktzoaTzvai', '')
            ))

    # Queue for background Neon sync (no blocking!)
    with _pending_lock:
        _pending_team_members[spreadsheet_id] = members

def get_team_members(spreadsheet_id):
    """Get team members from local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai
            FROM team_members WHERE spreadsheet_id = ?
        ''', (spreadsheet_id,))
        rows = cursor.fetchall()

        members = []
        for row in rows:
            members.append({
                'firstName': row['first_name'],
                'lastName': row['last_name'],
                'ma': row['ma'],
                'gdud': row['gdud'],
                'pluga': row['pluga'],
                'mahlaka': row['mahlaka'],
                'miktzoaTzvai': row['miktzoa_tzvai'] or ''
            })
        return members

def update_attendance(spreadsheet_id, ma, date, status, session_id=''):
    """Update attendance in local cache and queue for Neon sync"""
    timestamp = datetime.now().isoformat()

    # Update local cache immediately
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (spreadsheet_id, ma, date)
            DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at, updated_by_session = excluded.updated_by_session
        ''', (spreadsheet_id, ma, date, status, timestamp, session_id))

    # Queue for Neon sync
    with _pending_lock:
        _pending_attendance.append((spreadsheet_id, ma, date, status, timestamp, session_id))

    print(f"[LOCAL] Saved attendance: ma={ma}, date={date}, status={status}")

def update_attendance_batch(spreadsheet_id, updates, session_id=''):
    """Update multiple attendance records"""
    if not updates:
        return

    timestamp = datetime.now().isoformat()

    # Update local cache
    with local_db() as conn:
        cursor = conn.cursor()
        for u in updates:
            cursor.execute('''
                INSERT INTO attendance (spreadsheet_id, ma, date, status, updated_at, updated_by_session)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (spreadsheet_id, ma, date)
                DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at, updated_by_session = excluded.updated_by_session
            ''', (spreadsheet_id, u['ma'], u['date'], u['status'], timestamp, session_id))

    # Queue for Neon sync
    with _pending_lock:
        for u in updates:
            _pending_attendance.append((spreadsheet_id, u['ma'], u['date'], u['status'], timestamp, session_id))

    print(f"[LOCAL] Batch saved {len(updates)} attendance records")

def get_attendance(spreadsheet_id):
    """Get attendance from local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT ma, date, status FROM attendance WHERE spreadsheet_id = ?', (spreadsheet_id,))
        rows = cursor.fetchall()

        attendance_data = {}
        for row in rows:
            ma = row['ma']
            if ma not in attendance_data:
                attendance_data[ma] = {}
            attendance_data[ma][row['date']] = row['status']
        return attendance_data

def get_full_sheet_data(spreadsheet_id):
    """Get full sheet data from local cache - very fast!"""
    with local_db() as conn:
        cursor = conn.cursor()

        # Get sheet
        cursor.execute('SELECT * FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
        sheet_row = cursor.fetchone()
        sheet = dict(sheet_row) if sheet_row else None

        if not sheet:
            return None, [], {}

        # Get team members
        cursor.execute('''
            SELECT first_name, last_name, ma, gdud, pluga, mahlaka, miktzoa_tzvai
            FROM team_members WHERE spreadsheet_id = ?
        ''', (spreadsheet_id,))
        member_rows = cursor.fetchall()

        members = []
        for row in member_rows:
            members.append({
                'firstName': row['first_name'],
                'lastName': row['last_name'],
                'ma': row['ma'],
                'gdud': row['gdud'],
                'pluga': row['pluga'],
                'mahlaka': row['mahlaka'],
                'miktzoaTzvai': row['miktzoa_tzvai'] or ''
            })

        # Get attendance
        cursor.execute('SELECT ma, date, status FROM attendance WHERE spreadsheet_id = ?', (spreadsheet_id,))
        attendance_rows = cursor.fetchall()

        attendance_data = {}
        for row in attendance_rows:
            ma = row['ma']
            if ma not in attendance_data:
                attendance_data[ma] = {}
            attendance_data[ma][row['date']] = row['status']

        return sheet, members, attendance_data

def get_attendance_changes_since(spreadsheet_id, since_timestamp, exclude_session_id=''):
    """Get attendance changes since a timestamp"""
    with local_db() as conn:
        cursor = conn.cursor()

        if exclude_session_id:
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

        changes = []
        for row in rows:
            changes.append({
                'ma': row['ma'],
                'date': row['date'],
                'status': row['status'],
                'updated_at': row['updated_at']
            })
        return changes

def delete_sheet(spreadsheet_id):
    """Delete a sheet from local cache and Neon"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM attendance WHERE spreadsheet_id = ?', (spreadsheet_id,))
        cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = ?', (spreadsheet_id,))
        cursor.execute('DELETE FROM active_users WHERE spreadsheet_id = ?', (spreadsheet_id,))
        cursor.execute('DELETE FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))

    # Sync to Neon
    try:
        neon_conn = get_neon_connection()
        neon_cursor = neon_conn.cursor()
        neon_cursor.execute('DELETE FROM attendance WHERE spreadsheet_id = %s', (spreadsheet_id,))
        neon_cursor.execute('DELETE FROM team_members WHERE spreadsheet_id = %s', (spreadsheet_id,))
        neon_cursor.execute('DELETE FROM active_users WHERE spreadsheet_id = %s', (spreadsheet_id,))
        neon_cursor.execute('DELETE FROM sheets WHERE spreadsheet_id = %s', (spreadsheet_id,))
        neon_conn.commit()
        neon_conn.close()
    except Exception as e:
        print(f"delete_sheet Neon sync error: {e}")

# ============================================
# Active Users (local only for speed)
# ============================================

ACTIVE_USER_TIMEOUT_SECONDS = 30

def update_active_user(session_id, email, spreadsheet_id, last_seen):
    """Update active user in local cache"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO active_users (session_id, email, spreadsheet_id, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (session_id) DO UPDATE SET email = excluded.email, spreadsheet_id = excluded.spreadsheet_id, last_seen = excluded.last_seen
        ''', (session_id, email, spreadsheet_id, last_seen))

def cleanup_inactive_users():
    """Remove inactive users"""
    cutoff = time.time() - ACTIVE_USER_TIMEOUT_SECONDS
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM active_users WHERE last_seen < ?', (cutoff,))

def get_active_users_for_sheet(spreadsheet_id, exclude_session=None):
    """Get active users for a sheet"""
    cleanup_inactive_users()
    with local_db() as conn:
        cursor = conn.cursor()
        if exclude_session:
            cursor.execute('SELECT email FROM active_users WHERE spreadsheet_id = ? AND session_id != ?',
                          (spreadsheet_id, exclude_session))
        else:
            cursor.execute('SELECT email FROM active_users WHERE spreadsheet_id = ?', (spreadsheet_id,))
        rows = cursor.fetchall()
        return [row['email'] for row in rows]

def get_all_active_users_for_sheet(spreadsheet_id):
    """Get all active users"""
    cleanup_inactive_users()
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT email FROM active_users WHERE spreadsheet_id = ?', (spreadsheet_id,))
        rows = cursor.fetchall()
        return [row['email'] for row in rows]

def check_spreadsheet_exists(spreadsheet_id):
    """Check if spreadsheet exists"""
    with local_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT spreadsheet_id FROM sheets WHERE spreadsheet_id = ?', (spreadsheet_id,))
        row = cursor.fetchone()
        if row:
            return row['spreadsheet_id']
    return None

# ============================================
# Pending Sync Status
# ============================================

def get_pending_sync_count():
    """Get the total number of pending changes waiting to sync to Neon"""
    with _pending_lock:
        count = len(_pending_attendance)
        count += len(_pending_sheets)
        count += sum(len(members) for members in _pending_team_members.values())
        return count

def force_sync_now():
    """Force an immediate sync to Neon"""
    push_pending_to_neon()

# ============================================
# Startup
# ============================================

def initialize():
    """Initialize local cache and start sync"""
    init_local_cache()
    pull_from_neon()
    start_sync_thread()
    print("[LOCAL CACHE] Initialized with Neon sync")

# Backwards compatibility stubs
def init_database():
    """Backwards compatibility - calls initialize()"""
    initialize()

def migrate_old_data():
    """Not needed"""
    pass

def create_backup():
    """Not supported locally"""
    return None

def cleanup_old_backups():
    pass

def list_backups():
    return []

def restore_backup(backup_filename):
    return False, "Use cloud backups instead"

def _post_restore_sync_setup():
    increment_data_version()

def get_db_mtime():
    return None

def validate_db_modified(before_mtime, operation_name):
    return True

# Initialize on module import (like old database.py)
initialize()
