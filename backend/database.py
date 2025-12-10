import sqlite3
import os
import json
from datetime import datetime

DATABASE_FILE = 'data/attendance.db'

def get_db_connection():
    """Get a database connection with row factory"""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize the database schema"""
    os.makedirs('data', exist_ok=True)
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

    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_sheet ON attendance(sheet_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_attendance_ma ON attendance(ma)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_members_sheet ON team_members(sheet_id)')

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
            INSERT INTO team_members (sheet_id, first_name, last_name, ma, gdud, pluga, mahlaka)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            sheet_id,
            member.get('firstName', ''),
            member.get('lastName', ''),
            member.get('ma', ''),
            member.get('gdud', ''),
            member.get('pluga', ''),
            member.get('mahlaka', '')
        ))

    conn.commit()
    conn.close()

def get_team_members(sheet_id):
    """Get all team members for a sheet"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT first_name, last_name, ma, gdud, pluga, mahlaka
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
            'mahlaka': row['mahlaka']
        })
    return members

def update_attendance(sheet_id, ma, date, status):
    """Update attendance for a specific member and date"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO attendance (sheet_id, ma, date, status, updated_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (sheet_id, ma, date, status, datetime.now().isoformat()))

    conn.commit()
    conn.close()

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

# Initialize database when module is imported
init_database()
