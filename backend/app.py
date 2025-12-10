from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import json
import os
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import database as db

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Version
BE_VERSION = '0.0.4'

# Configuration
GOOGLE_CLIENT_ID = '651831609522-bvrgmop9hmdghlrn2tqm1hv0dmkhu933.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

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
                'mahlaka': get_value('mahlaka')  # No default - must be found in headers
            }

            # Only add if we have at least a name or ma
            if member['firstName'] or member['lastName'] or member['ma']:
                members.append(member)

        return jsonify({
            'success': True,
            'members': members,
            'headers': headers
        })

    except HttpError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
# Main
# ============================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    print("Starting Attendance Manager Backend...")
    print(f"Server running on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
