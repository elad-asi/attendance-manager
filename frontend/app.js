// ============================================
// Attendance Manager - Frontend JavaScript
// ============================================

// Version
const FE_VERSION = '0.3.0';

// API Base URL
const API_BASE = '/api';

// State management
let teamMembers = [];
let attendanceData = {};
let startDate = new Date('2025-12-21');
let endDate = new Date('2026-02-01');

// Current sheet ID (for multi-user support)
let currentSheetId = null;

// Filter state - now supports multi-select (arrays)
let filters = {
    gdud: [],
    pluga: [],
    mahlaka: []
};

// Current active unit tab
let activeUnitTab = 'all';

// Special constant for empty filter value
const EMPTY_FILTER_VALUE = 'ריק';

// Sort state
let sortConfig = {
    field: null,  // 'firstName', 'lastName', 'ma', 'gdud', 'pluga', 'mahlaka'
    direction: 'asc'  // 'asc' or 'desc'
};

// Google OAuth State
let tokenClient = null;
let accessToken = null;
let currentSpreadsheetId = null;

// Google OAuth Configuration
const GOOGLE_CLIENT_ID = '651831609522-bvrgmop9hmdghlrn2tqm1hv0dmkhu933.apps.googleusercontent.com';
const SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly';

// Attendance statuses
const STATUSES = ['unmarked', 'present', 'absent', 'arriving', 'leaving', 'counted'];
const STATUS_LABELS = {
    'unmarked': '',
    'present': '✓',
    'absent': '✗',
    'arriving': '+',
    'leaving': '-',
    'counted': '✓'
};

// Hebrew tooltips for each status
const STATUS_TOOLTIPS = {
    'unmarked': 'לא סומן - לחץ לשינוי',
    'present': 'נוכח (✓)',
    'absent': 'נעדר (✗)',
    'arriving': 'מגיע (+)',
    'leaving': 'יוצא (-)',
    'counted': 'חופש (✓)'
};

// Define which statuses count for each total
const TOTALS_CONFIG = {
    mission: ['present', 'arriving'],
    includeLeave: ['present', 'arriving', 'leaving'],
    counted: ['present', 'arriving', 'leaving', 'counted']
};

// Hebrew strings
const STRINGS = {
    index: '#',
    firstName: 'שם פרטי',
    lastName: 'שם משפחה',
    misparIshi: 'מ.א',
    gdud: 'גדוד',
    pluga: 'פלוגה',
    mahlaka: 'מחלקה',
    miktzoaTzvai: 'מקצוע צבאי',
    dorech: 'דורך',
    yamam: 'ימ"מ',
    totalMission: 'דורך',
    totalIncludeLeave: 'דו"ח 1',
    totalCounted: 'ימ"מ'
};

// ============================================
// Toast Notification Functions
// ============================================

let toastTimeout = null;

function showSaveToast(message = 'נשמר בשרת', isError = false) {
    const toast = document.getElementById('saveToast');
    if (!toast) return;

    // Clear any existing timeout
    if (toastTimeout) {
        clearTimeout(toastTimeout);
    }

    toast.textContent = message;
    toast.classList.remove('show', 'error');

    if (isError) {
        toast.classList.add('error');
    }

    // Force reflow for animation
    void toast.offsetWidth;

    toast.classList.add('show');

    // Hide after 2 seconds
    toastTimeout = setTimeout(() => {
        toast.classList.remove('show');
    }, 2000);
}

// ============================================
// API Functions
// ============================================

async function apiGet(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`);
    return response.json();
}

async function apiPost(endpoint, data) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });
    return response.json();
}

async function apiDelete(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
        method: 'DELETE'
    });
    return response.json();
}

async function loadBackendVersion() {
    try {
        const data = await apiGet('/version');
        document.getElementById('beVersion').textContent = `BE: ${data.version}`;
    } catch (error) {
        document.getElementById('beVersion').textContent = 'BE: N/A';
    }
}

// ============================================
// Sheet Management Functions
// ============================================

function updateSheetUI() {
    const refreshBtn = document.getElementById('refreshData');

    if (currentSheetId) {
        refreshBtn.style.display = 'inline-block';
    } else {
        refreshBtn.style.display = 'none';
    }
}

async function refreshDataFromBackend() {
    if (!currentSheetId) {
        alert('אין גיליון נטען');
        return;
    }

    const refreshBtn = document.getElementById('refreshData');
    refreshBtn.disabled = true;
    refreshBtn.textContent = '🔄 טוען...';

    try {
        const response = await apiGet(`/sheets/${currentSheetId}`);

        if (response.error) {
            throw new Error(response.error);
        }

        // Update local state
        teamMembers = response.teamMembers || [];
        attendanceData = response.attendanceData || {};

        if (response.sheet) {
            startDate = new Date(response.sheet.start_date);
            endDate = new Date(response.sheet.end_date);
            document.getElementById('startDate').value = response.sheet.start_date;
            document.getElementById('endDate').value = response.sheet.end_date;
        }

        renderTable();
        showSheetsStatus('הנתונים רועננו בהצלחה!', 'success');

    } catch (error) {
        console.error('Error refreshing data:', error);
        showSheetsStatus('שגיאה ברענון הנתונים: ' + error.message, 'error');
    } finally {
        refreshBtn.disabled = false;
        refreshBtn.textContent = '🔄 רענן נתונים';
    }
}

function disconnectCurrentSheet() {
    if (!confirm('האם אתה בטוח שברצונך לנתק את הגיליון הנוכחי? הנתונים יישארו שמורים בשרת.')) {
        return;
    }

    // Clear local state
    currentSheetId = null;
    currentSpreadsheetId = null;
    teamMembers = [];
    attendanceData = {};

    // Clear localStorage
    localStorage.removeItem('current_sheet_id');
    localStorage.removeItem('current_spreadsheet_id');
    localStorage.removeItem('current_sheet_info');
    localStorage.removeItem('skipped_columns');
    localStorage.removeItem('permanently_skipped_columns');
    skippedColumns = [];
    permanentlySkippedColumns = [];

    // Also sign out from Google
    accessToken = null;
    localStorage.removeItem('google_access_token');
    localStorage.removeItem('google_user_info');
    updateAuthUI(false);

    // Reset UI
    document.getElementById('sheetsUrl').value = '';
    document.getElementById('sheetSelectRow').style.display = 'none';
    document.getElementById('gdudPlugaRow').style.display = 'none';
    document.getElementById('loadFromSheets').style.display = 'none';
    document.getElementById('inputGdud').value = '';
    document.getElementById('inputPluga').value = '';
    document.getElementById('fetchSheetsBtn').disabled = true;

    // Update UI
    updateSheetUI();
    renderTable();
    showSheetsStatus('הגיליון נותק בהצלחה', 'success');
}

// ============================================
// Data Loading from Backend
// ============================================

async function loadFromBackend() {
    try {
        // Load permanently skipped columns (from mapping - no data loaded for these)
        const storedPermanentlySkipped = localStorage.getItem('permanently_skipped_columns');
        if (storedPermanentlySkipped) {
            permanentlySkippedColumns = JSON.parse(storedPermanentlySkipped);
        }

        // Load skipped columns preference (includes both permanent and user-toggled)
        const storedSkippedColumns = localStorage.getItem('skipped_columns');
        if (storedSkippedColumns) {
            skippedColumns = JSON.parse(storedSkippedColumns);
        }

        // Check if we have a stored sheet ID
        const storedSheetId = localStorage.getItem('current_sheet_id');
        if (storedSheetId) {
            currentSheetId = parseInt(storedSheetId);

            // Load sheet data
            const response = await apiGet(`/sheets/${currentSheetId}`);

            if (response.error) {
                // Sheet not found, clear storage
                localStorage.removeItem('current_sheet_id');
                localStorage.removeItem('current_sheet_info');
                currentSheetId = null;
            } else {
                teamMembers = response.teamMembers || [];
                attendanceData = response.attendanceData || {};

                if (response.sheet) {
                    startDate = new Date(response.sheet.start_date);
                    endDate = new Date(response.sheet.end_date);
                    document.getElementById('startDate').value = response.sheet.start_date;
                    document.getElementById('endDate').value = response.sheet.end_date;
                }
            }
        }

        renderTable();
        updateSheetUI();
        updateToggleButtonState();

        // Collapse upload section if we have data
        if (teamMembers.length > 0) {
            collapseUploadSection();
        }
    } catch (error) {
        console.error('Error loading from backend:', error);
    }
}

async function saveAttendanceToBackend(ma, date, status) {
    if (!currentSheetId) {
        console.error('No sheet ID set - data not saved to server');
        showSaveToast('לא מחובר לשרת', true);
        return;
    }

    try {
        const result = await apiPost(`/sheets/${currentSheetId}/attendance`, { ma, date, status });
        if (result.success) {
            showSaveToast('נשמר בשרת');
        } else {
            showSaveToast('שגיאה בשמירה', true);
        }
    } catch (error) {
        console.error('Error saving attendance:', error);
        showSaveToast('שגיאה בשמירה', true);
    }
}

async function saveTeamMembersToBackend(members) {
    try {
        await apiPost('/team-members', { members });
    } catch (error) {
        console.error('Error saving team members:', error);
    }
}

// ============================================
// Google OAuth Functions
// ============================================

function initializeGoogleAuth() {
    // Check if Google Identity Services is loaded
    if (typeof google === 'undefined' || !google.accounts) {
        console.log('Waiting for Google Identity Services to load...');
        setTimeout(initializeGoogleAuth, 100);
        return;
    }

    tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: GOOGLE_CLIENT_ID,
        scope: SCOPES,
        callback: handleAuthCallback
    });

    // Check for stored token
    const storedToken = localStorage.getItem('google_access_token');
    if (storedToken) {
        accessToken = storedToken;
        updateAuthUI(true);
        document.getElementById('fetchSheetsBtn').disabled = false;
    }
}

function handleAuthCallback(response) {
    if (response.error) {
        console.error('Auth error:', response);
        showSheetsStatus('שגיאה בהתחברות: ' + response.error, 'error');
        return;
    }

    accessToken = response.access_token;
    localStorage.setItem('google_access_token', accessToken);

    // Get user info
    fetchUserInfo();

    updateAuthUI(true);
    document.getElementById('fetchSheetsBtn').disabled = false;
    showSheetsStatus('התחברת בהצלחה!', 'success');
}

async function fetchUserInfo() {
    try {
        const response = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        const userInfo = await response.json();

        localStorage.setItem('google_user_info', JSON.stringify(userInfo));
    } catch (error) {
        console.error('Error fetching user info:', error);
    }
}

function updateAuthUI(isSignedIn) {
    const connectBtn = document.getElementById('googleConnectBtn');

    if (isSignedIn) {
        connectBtn.textContent = 'נתק גיליון';
        connectBtn.classList.remove('btn-connect-inline');
        connectBtn.classList.add('btn-disconnect-inline');
    } else {
        connectBtn.textContent = 'התחבר';
        connectBtn.classList.remove('btn-disconnect-inline');
        connectBtn.classList.add('btn-connect-inline');
    }
}

function handleGoogleConnectClick() {
    if (accessToken) {
        // Currently connected - disconnect
        disconnectCurrentSheet();
    } else {
        // Not connected - sign in
        handleGoogleSignIn();
    }
}

function handleGoogleSignIn() {
    if (tokenClient) {
        tokenClient.requestAccessToken();
    } else {
        showSheetsStatus('שגיאה: Google Identity Services לא נטען', 'error');
    }
}

function handleGoogleSignOut() {
    accessToken = null;
    localStorage.removeItem('google_access_token');
    localStorage.removeItem('google_user_info');

    if (google.accounts.oauth2) {
        google.accounts.oauth2.revoke(accessToken);
    }

    updateAuthUI(false);
    document.getElementById('fetchSheetsBtn').disabled = true;
    document.getElementById('sheetSelectRow').style.display = 'none';
    document.getElementById('gdudPlugaRow').style.display = 'none';
    document.getElementById('loadFromSheets').style.display = 'none';
    showSheetsStatus('התנתקת בהצלחה', 'success');
}

// ============================================
// Google Sheets Functions
// ============================================

function extractSpreadsheetId(url) {
    const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    return match ? match[1] : null;
}

async function fetchSheetsList() {
    const url = document.getElementById('sheetsUrl').value.trim();
    if (!url) {
        showSheetsStatus('נא להזין קישור ל-Google Sheet', 'error');
        return;
    }

    const spreadsheetId = extractSpreadsheetId(url);
    if (!spreadsheetId) {
        showSheetsStatus('קישור לא תקין. נא להזין קישור מלא ל-Google Sheet', 'error');
        return;
    }

    currentSpreadsheetId = spreadsheetId;
    showSheetsStatus('טוען גיליונות...', 'loading');

    try {
        const response = await apiPost('/sheets/fetch', {
            accessToken: accessToken,
            spreadsheetId: spreadsheetId
        });

        if (response.error) {
            throw new Error(response.error);
        }

        // Populate dropdown
        const sheetSelect = document.getElementById('sheetSelect');
        sheetSelect.innerHTML = '<option value="">בחר גיליון...</option>';

        response.sheets.forEach(sheet => {
            const option = document.createElement('option');
            option.value = sheet.title;
            option.textContent = sheet.title;
            sheetSelect.appendChild(option);
        });

        // Show dropdown
        document.getElementById('sheetSelectRow').style.display = 'flex';
        showSheetsStatus(`נמצאו ${response.sheets.length} גיליונות ב-"${response.title}"`, 'success');

    } catch (error) {
        console.error('Error fetching sheets:', error);
        showSheetsStatus('שגיאה בטעינת הגיליונות: ' + error.message, 'error');
    }
}

function handleSheetSelect() {
    const selectedSheet = document.getElementById('sheetSelect').value;

    if (selectedSheet) {
        document.getElementById('gdudPlugaRow').style.display = 'flex';
        document.getElementById('loadFromSheets').style.display = 'block';
        updateLoadButtonState();
    } else {
        document.getElementById('gdudPlugaRow').style.display = 'none';
        document.getElementById('loadFromSheets').style.display = 'none';
    }
}

function updateLoadButtonState() {
    const gdud = document.getElementById('inputGdud').value.trim();
    const pluga = document.getElementById('inputPluga').value.trim();
    const loadBtn = document.getElementById('loadFromSheets');

    if (gdud && pluga) {
        loadBtn.disabled = false;
        loadBtn.title = '';
    } else {
        loadBtn.disabled = true;
        loadBtn.title = 'יש למלא גדוד ופלוגה';
    }
}

// Column mapping state
let pendingSheetData = null;
let currentColumnMapping = {};

// Skipped columns state (columns user chose to hide via toggle button)
let skippedColumns = [];

// Permanently skipped columns (columns user chose to skip during mapping - no data loaded)
let permanentlySkippedColumns = [];

// Field definitions for mapping
const MAPPING_FIELDS = [
    { key: 'firstName', label: 'שם פרטי', required: true },
    { key: 'lastName', label: 'שם משפחה', required: true },
    { key: 'ma', label: 'מספר אישי (מ.א)', required: true },
    { key: 'mahlaka', label: 'מחלקה', required: false },
    { key: 'miktzoaTzvai', label: 'מקצוע צבאי', required: false }
];

async function loadFromGoogleSheets() {
    const sheetName = document.getElementById('sheetSelect').value;
    const gdud = document.getElementById('inputGdud').value.trim();
    const pluga = document.getElementById('inputPluga').value.trim();

    if (!sheetName) {
        showSheetsStatus('נא לבחור גיליון', 'error');
        return;
    }

    showSheetsStatus('טוען נתונים...', 'loading');

    try {
        // First fetch the data from Google Sheets
        const response = await apiPost('/sheets/data', {
            accessToken: accessToken,
            spreadsheetId: currentSpreadsheetId,
            sheetName: sheetName
        });

        if (response.error) {
            throw new Error(response.error);
        }

        // Store the data for after mapping confirmation
        pendingSheetData = {
            response: response,
            sheetName: sheetName,
            gdud: gdud,
            pluga: pluga
        };

        // Show column mapping modal
        showColumnMappingModal(response.headers, response.headerMap, response.sampleValues);

    } catch (error) {
        console.error('Error loading sheet data:', error);
        showSheetsStatus('שגיאה בטעינת הנתונים: ' + error.message, 'error');
    }
}

function showColumnMappingModal(headers, autoMapping, sampleValues) {
    const modal = document.getElementById('columnMappingModal');
    const grid = document.getElementById('mappingGrid');

    // Initialize current mapping from auto-detected values
    currentColumnMapping = { ...autoMapping };

    // Build the mapping grid
    let gridHtml = '';
    MAPPING_FIELDS.forEach(field => {
        const autoMappedIdx = autoMapping[field.key];
        const selectedValue = autoMappedIdx !== undefined ? autoMappedIdx : '';
        const sampleValue = autoMappedIdx !== undefined && sampleValues[autoMappedIdx] ? sampleValues[autoMappedIdx] : '';

        gridHtml += `
            <div class="mapping-row">
                <div class="mapping-label">
                    ${field.required ? '<span class="required">*</span>' : ''}
                    ${field.label}
                </div>
                <select class="mapping-select ${selectedValue !== '' ? 'mapped' : ''}"
                        data-field="${field.key}"
                        data-required="${field.required}"
                        onchange="handleMappingChange(this)">
                    <option value="">-- לא נבחר --</option>
                    ${!field.required ? '<option value="skip">דלג (הסתר עמודה)</option>' : ''}
                    ${headers.map((h, i) => `
                        <option value="${i}" ${selectedValue === i ? 'selected' : ''}>
                            ${h || `עמודה ${i + 1}`}
                        </option>
                    `).join('')}
                </select>
                <div class="mapping-preview" id="preview-${field.key}">
                    ${sampleValue ? `דוגמה: ${sampleValue}` : ''}
                </div>
            </div>
        `;
    });

    grid.innerHTML = gridHtml;

    // Store headers and sample values for preview updates
    modal.dataset.headers = JSON.stringify(headers);
    modal.dataset.sampleValues = JSON.stringify(sampleValues);

    // Show modal
    modal.style.display = 'flex';

    // Show link to Google Sheet in modal header
    const modalSheetLink = document.getElementById('modalSheetLink');
    if (modalSheetLink && currentSpreadsheetId) {
        modalSheetLink.href = `https://docs.google.com/spreadsheets/d/${currentSpreadsheetId}`;
        modalSheetLink.style.display = 'inline-block';
    }

    // Update confirm button state
    updateConfirmButtonState();
}

function handleMappingChange(selectElement) {
    const field = selectElement.dataset.field;
    const value = selectElement.value;
    const modal = document.getElementById('columnMappingModal');
    const sampleValues = JSON.parse(modal.dataset.sampleValues);

    // Update mapping
    if (value === '') {
        delete currentColumnMapping[field];
        selectElement.classList.remove('mapped', 'skipped');
    } else if (value === 'skip') {
        currentColumnMapping[field] = 'skip';
        selectElement.classList.remove('mapped');
        selectElement.classList.add('skipped');
    } else {
        currentColumnMapping[field] = parseInt(value);
        selectElement.classList.remove('skipped');
        selectElement.classList.add('mapped');
    }

    // Update preview
    const preview = document.getElementById(`preview-${field}`);
    if (value === 'skip') {
        preview.textContent = 'עמודה זו לא תוצג';
    } else if (value !== '' && sampleValues[parseInt(value)]) {
        preview.textContent = `דוגמה: ${sampleValues[parseInt(value)]}`;
    } else {
        preview.textContent = '';
    }

    // Update confirm button state
    updateConfirmButtonState();
}

function updateConfirmButtonState() {
    const confirmBtn = document.getElementById('confirmMappingBtn');
    const requiredFields = MAPPING_FIELDS.filter(f => f.required);
    const allRequiredMapped = requiredFields.every(f => currentColumnMapping[f.key] !== undefined);

    confirmBtn.disabled = !allRequiredMapped;

    // Update visual feedback for required fields
    MAPPING_FIELDS.forEach(field => {
        const select = document.querySelector(`select[data-field="${field.key}"]`);
        if (select) {
            if (field.required && currentColumnMapping[field.key] === undefined) {
                select.classList.add('error');
            } else {
                select.classList.remove('error');
            }
        }
    });
}

function hideColumnMappingModal() {
    const modal = document.getElementById('columnMappingModal');
    modal.style.display = 'none';
    pendingSheetData = null;
}

async function confirmColumnMapping() {
    if (!pendingSheetData) return;

    const { response, sheetName, gdud, pluga } = pendingSheetData;

    // Hide modal
    hideColumnMappingModal();

    showSheetsStatus('מעבד נתונים...', 'loading');

    try {
        // Re-parse members using the user's custom column mapping
        // We need to fetch all rows from backend with custom mapping
        const parseResponse = await apiPost('/sheets/parse-with-mapping', {
            accessToken: accessToken,
            spreadsheetId: currentSpreadsheetId,
            sheetName: sheetName,
            columnMapping: currentColumnMapping
        });

        if (parseResponse.error) {
            throw new Error(parseResponse.error);
        }

        // Add gdud and pluga to all members
        const mappedMembers = parseResponse.members.map(m => ({
            firstName: m.firstName || '',
            lastName: m.lastName || '',
            ma: m.ma || '',
            mahlaka: currentColumnMapping.mahlaka === 'skip' ? '' : (m.mahlaka || ''),
            miktzoaTzvai: currentColumnMapping.miktzoaTzvai === 'skip' ? '' : (m.miktzoaTzvai || ''),
            gdud: gdud,
            pluga: pluga
        }));

        // Load or create sheet in database and save members
        const loadResponse = await apiPost('/sheets/load', {
            spreadsheetId: currentSpreadsheetId,
            sheetName: sheetName,
            gdud: gdud,
            pluga: pluga,
            members: mappedMembers,
            columnMapping: currentColumnMapping  // Send mapping for backend to use
        });

        if (loadResponse.error) {
            throw new Error(loadResponse.error);
        }

        // Store sheet ID and info
        currentSheetId = loadResponse.sheetId;
        localStorage.setItem('current_sheet_id', currentSheetId);
        localStorage.setItem('current_sheet_info', JSON.stringify({
            sheetName: sheetName,
            gdud: gdud,
            pluga: pluga
        }));

        // Save permanently skipped columns (from mapping) to localStorage
        permanentlySkippedColumns = Object.keys(currentColumnMapping).filter(key => currentColumnMapping[key] === 'skip');
        localStorage.setItem('permanently_skipped_columns', JSON.stringify(permanentlySkippedColumns));

        // Also set skippedColumns to include permanently skipped ones
        skippedColumns = [...permanentlySkippedColumns];
        localStorage.setItem('skipped_columns', JSON.stringify(skippedColumns));

        // Update local state
        teamMembers = loadResponse.teamMembers || mappedMembers;
        attendanceData = loadResponse.attendanceData || {};

        if (loadResponse.sheet) {
            startDate = new Date(loadResponse.sheet.start_date);
            endDate = new Date(loadResponse.sheet.end_date);
            document.getElementById('startDate').value = loadResponse.sheet.start_date;
            document.getElementById('endDate').value = loadResponse.sheet.end_date;
        }

        renderTable();
        updateSheetUI();
        updateToggleButtonState();

        showSheetsStatus(`נטענו ${mappedMembers.length} חברי צוות בהצלחה!`, 'success');

        // Collapse the upload section after successful load
        collapseUploadSection();

    } catch (error) {
        console.error('Error loading sheet data:', error);
        showSheetsStatus('שגיאה בטעינת הנתונים: ' + error.message, 'error');
    }
}

function showSheetsStatus(message, type) {
    const statusEl = document.getElementById('sheetsStatus');
    statusEl.textContent = message;
    statusEl.className = 'sheets-status ' + type;
}

function collapseUploadSection() {
    const uploadSection = document.querySelector('.upload-section');
    if (uploadSection) {
        uploadSection.classList.add('collapsed');
    }
}

function expandUploadSection() {
    const uploadSection = document.querySelector('.upload-section');
    if (uploadSection) {
        uploadSection.classList.remove('collapsed');
    }
}

function toggleUploadSection() {
    const uploadSection = document.querySelector('.upload-section');
    if (uploadSection) {
        uploadSection.classList.toggle('collapsed');
    }
}

// ============================================
// XLS Upload Functions
// ============================================

function handleXLSUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async function(e) {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(firstSheet);

            const members = jsonData.map(row => ({
                firstName: row['שם פרטי'] || row['first name'] || '',
                lastName: row['שם משפחה'] || row['last name'] || '',
                ma: String(row['מ.א'] || row['ma'] || row['id'] || ''),
                gdud: row['גדוד'] || row['gdud'] || '',
                pluga: row['פלוגה'] || row['pluga'] || '',
                mahlaka: row['מחלקה'] || row['mahlaka'] || ''
            }));

            await saveTeamMembersToBackend(members);
            await loadFromBackend();

            alert(`נטענו ${members.length} חברי צוות בהצלחה!`);
        } catch (error) {
            console.error('Error parsing XLS:', error);
            alert('שגיאה בקריאת הקובץ');
        }
    };
    reader.readAsArrayBuffer(file);
}

// ============================================
// Unit Tabs Functions
// ============================================

function renderUnitTabs() {
    const unitTabs = document.getElementById('unitTabs');
    if (!unitTabs) return;

    // Get unique mahlaka values
    const mahlakas = getUniqueValues('mahlaka', false);

    // Calculate counts for each unit
    const allCount = teamMembers.length;

    // Build tabs HTML
    let tabsHtml = `<button class="unit-tab ${activeUnitTab === 'all' ? 'active' : ''}" data-unit="all">
        הכל <span class="tab-count">${allCount}</span>
    </button>`;

    mahlakas.forEach(mahlaka => {
        const count = teamMembers.filter(m => m.mahlaka === mahlaka).length;
        const isActive = activeUnitTab === mahlaka ? 'active' : '';
        tabsHtml += `<button class="unit-tab ${isActive}" data-unit="${mahlaka}">
            ${mahlaka} <span class="tab-count">${count}</span>
        </button>`;
    });

    unitTabs.innerHTML = tabsHtml;

    // Add click handlers
    unitTabs.querySelectorAll('.unit-tab').forEach(tab => {
        tab.addEventListener('click', () => handleUnitTabClick(tab.dataset.unit));
    });
}

function handleUnitTabClick(unit) {
    activeUnitTab = unit;

    // Update active state on tabs
    document.querySelectorAll('.unit-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.unit === unit);
    });

    // Re-render the table with the new filter
    renderTable();
}

function getUnitFilteredMembers(members) {
    if (activeUnitTab === 'all') {
        return members;
    }
    return members.filter(m => m.mahlaka === activeUnitTab);
}

// ============================================
// Table Rendering
// ============================================

function generateDateRange() {
    const dates = [];
    const current = new Date(startDate);
    while (current <= endDate) {
        dates.push(new Date(current));
        current.setDate(current.getDate() + 1);
    }
    return dates;
}

function formatDate(date) {
    return date.toISOString().split('T')[0];
}

function formatDateDisplay(date) {
    const day = date.getDate();
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = monthNames[date.getMonth()];
    return `${month} ${day}`;
}

function formatDateDisplayFull(date) {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear();
    return `${day}/${month}/${year}`;
}

function getTodayString() {
    const today = new Date();
    return formatDate(today);
}

function isPastDate(dateStr) {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const checkDate = new Date(dateStr);
    checkDate.setHours(0, 0, 0, 0);
    return checkDate < today;
}

function isWeekend(date) {
    // Friday = 5, Saturday = 6
    const day = date.getDay();
    return day === 5 || day === 6;
}

function getDayName(date) {
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    return dayNames[date.getDay()];
}

function updateCurrentDateDisplay() {
    const currentDateEl = document.getElementById('currentDate');
    if (currentDateEl) {
        currentDateEl.textContent = formatDateDisplayFull(new Date());
    }
}

function getUniqueValues(field, includeEmpty = true) {
    const values = new Set();
    let hasEmpty = false;
    teamMembers.forEach(member => {
        if (member[field]) {
            values.add(member[field]);
        } else {
            hasEmpty = true;
        }
    });
    const sorted = Array.from(values).sort();
    // Add empty option at the end if there are empty values
    if (includeEmpty && hasEmpty) {
        sorted.push(EMPTY_FILTER_VALUE);
    }
    return sorted;
}

function getFilteredMembers() {
    return teamMembers.filter(member => {
        // Check gdud filter (multi-select)
        if (filters.gdud.length > 0) {
            const memberValue = member.gdud || '';
            const matchesFilter = filters.gdud.some(filterVal => {
                if (filterVal === EMPTY_FILTER_VALUE) return memberValue === '';
                return memberValue === filterVal;
            });
            if (!matchesFilter) return false;
        }
        // Check pluga filter (multi-select)
        if (filters.pluga.length > 0) {
            const memberValue = member.pluga || '';
            const matchesFilter = filters.pluga.some(filterVal => {
                if (filterVal === EMPTY_FILTER_VALUE) return memberValue === '';
                return memberValue === filterVal;
            });
            if (!matchesFilter) return false;
        }
        // Check mahlaka filter (multi-select)
        if (filters.mahlaka.length > 0) {
            const memberValue = member.mahlaka || '';
            const matchesFilter = filters.mahlaka.some(filterVal => {
                if (filterVal === EMPTY_FILTER_VALUE) return memberValue === '';
                return memberValue === filterVal;
            });
            if (!matchesFilter) return false;
        }
        return true;
    });
}

function createFilterSelect(field, label) {
    const values = getUniqueValues(field);
    const currentValues = filters[field];
    const hasFilters = currentValues.length > 0;

    // Create dropdown with checkboxes
    let checkboxes = '';
    values.forEach(val => {
        const checked = currentValues.includes(val) ? 'checked' : '';
        const displayVal = val === EMPTY_FILTER_VALUE ? 'ריק' : val;
        checkboxes += `
            <label class="filter-checkbox-label">
                <input type="checkbox" value="${val}" ${checked} onchange="handleFilterCheckboxChange('${field}', this)" />
                ${displayVal}
            </label>`;
    });

    const filterText = hasFilters ? `${label} (${currentValues.length})` : `${label} - הכל`;

    return `<div class="multi-filter-dropdown" data-field="${field}">
        <button class="filter-dropdown-btn ${hasFilters ? 'has-filter' : ''}" onclick="toggleFilterDropdown(this)">
            ${filterText} ▼
        </button>
        <div class="filter-dropdown-content">
            <div class="filter-actions">
                <button class="filter-action-btn" onclick="selectAllFilters('${field}')">בחר הכל</button>
                <button class="filter-action-btn" onclick="clearFilters('${field}')">נקה</button>
            </div>
            ${checkboxes}
        </div>
    </div>`;
}

function toggleFilterDropdown(btn) {
    const dropdown = btn.parentElement;
    const content = dropdown.querySelector('.filter-dropdown-content');

    // Close all other dropdowns first
    document.querySelectorAll('.filter-dropdown-content.show').forEach(el => {
        if (el !== content) el.classList.remove('show');
    });

    content.classList.toggle('show');
}

function handleFilterCheckboxChange(field, checkbox) {
    const value = checkbox.value;
    if (checkbox.checked) {
        if (!filters[field].includes(value)) {
            filters[field].push(value);
        }
    } else {
        filters[field] = filters[field].filter(v => v !== value);
    }
    renderTable();
}

function selectAllFilters(field) {
    const values = getUniqueValues(field);
    filters[field] = [...values];
    renderTable();
}

function clearFilters(field) {
    filters[field] = [];
    renderTable();
}

// Close dropdowns when clicking outside
document.addEventListener('click', function(e) {
    if (!e.target.closest('.multi-filter-dropdown')) {
        document.querySelectorAll('.filter-dropdown-content.show').forEach(el => {
            el.classList.remove('show');
        });
    }
});

// Create sortable header
function createSortableHeader(field, label, isSticky = true, colClass = '', rightPosition = null) {
    const isSorted = sortConfig.field === field;
    const sortIndicator = isSorted ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : '';
    const stickyClass = isSticky ? 'sticky-col' : '';
    const activeClass = isSorted ? 'sort-active' : '';
    const styleAttr = rightPosition !== null ? `style="right: ${rightPosition}px"` : '';

    return `<th class="${stickyClass} ${colClass} sortable-header ${activeClass}" ${styleAttr} onclick="handleSort('${field}')">
        ${label}${sortIndicator}
    </th>`;
}

// Handle sort click
function handleSort(field) {
    if (sortConfig.field === field) {
        // Toggle direction
        sortConfig.direction = sortConfig.direction === 'asc' ? 'desc' : 'asc';
    } else {
        // New field, default to ascending
        sortConfig.field = field;
        sortConfig.direction = 'asc';
    }
    renderTable();
}

// Sort members by current config
function getSortedMembers(members) {
    if (!sortConfig.field) {
        return members;
    }

    return [...members].sort((a, b) => {
        let aVal = a[sortConfig.field] || '';
        let bVal = b[sortConfig.field] || '';

        // String comparison for Hebrew
        const comparison = aVal.localeCompare(bVal, 'he');

        return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
}

function renderTable() {
    const headerRow = document.getElementById('headerRow');
    const tbody = document.getElementById('attendanceBody');
    const noDataMsg = document.getElementById('noDataMsg');

    // Get unique values for filters
    const gdudFilter = createFilterSelect('gdud', STRINGS.gdud);
    const plugaFilter = createFilterSelect('pluga', STRINGS.pluga);
    const mahlakaFilter = createFilterSelect('mahlaka', STRINGS.mahlaka);

    // Check which columns are skipped
    const showMahlaka = !skippedColumns.includes('mahlaka');
    const showMiktzoaTzvai = !skippedColumns.includes('miktzoaTzvai');

    // Calculate right positions dynamically based on visible columns
    // Base widths: index=40, firstname=80, lastname=80, ma=70, gdud=70, pluga=70, mahlaka=60, miktzoa=80, dorech=50, yamam=50
    let rightPos = 0;
    const colPositions = {};
    colPositions.index = rightPos;
    rightPos += 40;
    colPositions.firstname = rightPos;
    rightPos += 80;
    colPositions.lastname = rightPos;
    rightPos += 80;
    colPositions.ma = rightPos;
    rightPos += 70;
    colPositions.gdud = rightPos;
    rightPos += 70;
    colPositions.pluga = rightPos;
    rightPos += 70;
    if (showMahlaka) {
        colPositions.mahlaka = rightPos;
        rightPos += 60;
    }
    if (showMiktzoaTzvai) {
        colPositions.miktzoa = rightPos;
        rightPos += 80;
    }
    colPositions.dorech = rightPos;
    rightPos += 50;
    colPositions.yamam = rightPos;

    // Clear existing content - columns vary based on skipped preferences
    headerRow.innerHTML = `
        <th class="sticky-col col-index" style="right: ${colPositions.index}px">${STRINGS.index}</th>
        ${createSortableHeader('firstName', STRINGS.firstName, true, 'col-firstname', colPositions.firstname)}
        ${createSortableHeader('lastName', STRINGS.lastName, true, 'col-lastname', colPositions.lastname)}
        ${createSortableHeader('ma', STRINGS.misparIshi, true, 'col-ma', colPositions.ma)}
        <th class="sticky-col col-gdud" style="right: ${colPositions.gdud}px">${gdudFilter}</th>
        <th class="sticky-col col-pluga" style="right: ${colPositions.pluga}px">${plugaFilter}</th>
        ${showMahlaka ? `<th class="sticky-col col-mahlaka" style="right: ${colPositions.mahlaka}px">${mahlakaFilter}</th>` : ''}
        ${showMiktzoaTzvai ? `<th class="sticky-col col-miktzoa" style="right: ${colPositions.miktzoa}px">${STRINGS.miktzoaTzvai}</th>` : ''}
        <th class="sticky-col col-dorech" style="right: ${colPositions.dorech}px">${STRINGS.dorech}</th>
        <th class="sticky-col col-yamam" style="right: ${colPositions.yamam}px">${STRINGS.yamam}</th>
    `;
    tbody.innerHTML = '';

    if (teamMembers.length === 0) {
        noDataMsg.style.display = 'block';
        return;
    }
    noDataMsg.style.display = 'none';

    const dates = generateDateRange();
    const filteredMembers = getFilteredMembers();
    const unitFilteredMembers = getUnitFilteredMembers(filteredMembers);
    const sortedMembers = getSortedMembers(unitFilteredMembers);

    // Update unit tabs
    renderUnitTabs();

    // Add date headers
    dates.forEach(date => {
        const th = document.createElement('th');
        th.innerHTML = `${formatDateDisplay(date)}<br><small>${getDayName(date)}</small>`;
        th.title = formatDate(date);
        if (isWeekend(date)) {
            th.classList.add('weekend-header');
        }
        headerRow.appendChild(th);
    });

    // Add member rows with running index
    sortedMembers.forEach((member, index) => {
        const row = document.createElement('tr');

        // Calculate dorech and yamam for this member across all dates
        const memberDorech = calculateMemberTotal(member.ma, dates, TOTALS_CONFIG.mission);
        const memberYamam = calculateMemberTotal(member.ma, dates, TOTALS_CONFIG.counted);

        row.innerHTML = `
            <td class="sticky-col col-index" style="right: ${colPositions.index}px">${index + 1}</td>
            <td class="sticky-col" style="right: ${colPositions.firstname}px">${member.firstName || ''}</td>
            <td class="sticky-col" style="right: ${colPositions.lastname}px">${member.lastName || ''}</td>
            <td class="sticky-col" style="right: ${colPositions.ma}px">${member.ma}</td>
            <td class="sticky-col" style="right: ${colPositions.gdud}px">${member.gdud || ''}</td>
            <td class="sticky-col" style="right: ${colPositions.pluga}px">${member.pluga || ''}</td>
            ${showMahlaka ? `<td class="sticky-col" style="right: ${colPositions.mahlaka}px">${member.mahlaka || ''}</td>` : ''}
            ${showMiktzoaTzvai ? `<td class="sticky-col col-miktzoa" style="right: ${colPositions.miktzoa}px">${member.miktzoaTzvai || ''}</td>` : ''}
            <td class="sticky-col col-dorech member-dorech" style="right: ${colPositions.dorech}px" data-ma="${member.ma}">${memberDorech}</td>
            <td class="sticky-col col-yamam member-yamam" style="right: ${colPositions.yamam}px" data-ma="${member.ma}">${memberYamam}</td>
        `;

        dates.forEach(date => {
            const dateStr = formatDate(date);
            const status = (attendanceData[member.ma] && attendanceData[member.ma][dateStr]) || 'unmarked';
            const cell = document.createElement('td');
            const isPast = isPastDate(dateStr);
            const weekend = isWeekend(date);
            cell.className = `attendance-cell ${status}${isPast ? ' past-date' : ''}${weekend ? ' weekend-cell' : ''}`;
            cell.textContent = STATUS_LABELS[status];
            cell.dataset.tooltip = STATUS_TOOLTIPS[status];
            cell.dataset.ma = member.ma;
            cell.dataset.date = dateStr;
            // Only allow clicking on today and future dates
            if (!isPast) {
                cell.addEventListener('click', () => cycleStatus(cell, member.ma, dateStr));
            }
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });

    // Add total rows (with unit-filtered members)
    renderTotalRows(tbody, dates, unitFilteredMembers);
}

function renderTotalRows(tbody, dates, filteredMembers) {
    // Using HTML for colored symbols: green ✓ for present, red ✓ for counted
    const totals = [
        { key: 'mission', label: STRINGS.totalMission, symbols: '(<span class="symbol-present">✓</span> +)', class: 'total-mission' },
        { key: 'includeLeave', label: STRINGS.totalIncludeLeave, symbols: '(<span class="symbol-present">✓</span> + -)', class: 'total-leave' },
        { key: 'counted', label: STRINGS.totalCounted, symbols: '(<span class="symbol-present">✓</span> + - <span class="symbol-counted">✓</span>)', class: 'total-counted' }
    ];

    const membersToCount = filteredMembers || teamMembers;

    // Calculate colspan based on skipped columns (base 10 minus skipped)
    let colspanCount = 10;
    if (skippedColumns.includes('mahlaka')) colspanCount--;
    if (skippedColumns.includes('miktzoaTzvai')) colspanCount--;

    totals.forEach(total => {
        const row = document.createElement('tr');
        row.className = 'total-row';
        row.innerHTML = `
            <td class="total-label" colspan="${colspanCount}">${total.label} <span class="total-symbols">${total.symbols}</span></td>
        `;

        dates.forEach(date => {
            const dateStr = formatDate(date);
            const count = calculateTotal(dateStr, TOTALS_CONFIG[total.key], membersToCount);
            const cell = document.createElement('td');
            cell.className = `total-cell ${total.class}`;
            cell.textContent = count;
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });
}

function calculateTotal(dateStr, statusList, membersToCount) {
    const members = membersToCount || teamMembers;
    let count = 0;
    members.forEach(member => {
        const status = (attendanceData[member.ma] && attendanceData[member.ma][dateStr]) || 'unmarked';
        if (statusList.includes(status)) {
            count++;
        }
    });
    return count;
}

// Calculate total for a specific member across all dates in range
function calculateMemberTotal(ma, dates, statusList) {
    let count = 0;
    dates.forEach(date => {
        const dateStr = formatDate(date);
        const status = (attendanceData[ma] && attendanceData[ma][dateStr]) || 'unmarked';
        if (statusList.includes(status)) {
            count++;
        }
    });
    return count;
}

// Get allowed statuses based on previous day's status
// Rules:
// 0. First date: only 'arriving' (+) allowed
// 1. prev = unmarked: only 'arriving' allowed
// 2. prev = present: 'leaving' or 'present' allowed
// 3. prev = absent: 'absent' or 'arriving' allowed
// 4. prev = arriving: any status allowed
// 5. prev = leaving: 'counted' or 'absent' or 'arriving' allowed
// 6. prev = counted: 'counted' or 'arriving' allowed
function getAllowedStatuses(ma, date) {
    const dates = generateDateRange();
    const dateIndex = dates.findIndex(d => formatDate(d) === date);

    // First date - only arriving allowed
    if (dateIndex === 0) {
        return ['unmarked', 'arriving'];
    }

    // Get previous day's status
    const prevDate = formatDate(dates[dateIndex - 1]);
    const prevStatus = (attendanceData[ma] && attendanceData[ma][prevDate]) || 'unmarked';

    // Define allowed transitions based on previous status
    switch (prevStatus) {
        case 'unmarked':
            // Can only arrive if previously unmarked
            return ['unmarked', 'arriving'];
        case 'present':
            // Can stay present or leave
            return ['present', 'leaving'];
        case 'absent':
            // Can stay absent or arrive
            return ['unmarked', 'absent', 'arriving'];
        case 'arriving':
            // After arriving, any status is allowed
            return ['unmarked', 'present', 'absent', 'arriving', 'leaving', 'counted'];
        case 'leaving':
            // After leaving: counted (vacation), absent, or arriving again
            return ['unmarked', 'counted', 'absent', 'arriving'];
        case 'counted':
            // After counted (vacation): can stay counted or arrive back
            return ['unmarked', 'counted', 'arriving'];
        default:
            return ['unmarked', 'arriving'];
    }
}

async function cycleStatus(cell, ma, date) {
    const currentStatus = cell.className.replace('attendance-cell ', '').trim();
    const allowedStatuses = getAllowedStatuses(ma, date);

    // Find current position in allowed statuses
    const currentAllowedIndex = allowedStatuses.indexOf(currentStatus);

    // Cycle to next allowed status
    let nextStatus;
    if (currentAllowedIndex === -1) {
        // Current status not allowed, go to first allowed
        nextStatus = allowedStatuses[0];
    } else {
        const nextIndex = (currentAllowedIndex + 1) % allowedStatuses.length;
        nextStatus = allowedStatuses[nextIndex];
    }

    // Update UI
    cell.className = `attendance-cell ${nextStatus}`;
    cell.textContent = STATUS_LABELS[nextStatus];
    cell.dataset.tooltip = STATUS_TOOLTIPS[nextStatus];

    // Update local state
    if (!attendanceData[ma]) {
        attendanceData[ma] = {};
    }
    attendanceData[ma][date] = nextStatus;

    // Save to backend immediately
    await saveAttendanceToBackend(ma, date, nextStatus);

    // Update totals
    updateTotals(date);

    // Update member's dorech and yamam totals
    updateMemberTotals(ma);
}

function updateTotals(dateStr) {
    const dates = generateDateRange();
    const dateIndex = dates.findIndex(d => formatDate(d) === dateStr);
    if (dateIndex === -1) return;

    const totalRows = document.querySelectorAll('.total-row');
    const totalsConfig = [
        { key: 'mission', statusList: TOTALS_CONFIG.mission },
        { key: 'includeLeave', statusList: TOTALS_CONFIG.includeLeave },
        { key: 'counted', statusList: TOTALS_CONFIG.counted }
    ];

    totalRows.forEach((row, rowIndex) => {
        const cells = row.querySelectorAll('.total-cell');
        if (cells[dateIndex]) {
            const count = calculateTotal(dateStr, totalsConfig[rowIndex].statusList);
            cells[dateIndex].textContent = count;
        }
    });
}

// Update a specific member's dorech and yamam totals
function updateMemberTotals(ma) {
    const dates = generateDateRange();
    const dorechCell = document.querySelector(`.member-dorech[data-ma="${ma}"]`);
    const yamamCell = document.querySelector(`.member-yamam[data-ma="${ma}"]`);

    if (dorechCell) {
        dorechCell.textContent = calculateMemberTotal(ma, dates, TOTALS_CONFIG.mission);
    }
    if (yamamCell) {
        yamamCell.textContent = calculateMemberTotal(ma, dates, TOTALS_CONFIG.counted);
    }
}

// ============================================
// Date Range Functions
// ============================================

async function applyDateRange() {
    const newStartDate = document.getElementById('startDate').value;
    const newEndDate = document.getElementById('endDate').value;

    if (new Date(newStartDate) > new Date(newEndDate)) {
        alert('תאריך התחלה חייב להיות לפני תאריך סיום');
        return;
    }

    startDate = new Date(newStartDate);
    endDate = new Date(newEndDate);

    // Save to backend (sheet-specific if we have a sheet ID)
    if (currentSheetId) {
        await apiPost(`/sheets/${currentSheetId}/date-range`, {
            startDate: newStartDate,
            endDate: newEndDate
        });
    }

    renderTable();
}

// ============================================
// Export Functions
// ============================================

async function exportData() {
    let data;
    if (currentSheetId) {
        data = await apiGet(`/sheets/${currentSheetId}/export`);
    } else {
        data = { teamMembers, attendanceData };
    }

    // Create workbook
    const wb = XLSX.utils.book_new();

    // Create main data sheet
    const exportRows = [];

    // Header row
    const dates = generateDateRange();
    const headers = [STRINGS.firstName, STRINGS.lastName, STRINGS.misparIshi, STRINGS.gdud, STRINGS.pluga, STRINGS.mahlaka];
    dates.forEach(d => headers.push(formatDateDisplay(d)));
    exportRows.push(headers);

    // Data rows
    teamMembers.forEach(member => {
        const row = [
            member.firstName || '',
            member.lastName || '',
            member.ma,
            member.gdud || '',
            member.pluga || '',
            member.mahlaka || ''
        ];
        dates.forEach(date => {
            const dateStr = formatDate(date);
            const status = (attendanceData[member.ma] && attendanceData[member.ma][dateStr]) || '';
            row.push(STATUS_LABELS[status] || '');
        });
        exportRows.push(row);
    });

    // Add total rows
    const totalLabels = [STRINGS.totalMission, STRINGS.totalIncludeLeave, STRINGS.totalCounted];
    const totalKeys = ['mission', 'includeLeave', 'counted'];

    totalKeys.forEach((key, index) => {
        const row = [totalLabels[index], '', '', '', '', ''];
        dates.forEach(date => {
            const count = calculateTotal(formatDate(date), TOTALS_CONFIG[key]);
            row.push(count);
        });
        exportRows.push(row);
    });

    const ws = XLSX.utils.aoa_to_sheet(exportRows);
    XLSX.utils.book_append_sheet(wb, ws, 'נוכחות');

    // Download
    const filename = `attendance_${formatDate(new Date())}.xlsx`;
    XLSX.writeFile(wb, filename);
}

// ============================================
// Clear Data Functions
// ============================================

async function clearAllData() {
    if (!confirm('האם אתה בטוח שברצונך למחוק את כל הנתונים?')) {
        return;
    }

    if (currentSheetId) {
        await apiDelete(`/sheets/${currentSheetId}`);
        currentSheetId = null;
        localStorage.removeItem('current_sheet_id');
        localStorage.removeItem('current_sheet_info');
    }

    teamMembers = [];
    attendanceData = {};
    renderTable();
    updateSheetUI();
}

// ============================================
// Column Visibility Toggle
// ============================================

function toggleMiktzoaColumn() {
    const btn = document.getElementById('toggleMiktzoa');
    const isCurrentlyHidden = skippedColumns.includes('miktzoaTzvai');

    if (isCurrentlyHidden) {
        // Show the column
        skippedColumns = skippedColumns.filter(col => col !== 'miktzoaTzvai');
        btn.textContent = 'הסתר מקצוע צבאי';
        btn.classList.remove('hidden');
    } else {
        // Hide the column
        if (!skippedColumns.includes('miktzoaTzvai')) {
            skippedColumns.push('miktzoaTzvai');
        }
        btn.textContent = 'הצג מקצוע צבאי';
        btn.classList.add('hidden');
    }

    // Save preference to localStorage
    localStorage.setItem('skipped_columns', JSON.stringify(skippedColumns));

    // Re-render table
    renderTable();
}

function updateToggleButtonState() {
    const btn = document.getElementById('toggleMiktzoa');
    if (!btn) return;

    // If the column was permanently skipped during mapping, hide the button entirely
    if (permanentlySkippedColumns.includes('miktzoaTzvai')) {
        btn.style.display = 'none';
        return;
    }

    // Show the button
    btn.style.display = '';

    const isHidden = skippedColumns.includes('miktzoaTzvai');
    if (isHidden) {
        btn.textContent = 'הצג מקצוע צבאי';
        btn.classList.add('hidden');
    } else {
        btn.textContent = 'הסתר מקצוע צבאי';
        btn.classList.remove('hidden');
    }
}

// ============================================
// Backup Management
// ============================================

let selectedBackupFilename = null;

async function openBackupModal() {
    const modal = document.getElementById('backupModal');
    modal.style.display = 'flex';

    // Reset state
    selectedBackupFilename = null;
    document.getElementById('restoreBackupBtn').style.display = 'none';
    document.getElementById('backupDiffSection').style.display = 'none';

    // Load backups
    await loadBackupList();
}

function closeBackupModal() {
    document.getElementById('backupModal').style.display = 'none';
    selectedBackupFilename = null;
}

async function loadBackupList() {
    const backupList = document.getElementById('backupList');
    backupList.innerHTML = '<p class="no-backups-message">טוען גיבויים...</p>';

    try {
        const response = await apiGet('/backups');
        const backups = response.backups || [];

        if (backups.length === 0) {
            backupList.innerHTML = '<p class="no-backups-message">אין גיבויים זמינים</p>';
            return;
        }

        backupList.innerHTML = backups.map(backup => {
            const date = new Date(backup.timestamp);
            const formattedDate = date.toLocaleString('he-IL', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
            const sizeKB = (backup.size / 1024).toFixed(1);

            return `
                <div class="backup-item" data-filename="${backup.filename}">
                    <div class="backup-item-info">
                        <span class="backup-item-date">${formattedDate}</span>
                        <span class="backup-item-size">${sizeKB} KB</span>
                    </div>
                    <div class="backup-item-actions">
                        <button class="btn-compare" onclick="compareBackup('${backup.filename}')">השווה</button>
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading backups:', error);
        backupList.innerHTML = '<p class="no-backups-message">שגיאה בטעינת גיבויים</p>';
    }
}

async function compareBackup(filename) {
    const diffSection = document.getElementById('backupDiffSection');
    const diffSummary = document.getElementById('diffSummary');
    const diffTableBody = document.getElementById('diffTableBody');
    const restoreBtn = document.getElementById('restoreBackupBtn');

    // Mark selected backup
    document.querySelectorAll('.backup-item').forEach(item => {
        item.classList.remove('selected');
        if (item.dataset.filename === filename) {
            item.classList.add('selected');
        }
    });

    selectedBackupFilename = filename;
    diffSection.style.display = 'block';
    diffSummary.textContent = 'טוען הבדלים...';
    diffSummary.className = 'diff-summary';
    diffTableBody.innerHTML = '';
    restoreBtn.style.display = 'none';

    try {
        const response = await apiGet(`/backups/compare/${filename}`);
        const differences = response.differences || [];

        if (differences.length === 0) {
            diffSummary.textContent = 'אין הבדלים בין הגיבוי למצב הנוכחי';
            diffSummary.className = 'diff-summary no-diff';
            restoreBtn.style.display = 'none';
        } else {
            diffSummary.textContent = `נמצאו ${differences.length} הבדלים`;
            diffSummary.className = 'diff-summary has-diff';
            restoreBtn.style.display = 'block';

            diffTableBody.innerHTML = differences.map(diff => {
                const name = `${diff.firstName} ${diff.lastName}`.trim() || diff.ma;
                const typeLabel = getChangeTypeLabel(diff.type);
                const currentStatus = diff.currentStatus ? getStatusBadge(diff.currentStatus) : '<span class="status-badge none">-</span>';
                const backupStatus = diff.backupStatus ? getStatusBadge(diff.backupStatus) : '<span class="status-badge none">-</span>';

                return `
                    <tr>
                        <td>${name}</td>
                        <td>${diff.ma}</td>
                        <td>${formatDateForDisplay(diff.date)}</td>
                        <td>${currentStatus}</td>
                        <td>${backupStatus}</td>
                        <td><span class="diff-type ${diff.type}">${typeLabel}</span></td>
                    </tr>
                `;
            }).join('');
        }

    } catch (error) {
        console.error('Error comparing backup:', error);
        diffSummary.textContent = 'שגיאה בהשוואת הגיבוי';
        diffSummary.className = 'diff-summary has-diff';
    }
}

function getChangeTypeLabel(type) {
    switch (type) {
        case 'changed': return 'שונה';
        case 'added': return 'נוסף';
        case 'removed': return 'הוסר';
        default: return type;
    }
}

function getStatusBadge(status) {
    const labels = {
        'present': 'נוכח',
        'absent': 'נעדר',
        'arriving': 'מגיע',
        'leaving': 'יוצא',
        'counted': 'חופש',
        'unmarked': 'לא סומן'
    };
    return `<span class="status-badge ${status}">${labels[status] || status}</span>`;
}

function formatDateForDisplay(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleDateString('he-IL', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric'
    });
}

async function restoreBackup() {
    if (!selectedBackupFilename) {
        alert('לא נבחר גיבוי לשחזור');
        return;
    }

    const confirmed = confirm('האם אתה בטוח שברצונך לשחזר את הגיבוי?\nפעולה זו תחליף את כל הנתונים הנוכחיים.');
    if (!confirmed) return;

    try {
        const response = await apiPost(`/backups/restore/${selectedBackupFilename}`, {});

        if (response.success) {
            alert('הגיבוי שוחזר בהצלחה!');
            closeBackupModal();
            // Reload data from backend
            await loadFromBackend();
        } else {
            alert('שגיאה בשחזור הגיבוי: ' + (response.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error restoring backup:', error);
        alert('שגיאה בשחזור הגיבוי');
    }
}

// ============================================
// Event Listeners
// ============================================

document.addEventListener('DOMContentLoaded', function() {
    // Set FE version
    document.getElementById('feVersion').textContent = `FE: ${FE_VERSION}`;

    // Load BE version
    loadBackendVersion();

    // Display current date
    updateCurrentDateDisplay();

    // Initialize Google Auth
    initializeGoogleAuth();

    // Load data from backend
    loadFromBackend();

    // Google Connect/Disconnect button (single button that changes state)
    document.getElementById('googleConnectBtn').addEventListener('click', handleGoogleConnectClick);

    // Google Sheets buttons
    document.getElementById('fetchSheetsBtn').addEventListener('click', fetchSheetsList);
    document.getElementById('sheetSelect').addEventListener('change', handleSheetSelect);
    document.getElementById('loadFromSheets').addEventListener('click', loadFromGoogleSheets);

    // Enable fetch button when URL is entered and user is signed in
    document.getElementById('sheetsUrl').addEventListener('input', function() {
        document.getElementById('fetchSheetsBtn').disabled = !accessToken || !this.value.trim();
    });

    // Update load button state when gdud/pluga change
    document.getElementById('inputGdud').addEventListener('input', updateLoadButtonState);
    document.getElementById('inputPluga').addEventListener('input', updateLoadButtonState);

    // XLS Upload
    document.getElementById('xlsUpload').addEventListener('change', handleXLSUpload);

    // Date range
    document.getElementById('applyDates').addEventListener('click', applyDateRange);

    // Export
    document.getElementById('exportData').addEventListener('click', exportData);

    // Refresh button
    document.getElementById('refreshData').addEventListener('click', refreshDataFromBackend);

    // Column mapping modal buttons
    document.getElementById('confirmMappingBtn').addEventListener('click', confirmColumnMapping);
    document.getElementById('cancelMappingBtn').addEventListener('click', hideColumnMappingModal);

    // Toggle column visibility
    document.getElementById('toggleMiktzoa').addEventListener('click', toggleMiktzoaColumn);

    // Backup modal buttons
    document.getElementById('backupBtn').addEventListener('click', openBackupModal);
    document.getElementById('closeBackupModalBtn').addEventListener('click', closeBackupModal);
    document.getElementById('restoreBackupBtn').addEventListener('click', restoreBackup);
});
