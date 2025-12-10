// ============================================
// Attendance Manager - Frontend JavaScript
// ============================================

// Version
const FE_VERSION = '0.0.4';

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
    const sheetInfo = document.getElementById('sheetInfo');
    const sheetNameEl = document.getElementById('currentSheetName');

    if (currentSheetId) {
        refreshBtn.style.display = 'inline-block';
        sheetInfo.style.display = 'flex';
        // Store sheet info in localStorage for display
        const storedSheetInfo = localStorage.getItem('current_sheet_info');
        if (storedSheetInfo) {
            const info = JSON.parse(storedSheetInfo);
            sheetNameEl.textContent = `${info.sheetName} | ${info.gdud} / ${info.pluga}`;
        }
    } else {
        refreshBtn.style.display = 'none';
        sheetInfo.style.display = 'none';
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

// ============================================
// Data Loading from Backend
// ============================================

async function loadFromBackend() {
    try {
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

        document.getElementById('userAvatar').src = userInfo.picture || '';
        document.getElementById('userName').textContent = userInfo.name || userInfo.email;
        localStorage.setItem('google_user_info', JSON.stringify(userInfo));
    } catch (error) {
        console.error('Error fetching user info:', error);
    }
}

function updateAuthUI(isSignedIn) {
    const signInBtn = document.getElementById('googleSignInBtn');
    const userInfo = document.getElementById('userInfo');

    if (isSignedIn) {
        signInBtn.style.display = 'none';
        userInfo.style.display = 'flex';

        // Restore user info from localStorage
        const storedUserInfo = localStorage.getItem('google_user_info');
        if (storedUserInfo) {
            const info = JSON.parse(storedUserInfo);
            document.getElementById('userAvatar').src = info.picture || '';
            document.getElementById('userName').textContent = info.name || info.email;
        }
    } else {
        signInBtn.style.display = 'flex';
        userInfo.style.display = 'none';
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

        // Add gdud and pluga to each member
        const members = response.members.map(m => ({
            ...m,
            gdud: gdud,
            pluga: pluga
        }));

        // Load or create sheet in database and save members
        const loadResponse = await apiPost('/sheets/load', {
            spreadsheetId: currentSpreadsheetId,
            sheetName: sheetName,
            gdud: gdud,
            pluga: pluga,
            members: members
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

        // Update local state
        teamMembers = loadResponse.teamMembers || members;
        attendanceData = loadResponse.attendanceData || {};

        if (loadResponse.sheet) {
            startDate = new Date(loadResponse.sheet.start_date);
            endDate = new Date(loadResponse.sheet.end_date);
            document.getElementById('startDate').value = loadResponse.sheet.start_date;
            document.getElementById('endDate').value = loadResponse.sheet.end_date;
        }

        renderTable();
        updateSheetUI();

        showSheetsStatus(`נטענו ${members.length} חברי צוות בהצלחה!`, 'success');

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
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    return `${day}/${month}`;
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
function createSortableHeader(field, label, isSticky = true, colClass = '') {
    const isSorted = sortConfig.field === field;
    const sortIndicator = isSorted ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : '';
    const stickyClass = isSticky ? 'sticky-col' : '';
    const activeClass = isSorted ? 'sort-active' : '';

    return `<th class="${stickyClass} ${colClass} sortable-header ${activeClass}" onclick="handleSort('${field}')">
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

    // Clear existing content - now with 7 columns (index, firstName, lastName, ma, gdud, pluga, mahlaka)
    headerRow.innerHTML = `
        <th class="sticky-col col-index">${STRINGS.index}</th>
        ${createSortableHeader('firstName', STRINGS.firstName, true, 'col-firstname')}
        ${createSortableHeader('lastName', STRINGS.lastName, true, 'col-lastname')}
        ${createSortableHeader('ma', STRINGS.misparIshi, true, 'col-ma')}
        <th class="sticky-col col-gdud">${gdudFilter}</th>
        <th class="sticky-col col-pluga">${plugaFilter}</th>
        <th class="sticky-col col-mahlaka">${mahlakaFilter}</th>
    `;
    tbody.innerHTML = '';

    if (teamMembers.length === 0) {
        noDataMsg.style.display = 'block';
        return;
    }
    noDataMsg.style.display = 'none';

    const dates = generateDateRange();
    const filteredMembers = getFilteredMembers();
    const sortedMembers = getSortedMembers(filteredMembers);

    // Add date headers
    dates.forEach(date => {
        const th = document.createElement('th');
        th.textContent = formatDateDisplay(date);
        th.title = formatDate(date);
        headerRow.appendChild(th);
    });

    // Add member rows with running index
    sortedMembers.forEach((member, index) => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="sticky-col col-index">${index + 1}</td>
            <td class="sticky-col">${member.firstName || ''}</td>
            <td class="sticky-col">${member.lastName || ''}</td>
            <td class="sticky-col">${member.ma}</td>
            <td class="sticky-col">${member.gdud || ''}</td>
            <td class="sticky-col">${member.pluga || ''}</td>
            <td class="sticky-col">${member.mahlaka || ''}</td>
        `;

        dates.forEach(date => {
            const dateStr = formatDate(date);
            const status = (attendanceData[member.ma] && attendanceData[member.ma][dateStr]) || 'unmarked';
            const cell = document.createElement('td');
            const isPast = isPastDate(dateStr);
            cell.className = `attendance-cell ${status}${isPast ? ' past-date' : ''}`;
            cell.textContent = STATUS_LABELS[status];
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

    // Add total rows (with filtered members)
    renderTotalRows(tbody, dates, sortedMembers);
}

function renderTotalRows(tbody, dates, filteredMembers) {
    // Using HTML for colored symbols: green ✓ for present, red ✓ for counted
    const totals = [
        { key: 'mission', label: STRINGS.totalMission, symbols: '(<span class="symbol-present">✓</span> +)', class: 'total-mission' },
        { key: 'includeLeave', label: STRINGS.totalIncludeLeave, symbols: '(<span class="symbol-present">✓</span> + -)', class: 'total-leave' },
        { key: 'counted', label: STRINGS.totalCounted, symbols: '(<span class="symbol-present">✓</span> + - <span class="symbol-counted">✓</span>)', class: 'total-counted' }
    ];

    const membersToCount = filteredMembers || teamMembers;

    totals.forEach(total => {
        const row = document.createElement('tr');
        row.className = 'total-row';
        row.innerHTML = `
            <td class="total-label" colspan="7">${total.label} <span class="total-symbols">${total.symbols}</span></td>
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

    // Update local state
    if (!attendanceData[ma]) {
        attendanceData[ma] = {};
    }
    attendanceData[ma][date] = nextStatus;

    // Save to backend immediately
    await saveAttendanceToBackend(ma, date, nextStatus);

    // Update totals
    updateTotals(date);
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

    // Google Auth buttons
    document.getElementById('googleSignInBtn').addEventListener('click', handleGoogleSignIn);
    document.getElementById('googleSignOutBtn').addEventListener('click', handleGoogleSignOut);

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
});
