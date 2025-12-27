// ============================================
// Attendance Manager - Frontend JavaScript
// ============================================

// Version
const FE_VERSION = '2.8.0';  // Add direct database loading (no Google needed)

// Auto-polling configuration
const POLL_INTERVAL_MS = 3000; // 3 seconds
let pollIntervalId = null;
let currentUserEmail = null;
let isSaving = false; // Flag to prevent poll from overwriting during save
let lastSyncTimestamp = ''; // Track last sync time for incremental updates
let currentDataVersion = 0; // Track data version to detect backup restores

// Unique session ID for this browser tab (allows same user on multiple machines)
const SESSION_ID = generateSessionId();

function generateSessionId() {
    // Check if we already have a session ID for this tab
    let sessionId = sessionStorage.getItem('attendanceSessionId');
    if (!sessionId) {
        // Generate a new random session ID
        sessionId = 'sess_' + Math.random().toString(36).substring(2, 10) + '_' + Date.now().toString(36);
        sessionStorage.setItem('attendanceSessionId', sessionId);
    }
    return sessionId;
}

// API Base URL - Use Render server for multi-user testing
const API_BASE = '/api';

// State management
let teamMembers = [];
let attendanceData = {};
let startDate = new Date('2025-12-21');
let endDate = new Date('2026-02-01');

// REMOVED: currentSheetId - now using currentSpreadsheetId (Google Sheet ID) as the only identifier

// Filter state - now supports multi-select (arrays)
let filters = {
    gdud: [],
    pluga: [],
    mahlaka: []
};

// Search filters (text-based)
let searchFilters = {
    firstName: '',
    lastName: '',
    ma: ''
};

// Current active unit tabs (multi-select: empty array = all units)
let activeUnitTabs = [];

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
let currentSpreadsheetTitle = null;  // Name of the Google Spreadsheet file
let currentSheetName = null;         // Name of the specific sheet (tab) within the file

// Google OAuth Configuration
const GOOGLE_CLIENT_ID = '651831609522-bvrgmop9hmdghlrn2tqm1hv0dmkhu933.apps.googleusercontent.com';
const SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/userinfo.email';

// Attendance statuses
const STATUSES = ['unmarked', 'present', 'absent', 'arriving', 'leaving', 'counted'];
const STATUS_LABELS = {
    'unmarked': '',
    'present': '✓',
    'absent': '✗',
    'arriving': '+',
    'leaving': '-',
    'counted': '○'
};

// Hebrew tooltips for each status
const STATUS_TOOLTIPS = {
    'unmarked': 'לא סומן - לחץ לשינוי',
    'present': 'נוכח (✓)',
    'absent': 'נעדר (✗)',
    'arriving': 'מגיע (+)',
    'leaving': 'יוצא (-)',
    'counted': 'חופש (○)'
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
    notes: 'הערות',
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
// Email Authentication Functions
// ============================================

// Auth state
let authSessionToken = localStorage.getItem('authSessionToken') || null;
let authUserEmail = localStorage.getItem('authUserEmail') || null;

function showLoginError(message) {
    const errorEl = document.getElementById('loginError');
    errorEl.textContent = message;
    errorEl.style.display = 'block';
}

function hideLoginError() {
    document.getElementById('loginError').style.display = 'none';
}

function showLoginLoading(show) {
    document.getElementById('loginLoading').style.display = show ? 'flex' : 'none';
}

function showLoginStep(step) {
    document.getElementById('loginStep1').style.display = step === 1 ? 'block' : 'none';
    document.getElementById('loginStep2').style.display = step === 2 ? 'block' : 'none';
}

async function requestVerificationCode() {
    const emailInput = document.getElementById('loginEmail');
    const email = emailInput.value.trim();

    if (!email) {
        showLoginError('נא להזין כתובת מייל');
        return;
    }

    hideLoginError();
    showLoginLoading(true);
    document.getElementById('sendCodeBtn').disabled = true;

    try {
        const response = await fetch(`${API_BASE}/auth/request-code`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });

        const data = await response.json();

        if (data.success) {
            // Store email for verification step
            document.getElementById('sentToEmail').textContent = email;
            showLoginStep(2);
            document.getElementById('verificationCode').focus();
            // Show dev mode message if present
            if (data.message && data.message.includes('development')) {
                showLoginError(data.message);
            }
        } else {
            showLoginError(data.error || 'שגיאה בשליחת הקוד');
        }
    } catch (error) {
        console.error('Request code error:', error);
        showLoginError('שגיאת תקשורת - נסה שוב');
    } finally {
        showLoginLoading(false);
        document.getElementById('sendCodeBtn').disabled = false;
    }
}

async function verifyCode() {
    const email = document.getElementById('sentToEmail').textContent;
    const code = document.getElementById('verificationCode').value.trim();

    if (!code) {
        showLoginError('נא להזין קוד אימות');
        return;
    }

    hideLoginError();
    showLoginLoading(true);
    document.getElementById('verifyCodeBtn').disabled = true;

    try {
        const response = await fetch(`${API_BASE}/auth/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, code })
        });

        const data = await response.json();

        if (data.success) {
            // Store session
            authSessionToken = data.sessionToken;
            authUserEmail = data.email;
            localStorage.setItem('authSessionToken', authSessionToken);
            localStorage.setItem('authUserEmail', authUserEmail);

            // Also use this email as the currentUserEmail for heartbeat
            currentUserEmail = authUserEmail;

            // Hide login, show app
            onLoginSuccess();
        } else {
            showLoginError(data.error || 'קוד שגוי');
        }
    } catch (error) {
        console.error('Verify code error:', error);
        showLoginError('שגיאת תקשורת - נסה שוב');
    } finally {
        showLoginLoading(false);
        document.getElementById('verifyCodeBtn').disabled = false;
    }
}

function onLoginSuccess() {
    // Hide login overlay
    document.getElementById('loginOverlay').classList.add('hidden');

    // Show logged-in user in header
    document.getElementById('loggedInEmail').textContent = authUserEmail;
    document.getElementById('loggedInUserDisplay').style.display = 'flex';

    // Set currentUserEmail for heartbeat
    currentUserEmail = authUserEmail;

    // Clear any previous data - user must load a sheet explicitly
    clearSheetData();

    // Initialize the app (without loading data)
    initializeAppClean();
}

function clearSheetData() {
    // Clear local data
    teamMembers = [];
    attendanceData = {};
    currentSpreadsheetId = null;
    currentSheetName = null;
    currentSpreadsheetTitle = null;

    // Clear localStorage sheet references
    localStorage.removeItem('current_spreadsheet_id');
    localStorage.removeItem('current_sheet_info');

    // Stop polling if running
    if (pollIntervalId) {
        clearInterval(pollIntervalId);
        pollIntervalId = null;
    }

    // Hide sheet info bar
    document.getElementById('sheetInfoDisplay').style.display = 'none';

    // Hide unit selector
    document.getElementById('unitSelectorSection').style.display = 'none';

    // Reset Google Sheets UI
    document.getElementById('sheetsUrl').value = '';
    document.getElementById('sheetSelectRow').style.display = 'none';
    document.getElementById('gdudPlugaRow').style.display = 'none';
    document.getElementById('loadFromSheets').style.display = 'none';
    document.getElementById('sheetsStatus').textContent = '';

    // Render empty table
    renderTable();
}

function initializeAppClean() {
    // Set FE version
    document.getElementById('feVersion').textContent = `FE: ${FE_VERSION}`;

    // Load BE version
    loadBackendVersion();

    // Start sync status polling
    startSyncStatusPolling();

    // Display current date
    updateCurrentDateDisplay();

    // Initialize Google Auth
    initializeGoogleAuth();

    // DO NOT load from backend - user must load sheet explicitly

    // Google Connect/Disconnect button
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

    // Date range
    document.getElementById('applyDates').addEventListener('click', applyDateRange);

    // Export
    document.getElementById('exportData').addEventListener('click', exportData);

    // Daily report modal
    document.getElementById('dailyReportBtn').addEventListener('click', openDailyReportModal);
    document.getElementById('generateReportBtn').addEventListener('click', generateDailyReport);
    document.getElementById('copyReportBtn').addEventListener('click', copyDailyReport);
    document.getElementById('closeDailyReportBtn').addEventListener('click', closeDailyReportModal);

    // Database sheets loading (no Google needed)
    document.getElementById('loadFromDbBtn').addEventListener('click', openDbSheetsModal);
    document.getElementById('closeDbSheetsBtn').addEventListener('click', closeDbSheetsModal);

    // Column mapping modal buttons
    document.getElementById('confirmMappingBtn').addEventListener('click', confirmColumnMapping);
    document.getElementById('cancelMappingBtn').addEventListener('click', hideColumnMappingModal);

    // Toggle column visibility
    document.getElementById('toggleMiktzoa').addEventListener('click', toggleMiktzoaColumn);
    document.getElementById('toggleNotes').addEventListener('click', toggleNotesColumn);

    // Render empty table with "no data" message
    renderTable();
}

async function validateExistingSession() {
    if (!authSessionToken) {
        return false;
    }

    try {
        const response = await fetch(`${API_BASE}/auth/validate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionToken: authSessionToken })
        });

        const data = await response.json();

        if (data.success && data.valid) {
            authUserEmail = data.email;
            localStorage.setItem('authUserEmail', authUserEmail);
            return true;
        } else {
            // Clear invalid session
            clearAuthSession();
            return false;
        }
    } catch (error) {
        console.error('Session validation error:', error);
        return false;
    }
}

function logout() {
    // Fire-and-forget: notify backend (don't wait)
    fetch(`${API_BASE}/auth/logout`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionToken: authSessionToken })
    }).catch(error => console.error('Logout error:', error));

    clearAuthSession();

    // Also disconnect from Google (fire-and-forget)
    handleGoogleSignOut();

    // Show login screen immediately
    document.getElementById('loginOverlay').classList.remove('hidden');
    document.getElementById('loggedInUserDisplay').style.display = 'none';

    // Reset login form
    document.getElementById('loginEmail').value = '';
    document.getElementById('verificationCode').value = '';
    showLoginStep(1);
    hideLoginError();

    // Stop polling
    if (pollIntervalId) {
        clearInterval(pollIntervalId);
        pollIntervalId = null;
    }
}

function clearAuthSession() {
    authSessionToken = null;
    authUserEmail = null;
    localStorage.removeItem('authSessionToken');
    localStorage.removeItem('authUserEmail');
}

function initializeLoginListeners() {
    // Send code button
    document.getElementById('sendCodeBtn').addEventListener('click', requestVerificationCode);

    // Verify code button
    document.getElementById('verifyCodeBtn').addEventListener('click', verifyCode);

    // Back to email button
    document.getElementById('backToEmailBtn').addEventListener('click', function() {
        showLoginStep(1);
        hideLoginError();
    });

    // Logout button
    document.getElementById('logoutBtn').addEventListener('click', logout);

    // Allow Enter key to submit
    document.getElementById('loginEmail').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            requestVerificationCode();
        }
    });

    document.getElementById('verificationCode').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            verifyCode();
        }
    });
}

function initializeApp() {
    // Set FE version
    document.getElementById('feVersion').textContent = `FE: ${FE_VERSION}`;

    // Load BE version
    loadBackendVersion();

    // Start sync status polling
    startSyncStatusPolling();

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

    // Date range
    document.getElementById('applyDates').addEventListener('click', applyDateRange);

    // Export
    document.getElementById('exportData').addEventListener('click', exportData);

    // Daily report modal
    document.getElementById('dailyReportBtn').addEventListener('click', openDailyReportModal);
    document.getElementById('generateReportBtn').addEventListener('click', generateDailyReport);
    document.getElementById('copyReportBtn').addEventListener('click', copyDailyReport);
    document.getElementById('closeDailyReportBtn').addEventListener('click', closeDailyReportModal);

    // Database sheets loading (no Google needed)
    document.getElementById('loadFromDbBtn').addEventListener('click', openDbSheetsModal);
    document.getElementById('closeDbSheetsBtn').addEventListener('click', closeDbSheetsModal);

    // Column mapping modal buttons
    document.getElementById('confirmMappingBtn').addEventListener('click', confirmColumnMapping);
    document.getElementById('cancelMappingBtn').addEventListener('click', hideColumnMappingModal);

    // Toggle column visibility
    document.getElementById('toggleMiktzoa').addEventListener('click', toggleMiktzoaColumn);
    document.getElementById('toggleNotes').addEventListener('click', toggleNotesColumn);
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
// Sync Status Indicator
// ============================================

let syncStatusInterval = null;

async function updateSyncStatus() {
    try {
        const data = await apiGet('/sync-status');
        const syncEl = document.getElementById('syncStatus');
        if (!syncEl) return;

        if (data.synced) {
            syncEl.className = 'sync-status synced';
            syncEl.title = 'מסונכרן לענן';
            syncEl.innerHTML = '&#x2601;'; // cloud icon
        } else {
            syncEl.className = 'sync-status pending';
            syncEl.title = `ממתין לסנכרון: ${data.pendingCount} שינויים`;
            syncEl.innerHTML = `&#x2601; ${data.pendingCount}`;
        }
    } catch (error) {
        const syncEl = document.getElementById('syncStatus');
        if (syncEl) {
            syncEl.className = 'sync-status error';
            syncEl.title = 'שגיאה בבדיקת סנכרון';
        }
    }
}

function startSyncStatusPolling() {
    // Poll sync status every 2 seconds
    if (!syncStatusInterval) {
        syncStatusInterval = setInterval(updateSyncStatus, 2000);
        updateSyncStatus(); // Initial check
    }
}

function stopSyncStatusPolling() {
    if (syncStatusInterval) {
        clearInterval(syncStatusInterval);
        syncStatusInterval = null;
    }
}

// ============================================
// Sheet Management Functions
// ============================================

function updateSheetUI() {
    // Update sheet info display - show file name, sheet name, and Google Spreadsheet ID
    const sheetInfoDisplay = document.getElementById('sheetInfoDisplay');
    const spreadsheetTitleEl = document.getElementById('spreadsheetTitle');
    const sheetNameDisplayEl = document.getElementById('sheetNameDisplay');
    const spreadsheetIdDisplayEl = document.getElementById('spreadsheetIdDisplay');

    if (sheetInfoDisplay && currentSpreadsheetId) {
        // Show the sheet info section
        sheetInfoDisplay.style.display = 'block';

        // Update spreadsheet title (file name)
        if (spreadsheetTitleEl) {
            spreadsheetTitleEl.textContent = currentSpreadsheetTitle || '-';
        }

        // Update sheet name
        if (sheetNameDisplayEl) {
            sheetNameDisplayEl.textContent = currentSheetName || '-';
        }

        // Update Google Spreadsheet ID
        if (spreadsheetIdDisplayEl) {
            spreadsheetIdDisplayEl.textContent = currentSpreadsheetId || '-';
        }
    } else if (sheetInfoDisplay) {
        // Hide the sheet info section when no sheet is loaded
        sheetInfoDisplay.style.display = 'none';
    }

    // Start or stop polling based on whether we have an active sheet
    if (currentSpreadsheetId) {
        startPolling();
    } else {
        stopPolling();
        updateActiveUsersDisplay([]);
    }
}

// ============================================
// Auto-Polling Functions
// ============================================

function startPolling() {
    // Don't start if already polling
    if (pollIntervalId) return;

    // Get email from localStorage if not set
    if (!currentUserEmail) {
        const storedUserInfo = localStorage.getItem('google_user_info');
        if (storedUserInfo) {
            const userInfo = JSON.parse(storedUserInfo);
            currentUserEmail = userInfo.email || null;
        }
    }

    // Start polling
    pollIntervalId = setInterval(pollForUpdates, POLL_INTERVAL_MS);
    console.log('Polling started (every 3 seconds)');

    // Do an initial poll immediately
    pollForUpdates();
}

function stopPolling() {
    if (pollIntervalId) {
        clearInterval(pollIntervalId);
        pollIntervalId = null;
        console.log('Polling stopped');
    }
}

async function pollForUpdates() {
    if (!currentSpreadsheetId) return;

    // Skip polling if we're currently saving to avoid race condition
    if (isSaving) {
        console.log('Skipping poll - save in progress');
        return;
    }

    try {
        const response = await apiPost(`/sheets/${currentSpreadsheetId}/heartbeat`, {
            email: currentUserEmail || 'Anonymous',
            sessionId: SESSION_ID,
            lastSync: lastSyncTimestamp,  // Send last sync time for incremental updates
            dataVersion: currentDataVersion  // Send data version to detect backup restores
        });

        if (response.error) {
            console.error('Polling error:', response.error);
            return;
        }

        // Skip update if save started while we were fetching
        if (isSaving) {
            console.log('Skipping poll update - save started during fetch');
            return;
        }

        // Update lastSync timestamp for next poll
        if (response.serverTimestamp) {
            lastSyncTimestamp = response.serverTimestamp;
        }

        // Update data version
        if (response.dataVersion) {
            currentDataVersion = response.dataVersion;
        }

        if (response.mode === 'incremental') {
            // Apply only changes from other users
            console.log(`Poll incremental: ${response.changes?.length || 0} changes`);
            if (response.changes && response.changes.length > 0) {
                console.log(`Applying ${response.changes.length} changes from other users`, response.changes);
                applyAttendanceChanges(response.changes);
            }
        } else if (response.mode === 'full') {
            // Full data load (first sync OR backup restore detected)
            if (response.reason === 'data_version_changed') {
                console.log('Full reload triggered - backup was restored');
            } else {
                console.log('Full sync from server');
            }
            teamMembers = response.teamMembers || [];
            attendanceData = response.attendanceData || {};

            if (response.sheet) {
                startDate = new Date(response.sheet.start_date);
                endDate = new Date(response.sheet.end_date);
            }

            renderTable();
        }

        // Update active users display
        updateActiveUsersDisplay(response.activeUsers || []);

    } catch (error) {
        console.error('Polling error:', error);
    }
}

function applyAttendanceChanges(changes) {
    // Apply each change to local state and update only those cells
    let needsRerender = false;

    for (const change of changes) {
        const { ma, date, status } = change;

        // Update local attendanceData
        if (!attendanceData[ma]) {
            attendanceData[ma] = {};
        }
        attendanceData[ma][date] = status;

        // Try to update the cell directly without full re-render
        const cell = document.querySelector(`td[data-ma="${ma}"][data-date="${date}"]`);
        if (cell) {
            cell.className = `attendance-cell ${status}`;
            cell.textContent = STATUS_LABELS[status] || '';
        } else {
            needsRerender = true;
        }
    }

    // Update totals for affected dates
    const affectedDates = [...new Set(changes.map(c => c.date))];
    for (const date of affectedDates) {
        updateTotals(date);
    }

    // Update member totals for affected members
    const affectedMembers = [...new Set(changes.map(c => c.ma))];
    for (const ma of affectedMembers) {
        updateMemberTotals(ma);
    }

    // Update unit selector
    renderUnitSelector();

    // If any cell wasn't found, do a full re-render
    if (needsRerender) {
        renderTable();
    }
}

function updateActiveUsersDisplay(activeUsers) {
    let container = document.getElementById('activeUsersContainer');

    // Create container if it doesn't exist
    if (!container) {
        container = document.createElement('div');
        container.id = 'activeUsersContainer';
        container.className = 'active-users-container';

        // Insert after the header
        const header = document.querySelector('header');
        if (header) {
            header.after(container);
        }
    }

    if (activeUsers.length === 0) {
        container.innerHTML = '';
        container.style.display = 'none';
    } else {
        container.style.display = 'block';
        const usersList = activeUsers.join(', ');
        container.innerHTML = `<span class="active-users-label">משתמשים פעילים:</span> <span class="active-users-list">${usersList}</span>`;
    }
}

async function refreshDataFromBackend() {
    if (!currentSpreadsheetId) {
        alert('אין גיליון נטען');
        return;
    }

    const refreshBtn = document.getElementById('refreshData');
    refreshBtn.disabled = true;
    refreshBtn.textContent = '🔄 טוען...';

    try {
        const response = await apiGet(`/sheets/${currentSpreadsheetId}`);

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

    // Stop polling
    stopPolling();

    // Clear local state
    currentSpreadsheetId = null;
    currentSpreadsheetTitle = null;
    currentSheetName = null;
    currentUserEmail = null;
    teamMembers = [];
    attendanceData = {};

    // Clear localStorage
    localStorage.removeItem('current_spreadsheet_id');
    localStorage.removeItem('current_sheet_info');
    localStorage.removeItem('skipped_columns');
    localStorage.removeItem('permanently_skipped_columns');
    skippedColumns = ['miktzoaTzvai', 'notes'];  // Default: hide miktzoaTzvai and notes
    permanentlySkippedColumns = [];

    // Also sign out from Google
    accessToken = null;
    localStorage.removeItem('google_access_token');
    localStorage.removeItem('google_user_info');
    updateAuthUI(false);
    updateUserDisplay(null);  // Hide user display

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
        // Default: hide miktzoaTzvai and notes columns
        const storedSkippedColumns = localStorage.getItem('skipped_columns');
        if (storedSkippedColumns) {
            const parsed = JSON.parse(storedSkippedColumns);
            // Use default if localStorage is empty array (fresh user)
            skippedColumns = parsed.length > 0 ? parsed : ['miktzoaTzvai', 'notes'];
        } else {
            // No localStorage - use default
            skippedColumns = ['miktzoaTzvai', 'notes'];
        }

        // Check if we have a stored spreadsheet ID (Google Sheet ID is now the primary key)
        const storedSpreadsheetId = localStorage.getItem('current_spreadsheet_id');
        if (storedSpreadsheetId) {
            currentSpreadsheetId = storedSpreadsheetId;

            // Load sheet data using Google Sheet ID
            const response = await apiGet(`/sheets/${currentSpreadsheetId}`);

            if (response.error) {
                // Sheet not found, clear storage
                localStorage.removeItem('current_spreadsheet_id');
                localStorage.removeItem('current_sheet_info');
                currentSpreadsheetId = null;
            } else {
                teamMembers = response.teamMembers || [];
                attendanceData = response.attendanceData || {};

                if (response.sheet) {
                    startDate = new Date(response.sheet.start_date);
                    endDate = new Date(response.sheet.end_date);
                    document.getElementById('startDate').value = response.sheet.start_date;
                    document.getElementById('endDate').value = response.sheet.end_date;
                    currentSheetName = response.sheet.sheet_name || null;
                    currentSpreadsheetTitle = response.sheet.spreadsheet_title || null;
                }

                // Restore additional sheet info from localStorage (for title if not in DB)
                const storedSheetInfo = localStorage.getItem('current_sheet_info');
                if (storedSheetInfo) {
                    const sheetInfo = JSON.parse(storedSheetInfo);
                    if (!currentSpreadsheetTitle) {
                        currentSpreadsheetTitle = sheetInfo.spreadsheetTitle || null;
                    }
                    if (!currentSheetName) {
                        currentSheetName = sheetInfo.sheetName || null;
                    }
                }
            }
        }

        renderTable();
        updateSheetUI();
        updateToggleButtonState();
        updateNotesToggleButtonState();

        // Collapse upload section if we have data
        if (teamMembers.length > 0) {
            collapseUploadSection();
        }
    } catch (error) {
        console.error('Error loading from backend:', error);
    }
}

async function saveAttendanceToBackend(ma, date, status) {
    if (!currentSpreadsheetId) {
        console.error('No spreadsheet ID set - data not saved to server');
        showSaveToast('לא מחובר לשרת', true);
        return;
    }

    // Set saving flag to prevent poll from overwriting
    isSaving = true;

    try {
        const result = await apiPost(`/sheets/${currentSpreadsheetId}/attendance`, {
            ma,
            date,
            status,
            sessionId: SESSION_ID  // Track who made this change
        });
        if (result.success) {
            showSaveToast('נשמר בשרת');
            // Update lastSyncTimestamp so we don't re-fetch our own change
            if (result.serverTimestamp) {
                lastSyncTimestamp = result.serverTimestamp;
            }
        } else {
            showSaveToast('שגיאה בשמירה', true);
        }
    } catch (error) {
        console.error('Error saving attendance:', error);
        showSaveToast('שגיאה בשמירה', true);
    } finally {
        // Reset flag after a short delay to ensure poll gets updated data
        setTimeout(() => {
            isSaving = false;
        }, 500);
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

    // Check for stored token - validate it's still valid
    const storedToken = localStorage.getItem('google_access_token');
    if (storedToken) {
        // Validate token before using it
        validateGoogleToken(storedToken);
    }
}

async function validateGoogleToken(token) {
    try {
        const response = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
            headers: { 'Authorization': `Bearer ${token}` }
        });

        if (response.ok) {
            // Token is valid - restore connection
            accessToken = token;
            updateAuthUI(true);
            document.getElementById('fetchSheetsBtn').disabled = false;

            const userInfo = await response.json();
            localStorage.setItem('google_user_info', JSON.stringify(userInfo));
            updateUserDisplay(userInfo);
            console.log('[Google Auth] Token validated successfully');
        } else {
            // Token is invalid/expired - clear it
            console.log('[Google Auth] Stored token invalid (status ' + response.status + ') - clearing');
            localStorage.removeItem('google_access_token');
            localStorage.removeItem('google_user_info');
            accessToken = null;
            updateAuthUI(false);
        }
    } catch (error) {
        console.error('[Google Auth] Token validation error:', error);
        // Network error - keep token but don't mark as connected
        // User can try to reconnect manually
        localStorage.removeItem('google_access_token');
        localStorage.removeItem('google_user_info');
        accessToken = null;
        updateAuthUI(false);
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

        // Check if token is invalid/expired (401) - need to re-authenticate with new scopes
        if (response.status === 401) {
            console.log('Token missing userinfo scope - revoking and requesting re-authentication');
            // Revoke the token to force Google to issue a new one with all scopes
            if (accessToken && google.accounts && google.accounts.oauth2) {
                google.accounts.oauth2.revoke(accessToken, () => {
                    console.log('Token revoked');
                });
            }
            // Clear invalid token
            localStorage.removeItem('google_access_token');
            localStorage.removeItem('google_user_info');
            accessToken = null;
            updateAuthUI(false);
            updateUserDisplay(null);
            // User info won't be shown, but they can still use Sheets with the current token
            // They'll need to click "התחבר" again to get email scope
            return;
        }

        const userInfo = await response.json();

        if (userInfo.email) {
            localStorage.setItem('google_user_info', JSON.stringify(userInfo));
            currentUserEmail = userInfo.email;
            updateUserDisplay(userInfo);
        }
    } catch (error) {
        console.error('Error fetching user info:', error);
    }
}

function updateUserDisplay(userInfo) {
    const userInfoDisplay = document.getElementById('userInfoDisplay');
    const loggedInUserName = document.getElementById('loggedInUserName');

    if (userInfoDisplay && loggedInUserName) {
        if (userInfo && userInfo.email) {
            // Show user's email
            loggedInUserName.textContent = userInfo.email;
            userInfoDisplay.style.display = 'flex';
        } else {
            userInfoDisplay.style.display = 'none';
        }
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
        // Force consent to get new scopes (like userinfo.email)
        tokenClient.requestAccessToken({prompt: 'consent'});
    } else {
        showSheetsStatus('שגיאה: Google Identity Services לא נטען', 'error');
    }
}

function handleGoogleSignOut() {
    const tokenToRevoke = accessToken;
    accessToken = null;
    localStorage.removeItem('google_access_token');
    localStorage.removeItem('google_user_info');

    // Fire-and-forget: revoke Google token (don't wait)
    if (tokenToRevoke && google.accounts && google.accounts.oauth2) {
        try {
            google.accounts.oauth2.revoke(tokenToRevoke);
        } catch (e) {
            console.error('Google revoke error:', e);
        }
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

        // Store spreadsheet title for later display
        currentSpreadsheetTitle = response.title;

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
// Default: hide miktzoaTzvai (מקצוע צבאי) and notes (הערות)
let skippedColumns = ['miktzoaTzvai', 'notes'];

// Permanently skipped columns (columns user chose to skip during mapping - no data loaded)
let permanentlySkippedColumns = [];

// Field definitions for mapping
const MAPPING_FIELDS = [
    { key: 'firstName', label: 'שם פרטי', required: true },
    { key: 'lastName', label: 'שם משפחה', required: true },
    { key: 'ma', label: 'מספר אישי (מ.א)', required: true },
    { key: 'mahlaka', label: 'מחלקה', required: false },
    { key: 'miktzoaTzvai', label: 'מקצוע צבאי', required: false },
    { key: 'notes', label: 'הערות', required: false }
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

// Parse rows locally using user's column mapping (avoids second API call to Google)
function parseRowsWithMapping(rows, columnMapping, gdud, pluga) {
    const members = [];

    for (const row of rows) {
        if (!row || row.length === 0) continue;

        function getValue(field) {
            const idx = columnMapping[field];
            if (idx === 'skip' || idx === undefined || idx === null) return '';
            if (row.length > idx) {
                const val = row[idx];
                if (val === '?' || (val && val.trim() === '?')) return '';
                return val || '';
            }
            return '';
        }

        const member = {
            firstName: getValue('firstName'),
            lastName: getValue('lastName'),
            ma: getValue('ma'),
            mahlaka: columnMapping.mahlaka === 'skip' ? '' : getValue('mahlaka'),
            miktzoaTzvai: columnMapping.miktzoaTzvai === 'skip' ? '' : getValue('miktzoaTzvai'),
            notes: columnMapping.notes === 'skip' ? '' : getValue('notes'),
            gdud: gdud,
            pluga: pluga
        };

        // Only add if we have at least a name or ma
        if (member.firstName || member.lastName || member.ma) {
            members.push(member);
        }
    }

    return members;
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
        // Parse members locally using the user's column mapping (no extra API call!)
        const mappedMembers = parseRowsWithMapping(response.allRows, currentColumnMapping, gdud, pluga);

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

        // Store spreadsheet ID (Google Sheet ID is now the only identifier)
        currentSpreadsheetId = loadResponse.spreadsheetId;
        currentSheetName = sheetName;  // Set the current sheet name for display
        localStorage.setItem('current_spreadsheet_id', currentSpreadsheetId);
        localStorage.setItem('current_sheet_info', JSON.stringify({
            sheetName: sheetName,
            spreadsheetTitle: currentSpreadsheetTitle,
            spreadsheetId: currentSpreadsheetId,
            gdud: gdud,
            pluga: pluga
        }));

        // Save permanently skipped columns (from mapping) to localStorage
        permanentlySkippedColumns = Object.keys(currentColumnMapping).filter(key => currentColumnMapping[key] === 'skip');
        localStorage.setItem('permanently_skipped_columns', JSON.stringify(permanentlySkippedColumns));

        // Also set skippedColumns to include permanently skipped ones + default hidden (miktzoaTzvai, notes)
        skippedColumns = [...permanentlySkippedColumns];
        if (!skippedColumns.includes('miktzoaTzvai')) {
            skippedColumns.push('miktzoaTzvai');
        }
        if (!skippedColumns.includes('notes')) {
            skippedColumns.push('notes');
        }
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
        updateNotesToggleButtonState();

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
// Unit Selector Functions (combined filter + summary)
// ============================================

function renderUnitSelector() {
    const selectorSection = document.getElementById('unitSelectorSection');
    const selectorContent = document.getElementById('unitSelectorContent');
    const totalYamamValue = document.getElementById('totalYamamValue');

    if (!selectorSection || !selectorContent) return;

    // Only show if we have team members
    if (teamMembers.length === 0) {
        selectorSection.style.display = 'none';
        return;
    }

    const dates = generateDateRange();
    const filteredMembers = getFilteredMembers(); // filtered by gdud/pluga only

    // Calculate counts and yamam per unit
    const unitData = {};
    let grandTotalYamam = 0;

    filteredMembers.forEach(member => {
        const unit = member.mahlaka || 'ללא מחלקה';
        const memberYamam = calculateMemberTotal(member.ma, dates, TOTALS_CONFIG.counted);

        if (!unitData[unit]) {
            unitData[unit] = { count: 0, yamam: 0 };
        }
        unitData[unit].count++;
        unitData[unit].yamam += memberYamam;
        grandTotalYamam += memberYamam;
    });

    // Sort units alphabetically but put 'ללא מחלקה' last
    const sortedUnits = Object.keys(unitData).sort((a, b) => {
        if (a === 'ללא מחלקה') return 1;
        if (b === 'ללא מחלקה') return -1;
        return a.localeCompare(b, 'he');
    });

    // Build HTML - "הכל" card first
    const allActive = activeUnitTabs.length === 0 ? 'active' : '';
    let html = `
        <div class="unit-card all-card ${allActive}" data-unit="all">
            <div class="unit-card-name">הכל</div>
            <div class="unit-card-count">${filteredMembers.length}</div>
        </div>
    `;

    sortedUnits.forEach(unit => {
        const data = unitData[unit];
        const isActive = activeUnitTabs.includes(unit) ? 'active' : '';
        // Escape quotes in unit name for HTML attribute safety
        const escapedUnit = unit.replace(/"/g, '&quot;');
        html += `
            <div class="unit-card ${isActive}" data-unit="${escapedUnit}">
                <div class="unit-card-name">${unit}</div>
                <div class="unit-card-count">${data.count}</div>
                <div class="unit-card-yamam">ימ"מ: ${data.yamam}</div>
            </div>
        `;
    });

    selectorContent.innerHTML = html;
    totalYamamValue.textContent = grandTotalYamam;
    selectorSection.style.display = 'block';

    // Add click handlers
    selectorContent.querySelectorAll('.unit-card').forEach(card => {
        card.addEventListener('click', () => handleUnitCardClick(card.dataset.unit));
    });
}

function handleUnitCardClick(unit) {
    if (unit === 'all') {
        // Clear all selections - show all units
        activeUnitTabs = [];
    } else {
        // Toggle selection for this unit
        const index = activeUnitTabs.indexOf(unit);
        if (index === -1) {
            activeUnitTabs.push(unit);
        } else {
            activeUnitTabs.splice(index, 1);
        }
    }

    // Update active state on cards
    document.querySelectorAll('.unit-card').forEach(card => {
        if (card.dataset.unit === 'all') {
            card.classList.toggle('active', activeUnitTabs.length === 0);
        } else {
            card.classList.toggle('active', activeUnitTabs.includes(card.dataset.unit));
        }
    });

    // Re-render the table with the new filter
    renderTable();
}

// Legacy function names for compatibility
function renderUnitTabs() {
    renderUnitSelector();
}

function handleUnitTabClick(unit) {
    handleUnitCardClick(unit);
}

function getUnitFilteredMembers(members) {
    // Empty array means all units (no filter)
    if (activeUnitTabs.length === 0) {
        return members;
    }
    return members.filter(m => {
        // Handle "ללא מחלקה" - matches empty/undefined mahlaka
        const memberUnit = m.mahlaka || 'ללא מחלקה';
        return activeUnitTabs.includes(memberUnit);
    });
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
        // Check search filters (text-based, case-insensitive, partial match)
        if (searchFilters.firstName) {
            const memberValue = (member.firstName || '').toLowerCase();
            if (!memberValue.includes(searchFilters.firstName.toLowerCase())) return false;
        }
        if (searchFilters.lastName) {
            const memberValue = (member.lastName || '').toLowerCase();
            if (!memberValue.includes(searchFilters.lastName.toLowerCase())) return false;
        }
        if (searchFilters.ma) {
            const memberValue = (member.ma || '').toString().toLowerCase();
            if (!memberValue.includes(searchFilters.ma.toLowerCase())) return false;
        }

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
        // Escape quotes in value for HTML attribute safety
        const escapedVal = val.replace(/"/g, '&quot;');
        checkboxes += `
            <label class="filter-checkbox-label">
                <input type="checkbox" value="${escapedVal}" ${checked} onchange="handleFilterCheckboxChange('${field}', this)" />
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

// Create searchable header with input in the column header itself
function createSearchableHeader(field, label, searchField, placeholder, isSticky = true, colClass = '', rightPosition = null) {
    const isSorted = sortConfig.field === field;
    const sortIndicator = isSorted ? (sortConfig.direction === 'asc' ? ' ↑' : ' ↓') : '';
    const stickyClass = isSticky ? 'sticky-col' : '';
    const activeClass = isSorted ? 'sort-active' : '';
    const styleAttr = rightPosition !== null ? `style="right: ${rightPosition}px"` : '';
    const currentValue = searchFilters[searchField] || '';

    return `<th class="${stickyClass} ${colClass} searchable-header ${activeClass}" ${styleAttr}>
        <div class="header-with-search">
            <span class="header-label" onclick="handleSort('${field}')">${label}${sortIndicator}</span>
            <input type="text"
                   class="header-search-input"
                   data-search-field="${searchField}"
                   placeholder="${placeholder}"
                   value="${currentValue}"
                   onclick="event.stopPropagation()" />
        </div>
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
    const showNotes = !skippedColumns.includes('notes');

    // Calculate right positions dynamically based on visible columns
    // Base widths: index=40, firstname=80, lastname=80, ma=70, gdud=70, pluga=70, mahlaka=60, miktzoa=80, notes=100, dorech=50, yamam=50
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
    if (showNotes) {
        colPositions.notes = rightPos;
        rightPos += 100;
    }
    colPositions.dorech = rightPos;
    rightPos += 50;
    colPositions.yamam = rightPos;
    rightPos += 50;
    colPositions.setall = rightPos;

    // Clear existing content - columns vary based on skipped preferences
    headerRow.innerHTML = `
        <th class="sticky-col col-index" style="right: ${colPositions.index}px">${STRINGS.index}</th>
        ${createSearchableHeader('firstName', STRINGS.firstName, 'firstName', 'חיפוש...', true, 'col-firstname', colPositions.firstname)}
        ${createSearchableHeader('lastName', STRINGS.lastName, 'lastName', 'חיפוש...', true, 'col-lastname', colPositions.lastname)}
        ${createSearchableHeader('ma', STRINGS.misparIshi, 'ma', 'חיפוש...', true, 'col-ma', colPositions.ma)}
        <th class="sticky-col col-gdud" style="right: ${colPositions.gdud}px">${gdudFilter}</th>
        <th class="sticky-col col-pluga" style="right: ${colPositions.pluga}px">${plugaFilter}</th>
        ${showMahlaka ? `<th class="sticky-col col-mahlaka" style="right: ${colPositions.mahlaka}px">${mahlakaFilter}</th>` : ''}
        ${showMiktzoaTzvai ? `<th class="sticky-col col-miktzoa" style="right: ${colPositions.miktzoa}px">${STRINGS.miktzoaTzvai}</th>` : ''}
        ${showNotes ? `<th class="sticky-col col-notes" style="right: ${colPositions.notes}px">${STRINGS.notes}</th>` : ''}
        <th class="sticky-col col-dorech" style="right: ${colPositions.dorech}px">${STRINGS.dorech}</th>
        <th class="sticky-col col-yamam" style="right: ${colPositions.yamam}px">${STRINGS.yamam}</th>
        <th class="sticky-col col-setall" style="right: ${colPositions.setall}px">מלא</th>
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
        // Mark rows with missing ma in red
        const maValue = (member.ma || '').toString().trim();
        if (!maValue) {
            row.classList.add('missing-ma');
        }

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
            ${showNotes ? `<td class="sticky-col col-notes" style="right: ${colPositions.notes}px" title="${member.notes || ''}">${member.notes || ''}</td>` : ''}
            <td class="sticky-col col-dorech member-dorech" style="right: ${colPositions.dorech}px" data-ma="${member.ma}">${memberDorech}</td>
            <td class="sticky-col col-yamam member-yamam" style="right: ${colPositions.yamam}px" data-ma="${member.ma}">${memberYamam}</td>
            <td class="sticky-col col-setall" style="right: ${colPositions.setall}px"><button class="btn-setall ${isRowFilled(member.ma) ? 'clear-mode' : ''}" data-ma="${member.ma}" title="${isRowFilled(member.ma) ? 'נקה הכל' : 'מלא הכל'}">${isRowFilled(member.ma) ? '✕' : '▶'}</button></td>
        `;

        // Add click handler for Set All / Clear All toggle button
        const setAllBtn = row.querySelector('.btn-setall');
        if (setAllBtn) {
            setAllBtn.addEventListener('click', () => toggleRowForMember(member.ma));
        }

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
            // Allow clicking on all dates (including past)
            cell.addEventListener('click', () => cycleStatus(cell, member.ma, dateStr));
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });

    // Render totals to separate sticky container
    const totalsTbody = document.getElementById('totalsTbody');
    renderTotalRows(totalsTbody, dates, unitFilteredMembers);

    // Sync totals table width and scroll with main table
    syncTotalsWithMainTable();

    // Update unit selector (includes יממ summary)
    renderUnitSelector();

    // Attach event listeners to header search inputs
    attachHeaderSearchListeners();
}

// Sync totals table horizontal scroll and column widths with main table
function syncTotalsWithMainTable() {
    const mainContainer = document.getElementById('mainTableContainer');
    const totalsContainer = document.getElementById('totalsContainer');
    const mainTable = document.getElementById('attendanceTable');
    const totalsTable = document.getElementById('totalsTable');

    if (!mainContainer || !totalsContainer || !mainTable || !totalsTable) return;

    // Copy main table width to totals table
    totalsTable.style.width = mainTable.offsetWidth + 'px';

    // Sync column widths from main table header to totals table
    const headerCells = mainTable.querySelectorAll('thead tr th');
    const totalsRows = totalsTable.querySelectorAll('tbody tr');

    // Get widths of all header cells
    const columnWidths = Array.from(headerCells).map(th => th.offsetWidth);

    // Calculate the total width of fixed columns (for the label colspan cell)
    // Find the first date column (after all fixed columns)
    let fixedColsCount = 12; // default (includes notes column)
    if (skippedColumns.includes('mahlaka')) fixedColsCount--;
    if (skippedColumns.includes('miktzoaTzvai')) fixedColsCount--;
    if (skippedColumns.includes('notes')) fixedColsCount--;

    const fixedColsWidth = columnWidths.slice(0, fixedColsCount).reduce((sum, w) => sum + w, 0);

    // Apply widths to totals rows
    totalsRows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length === 0) return;

        // First cell is the label with colspan - set its width to match all fixed columns
        const labelCell = cells[0];
        if (labelCell.classList.contains('total-label')) {
            labelCell.style.width = fixedColsWidth + 'px';
            labelCell.style.minWidth = fixedColsWidth + 'px';
        }

        // Remaining cells are date cells - match their widths to date columns
        for (let i = 1; i < cells.length; i++) {
            const mainColIndex = fixedColsCount + (i - 1);
            if (columnWidths[mainColIndex]) {
                cells[i].style.width = columnWidths[mainColIndex] + 'px';
                cells[i].style.minWidth = columnWidths[mainColIndex] + 'px';
            }
        }
    });

    // Sync horizontal scroll from main to totals
    mainContainer.removeEventListener('scroll', syncScrollHandler);
    mainContainer.addEventListener('scroll', syncScrollHandler);

    // Initial sync of scroll position
    syncScrollHandler();
}

function syncScrollHandler() {
    const mainContainer = document.getElementById('mainTableContainer');
    const totalsContainer = document.getElementById('totalsContainer');
    if (mainContainer && totalsContainer) {
        // Sync scrollLeft - works now with overflow-x:scroll and hidden scrollbar
        totalsContainer.scrollLeft = mainContainer.scrollLeft;
    }
}

// Attach event listeners for header search inputs (with debounce)
let headerSearchTimeout = null;
function attachHeaderSearchListeners() {
    document.querySelectorAll('.header-search-input').forEach(input => {
        input.addEventListener('input', function() {
            const field = this.dataset.searchField;
            const value = this.value.trim();

            // Clear previous timeout
            if (headerSearchTimeout) {
                clearTimeout(headerSearchTimeout);
            }

            // Debounce the search
            headerSearchTimeout = setTimeout(() => {
                searchFilters[field] = value;
                renderTable();
            }, 200);
        });

        // Clear on Escape key
        input.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                this.value = '';
                searchFilters[this.dataset.searchField] = '';
                renderTable();
            }
        });
    });
}

function renderTotalRows(totalsTbody, dates, filteredMembers) {
    // Clear the totals tbody completely (it's separate from the main table)
    totalsTbody.innerHTML = '';

    // Using HTML for colored symbols: green ✓ for present, red ○ for counted
    const totals = [
        { key: 'mission', label: STRINGS.totalMission, symbols: '(<span class="symbol-present">✓</span> +)', class: 'total-mission' },
        { key: 'includeLeave', label: STRINGS.totalIncludeLeave, symbols: '(<span class="symbol-present">✓</span> + -)', class: 'total-leave' },
        { key: 'counted', label: STRINGS.totalCounted, symbols: '(<span class="symbol-present">✓</span> + - <span class="symbol-counted">○</span>)', class: 'total-counted' }
    ];

    const membersToCount = filteredMembers || teamMembers;

    // Fixed columns that appear before date columns
    // These must match the main table header exactly
    const fixedColumns = [
        { class: 'col-index', width: 40 },
        { class: 'col-firstname', width: 80 },
        { class: 'col-lastname', width: 80 },
        { class: 'col-ma', width: 70 },
        { class: 'col-gdud', width: 70 },
        { class: 'col-pluga', width: 70 }
    ];

    // Add optional columns if not skipped
    if (!skippedColumns.includes('mahlaka')) {
        fixedColumns.push({ class: 'col-mahlaka', width: 60 });
    }
    if (!skippedColumns.includes('miktzoaTzvai')) {
        fixedColumns.push({ class: 'col-miktzoa', width: 80 });
    }
    if (!skippedColumns.includes('notes')) {
        fixedColumns.push({ class: 'col-notes', width: 100 });
    }

    // Add dorech, yamam, setall columns
    fixedColumns.push({ class: 'col-dorech', width: 50 });
    fixedColumns.push({ class: 'col-yamam', width: 50 });
    fixedColumns.push({ class: 'col-setall', width: 45 });

    totals.forEach((total, idx) => {
        const row = document.createElement('tr');
        row.className = 'total-row';

        // Create individual cells for each fixed column
        fixedColumns.forEach((col, colIdx) => {
            const cell = document.createElement('td');
            cell.className = `total-fixed-cell ${col.class}`;
            cell.style.minWidth = col.width + 'px';

            // Put the label in the first cell (index column)
            if (colIdx === 0) {
                cell.innerHTML = `${total.label} <span class="total-symbols">${total.symbols}</span>`;
                cell.className = 'total-label';
                cell.colSpan = fixedColumns.length; // Span all fixed columns
                row.appendChild(cell);
                return; // Skip adding other fixed cells since we're using colspan
            }
        });

        // Add date cells
        dates.forEach(date => {
            const dateStr = formatDate(date);
            const count = calculateTotal(dateStr, TOTALS_CONFIG[total.key], membersToCount);
            const cell = document.createElement('td');
            cell.className = `total-cell ${total.class}`;
            cell.textContent = count;
            row.appendChild(cell);
        });

        totalsTbody.appendChild(row);
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
    // Don't allow updates for members without ma
    if (!ma || ma.startsWith('TEMP_')) {
        alert('לא ניתן לעדכן נוכחות - חסר מספר אישי (מ.א)');
        return;
    }

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

    // Update יממ summary
    renderUnitSelector();
}

// Check if row is filled (has any non-unmarked status)
function isRowFilled(ma) {
    const dates = generateDateRange();
    if (!attendanceData[ma]) return false;

    for (const date of dates) {
        const dateStr = formatDate(date);
        const status = attendanceData[ma][dateStr];
        if (status && status !== 'unmarked') {
            return true;
        }
    }
    return false;
}

// Toggle between set all and clear all for a member
async function toggleRowForMember(ma) {
    if (isRowFilled(ma)) {
        await clearAllForMember(ma);
    } else {
        await setAllForMember(ma);
    }
}

// Set all dates for a member: first day = arriving (+), rest = present (✓)
async function setAllForMember(ma) {
    if (!ma || ma.startsWith('TEMP_')) {
        alert('לא ניתן לעדכן נוכחות - חסר מספר אישי (מ.א)');
        return;
    }

    const dates = generateDateRange();
    if (dates.length === 0) return;

    // Show busy cursor
    document.body.style.cursor = 'wait';
    const btn = document.querySelector(`.btn-setall[data-ma="${ma}"]`);
    if (btn) {
        btn.disabled = true;
        btn.style.cursor = 'wait';
        btn.textContent = '⏳';
    }

    try {
        // Initialize attendance data for this member if needed
        if (!attendanceData[ma]) {
            attendanceData[ma] = {};
        }

        // First: Update all UI cells immediately
        const updates = [];
        for (let i = 0; i < dates.length; i++) {
            const dateStr = formatDate(dates[i]);
            const status = (i === 0) ? 'arriving' : 'present';  // First day = +, rest = ✓

            // Update local state
            attendanceData[ma][dateStr] = status;

            // Update UI cell
            const cell = document.querySelector(`.attendance-cell[data-ma="${ma}"][data-date="${dateStr}"]`);
            if (cell) {
                cell.className = `attendance-cell ${status}${isPastDate(dateStr) ? ' past-date' : ''}${isWeekend(dates[i]) ? ' weekend-cell' : ''}`;
                cell.textContent = STATUS_LABELS[status];
                cell.dataset.tooltip = STATUS_TOOLTIPS[status];
            }

            // Collect updates for batch save
            updates.push({ ma, date: dateStr, status });
        }

        // Update ALL totals once (not per date - much faster!)
        updateAllTotals();

        // Update member's totals
        updateMemberTotals(ma);

        // Update יממ summary
        renderUnitSelector();

        // Save all to backend in single batch request
        await saveAttendanceBatch(updates);

        showSaveToast('כל התאריכים עודכנו');
    } finally {
        // Restore cursor and update button to show clear mode
        document.body.style.cursor = '';
        if (btn) {
            btn.disabled = false;
            btn.style.cursor = '';
            btn.textContent = '✕';
            btn.title = 'נקה הכל';
            btn.classList.add('clear-mode');
        }
    }
}

// Clear all dates for a member (set to unmarked)
async function clearAllForMember(ma) {
    if (!ma || ma.startsWith('TEMP_')) {
        alert('לא ניתן לעדכן נוכחות - חסר מספר אישי (מ.א)');
        return;
    }

    const dates = generateDateRange();
    if (dates.length === 0) return;

    // Show busy cursor
    document.body.style.cursor = 'wait';
    const btn = document.querySelector(`.btn-setall[data-ma="${ma}"]`);
    if (btn) {
        btn.disabled = true;
        btn.style.cursor = 'wait';
        btn.textContent = '⏳';
    }

    try {
        // Initialize attendance data for this member if needed
        if (!attendanceData[ma]) {
            attendanceData[ma] = {};
        }

        // First: Update all UI cells immediately
        const updates = [];
        for (let i = 0; i < dates.length; i++) {
            const dateStr = formatDate(dates[i]);
            const status = 'unmarked';

            // Update local state
            attendanceData[ma][dateStr] = status;

            // Update UI cell
            const cell = document.querySelector(`.attendance-cell[data-ma="${ma}"][data-date="${dateStr}"]`);
            if (cell) {
                cell.className = `attendance-cell ${status}${isPastDate(dateStr) ? ' past-date' : ''}${isWeekend(dates[i]) ? ' weekend-cell' : ''}`;
                cell.textContent = STATUS_LABELS[status];
                cell.dataset.tooltip = STATUS_TOOLTIPS[status];
            }

            // Collect updates for batch save
            updates.push({ ma, date: dateStr, status });
        }

        // Update ALL totals once (not per date - much faster!)
        updateAllTotals();

        // Update member's totals
        updateMemberTotals(ma);

        // Update יממ summary
        renderUnitSelector();

        // Save all to backend in single batch request
        await saveAttendanceBatch(updates);

        showSaveToast('כל התאריכים נוקו');
    } finally {
        // Restore cursor and update button to show fill mode
        document.body.style.cursor = '';
        if (btn) {
            btn.disabled = false;
            btn.style.cursor = '';
            btn.textContent = '▶';
            btn.title = 'מלא הכל';
            btn.classList.remove('clear-mode');
        }
    }
}

// Save multiple attendance records in a single batch request
async function saveAttendanceBatch(updates) {
    if (!currentSpreadsheetId) {
        console.error('No spreadsheet ID set - data not saved to server');
        showSaveToast('לא מחובר לשרת', true);
        return;
    }

    // Set saving flag to prevent poll from overwriting
    isSaving = true;

    try {
        // Send all updates in a single batch request
        const result = await apiPost(`/sheets/${currentSpreadsheetId}/attendance/batch`, {
            updates: updates,
            sessionId: SESSION_ID
        });

        if (result && result.serverTimestamp) {
            lastSyncTimestamp = result.serverTimestamp;
        }
    } catch (error) {
        console.error('Error saving attendance batch:', error);
        showSaveToast('שגיאה בשמירה', true);
    } finally {
        setTimeout(() => {
            isSaving = false;
        }, 500);
    }
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

// Update all totals at once (more efficient than calling updateTotals per date)
function updateAllTotals() {
    const dates = generateDateRange();
    const totalRows = document.querySelectorAll('.total-row');
    const totalsConfig = [
        { key: 'mission', statusList: TOTALS_CONFIG.mission },
        { key: 'includeLeave', statusList: TOTALS_CONFIG.includeLeave },
        { key: 'counted', statusList: TOTALS_CONFIG.counted }
    ];

    totalRows.forEach((row, rowIndex) => {
        const cells = row.querySelectorAll('.total-cell');
        dates.forEach((date, dateIndex) => {
            if (cells[dateIndex]) {
                const dateStr = formatDate(date);
                const count = calculateTotal(dateStr, totalsConfig[rowIndex].statusList);
                cells[dateIndex].textContent = count;
            }
        });
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

function applyDateRange() {
    const newStartDate = document.getElementById('startDate').value;
    const newEndDate = document.getElementById('endDate').value;

    if (new Date(newStartDate) > new Date(newEndDate)) {
        alert('תאריך התחלה חייב להיות לפני תאריך סיום');
        return;
    }

    startDate = new Date(newStartDate);
    endDate = new Date(newEndDate);

    // Render table immediately
    renderTable();

    // Save to backend in background (fire-and-forget)
    if (currentSpreadsheetId) {
        apiPost(`/sheets/${currentSpreadsheetId}/date-range`, {
            startDate: newStartDate,
            endDate: newEndDate
        }).catch(error => console.error('Error saving date range:', error));
    }
}

// ============================================
// Export Functions
// ============================================

async function exportData() {
    let data;
    if (currentSpreadsheetId) {
        data = await apiGet(`/sheets/${currentSpreadsheetId}/export`);
    } else {
        data = { teamMembers, attendanceData };
    }

    // Create workbook
    const wb = XLSX.utils.book_new();

    // Create main data sheet
    const exportRows = [];

    // Header row
    const dates = generateDateRange();
    const headers = [STRINGS.firstName, STRINGS.lastName, STRINGS.misparIshi, STRINGS.gdud, STRINGS.pluga, STRINGS.mahlaka, STRINGS.notes];
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
            member.mahlaka || '',
            member.notes || ''
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
// Daily Report Functions
// ============================================

function openDailyReportModal() {
    const modal = document.getElementById('dailyReportModal');
    const dateInput = document.getElementById('reportDate');
    const unitSelect = document.getElementById('reportUnit');
    const reportPreview = document.getElementById('reportPreview');
    const copyBtn = document.getElementById('copyReportBtn');

    // Set default date to today
    dateInput.value = formatDate(new Date());

    // Populate unit select with available units
    const units = getUniqueValues('mahlaka', false);
    unitSelect.innerHTML = '<option value="all">הכל (מקובץ לפי מחלקה)</option>';
    units.forEach(unit => {
        if (unit && unit !== EMPTY_FILTER_VALUE) {
            // Escape quotes for HTML attribute safety
            const escapedUnit = unit.replace(/"/g, '&quot;');
            unitSelect.innerHTML += `<option value="${escapedUnit}">${unit}</option>`;
        }
    });

    // Hide report preview initially
    reportPreview.style.display = 'none';
    copyBtn.style.display = 'none';

    modal.style.display = 'flex';
}

function closeDailyReportModal() {
    document.getElementById('dailyReportModal').style.display = 'none';
}

function generateDailyReport() {
    const dateStr = document.getElementById('reportDate').value;
    const selectedUnit = document.getElementById('reportUnit').value;
    const reportPreview = document.getElementById('reportPreview');
    const reportText = document.getElementById('reportText');
    const copyBtn = document.getElementById('copyReportBtn');

    if (!dateStr) {
        alert('נא לבחור תאריך');
        return;
    }

    // Format the date for display
    const dateObj = new Date(dateStr);
    const hebrewDayNames = ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת'];
    const dayName = hebrewDayNames[dateObj.getDay()];
    const formattedDate = `${dateObj.getDate()}/${dateObj.getMonth() + 1}/${dateObj.getFullYear()}`;

    let report = '';
    report += `דוח נוכחות יומי\n`;
    report += `תאריך: ${formattedDate} (יום ${dayName})\n`;
    report += `${'='.repeat(40)}\n\n`;

    // Get members to include
    let membersToReport = teamMembers;

    if (selectedUnit === 'all') {
        // Group by unit
        const unitGroups = {};
        membersToReport.forEach(member => {
            const unit = member.mahlaka || 'ללא מחלקה';
            if (!unitGroups[unit]) {
                unitGroups[unit] = [];
            }
            unitGroups[unit].push(member);
        });

        // Sort units alphabetically, but put 'ללא מחלקה' last
        const sortedUnits = Object.keys(unitGroups).sort((a, b) => {
            if (a === 'ללא מחלקה') return 1;
            if (b === 'ללא מחלקה') return -1;
            return a.localeCompare(b, 'he');
        });

        sortedUnits.forEach(unit => {
            report += generateUnitReport(unit, unitGroups[unit], dateStr);
            report += '\n';
        });
    } else {
        // Filter by selected unit
        membersToReport = teamMembers.filter(m => (m.mahlaka || '') === selectedUnit);
        report += generateUnitReport(selectedUnit, membersToReport, dateStr);
    }

    // Add summary
    const totalPresent = countByStatus(membersToReport, dateStr, ['present', 'arriving']);
    const totalAbsent = countByStatus(membersToReport, dateStr, ['absent']);
    const totalCounted = countByStatus(membersToReport, dateStr, ['present', 'arriving', 'leaving', 'counted']);

    report += `${'='.repeat(40)}\n`;
    report += `סיכום כללי:\n`;
    report += `סה"כ אנשים: ${membersToReport.length}\n`;
    report += `נוכחים (דורך): ${totalPresent}\n`;
    report += `נעדרים: ${totalAbsent}\n`;
    report += `ימ"מ: ${totalCounted}\n`;

    reportText.value = report;
    reportPreview.style.display = 'block';
    copyBtn.style.display = 'inline-block';
}

function generateUnitReport(unitName, members, dateStr) {
    let report = `📌 ${unitName}\n`;
    report += `${'-'.repeat(30)}\n`;

    // Sort members by status, then by name
    const sortedMembers = [...members].sort((a, b) => {
        const statusA = getAttendanceStatus(a.ma, dateStr);
        const statusB = getAttendanceStatus(b.ma, dateStr);
        // Sort by status priority: present/arriving first, then others
        const priorityA = ['present', 'arriving'].includes(statusA) ? 0 : 1;
        const priorityB = ['present', 'arriving'].includes(statusB) ? 0 : 1;
        if (priorityA !== priorityB) return priorityA - priorityB;
        // Then sort by name
        const nameA = `${a.firstName || ''} ${a.lastName || ''}`;
        const nameB = `${b.firstName || ''} ${b.lastName || ''}`;
        return nameA.localeCompare(nameB, 'he');
    });

    sortedMembers.forEach(member => {
        const status = getAttendanceStatus(member.ma, dateStr);
        const statusText = getStatusText(status);
        const name = `${member.firstName || ''} ${member.lastName || ''}`.trim();
        report += `${name} - ${statusText}\n`;
    });

    // Unit summary
    const unitPresent = countByStatus(members, dateStr, ['present', 'arriving']);
    const unitCounted = countByStatus(members, dateStr, ['present', 'arriving', 'leaving', 'counted']);
    report += `\nסה"כ ${unitName}: ${members.length} | דורך: ${unitPresent} | ימ"מ: ${unitCounted}\n`;

    return report;
}

function getAttendanceStatus(ma, dateStr) {
    return (attendanceData[ma] && attendanceData[ma][dateStr]) || 'unmarked';
}

function getStatusText(status) {
    const texts = {
        'unmarked': 'לא בשמ"פ',
        'present': 'נוכח',
        'absent': 'לא בשמ"פ',
        'arriving': 'מגיע',
        'leaving': 'יוצא',
        'counted': 'בבית בשמ"פ'
    };
    return texts[status] || status;
}

function countByStatus(members, dateStr, statusList) {
    return members.filter(m => {
        const status = getAttendanceStatus(m.ma, dateStr);
        return statusList.includes(status);
    }).length;
}

function copyDailyReport() {
    const reportText = document.getElementById('reportText');
    reportText.select();
    document.execCommand('copy');

    // Show feedback
    const copyBtn = document.getElementById('copyReportBtn');
    const originalText = copyBtn.textContent;
    copyBtn.textContent = '✓ הועתק!';
    setTimeout(() => {
        copyBtn.textContent = originalText;
    }, 2000);
}

// ============================================
// Database Sheets Loading (No Google Connection)
// ============================================

async function openDbSheetsModal() {
    const modal = document.getElementById('dbSheetsModal');
    const listEl = document.getElementById('dbSheetsList');

    // Show modal with loading message
    listEl.innerHTML = '<p class="loading-msg">טוען גיליונות...</p>';
    modal.style.display = 'flex';

    try {
        // Fetch available sheets from backend
        const response = await fetch(`${API_BASE}/sheets`);
        const sheets = await response.json();

        if (!Array.isArray(sheets) || sheets.length === 0) {
            listEl.innerHTML = '<p class="no-sheets-msg">אין גיליונות שמורים במאגר.<br>יש לטעון גיליון מ-Google Sheets תחילה.</p>';
            return;
        }

        // Render sheets list
        let html = '';
        sheets.forEach(sheet => {
            const title = sheet.spreadsheet_title || sheet.sheet_name || 'ללא שם';
            const sheetName = sheet.sheet_name || '';
            const gdud = sheet.gdud || '';
            const pluga = sheet.pluga || '';
            const info = [gdud, pluga].filter(Boolean).join(' / ');

            html += `
                <div class="db-sheet-item" onclick="loadSheetFromDb('${sheet.spreadsheet_id}')">
                    <div class="sheet-title">${title}</div>
                    ${sheetName ? `<div class="sheet-info">גיליון: ${sheetName}</div>` : ''}
                    ${info ? `<div class="sheet-info">${info}</div>` : ''}
                </div>
            `;
        });

        listEl.innerHTML = html;

    } catch (error) {
        console.error('Error fetching sheets from database:', error);
        listEl.innerHTML = '<p class="error-msg">שגיאה בטעינת גיליונות מהמאגר</p>';
    }
}

function closeDbSheetsModal() {
    document.getElementById('dbSheetsModal').style.display = 'none';
}

async function loadSheetFromDb(spreadsheetId) {
    closeDbSheetsModal();

    // Show loading state
    const sheetsStatus = document.getElementById('sheetsStatus');
    sheetsStatus.innerHTML = '<span class="loading">טוען נתונים מהמאגר...</span>';

    try {
        // Fetch sheet data from backend
        const response = await fetch(`${API_BASE}/sheets/${spreadsheetId}`);
        const data = await response.json();

        if (data.error) {
            throw new Error(data.error);
        }

        // Store current sheet info
        currentSpreadsheetId = spreadsheetId;
        currentSpreadsheetTitle = data.sheet?.spreadsheet_title || 'ללא שם';
        currentSheetName = data.sheet?.sheet_name || '';

        // Update team members and attendance data
        teamMembers = data.teamMembers || [];
        attendanceData = data.attendanceData || {};

        // Update date range if available from sheet
        if (data.sheet?.start_date) {
            startDate = new Date(data.sheet.start_date);
            document.getElementById('startDate').value = data.sheet.start_date;
        }
        if (data.sheet?.end_date) {
            endDate = new Date(data.sheet.end_date);
            document.getElementById('endDate').value = data.sheet.end_date;
        }

        // Update sheet info display
        document.getElementById('spreadsheetTitle').textContent = currentSpreadsheetTitle;
        document.getElementById('sheetNameDisplay').textContent = currentSheetName || '-';
        document.getElementById('spreadsheetIdDisplay').textContent = spreadsheetId;
        document.getElementById('sheetInfoDisplay').style.display = 'flex';

        // Collapse the upload section
        const uploadContent = document.querySelector('.upload-content');
        const toggleIcon = document.querySelector('.toggle-icon');
        if (uploadContent) {
            uploadContent.classList.add('collapsed');
            if (toggleIcon) toggleIcon.textContent = '▶';
        }

        // Render the table
        renderTable();

        // Show unit selector if there are units
        renderUnitSelector();

        // Start polling for updates
        startPolling();

        sheetsStatus.innerHTML = `<span class="success">נטען מהמאגר: ${currentSpreadsheetTitle}</span>`;
        showSaveToast(`נטענו ${teamMembers.length} חברי צוות`);

    } catch (error) {
        console.error('Error loading sheet from database:', error);
        sheetsStatus.innerHTML = `<span class="error">שגיאה בטעינה: ${error.message}</span>`;
    }
}

// ============================================
// Clear Data Functions
// ============================================

async function clearAllData() {
    if (!confirm('האם אתה בטוח שברצונך למחוק את כל הנתונים?')) {
        return;
    }

    if (currentSpreadsheetId) {
        await apiDelete(`/sheets/${currentSpreadsheetId}`);
        currentSpreadsheetId = null;
        localStorage.removeItem('current_spreadsheet_id');
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

function toggleNotesColumn() {
    const btn = document.getElementById('toggleNotes');
    const isCurrentlyHidden = skippedColumns.includes('notes');

    if (isCurrentlyHidden) {
        // Show the column
        skippedColumns = skippedColumns.filter(col => col !== 'notes');
        btn.textContent = 'הסתר הערות';
        btn.classList.remove('hidden');
    } else {
        // Hide the column
        if (!skippedColumns.includes('notes')) {
            skippedColumns.push('notes');
        }
        btn.textContent = 'הצג הערות';
        btn.classList.add('hidden');
    }

    // Save preference to localStorage
    localStorage.setItem('skipped_columns', JSON.stringify(skippedColumns));

    // Re-render table
    renderTable();
}

function updateNotesToggleButtonState() {
    const btn = document.getElementById('toggleNotes');
    if (!btn) return;

    // If the column was permanently skipped during mapping, hide the button entirely
    if (permanentlySkippedColumns.includes('notes')) {
        btn.style.display = 'none';
        return;
    }

    // Show the button
    btn.style.display = '';

    const isHidden = skippedColumns.includes('notes');
    if (isHidden) {
        btn.textContent = 'הצג הערות';
        btn.classList.add('hidden');
    } else {
        btn.textContent = 'הסתר הערות';
        btn.classList.remove('hidden');
    }
}


// ============================================
// Event Listeners
// ============================================

document.addEventListener('DOMContentLoaded', async function() {
    // Always initialize login listeners
    initializeLoginListeners();

    // Check for existing valid session
    const hasValidSession = await validateExistingSession();

    if (hasValidSession) {
        // User already logged in - hide login, show app
        document.getElementById('loginOverlay').classList.add('hidden');
        document.getElementById('loggedInEmail').textContent = authUserEmail;
        document.getElementById('loggedInUserDisplay').style.display = 'flex';
        currentUserEmail = authUserEmail;

        // Initialize the main app
        initializeApp();
    } else {
        // Show login screen (it's already visible by default)
        document.getElementById('loginOverlay').classList.remove('hidden');
    }
});
