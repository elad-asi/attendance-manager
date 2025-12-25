// ============================================
// Attendance Manager - Mobile JavaScript
// ============================================

const MOBILE_VERSION = '2.8.0';

// API Base URL
const API_BASE = '/api';

// State
let teamMembers = [];
let attendanceData = {};
let currentSpreadsheetId = null;
let selectedDate = null;
let dates = [];
let authSessionToken = null;
let currentUserEmail = null;

// Status configuration
const STATUSES = ['unmarked', 'present', 'absent', 'arriving', 'leaving', 'counted'];
const STATUS_LABELS = {
    'unmarked': '',
    'present': '✓',
    'absent': '✗',
    'arriving': '+',
    'leaving': '-',
    'counted': '○'
};

// Totals configuration
const TOTALS_CONFIG = {
    mission: ['present', 'arriving'],
    includeLeave: ['present', 'arriving', 'leaving'],
    counted: ['present', 'arriving', 'leaving', 'counted']
};

// Day names in Hebrew
const DAY_NAMES = ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת'];

// ============================================
// Initialization
// ============================================

document.addEventListener('DOMContentLoaded', async () => {
    // Check for existing session
    authSessionToken = localStorage.getItem('auth_session_token');
    currentUserEmail = localStorage.getItem('auth_user_email');
    currentSpreadsheetId = localStorage.getItem('current_spreadsheet_id');

    if (authSessionToken && currentUserEmail) {
        // Validate session
        const valid = await validateSession();
        if (valid) {
            showApp();
            await loadData();
        } else {
            showLogin();
        }
    } else {
        showLogin();
    }

    // Setup event listeners
    setupEventListeners();
});

function setupEventListeners() {
    // Login
    document.getElementById('sendCodeBtn').addEventListener('click', sendCode);
    document.getElementById('verifyCodeBtn').addEventListener('click', verifyCode);

    // Date navigation
    document.getElementById('prevDate').addEventListener('click', () => navigateDate(-1));
    document.getElementById('nextDate').addEventListener('click', () => navigateDate(1));

    // Search
    document.getElementById('searchInput').addEventListener('input', renderCards);
}

// ============================================
// Authentication
// ============================================

async function validateSession() {
    try {
        const response = await fetch(`${API_BASE}/auth/validate`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionToken: authSessionToken })
        });
        const data = await response.json();
        return data.valid === true;
    } catch (error) {
        console.error('Session validation error:', error);
        return false;
    }
}

async function sendCode() {
    const email = document.getElementById('loginEmail').value.trim();
    if (!email) {
        showStatus('נא להזין אימייל', 'error');
        return;
    }

    const btn = document.getElementById('sendCodeBtn');
    btn.disabled = true;
    showStatus('שולח קוד...');

    try {
        const response = await fetch(`${API_BASE}/auth/request-code`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email })
        });
        const data = await response.json();

        if (data.success) {
            showStatus('קוד נשלח לאימייל', 'success');
            document.getElementById('verifyForm').style.display = 'flex';
            currentUserEmail = email;
        } else {
            showStatus(data.error || 'שגיאה בשליחת קוד', 'error');
        }
    } catch (error) {
        showStatus('שגיאת תקשורת', 'error');
    } finally {
        btn.disabled = false;
    }
}

async function verifyCode() {
    const code = document.getElementById('verifyCode').value.trim();
    if (!code) {
        showStatus('נא להזין קוד', 'error');
        return;
    }

    const btn = document.getElementById('verifyCodeBtn');
    btn.disabled = true;
    showStatus('מאמת...');

    try {
        const response = await fetch(`${API_BASE}/auth/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email: currentUserEmail, code })
        });
        const data = await response.json();

        if (data.success) {
            authSessionToken = data.sessionToken;
            localStorage.setItem('auth_session_token', authSessionToken);
            localStorage.setItem('auth_user_email', currentUserEmail);
            showApp();
            await loadData();
        } else {
            showStatus(data.error || 'קוד שגוי', 'error');
        }
    } catch (error) {
        showStatus('שגיאת תקשורת', 'error');
    } finally {
        btn.disabled = false;
    }
}

function showStatus(message, type = '') {
    const status = document.getElementById('loginStatus');
    status.textContent = message;
    status.className = 'login-status ' + type;
}

function showLogin() {
    document.getElementById('loginSection').style.display = 'block';
    document.getElementById('appSection').style.display = 'none';
}

function showApp() {
    document.getElementById('loginSection').style.display = 'none';
    document.getElementById('appSection').style.display = 'block';
}

// ============================================
// Data Loading
// ============================================

async function loadData() {
    const container = document.getElementById('cardsContainer');
    container.innerHTML = '<p class="loading-msg">טוען נתונים...</p>';

    try {
        // Get spreadsheet ID from localStorage or fetch default
        if (!currentSpreadsheetId) {
            const sheetsResponse = await fetch(`${API_BASE}/sheets`);
            const sheetsData = await sheetsResponse.json();
            if (sheetsData.sheets && sheetsData.sheets.length > 0) {
                currentSpreadsheetId = sheetsData.sheets[0].id;
                localStorage.setItem('current_spreadsheet_id', currentSpreadsheetId);
            }
        }

        if (!currentSpreadsheetId) {
            container.innerHTML = '<p class="no-data-msg">אין נתונים. נא לטעון נתונים מהמחשב תחילה.</p>';
            return;
        }

        // Load data from backend
        const response = await fetch(`${API_BASE}/sheets/${currentSpreadsheetId}/load`);
        const data = await response.json();

        if (data.error) {
            container.innerHTML = `<p class="no-data-msg">${data.error}</p>`;
            return;
        }

        teamMembers = data.teamMembers || [];
        attendanceData = data.attendanceData || {};

        // Get date range from sheet info or use defaults
        const startDate = data.sheet?.start_date ? new Date(data.sheet.start_date) : new Date('2025-12-21');
        const endDate = data.sheet?.end_date ? new Date(data.sheet.end_date) : new Date('2026-02-01');

        // Generate dates array
        dates = generateDateRange(startDate, endDate);

        // Set initial date to today if in range
        const today = formatDate(new Date());
        if (dates.some(d => formatDate(d) === today)) {
            selectedDate = today;
        } else {
            selectedDate = formatDate(dates[0]);
        }

        renderAll();
    } catch (error) {
        console.error('Error loading data:', error);
        container.innerHTML = '<p class="no-data-msg">שגיאה בטעינת נתונים</p>';
    }
}

function generateDateRange(start, end) {
    const dates = [];
    const current = new Date(start);
    while (current <= end) {
        dates.push(new Date(current));
        current.setDate(current.getDate() + 1);
    }
    return dates;
}

// ============================================
// Rendering
// ============================================

function renderAll() {
    renderDateNav();
    renderSummary();
    renderCards();
}

function renderDateNav() {
    const dateObj = dates.find(d => formatDate(d) === selectedDate);
    if (!dateObj) return;

    const currentIndex = dates.findIndex(d => formatDate(d) === selectedDate);

    document.getElementById('currentDateText').textContent = formatDateDisplay(dateObj);
    document.getElementById('currentDayName').textContent = getDayName(dateObj);

    document.getElementById('prevDate').disabled = currentIndex <= 0;
    document.getElementById('nextDate').disabled = currentIndex >= dates.length - 1;

    // Highlight weekend
    const dateNav = document.getElementById('dateNav');
    if (isWeekend(dateObj)) {
        dateNav.classList.add('weekend');
    } else {
        dateNav.classList.remove('weekend');
    }
}

function renderSummary() {
    const dorechCount = calculateTotal(selectedDate, TOTALS_CONFIG.mission);
    const yamamCount = calculateTotal(selectedDate, TOTALS_CONFIG.counted);

    document.getElementById('dorechCount').textContent = dorechCount;
    document.getElementById('yamamCount').textContent = yamamCount;
    document.getElementById('totalCount').textContent = teamMembers.length;
}

function renderCards() {
    const container = document.getElementById('cardsContainer');
    const searchQuery = document.getElementById('searchInput').value.trim().toLowerCase();

    if (teamMembers.length === 0) {
        container.innerHTML = '<p class="no-data-msg">אין נתונים</p>';
        return;
    }

    // Filter members by search
    let filteredMembers = teamMembers;
    if (searchQuery) {
        filteredMembers = teamMembers.filter(m => {
            const fullName = `${m.firstName || ''} ${m.lastName || ''}`.toLowerCase();
            return fullName.includes(searchQuery);
        });
    }

    if (filteredMembers.length === 0) {
        container.innerHTML = '<p class="no-data-msg">לא נמצאו תוצאות</p>';
        return;
    }

    let html = '';
    filteredMembers.forEach(member => {
        const status = (attendanceData[member.ma] && attendanceData[member.ma][selectedDate]) || 'unmarked';
        const statusLabel = STATUS_LABELS[status] || '?';

        html += `
            <div class="person-card" data-ma="${member.ma}">
                <div class="person-info">
                    <div class="person-name">${member.firstName || ''} ${member.lastName || ''}</div>
                    <div class="person-details">${member.pluga || ''}${member.mahlaka ? ' / ' + member.mahlaka : ''}</div>
                    ${member.notes ? `<div class="person-notes">${member.notes}</div>` : ''}
                </div>
                <button class="status-btn ${status}" data-ma="${member.ma}" onclick="cycleStatus('${member.ma}')">
                    ${statusLabel}
                </button>
            </div>
        `;
    });

    container.innerHTML = html;
}

// ============================================
// Status Management
// ============================================

function cycleStatus(ma) {
    const currentStatus = (attendanceData[ma] && attendanceData[ma][selectedDate]) || 'unmarked';
    const allowedStatuses = getAllowedStatuses(ma, selectedDate);

    // Find next status
    const currentIndex = allowedStatuses.indexOf(currentStatus);
    let nextStatus;
    if (currentIndex === -1) {
        nextStatus = allowedStatuses[0];
    } else {
        nextStatus = allowedStatuses[(currentIndex + 1) % allowedStatuses.length];
    }

    // Update local state
    if (!attendanceData[ma]) {
        attendanceData[ma] = {};
    }
    attendanceData[ma][selectedDate] = nextStatus;

    // Update UI immediately
    const btn = document.querySelector(`.status-btn[data-ma="${ma}"]`);
    if (btn) {
        btn.className = `status-btn ${nextStatus}`;
        btn.textContent = STATUS_LABELS[nextStatus] || '?';
    }

    // Update summary
    renderSummary();

    // Save to server
    saveAttendance(ma, selectedDate, nextStatus);
}

function getAllowedStatuses(ma, date) {
    const dateIndex = dates.findIndex(d => formatDate(d) === date);

    // First date - only arriving allowed
    if (dateIndex === 0) {
        return ['unmarked', 'arriving'];
    }

    // Get previous day's status
    const prevDate = formatDate(dates[dateIndex - 1]);
    const prevStatus = (attendanceData[ma] && attendanceData[ma][prevDate]) || 'unmarked';

    switch (prevStatus) {
        case 'unmarked':
            return ['unmarked', 'arriving'];
        case 'present':
            return ['present', 'leaving'];
        case 'absent':
            return ['unmarked', 'absent', 'arriving'];
        case 'arriving':
            return ['unmarked', 'present', 'absent', 'arriving', 'leaving', 'counted'];
        case 'leaving':
            return ['unmarked', 'counted', 'absent', 'arriving'];
        case 'counted':
            return ['unmarked', 'counted', 'arriving'];
        default:
            return STATUSES;
    }
}

async function saveAttendance(ma, date, status) {
    try {
        const response = await fetch(`${API_BASE}/sheets/${currentSpreadsheetId}/attendance`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                updates: [{ ma, date, status }]
            })
        });

        if (!response.ok) {
            throw new Error('Save failed');
        }

        showToast('נשמר');
    } catch (error) {
        console.error('Save error:', error);
        showToast('שגיאה בשמירה', 'error');
    }
}

// ============================================
// Navigation
// ============================================

function navigateDate(direction) {
    const currentIndex = dates.findIndex(d => formatDate(d) === selectedDate);
    const newIndex = currentIndex + direction;

    if (newIndex >= 0 && newIndex < dates.length) {
        selectedDate = formatDate(dates[newIndex]);
        renderAll();
    }
}

// ============================================
// Utilities
// ============================================

function formatDate(date) {
    const d = new Date(date);
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function formatDateDisplay(date) {
    const d = new Date(date);
    return `${d.getDate()}/${d.getMonth() + 1}`;
}

function getDayName(date) {
    const d = new Date(date);
    return DAY_NAMES[d.getDay()];
}

function isWeekend(date) {
    const d = new Date(date);
    const day = d.getDay();
    return day === 5 || day === 6; // Friday or Saturday
}

function calculateTotal(dateStr, statusList) {
    let count = 0;
    teamMembers.forEach(member => {
        const status = (attendanceData[member.ma] && attendanceData[member.ma][dateStr]) || 'unmarked';
        if (statusList.includes(status)) {
            count++;
        }
    });
    return count;
}

function showToast(message, type = 'success') {
    const toast = document.getElementById('saveToast');
    toast.textContent = message;
    toast.className = `toast show ${type}`;

    setTimeout(() => {
        toast.className = 'toast';
    }, 2000);
}
