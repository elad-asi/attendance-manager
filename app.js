// State management
let teamMembers = [];
let attendanceData = {};
let startDate = new Date('2025-12-21');
let endDate = new Date('2026-02-01');

// Google OAuth State
let tokenClient = null;
let accessToken = null;
let currentSpreadsheetId = null;

// ============================================
// IMPORTANT: You need to create your own Google Cloud Project
// and get a Client ID. Follow these steps:
// 1. Go to https://console.cloud.google.com/
// 2. Create a new project
// 3. Enable Google Sheets API
// 4. Create OAuth 2.0 credentials (Web application)
// 5. Add your domain to Authorized JavaScript origins
// 6. Replace the CLIENT_ID below with your own
// ============================================
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
    'counted': '$'
};

// Define which statuses count for each total
const TOTALS_CONFIG = {
    mission: ['present', 'arriving'],
    includeLeave: ['present', 'arriving', 'leaving'],
    counted: ['present', 'arriving', 'leaving', 'counted']
};

// Hebrew strings
const STRINGS = {
    name: 'שם',
    misparIshi: 'מ.א',
    gdud: 'גדוד',
    pluga: 'פלוגה',
    mahlaka: 'מחלקה',
    action: 'פעולה',
    removeMember: 'הסר חבר',
    availableForMission: 'זמין למשימה',
    includingLeave: 'כולל חופשה',
    totalCounted: 'סה"כ נספרים',
    importSuccess: 'יובאו בהצלחה {count} חברי צוות!',
    importError: 'שגיאה בקריאת הקובץ. וודא שהקובץ תקין ומכיל עמודות: שם פרטי, שם משפחה, מ.א, מחלקה',
    dateError: 'תאריך התחלה חייב להיות לפני תאריך סיום!',
    noDataExport: 'אין נתונים לייצוא!',
    confirmClear: 'האם אתה בטוח שברצונך למחוק את כל הנתונים? לא ניתן לבטל פעולה זו!',
    confirmRemove: 'להסיר את {name}?',
    clickToChange: 'לחץ לשינוי סטטוס',
    sheetsLoading: '⏳ טוען נתונים מהגיליון...',
    sheetsSuccess: '✅ נטענו בהצלחה {count} חברי צוות!',
    sheetsError: '❌ שגיאה: {error}',
    sheetsUrlError: 'נא להזין קישור תקין ל-Google Sheet',
    sheetsGdudPlugaError: 'נא למלא גדוד ופלוגה',
    signInFirst: 'נא להתחבר עם Google תחילה',
    fetchingSheets: '⏳ טוען רשימת גיליונות...',
    selectSheet: 'בחר גיליון...',
    noSheetsFound: 'לא נמצאו גיליונות',
    configureClientId: '⚠️ נא להגדיר GOOGLE_CLIENT_ID בקובץ app.js'
};

// DOM Elements
const xlsUpload = document.getElementById('xlsUpload');
const sheetsUrl = document.getElementById('sheetsUrl');
const sheetSelect = document.getElementById('sheetSelect');
const sheetSelectRow = document.getElementById('sheetSelectRow');
const gdudPlugaRow = document.getElementById('gdudPlugaRow');
const inputGdud = document.getElementById('inputGdud');
const inputPluga = document.getElementById('inputPluga');
const loadFromSheetsBtn = document.getElementById('loadFromSheets');
const fetchSheetsBtn = document.getElementById('fetchSheetsBtn');
const sheetsStatus = document.getElementById('sheetsStatus');
const googleSignInBtn = document.getElementById('googleSignInBtn');
const googleSignOutBtn = document.getElementById('googleSignOutBtn');
const userInfo = document.getElementById('userInfo');
const userAvatar = document.getElementById('userAvatar');
const userName = document.getElementById('userName');
const startDateInput = document.getElementById('startDate');
const endDateInput = document.getElementById('endDate');
const applyDatesBtn = document.getElementById('applyDates');
const exportDataBtn = document.getElementById('exportData');
const clearDataBtn = document.getElementById('clearData');
const attendanceTable = document.getElementById('attendanceTable');
const headerRow = document.getElementById('headerRow');
const attendanceBody = document.getElementById('attendanceBody');
const noDataMsg = document.getElementById('noDataMsg');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadFromStorage();
    renderTable();
    initGoogleAuth();
});

// ============================================
// Google OAuth Functions
// ============================================

function initGoogleAuth() {
    // Check if client ID is configured
    if (GOOGLE_CLIENT_ID === 'YOUR_CLIENT_ID_HERE.apps.googleusercontent.com') {
        showSheetsStatus('error', STRINGS.configureClientId);
        googleSignInBtn.disabled = true;
        return;
    }

    // Wait for Google Identity Services to load
    if (typeof google === 'undefined') {
        setTimeout(initGoogleAuth, 100);
        return;
    }

    tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: GOOGLE_CLIENT_ID,
        scope: SCOPES,
        callback: handleAuthResponse
    });

    // Check if we have a saved token
    const savedToken = localStorage.getItem('google_access_token');
    if (savedToken) {
        accessToken = savedToken;
        updateUIForSignedIn();
    }
}

function handleAuthResponse(response) {
    if (response.error) {
        console.error('Auth error:', response.error);
        showSheetsStatus('error', 'שגיאה בהתחברות: ' + response.error);
        return;
    }

    accessToken = response.access_token;
    localStorage.setItem('google_access_token', accessToken);

    // Fetch user info
    fetchUserInfo();
    updateUIForSignedIn();
}

async function fetchUserInfo() {
    try {
        const response = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
            headers: { Authorization: `Bearer ${accessToken}` }
        });
        const data = await response.json();

        userName.textContent = data.name || data.email;
        userAvatar.src = data.picture || '';
        localStorage.setItem('google_user_name', data.name || data.email);
        localStorage.setItem('google_user_avatar', data.picture || '');
    } catch (error) {
        console.error('Error fetching user info:', error);
    }
}

function updateUIForSignedIn() {
    googleSignInBtn.style.display = 'none';
    userInfo.style.display = 'flex';
    fetchSheetsBtn.disabled = false;

    // Restore saved user info
    const savedName = localStorage.getItem('google_user_name');
    const savedAvatar = localStorage.getItem('google_user_avatar');
    if (savedName) userName.textContent = savedName;
    if (savedAvatar) userAvatar.src = savedAvatar;
}

function updateUIForSignedOut() {
    googleSignInBtn.style.display = 'flex';
    userInfo.style.display = 'none';
    fetchSheetsBtn.disabled = true;
    sheetSelectRow.style.display = 'none';
    gdudPlugaRow.style.display = 'none';
    loadFromSheetsBtn.style.display = 'none';
}

// Google Sign In Button
googleSignInBtn.addEventListener('click', () => {
    if (tokenClient) {
        tokenClient.requestAccessToken();
    }
});

// Google Sign Out Button
googleSignOutBtn.addEventListener('click', () => {
    accessToken = null;
    localStorage.removeItem('google_access_token');
    localStorage.removeItem('google_user_name');
    localStorage.removeItem('google_user_avatar');

    if (google.accounts.oauth2.revoke) {
        google.accounts.oauth2.revoke(accessToken);
    }

    updateUIForSignedOut();
    showSheetsStatus('', '');
});

// ============================================
// Google Sheets Functions
// ============================================

// Fetch sheets from spreadsheet
fetchSheetsBtn.addEventListener('click', async () => {
    const url = sheetsUrl.value.trim();

    if (!url) {
        showSheetsStatus('error', STRINGS.sheetsUrlError);
        return;
    }

    if (!accessToken) {
        showSheetsStatus('error', STRINGS.signInFirst);
        return;
    }

    currentSpreadsheetId = extractSpreadsheetId(url);
    if (!currentSpreadsheetId) {
        showSheetsStatus('error', 'לא ניתן לחלץ מזהה גיליון מהקישור');
        return;
    }

    showSheetsStatus('loading', STRINGS.fetchingSheets);
    fetchSheetsBtn.disabled = true;

    try {
        const response = await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${currentSpreadsheetId}?fields=sheets.properties`,
            {
                headers: { Authorization: `Bearer ${accessToken}` }
            }
        );

        if (!response.ok) {
            if (response.status === 401) {
                // Token expired
                accessToken = null;
                localStorage.removeItem('google_access_token');
                updateUIForSignedOut();
                throw new Error('פג תוקף ההתחברות. נא להתחבר מחדש.');
            }
            throw new Error('לא ניתן לגשת לגיליון. וודא שיש לך הרשאות צפייה.');
        }

        const data = await response.json();
        const sheets = data.sheets || [];

        // Populate dropdown
        sheetSelect.innerHTML = `<option value="">${STRINGS.selectSheet}</option>`;
        sheets.forEach(sheet => {
            const option = document.createElement('option');
            option.value = sheet.properties.title;
            option.textContent = sheet.properties.title;
            sheetSelect.appendChild(option);
        });

        if (sheets.length === 0) {
            showSheetsStatus('error', STRINGS.noSheetsFound);
        } else {
            showSheetsStatus('success', `נמצאו ${sheets.length} גיליונות`);
            sheetSelectRow.style.display = 'flex';
        }

    } catch (error) {
        console.error('Error fetching sheets:', error);
        showSheetsStatus('error', STRINGS.sheetsError.replace('{error}', error.message));
    } finally {
        fetchSheetsBtn.disabled = false;
    }
});

// Sheet selection change
sheetSelect.addEventListener('change', () => {
    if (sheetSelect.value) {
        gdudPlugaRow.style.display = 'flex';
        loadFromSheetsBtn.style.display = 'block';
    } else {
        gdudPlugaRow.style.display = 'none';
        loadFromSheetsBtn.style.display = 'none';
    }
});

// Load data from selected sheet
loadFromSheetsBtn.addEventListener('click', async () => {
    const selectedSheet = sheetSelect.value;
    const gdud = inputGdud.value.trim();
    const pluga = inputPluga.value.trim();

    if (!selectedSheet) {
        showSheetsStatus('error', 'נא לבחור גיליון');
        return;
    }

    if (!gdud || !pluga) {
        showSheetsStatus('error', STRINGS.sheetsGdudPlugaError);
        return;
    }

    if (!accessToken) {
        showSheetsStatus('error', STRINGS.signInFirst);
        return;
    }

    showSheetsStatus('loading', STRINGS.sheetsLoading);
    loadFromSheetsBtn.disabled = true;

    try {
        // Fetch sheet data using Google Sheets API
        const response = await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${currentSpreadsheetId}/values/${encodeURIComponent(selectedSheet)}`,
            {
                headers: { Authorization: `Bearer ${accessToken}` }
            }
        );

        if (!response.ok) {
            throw new Error('לא ניתן לקרוא את הנתונים מהגיליון');
        }

        const data = await response.json();
        const rows = data.values || [];

        if (rows.length < 2) {
            throw new Error('הגיליון ריק או אין בו מספיק נתונים');
        }

        // Get headers (first row)
        const headers = rows[0].map(h => (h || '').toString().trim().toLowerCase());

        // Find column indices
        const firstNameIdx = findColumnIndex(headers, ['שם פרטי', 'first name', 'firstname']);
        const lastNameIdx = findColumnIndex(headers, ['שם משפחה', 'last name', 'lastname']);
        const maIdx = findColumnIndex(headers, ['מ.א', 'מא', 'מספר אישי', 'id']);
        const mahlakaIdx = findColumnIndex(headers, ['מחלקה', 'mahlaka', 'platoon']);

        if (firstNameIdx === -1 && lastNameIdx === -1) {
            throw new Error('לא נמצאו עמודות שם פרטי/שם משפחה');
        }

        // Parse data rows
        let count = 0;
        for (let i = 1; i < rows.length; i++) {
            const row = rows[i];
            if (!row || row.length === 0 || row.every(cell => !cell || !cell.toString().trim())) continue;

            const firstName = firstNameIdx !== -1 ? (row[firstNameIdx] || '').toString().trim() : '';
            const lastName = lastNameIdx !== -1 ? (row[lastNameIdx] || '').toString().trim() : '';
            const misparIshi = maIdx !== -1 ? (row[maIdx] || '').toString().trim() : '';
            const mahlaka = mahlakaIdx !== -1 ? (row[mahlakaIdx] || '').toString().trim() : '';

            if (firstName || lastName) {
                const memberId = `member_${Date.now()}_${i}`;
                teamMembers.push({
                    id: memberId,
                    firstName,
                    lastName,
                    misparIshi,
                    gdud,
                    pluga,
                    mahlaka
                });
                count++;
            }
        }

        saveToStorage();
        renderTable();
        showSheetsStatus('success', STRINGS.sheetsSuccess.replace('{count}', count));

    } catch (error) {
        console.error('Error loading from Google Sheets:', error);
        showSheetsStatus('error', STRINGS.sheetsError.replace('{error}', error.message));
    } finally {
        loadFromSheetsBtn.disabled = false;
    }
});

// Extract spreadsheet ID from Google Sheets URL
function extractSpreadsheetId(url) {
    const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    return match ? match[1] : null;
}

// Find column index by possible names
function findColumnIndex(headers, possibleNames) {
    for (const name of possibleNames) {
        const idx = headers.indexOf(name.toLowerCase());
        if (idx !== -1) return idx;
    }
    return -1;
}

// Show status message
function showSheetsStatus(type, message) {
    if (!message) {
        sheetsStatus.className = 'sheets-status';
        sheetsStatus.textContent = '';
        return;
    }
    sheetsStatus.className = 'sheets-status ' + type;
    sheetsStatus.textContent = message;
}

// ============================================
// XLS Upload Handler
// ============================================
xlsUpload.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (event) => {
        try {
            const data = new Uint8Array(event.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
            const jsonData = XLSX.utils.sheet_to_json(firstSheet);

            jsonData.forEach((row, index) => {
                const firstName = row['First Name'] || row['FirstName'] || row['first_name'] || row['firstName'] ||
                                  row['שם פרטי'] || row['שם_פרטי'] || '';
                const lastName = row['Last Name'] || row['LastName'] || row['last_name'] || row['lastName'] ||
                                 row['שם משפחה'] || row['שם_משפחה'] || '';
                const misparIshi = row['מ.א'] || row['מא'] || row['מספר אישי'] || row['ID'] || row['id'] || '';
                const gdud = row['גדוד'] || row['Gdud'] || row['Battalion'] || '';
                const pluga = row['פלוגה'] || row['Pluga'] || row['Company'] || '';
                const mahlaka = row['מחלקה'] || row['Mahlaka'] || row['Platoon'] || row['מח'] || '';

                if (firstName || lastName) {
                    const memberId = `member_${Date.now()}_${index}`;
                    teamMembers.push({
                        id: memberId,
                        firstName: firstName.toString().trim(),
                        lastName: lastName.toString().trim(),
                        misparIshi: misparIshi.toString().trim(),
                        gdud: gdud.toString().trim(),
                        pluga: pluga.toString().trim(),
                        mahlaka: mahlaka.toString().trim()
                    });
                }
            });

            saveToStorage();
            renderTable();
            alert(STRINGS.importSuccess.replace('{count}', jsonData.length));
        } catch (error) {
            console.error('Error parsing file:', error);
            alert(STRINGS.importError);
        }
    };
    reader.readAsArrayBuffer(file);
    e.target.value = '';
});

// ============================================
// Date Range
// ============================================
applyDatesBtn.addEventListener('click', () => {
    startDate = new Date(startDateInput.value);
    endDate = new Date(endDateInput.value);

    if (startDate > endDate) {
        alert(STRINGS.dateError);
        return;
    }

    saveToStorage();
    renderTable();
});

// ============================================
// Export Data
// ============================================
exportDataBtn.addEventListener('click', () => {
    if (teamMembers.length === 0) {
        alert(STRINGS.noDataExport);
        return;
    }

    const dates = getDateRange();
    const exportData = teamMembers.map(member => {
        const row = {
            'שם פרטי': member.firstName,
            'שם משפחה': member.lastName,
            'מ.א': member.misparIshi,
            'גדוד': member.gdud,
            'פלוגה': member.pluga,
            'מחלקה': member.mahlaka
        };

        dates.forEach(date => {
            const dateKey = formatDateKey(date);
            const status = attendanceData[member.id]?.[dateKey] || 'unmarked';
            row[formatDateHeader(date)] = STATUS_LABELS[status];
        });

        return row;
    });

    const ws = XLSX.utils.json_to_sheet(exportData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'נוכחות');
    XLSX.writeFile(wb, `נוכחות_${formatDateKey(new Date())}.xlsx`);
});

// ============================================
// Clear All Data
// ============================================
clearDataBtn.addEventListener('click', () => {
    if (confirm(STRINGS.confirmClear)) {
        teamMembers = [];
        attendanceData = {};
        saveToStorage();
        renderTable();
    }
});

// ============================================
// Helper Functions
// ============================================
function getDateRange() {
    const dates = [];
    const current = new Date(startDate);

    while (current <= endDate) {
        dates.push(new Date(current));
        current.setDate(current.getDate() + 1);
    }

    return dates;
}

function formatDateHeader(date) {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    return `${day}/${month}`;
}

function formatDateKey(date) {
    return date.toISOString().split('T')[0];
}

// ============================================
// Render Table
// ============================================
function renderTable() {
    headerRow.innerHTML = `
        <th class="sticky-col col-name">${STRINGS.name}</th>
        <th class="sticky-col col-ma">${STRINGS.misparIshi}</th>
        <th class="sticky-col col-gdud">${STRINGS.gdud}</th>
        <th class="sticky-col col-pluga">${STRINGS.pluga}</th>
        <th class="sticky-col col-mahlaka">${STRINGS.mahlaka}</th>
    `;
    attendanceBody.innerHTML = '';

    if (teamMembers.length === 0) {
        noDataMsg.style.display = 'block';
        attendanceTable.style.display = 'none';
        return;
    }

    noDataMsg.style.display = 'none';
    attendanceTable.style.display = 'table';

    const dates = getDateRange();

    dates.forEach(date => {
        const th = document.createElement('th');
        th.textContent = formatDateHeader(date);
        th.title = date.toLocaleDateString('he-IL');
        headerRow.appendChild(th);
    });

    const actionTh = document.createElement('th');
    actionTh.textContent = STRINGS.action;
    headerRow.appendChild(actionTh);

    teamMembers.forEach(member => {
        const row = document.createElement('tr');

        const nameCell = document.createElement('td');
        nameCell.textContent = `${member.firstName} ${member.lastName}`;
        row.appendChild(nameCell);

        const maCell = document.createElement('td');
        maCell.textContent = member.misparIshi || '';
        row.appendChild(maCell);

        const gdudCell = document.createElement('td');
        gdudCell.textContent = member.gdud || '';
        row.appendChild(gdudCell);

        const plugaCell = document.createElement('td');
        plugaCell.textContent = member.pluga || '';
        row.appendChild(plugaCell);

        const mahlakaCell = document.createElement('td');
        mahlakaCell.textContent = member.mahlaka || '';
        row.appendChild(mahlakaCell);

        dates.forEach(date => {
            const dateKey = formatDateKey(date);
            const cell = document.createElement('td');
            cell.className = 'attendance-cell';

            const status = attendanceData[member.id]?.[dateKey] || 'unmarked';
            cell.classList.add(status);
            cell.textContent = STATUS_LABELS[status];
            cell.title = `${member.firstName} ${member.lastName} - ${date.toLocaleDateString('he-IL')}\n${STRINGS.clickToChange}`;

            cell.addEventListener('click', () => {
                cycleStatus(member.id, dateKey, cell);
            });

            row.appendChild(cell);
        });

        const actionCell = document.createElement('td');
        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'delete-btn';
        deleteBtn.textContent = '✕';
        deleteBtn.title = STRINGS.removeMember;
        deleteBtn.addEventListener('click', () => {
            if (confirm(STRINGS.confirmRemove.replace('{name}', `${member.firstName} ${member.lastName}`))) {
                removeMember(member.id);
            }
        });
        actionCell.appendChild(deleteBtn);
        row.appendChild(actionCell);

        attendanceBody.appendChild(row);
    });

    renderTotalsRows(dates);
}

function renderTotalsRows(dates) {
    const totalsData = calculateTotals(dates);

    const missionRow = createTotalRow(
        STRINGS.availableForMission,
        '✓ +',
        totalsData.mission,
        'total-mission'
    );
    attendanceBody.appendChild(missionRow);

    const leaveRow = createTotalRow(
        STRINGS.includingLeave,
        '✓ + -',
        totalsData.includeLeave,
        'total-leave'
    );
    attendanceBody.appendChild(leaveRow);

    const countedRow = createTotalRow(
        STRINGS.totalCounted,
        '✓ + - $',
        totalsData.counted,
        'total-counted'
    );
    attendanceBody.appendChild(countedRow);
}

function createTotalRow(label, symbols, dailyTotals, className) {
    const row = document.createElement('tr');
    row.className = `total-row ${className}`;

    const labelCell = document.createElement('td');
    labelCell.className = 'total-label';
    labelCell.textContent = label;
    row.appendChild(labelCell);

    const symbolsCell = document.createElement('td');
    symbolsCell.className = 'total-symbols';
    symbolsCell.textContent = symbols;
    row.appendChild(symbolsCell);

    for (let i = 0; i < 3; i++) {
        const emptyCell = document.createElement('td');
        emptyCell.className = 'total-empty';
        row.appendChild(emptyCell);
    }

    dailyTotals.forEach(count => {
        const cell = document.createElement('td');
        cell.className = 'total-cell';
        cell.textContent = count;
        row.appendChild(cell);
    });

    const actionCell = document.createElement('td');
    actionCell.className = 'total-action';
    row.appendChild(actionCell);

    return row;
}

function calculateTotals(dates) {
    const totals = {
        mission: [],
        includeLeave: [],
        counted: []
    };

    dates.forEach(date => {
        const dateKey = formatDateKey(date);
        let missionCount = 0;
        let leaveCount = 0;
        let countedCount = 0;

        teamMembers.forEach(member => {
            const status = attendanceData[member.id]?.[dateKey] || 'unmarked';

            if (TOTALS_CONFIG.mission.includes(status)) {
                missionCount++;
            }
            if (TOTALS_CONFIG.includeLeave.includes(status)) {
                leaveCount++;
            }
            if (TOTALS_CONFIG.counted.includes(status)) {
                countedCount++;
            }
        });

        totals.mission.push(missionCount);
        totals.includeLeave.push(leaveCount);
        totals.counted.push(countedCount);
    });

    return totals;
}

function cycleStatus(memberId, dateKey, cell) {
    if (!attendanceData[memberId]) {
        attendanceData[memberId] = {};
    }

    const currentStatus = attendanceData[memberId][dateKey] || 'unmarked';
    const currentIndex = STATUSES.indexOf(currentStatus);
    const nextIndex = (currentIndex + 1) % STATUSES.length;
    const nextStatus = STATUSES[nextIndex];

    attendanceData[memberId][dateKey] = nextStatus;

    cell.classList.remove(...STATUSES);
    cell.classList.add(nextStatus);
    cell.textContent = STATUS_LABELS[nextStatus];

    saveToStorage();
    renderTable();
}

function removeMember(memberId) {
    teamMembers = teamMembers.filter(m => m.id !== memberId);
    delete attendanceData[memberId];
    saveToStorage();
    renderTable();
}

function saveToStorage() {
    localStorage.setItem('presenceManager_members', JSON.stringify(teamMembers));
    localStorage.setItem('presenceManager_attendance', JSON.stringify(attendanceData));
    localStorage.setItem('presenceManager_startDate', startDate.toISOString());
    localStorage.setItem('presenceManager_endDate', endDate.toISOString());
}

function loadFromStorage() {
    const savedMembers = localStorage.getItem('presenceManager_members');
    const savedAttendance = localStorage.getItem('presenceManager_attendance');
    const savedStartDate = localStorage.getItem('presenceManager_startDate');
    const savedEndDate = localStorage.getItem('presenceManager_endDate');

    if (savedMembers) {
        teamMembers = JSON.parse(savedMembers);
    }
    if (savedAttendance) {
        attendanceData = JSON.parse(savedAttendance);
    }
    if (savedStartDate) {
        startDate = new Date(savedStartDate);
        startDateInput.value = formatDateKey(startDate);
    }
    if (savedEndDate) {
        endDate = new Date(savedEndDate);
        endDateInput.value = formatDateKey(endDate);
    }
}
