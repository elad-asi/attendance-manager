# ============================================
# Email Authentication Module
# Sends verification codes via email
# ============================================

import smtplib
import random
import string
import time
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Email configuration - using Gmail SMTP
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SENDER_EMAIL = 'asi.michal.elad@gmail.com'
# App password should be set in environment variable for security
# Generate one at: https://myaccount.google.com/apppasswords
SENDER_APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', '')

# Store pending verification codes (in production, use Redis or database)
# Format: {email: {'code': '123456', 'expires': timestamp, 'attempts': 0}}
pending_codes = {}

# Store verified sessions
# Format: {session_token: {'email': 'user@example.com', 'expires': timestamp}}
verified_sessions = {}

# Configuration
CODE_LENGTH = 6
CODE_EXPIRY_SECONDS = 300  # 5 minutes
SESSION_EXPIRY_SECONDS = 86400 * 7  # 7 days
MAX_ATTEMPTS = 3


def generate_code():
    """Generate a random 6-digit verification code"""
    return ''.join(random.choices(string.digits, k=CODE_LENGTH))


def generate_session_token():
    """Generate a random session token"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=32))


def send_verification_email(to_email, code):
    """Send verification code email"""
    if not SENDER_APP_PASSWORD:
        print("WARNING: GMAIL_APP_PASSWORD not set, skipping email send")
        return False, "Email service not configured"

    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'קוד אימות - מנהל נוכחות'
        msg['From'] = SENDER_EMAIL
        msg['To'] = to_email

        # Email body in Hebrew
        html_content = f"""
        <html dir="rtl">
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; direction: rtl; text-align: right; }}
                .code {{ font-size: 32px; font-weight: bold; letter-spacing: 8px;
                         background: #f0f0f0; padding: 20px; text-align: center;
                         border-radius: 8px; margin: 20px 0; }}
                .warning {{ color: #666; font-size: 12px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <h2>קוד אימות</h2>
            <p>שלום,</p>
            <p>הקוד שלך לכניסה למערכת מנהל נוכחות:</p>
            <div class="code">{code}</div>
            <p>הקוד תקף ל-5 דקות.</p>
            <p class="warning">אם לא ביקשת קוד זה, התעלם מהודעה זו.</p>
        </body>
        </html>
        """

        text_content = f"""
        קוד אימות - מנהל נוכחות

        שלום,
        הקוד שלך: {code}

        הקוד תקף ל-5 דקות.
        """

        part1 = MIMEText(text_content, 'plain', 'utf-8')
        part2 = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(part1)
        msg.attach(part2)

        # Send email with timeout
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())

        print(f"Verification email sent to {to_email}")
        return True, "Email sent"

    except smtplib.SMTPAuthenticationError:
        print("SMTP authentication failed - check app password")
        return False, "Email authentication failed"
    except TimeoutError:
        print("SMTP connection timed out")
        return False, "Email service timeout"
    except Exception as e:
        print(f"Error sending email: {e}")
        return False, str(e)


def request_verification_code(email):
    """Request a new verification code for an email address"""
    email = email.lower().strip()

    # Generate code
    code = generate_code()
    expires = time.time() + CODE_EXPIRY_SECONDS

    # Store code
    pending_codes[email] = {
        'code': code,
        'expires': expires,
        'attempts': 0
    }

    # Send email
    success, message = send_verification_email(email, code)

    if success:
        return True, "קוד אימות נשלח למייל"
    else:
        # For development - if email fails, return the code in console
        print(f"DEV MODE: Verification code for {email}: {code}")
        return True, f"קוד אימות: {code} (development mode)"


def verify_code(email, code):
    """Verify a code and create a session if valid"""
    email = email.lower().strip()

    # Check if code exists
    if email not in pending_codes:
        return False, None, "לא נמצא קוד אימות - בקש קוד חדש"

    stored = pending_codes[email]

    # Check if expired
    if time.time() > stored['expires']:
        del pending_codes[email]
        return False, None, "הקוד פג תוקף - בקש קוד חדש"

    # Check attempts
    if stored['attempts'] >= MAX_ATTEMPTS:
        del pending_codes[email]
        return False, None, "יותר מדי ניסיונות - בקש קוד חדש"

    # Verify code
    if code != stored['code']:
        stored['attempts'] += 1
        remaining = MAX_ATTEMPTS - stored['attempts']
        return False, None, f"קוד שגוי - נותרו {remaining} ניסיונות"

    # Code is valid - create session
    del pending_codes[email]
    session_token = generate_session_token()
    verified_sessions[session_token] = {
        'email': email,
        'expires': time.time() + SESSION_EXPIRY_SECONDS
    }

    return True, session_token, "אומת בהצלחה"


def validate_session(session_token):
    """Validate a session token and return the email if valid"""
    if not session_token or session_token not in verified_sessions:
        return None

    session = verified_sessions[session_token]

    # Check if expired
    if time.time() > session['expires']:
        del verified_sessions[session_token]
        return None

    return session['email']


def logout(session_token):
    """Invalidate a session"""
    if session_token in verified_sessions:
        del verified_sessions[session_token]
        return True
    return False


def cleanup_expired():
    """Clean up expired codes and sessions"""
    current_time = time.time()

    # Clean expired codes
    expired_codes = [email for email, data in pending_codes.items()
                    if current_time > data['expires']]
    for email in expired_codes:
        del pending_codes[email]

    # Clean expired sessions
    expired_sessions = [token for token, data in verified_sessions.items()
                       if current_time > data['expires']]
    for token in expired_sessions:
        del verified_sessions[token]
