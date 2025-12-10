#!/bin/bash
echo "========================================"
echo "  Attendance Manager - Starting Server"
echo "========================================"
echo

cd "$(dirname "$0")/backend"

echo "Installing dependencies..."
pip install -r requirements.txt

echo
echo "Starting server..."
echo
echo "========================================"
echo "  Server running at: http://localhost:5000"
echo "  Press Ctrl+C to stop"
echo "========================================"
echo

python app.py
