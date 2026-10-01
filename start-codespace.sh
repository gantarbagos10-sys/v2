#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python3 -m pip install -r requirements.txt
export PORT="${PORT:-3000}"
exec python3 app.py
