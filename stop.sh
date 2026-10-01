#!/usr/bin/env bash
PID=$(pgrep -f "python3? .*migsock/app.py" || true)
if [ -n "$PID" ]; then kill $PID || true; fi
