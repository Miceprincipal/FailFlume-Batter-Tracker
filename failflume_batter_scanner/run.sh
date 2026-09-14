#!/usr/bin/env sh
set -u
cd "$(dirname "$0")"
unset PYTHONHOME
unset PYTHONPATH
PYTHON_EXE="${PYTHON_EXE:-python3}"
printf 'Python: %s\n' "$PYTHON_EXE"
"$PYTHON_EXE" -c "import re,argparse; print('Python stdlib OK')" || exit 1
exec "$PYTHON_EXE" app.py --open --auto-backfill-days 90
