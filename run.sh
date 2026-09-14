#!/usr/bin/env sh
set -u
cd "$(dirname "$0")"

if [ ! -f "failflume_batter_scanner/run.sh" ]; then
  printf '%s\n' 'FAILFLUME launcher could not find failflume_batter_scanner/run.sh' >&2
  printf '%s\n' 'Make sure the full repository/ZIP was extracted before running.' >&2
  exit 1
fi

exec sh "failflume_batter_scanner/run.sh"
