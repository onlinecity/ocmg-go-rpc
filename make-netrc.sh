#!/usr/bin/env bash
set -euo pipefail

cat <<EOF
machine github.com
login $GIT_USER
password $GIT_TOKEN
EOF
