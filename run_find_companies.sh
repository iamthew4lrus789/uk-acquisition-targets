#!/bin/bash
# Wrapper script for find_companies.py
# Sets up PYTHONPATH and activates virtual environment if present

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment if it exists
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Run with PYTHONPATH set to project root
PYTHONPATH="$SCRIPT_DIR" python "$SCRIPT_DIR/src/find_companies.py" "$@"
