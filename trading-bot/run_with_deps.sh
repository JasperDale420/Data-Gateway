#!/bin/bash
# Run trading bot scripts using Data-Gateway's virtual environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_GATEWAY_DIR="$(dirname "$SCRIPT_DIR")"

# Activate Data-Gateway virtual environment
source "$DATA_GATEWAY_DIR/.venv/bin/activate"

# Run the requested script
if [ $# -eq 0 ]; then
    echo "Usage: $0 <script.py> [args...]"
    echo ""
    echo "Available scripts:"
    echo "  test_connectivity.py    - Test Data-Gateway and Alpaca connectivity"
    echo "  test_integration.py     - Run integration tests"
    echo "  src/core/trading_bot.py - Run trading bot (main engine)"
    echo "  run_hourly.py          - Run hourly iteration"
    exit 1
fi

SCRIPT="$1"
shift

if [ ! -f "$SCRIPT" ]; then
    # Try to find it in the current directory
    if [ -f "$SCRIPT_DIR/$SCRIPT" ]; then
        SCRIPT="$SCRIPT_DIR/$SCRIPT"
    else
        echo "Error: Script not found: $SCRIPT"
        exit 1
    fi
fi

echo "Running $SCRIPT with Data-Gateway dependencies..."
echo "=========================================="

python3 "$SCRIPT" "$@"
