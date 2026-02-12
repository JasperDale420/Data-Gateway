#!/bin/bash
# Hourly wake-up script for Jarvis to work on trading bot
# This cron job wakes up Jarvis (OpenClaw) to continue building/fixing/monitoring the trading bot

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/logs/jarvis_wakeup.log"
PROJECT_DIR="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot"

echo "==========================================" >> "$LOG_FILE"
echo "Jarvis Wake-up Call - $TIMESTAMP" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

# Check if OpenClaw gateway is running
if curl -s http://localhost:18789/health > /dev/null 2>&1; then
    echo "OpenClaw gateway is running" >> "$LOG_FILE"

    # Send wake event to OpenClaw
    curl -X POST http://localhost:18789/api/cron/wake \
        -H "Content-Type: application/json" \
        -d '{
            "text": "[Hourly Trading Bot Check] Time to work on the trading bot. Check dependencies, run tests, improve strategy, and monitor progress. Current status logged in trading-bot/ARTIFACT_TRAIL.md",
            "mode": "now"
        }' >> "$LOG_FILE" 2>&1

    echo "" >> "$LOG_FILE"
    echo "Wake event sent to OpenClaw" >> "$LOG_FILE"
else
    echo "OpenClaw gateway not running on port 18789" >> "$LOG_FILE"
    echo "Trying to start OpenClaw..." >> "$LOG_FILE"

    # Try to start OpenClaw
    if command -v openclaw > /dev/null 2>&1; then
        openclaw gateway start >> "$LOG_FILE" 2>&1 &
        sleep 5
        echo "OpenClaw started" >> "$LOG_FILE"
    else
        echo "OpenClaw command not found" >> "$LOG_FILE"
    fi
fi

# Check for daily trading results
echo "" >> "$LOG_FILE"
echo "Checking daily trading results..." >> "$LOG_FILE"

DAILY_LOG="$PROJECT_DIR/logs/daily_trading.log"
TRADES_LOG="$PROJECT_DIR/logs/trades.jsonl"

if [ -f "$DAILY_LOG" ]; then
    LAST_RUN=$(tail -5 "$DAILY_LOG" | grep -i "completed\|exit code" || echo "No recent runs")
    echo "Daily trading log: $LAST_RUN" >> "$LOG_FILE"
fi

if [ -f "$TRADES_LOG" ]; then
    TRADE_COUNT=$(wc -l < "$TRADES_LOG" | tr -d ' ')
    echo "Total trades: $TRADE_COUNT" >> "$LOG_FILE"
    if [ $TRADE_COUNT -gt 0 ]; then
        LAST_TRADE=$(tail -1 "$TRADES_LOG")
        echo "Last trade: $LAST_TRADE" >> "$LOG_FILE"
    fi
fi

# Update artifact trail with wake-up record
echo "" >> "$LOG_FILE"
echo "Updating artifact trail..." >> "$LOG_FILE"

if [ -f "$PROJECT_DIR/ARTIFACT_TRAIL.md" ]; then
    # Add wake-up entry to artifact trail
    WAKE_ENTRY="\n## Jarvis Wake-up - $TIMESTAMP\n\n### Tasks for this hour:\n1. Check daily trading results (if any)\n2. Review recent trades and performance\n3. Fix any issues that arose\n4. Improve strategy based on results\n5. Update documentation\n\n### Recent activity:\n- Daily trades executed: $( [ -f "$TRADES_LOG" ] && wc -l < "$TRADES_LOG" || echo 0 )\n- Last daily run: $( [ -f "$DAILY_LOG" ] && tail -1 "$DAILY_LOG" | grep -o 'Completed at: [^ ]*' | cut -d' ' -f3- || echo 'None' )\n\n### Previous hour progress:\n$(tail -20 "$PROJECT_DIR/ARTIFACT_TRAIL.md" | grep -A5 -B5 "## Iteration\|## Jarvis Wake-up" | head -10)\n"

    # Insert at appropriate location
    sed -i '' "/## Phase 1 (COMPLETED/a\\
$WAKE_ENTRY" "$PROJECT_DIR/ARTIFACT_TRAIL.md"

    echo "Artifact trail updated" >> "$LOG_FILE"
else
    echo "Artifact trail not found at $PROJECT_DIR/ARTIFACT_TRAIL.md" >> "$LOG_FILE"
fi

echo "==========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
