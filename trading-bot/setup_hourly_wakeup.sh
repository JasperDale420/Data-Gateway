#!/bin/bash
# Setup hourly wake-up cron job for Jarvis to work on trading bot

SCRIPT_PATH="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/wake_jarvis_hourly.sh"
LOG_DIR="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Create cron job command (run at minute 0 of every hour)
CRON_CMD="0 * * * * $SCRIPT_PATH"

echo "=========================================="
echo "Setting up hourly wake-up cron job for Jarvis"
echo "=========================================="
echo ""
echo "Script: $SCRIPT_PATH"
echo "Logs: $LOG_DIR/jarvis_wakeup.log"
echo ""
echo "Cron job will run: $CRON_CMD"
echo ""

# Check current crontab
echo "Current crontab:"
crontab -l 2>/dev/null || echo "(no crontab)"
echo ""

# Ask for confirmation
read -p "Add this cron job? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -

    echo ""
    echo "✅ Cron job added!"
    echo ""
    echo "New crontab:"
    crontab -l
    echo ""
    echo "The cron job will:"
    echo "1. Run at the start of every hour"
    echo "2. Wake up Jarvis (OpenClaw) to work on trading bot"
    echo "3. Log activity to $LOG_DIR/jarvis_wakeup.log"
    echo "4. Update ARTIFACT_TRAIL.md with wake-up records"
    echo ""
    echo "To test immediately:"
    echo "  $SCRIPT_PATH"
    echo ""
    echo "To remove the cron job later:"
    echo "  crontab -e"
    echo "  (delete the line with 'wake_jarvis_hourly.sh')"
else
    echo ""
    echo "❌ Cron job not added."
    echo ""
    echo "To add manually later:"
    echo "1. Edit crontab: crontab -e"
    echo "2. Add this line: $CRON_CMD"
fi

echo "=========================================="
