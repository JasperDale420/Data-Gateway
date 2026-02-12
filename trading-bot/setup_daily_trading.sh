#!/bin/bash
# Setup daily trading cron job for the trading bot

SCRIPT_PATH="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/run_daily_trading.sh"
LOG_DIR="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Choose trading schedule
echo "=========================================="
echo "SETUP DAILY TRADING CRON JOB"
echo "=========================================="
echo ""
echo "Choose trading schedule:"
echo "1. Market open (6:30 AM PST / 9:30 AM EST)"
echo "2. Mid-day (10:00 AM PST / 1:00 PM EST)"
echo "3. Market close (1:00 PM PST / 4:00 PM EST)"
echo "4. Custom time"
echo ""
read -p "Select option (1-4): " -n 1 -r
echo ""

case $REPLY in
    1)
        CRON_TIME="30 6 * * 1-5"  # 6:30 AM PST Mon-Fri
        SCHEDULE_NAME="Market Open"
        ;;
    2)
        CRON_TIME="0 10 * * 1-5"  # 10:00 AM PST Mon-Fri
        SCHEDULE_NAME="Mid-day"
        ;;
    3)
        CRON_TIME="0 13 * * 1-5"  # 1:00 PM PST Mon-Fri
        SCHEDULE_NAME="Market Close"
        ;;
    4)
        echo "Enter custom cron time (minute hour * * 1-5 for weekdays):"
        echo "Example: '30 9 * * 1-5' for 9:30 AM PST Mon-Fri"
        read -p "Cron time: " CRON_TIME
        SCHEDULE_NAME="Custom"
        ;;
    *)
        echo "Invalid selection, using Market Open (6:30 AM PST)"
        CRON_TIME="30 6 * * 1-5"
        SCHEDULE_NAME="Market Open"
        ;;
esac

CRON_CMD="$CRON_TIME $SCRIPT_PATH"

echo ""
echo "=========================================="
echo "CRON JOB DETAILS"
echo "=========================================="
echo "Schedule: $SCHEDULE_NAME"
echo "Cron time: $CRON_TIME"
echo "Command: $SCRIPT_PATH"
echo "Logs: $LOG_DIR/daily_trading.log"
echo ""

# Check current crontab
echo "Current crontab:"
crontab -l 2>/dev/null || echo "(no crontab)"
echo ""

# Ask for confirmation
read -p "Add this daily trading cron job? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Add to crontab
    (crontab -l 2>/dev/null; echo "# Trading Bot - Daily Trading"; echo "$CRON_CMD") | crontab -

    echo ""
    echo "✅ Daily trading cron job added!"
    echo ""
    echo "New crontab:"
    crontab -l
    echo ""
    echo "The bot will:"
    echo "1. Run automatically at scheduled time (market days only)"
    echo "2. Trade SPY with mean reversion strategy"
    echo "3. Use paper trading (no real money)"
    echo "4. Log all trades to $LOG_DIR/"
    echo ""
    echo "To test immediately:"
    echo "  $SCRIPT_PATH"
    echo ""
    echo "To monitor:"
    echo "  tail -f $LOG_DIR/daily_trading.log"
    echo "  cat $LOG_DIR/trades.jsonl"
    echo ""
    echo "To remove later:"
    echo "  crontab -e"
    echo "  (delete lines with 'run_daily_trading.sh')"
else
    echo ""
    echo "❌ Daily trading cron job not added."
    echo ""
    echo "To add manually later:"
    echo "1. Edit crontab: crontab -e"
    echo "2. Add this line: $CRON_CMD"
fi

echo "=========================================="
