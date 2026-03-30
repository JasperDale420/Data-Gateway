#!/bin/bash
# Master setup script for trading bot
# Sets up both daily trading and hourly monitoring

echo "=========================================="
echo "TRADING BOT - COMPLETE SETUP"
echo "=========================================="
echo ""
echo "This will set up:"
echo "1. Daily trading cron job (bot runs automatically)"
echo "2. Hourly wake-up cron job (Jarvis monitors and improves)"
echo ""
echo "Both use PAPER TRADING only - no real money."
echo ""

# Check if Data-Gateway is running
echo "Checking Data-Gateway..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Data-Gateway is running"
else
    echo "⚠️  Data-Gateway not running on port 8080"
    echo "   Start it with: cd ../ && ./scripts/run.sh"
    read -p "Continue anyway? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check Alpaca credentials
echo ""
echo "Checking Alpaca credentials..."
if [ -f "../.env" ] && grep -q "APCA_API_KEY_ID" "../.env"; then
    echo "✅ Alpaca credentials found"
else
    echo "❌ Alpaca credentials not found in ../.env"
    exit 1
fi

# Create necessary directories
echo ""
echo "Creating directories..."
mkdir -p logs
mkdir -p test_logs

# Setup daily trading
echo ""
echo "=========================================="
echo "SETUP DAILY TRADING"
echo "=========================================="
./setup_daily_trading.sh

echo ""
echo "=========================================="
echo "SETUP HOURLY MONITORING"
echo "=========================================="
./setup_hourly_wakeup.sh

echo ""
echo "=========================================="
echo "SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "✅ Trading bot is now fully automated:"
echo ""
echo "DAILY TRADING:"
echo "  - Bot runs automatically at scheduled time"
echo "  - Trades SPY with mean reversion strategy"
echo "  - Uses Alpaca paper trading ($99,868 test equity)"
echo "  - Logs all trades to logs/"
echo ""
echo "HOURLY MONITORING:"
echo "  - Jarvis wakes up every hour"
echo "  - Checks trading results and performance"
echo "  - Fixes issues and improves strategy"
echo "  - Updates ARTIFACT_TRAIL.md with progress"
echo ""
echo "MANUAL COMMANDS:"
echo "  ./run_with_deps.sh hourly_checkin.py     # Check current status"
echo "  ./run_with_deps.sh src/core/trading_bot.py # Run bot manually"
echo "  tail -f logs/daily_trading.log          # Watch daily trading"
echo "  cat logs/trades.jsonl                   # View all trades"
echo ""
echo "FILES:"
echo "  ARTIFACT_TRAIL.md                       # Development history"
echo "  logs/                                   # All logs and results"
echo "  src/                                    # Source code"
echo ""
echo "To test immediately:"
echo "  ./run_daily_trading.sh                  # Run trading bot now"
echo ""
echo "=========================================="
