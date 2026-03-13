#!/bin/bash
# Daily trading script - runs the trading bot once per day
# This executes trades automatically in paper trading

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot/logs/daily_trading.log"
SCRIPT_DIR="/Users/jacobmcmillan/Empire/Data-Gateway/trading-bot"

echo "==========================================" >> "$LOG_FILE"
echo "DAILY TRADING BOT EXECUTION - $TIMESTAMP" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"

# Activate Data-Gateway virtual environment
source "$SCRIPT_DIR/../.venv/bin/activate"

# Run the trading bot
cd "$SCRIPT_DIR"
echo "Running trading bot..." >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Run with a timeout (1 hour max) to prevent hanging
timeout 3600 python3 -c "
import sys
sys.path.insert(0, 'src')
from core.trading_bot import TradingBot

print('Starting daily trading session...')
print('=' * 60)

bot = TradingBot(
    symbol='SPY',
    strategy_type='mean_reversion',
    risk_per_trade=0.02,
    max_positions=3,
    log_dir='logs'
)

bot.run_iteration()

print('=' * 60)
print('Daily trading session complete.')
print(f'Logs saved to: {bot.log_dir}')
" >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

echo "" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "Exit code: $EXIT_CODE" >> "$LOG_FILE"
echo "Completed at: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
echo "==========================================" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Also log to a separate results file
RESULTS_FILE="$SCRIPT_DIR/logs/daily_results.jsonl"
echo "{\"timestamp\": \"$TIMESTAMP\", \"exit_code\": $EXIT_CODE}" >> "$RESULTS_FILE"

exit $EXIT_CODE
