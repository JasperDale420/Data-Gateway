# Automated Trading Bot Project

## Overview
This project aims to build a consistently profitable trading bot using Alpaca paper trading through the Data-Gateway. The bot will trade crypto, equities, or options with a focus on proving profitability through systematic backtesting and live paper trading.

## Project Structure
```
trading-bot/
├── README.md              # This file
├── CHANGELOG.md           # Development history
├── ARTIFACT_TRAIL.md      # Progress tracking
├── requirements.txt       # Python dependencies
├── config/
│   ├── bot_config.yaml    # Bot configuration
│   └── strategies/        # Strategy definitions
├── src/
│   ├── core/             # Core trading engine
│   ├── strategies/       # Strategy implementations
│   ├── data/            # Data fetching and processing
│   ├── backtesting/     # Backtesting framework
│   └── execution/       # Order execution
├── tests/               # Unit and integration tests
├── notebooks/           # Research and analysis notebooks
└── logs/               # Trading logs and performance metrics
```

## Initial Setup
1. Connect to Data-Gateway for market data
2. Set up Alpaca paper trading account access
3. Create basic backtesting framework
4. Implement first simple strategy (mean reversion)
5. Run hourly cron job for continuous improvement

## Development Principles
- **Incremental Improvement**: Start simple, iterate based on results
- **Risk Management**: Never risk more than 2% per trade
- **Documentation**: Keep detailed artifact trail for session continuity
- **Validation**: Prove profitability through backtesting before live trading
- **Transparency**: All decisions and results logged and version controlled

## Success Metrics
- Positive Sharpe ratio (>1.0)
- Consistent monthly returns (>5%)
- Maximum drawdown (<15%)
- Win rate (>55%)
- Profit factor (>1.5)