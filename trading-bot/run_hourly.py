#!/usr/bin/env python3
"""
Hourly cron job for trading bot.
This script runs the trading bot and updates the artifact trail.
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core.trading_bot import TradingBot


def update_artifact_trail(iteration: int, result: dict):
    """Update the artifact trail with results from this iteration."""
    artifact_file = Path(__file__).parent / 'ARTIFACT_TRAIL.md'
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    entry = f"""
## Iteration {iteration} - {timestamp}

### Execution Results
- **Status**: {'SUCCESS' if result['success'] else 'FAILED'}
- **Signal Generated**: {result.get('signal', 'None')}
- **Trade Executed**: {result.get('trade_executed', False)}
- **Account Equity**: ${result.get('account_equity', 0):,.2f}
- **Positions**: {result.get('positions_count', 0)}
- **Error**: {result.get('error', 'None')}

### Market Conditions
- **Symbol**: {result.get('symbol', 'N/A')}
- **Data Points**: {result.get('data_points', 0)}
- **Signal Confidence**: {result.get('signal_confidence', 0):.2%}

### Next Steps
{result.get('next_steps', 'Continue hourly iterations')}
"""
    
    # Read current content
    with open(artifact_file, 'r') as f:
        content = f.read()
    
    # Find where to insert (after the initial setup section)
    if '## 2026-02-11 - Project Initiation' in content:
        parts = content.split('## 2026-02-11 - Project Initiation', 1)
        new_content = parts[0] + '## 2026-02-11 - Project Initiation' + parts[1].split('\n### Next Steps')[0] + entry + '\n'.join(parts[1].split('\n### Next Steps')[1:])
    else:
        new_content = content + entry
    
    # Write back
    with open(artifact_file, 'w') as f:
        f.write(new_content)


def run_hourly_iteration():
    """Run one hourly iteration of the trading bot."""
    print("=" * 70)
    print(f"TRADING BOT - HOURLY ITERATION")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    result = {
        'success': False,
        'timestamp': datetime.now().isoformat(),
        'iteration': 1,  # Will be updated from artifact trail
        'symbol': 'SPY',
        'error': None
    }
    
    try:
        # Initialize trading bot
        print("\n1. Initializing Trading Bot...")
        bot = TradingBot(
            symbol='SPY',
            strategy_type='mean_reversion',
            risk_per_trade=0.02,
            max_positions=3,
            log_dir='logs'
        )
        
        # Run iteration
        print("\n2. Running Trading Iteration...")
        bot.run_iteration()
        
        # Collect results
        result['success'] = True
        result['account_equity'] = bot.account_info['equity'] if bot.account_info else 0
        result['positions_count'] = len(bot.positions)
        result['data_points'] = 0  # Would need to track this
        
        print(f"\n3. Results:")
        print(f"   - Account Equity: ${result['account_equity']:,.2f}")
        print(f"   - Positions: {result['positions_count']}")
        print(f"   - Status: SUCCESS")
        
    except Exception as e:
        result['success'] = False
        result['error'] = str(e)
        print(f"\n❌ ERROR: {e}")
    
    # Update artifact trail
    print("\n4. Updating Artifact Trail...")
    
    # Read current iteration count from artifact trail
    artifact_file = Path(__file__).parent / 'ARTIFACT_TRAIL.md'
    if artifact_file.exists():
        with open(artifact_file, 'r') as f:
            content = f.read()
        
        # Count iterations
        iteration_count = content.count('## Iteration')
        result['iteration'] = iteration_count + 1
    else:
        result['iteration'] = 1
    
    update_artifact_trail(result['iteration'], result)
    
    print(f"\n5. Complete!")
    print(f"   - Iteration: {result['iteration']}")
    print(f"   - Artifact trail updated")
    print(f"   - Logs saved to: logs/")
    
    # Save detailed results to JSON
    results_file = Path(__file__).parent / 'logs' / 'hourly_results.jsonl'
    results_file.parent.mkdir(exist_ok=True)
    
    with open(results_file, 'a') as f:
        f.write(json.dumps(result) + '\n')
    
    return result


def setup_cron_job():
    """Setup the hourly cron job on the system."""
    print("\n" + "=" * 70)
    print("CRON JOB SETUP")
    print("=" * 70)
    
    cron_command = f"cd {Path(__file__).parent} && {sys.executable} {__file__}"
    
    print(f"\nCron command:")
    print(f"  {cron_command}")
    
    print(f"\nTo set up hourly cron job, run:")
    print(f"  crontab -e")
    print(f"\nAdd this line:")
    print(f"  0 * * * * {cron_command} >> {Path(__file__).parent}/logs/cron.log 2>&1")
    
    print(f"\nOr for testing every 5 minutes:")
    print(f"  */5 * * * * {cron_command} >> {Path(__file__).parent}/logs/cron.log 2>&1")
    
    # Create a simple setup script
    setup_script = Path(__file__).parent / 'setup_cron.sh'
    setup_script.write_text(f"""#!/bin/bash
# Setup hourly cron job for trading bot

CRON_CMD="{cron_command} >> {Path(__file__).parent}/logs/cron.log 2>&1"
CRON_JOB="0 * * * * $CRON_CMD"

echo "Setting up cron job:"
echo "$CRON_JOB"
echo ""
echo "Current crontab:"
crontab -l 2>/dev/null || echo "(no crontab)"
echo ""
echo "Adding new job..."
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
echo ""
echo "New crontab:"
crontab -l
echo ""
echo "Cron job setup complete!"
""")
    
    setup_script.chmod(0o755)
    print(f"\nSetup script created: {setup_script}")
    print(f"Run: ./{setup_script.name}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--setup-cron':
        setup_cron_job()
    else:
        run_hourly_iteration()