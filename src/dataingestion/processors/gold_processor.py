import logging
import pandas as pd
from pathlib import Path
from dataingestion.shared.storage import data_lake
from dataingestion.shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class GoldProcessor:
    """
    Processor for the Gold Layer (Canonical).
    Aggregates Silver data, handles Corporate Actions, and produces Canonical History.
    """

    def process_ticker(self, symbol: str) -> str:
        """
        Build Gold Layer history for a ticker from Silver data.
        """
        try:
            # 1. Load Silver Prices
            # We need to find all silver price files for this symbol.
            # Path pattern: silver/{provider}/prices/equity/year=*/month=*/day=*/symbol={symbol}/*.parquet
            # This is hard to glob efficiently without a catalog.
            # But we know the structure.
            # Let's use a helper to find files.

            # For now, let's assume we scan the silver directory.
            # This might be slow for many files.
            # Ideally we have a catalog or we just look at known providers.

            silver_base = Path(settings.DATA_DIR) / "silver"
            price_dfs = []

            # Providers to check (Priority Order: Alpaca > YFinance)
            providers = ["alpaca", "yfinance"]

            for provider in providers:
                provider_dir = silver_base / provider / "prices" / "equity"
                if not provider_dir.exists():
                    continue

                # Glob for symbol files
                # We need to walk the date partitions
                # Or we can use glob with wildcards if supported
                # glob('**/symbol=SYMBOL/*.parquet')

                # Note: symbol in path might be sanitized or not.
                # Our storage uses symbol={symbol}.
                files = list(provider_dir.glob(f"**/symbol={symbol}/*.parquet"))

                for f in files:
                    try:
                        df = pd.read_parquet(f)
                        df["source"] = provider
                        price_dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to read silver file {f}: {e}")

            if not price_dfs:
                logger.warning(f"No silver prices found for {symbol}")
                return ""

            # Concat all prices
            full_df = pd.concat(price_dfs, ignore_index=True)

            # Deduplicate: Keep Alpaca over YFinance for same timestamp
            # Sort by timestamp and source priority (Alpaca=0, YFinance=1)
            full_df["source_rank"] = full_df["source"].map({"alpaca": 0, "yfinance": 1})
            full_df.sort_values(["timestamp_utc", "source_rank"], inplace=True)

            # Drop duplicates on timestamp, keeping first (Alpaca)
            full_df.drop_duplicates(
                subset=["timestamp_utc"], keep="first", inplace=True
            )

            # 2. Load Corporate Actions
            action_dfs = []
            for provider in providers:
                provider_dir = silver_base / provider / "corporate_actions" / "equity"
                if not provider_dir.exists():
                    continue
                files = list(provider_dir.glob(f"**/symbol={symbol}/*.parquet"))
                for f in files:
                    try:
                        df = pd.read_parquet(f)
                        action_dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to read silver actions {f}: {e}")

            actions_df = pd.DataFrame()
            if action_dfs:
                actions_df = pd.concat(action_dfs, ignore_index=True)
                # Deduplicate actions?
                # If both providers report same dividend, we might double count.
                # Dedupe by ex_date, action_type, value?
                actions_df.drop_duplicates(
                    subset=["ex_date_utc", "action_type", "value"], inplace=True
                )

            # 3. Calculate Adjusted Prices
            # We need to apply splits and dividends backwards.
            # Sort prices descending
            full_df.sort_values("timestamp_utc", ascending=False, inplace=True)

            # Initialize adjusted columns
            full_df["adj_open"] = full_df["open"]
            full_df["adj_high"] = full_df["high"]
            full_df["adj_low"] = full_df["low"]
            full_df["adj_close"] = full_df["close"]
            full_df["adj_volume"] = full_df["volume"]

            if not actions_df.empty:
                # Sort actions descending
                actions_df.sort_values("ex_date_utc", ascending=False, inplace=True)

                # Apply adjustments
                # Cumulative Split Ratio
                # Cumulative Split Ratio
                # split_ratio = 1.0
                # Cumulative Dividend Adjustment (simplified)
                # Usually dividends are subtracted from price.
                # Adjusted Price = Price * SplitRatio - Dividend?
                # Standard CRSP/Yahoo method:
                # Split: Price = Price * Ratio, Vol = Vol / Ratio
                # Dividend: Price = Price * (1 - Div/Close)? Or just subtract?
                # Yahoo uses "Adjusted Close" which accounts for both.

                # Let's use a simple loop for now (vectorization is harder with mixed events)
                # Actually, we can iterate through events and apply to all records < ex_date.

                for _, action in actions_df.iterrows():
                    ex_date = action["ex_date_utc"]
                    val = action["value"]

                    # Apply to all records strictly BEFORE ex_date
                    mask = full_df["timestamp_utc"] < ex_date

                    if action["action_type"] == "split":
                        # Split: e.g. 2-for-1 (ratio 0.5? or 2.0?)
                        # If 2-for-1, you get 2 shares for 1. Price halves.
                        # If 'value' is the ratio "shares after / shares before":
                        # 2-for-1 => value = 2.0?
                        # Alpaca/YFinance usually report the ratio.
                        # If price drops, ratio is likely > 1 (e.g. 2.0).
                        # Adjusted Price (Historical) = Raw Price / Ratio
                        # Adjusted Vol = Raw Vol * Ratio

                        # Wait, if we go backwards:
                        # Current price is 100. Split was 2:1 yesterday.
                        # Yesterday's raw price was 200.
                        # Adjusted yesterday = 200 / 2 = 100.
                        # So we divide historical prices by ratio.

                        # Check YFinance 'stock_splits' convention:
                        # It usually reports "2.0" for 2:1.

                        full_df.loc[
                            mask, ["adj_open", "adj_high", "adj_low", "adj_close"]
                        ] /= val
                        full_df.loc[mask, "adj_volume"] *= val

                    elif action["action_type"] == "dividend":
                        # Dividend: Price drops by dividend amount on ex-date.
                        # We need to adjust historical prices DOWN to match the drop?
                        # No, we adjust historical prices DOWN to match the current price which HAS dropped.
                        # Wait.
                        # Today (Ex-Date): Price = 90. Div = 10.
                        # Yesterday (Cum-Date): Price = 100.
                        # Adjusted Yesterday = 100 * (1 - 10/100) = 90?
                        # The standard formula is using a factor.
                        # Factor = (Close - Div) / Close
                        # Apply this factor to all historical prices.

                        # We need the Close price on the day BEFORE ex-date (Cum-Date) to calculate factor?
                        # Or the Close price ON ex-date?
                        # Usually it's based on the closing price of the cum-dividend date (yesterday).

                        # Find the record just before ex_date
                        # Since we sorted descending, it's the first record where ts < ex_date?
                        # No, that's the one we want to adjust.
                        # We need the reference price.

                        # Let's simplify: Just subtract dividend? No, that leads to negative prices long term.
                        # Must use proportional adjustment.

                        # Get the close price of the day BEFORE ex_date (the last day with dividend attached)
                        # mask selects all days before. The "last" one (closest to ex_date) is full_df[mask].iloc[0] (since sorted desc)?
                        # No, sorted desc means index 0 is newest.
                        # So full_df[mask] are older dates. The newest of the older dates is the first one.

                        subset = full_df[mask]
                        if not subset.empty:
                            # This is the day before ex-date
                            ref_close = subset.iloc[0]["close"]
                            if ref_close > 0:
                                factor = 1 - (val / ref_close)
                                full_df.loc[
                                    mask,
                                    ["adj_open", "adj_high", "adj_low", "adj_close"],
                                ] *= factor

            # Add Session Column (Default to REG for Daily Bars)
            # Future: If we have minute bars, calculate based on ET time.
            full_df["session"] = "REG"

            # 4. Save to Gold
            # Sort ascending for storage
            full_df.sort_values("timestamp_utc", inplace=True)

            # Save as single canonical file? Or partitioned?
            # PRD: "Gold Layer (Canonical)".
            # Usually we save the full history as one file per symbol (for easy access)
            # OR partitioned by year if huge.
            # For daily bars, one file per symbol is fine (30 years * 252 days = 7500 rows).
            # Let's save as one file per symbol.

            # But DataLake enforces partition?
            # We can use a custom path or just partition by "latest date".
            # Let's use `save_gold` (if we implement it) or just `save_parquet` with custom path.
            # `ForensicDataLake` doesn't have `save_gold` yet.
            # I should add `save_gold` to `storage.py` or use `save_silver` with layer="gold"?
            # `save_silver` hardcodes layer="silver".

            # I need to add `save_gold` to `storage.py`.
            # For now, I'll use `save_silver` but hack the layer? No, that's bad.
            # I'll add `save_gold` to `storage.py` in a separate step.
            # For this file, I'll assume `data_lake.save_gold` exists.

            gold_path = data_lake.save_gold(
                df=full_df,
                provider="canonical",  # It's a derived provider
                slice_name="prices",
                asset_class="equity",
                symbol=symbol,
                date=full_df["timestamp_utc"].max().to_pydatetime(),
                filename="history",
            )

            return gold_path

        except Exception as e:
            logger.error(f"Failed to process gold for {symbol}: {e}")
            raise


gold_processor = GoldProcessor()
