import logging
import pandas as pd
import numpy as np
from pathlib import Path
from dataingestion.shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class FeatureProcessor:
    """
    Processor for the Feature Layer.
    Fuses Gold Prices + Silver Financials -> Model-Ready Feature Vectors.
    Enforces Point-in-Time (PIT) Correctness.
    """

    def process_ticker(self, symbol: str) -> str:
        """
        Generate feature vectors for a ticker.
        """
        try:
            # 1. Load Gold Prices (Canonical)
            gold_path = (
                Path(settings.DATA_DIR) / "gold" / "canonical" / "prices" / "equity"
            )
            # Find the file (assuming one file per symbol as per GoldProcessor)
            # We need to find the latest partition or scan.
            # GoldProcessor saves to year/month/day of max date.
            # Let's glob.
            files = list(gold_path.glob(f"**/symbol={symbol}/*.parquet"))
            if not files:
                logger.warning(f"No Gold prices found for {symbol}")
                return ""

            # Load all and concat (if multiple)
            price_df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            price_df.sort_values("timestamp_utc", inplace=True)
            price_df.drop_duplicates("timestamp_utc", inplace=True)

            # 2. Load Silver Financials (PIT)
            # Silver Financials are stored by CIK (symbol=CIK)
            # We need to resolve Ticker -> CIK
            # We can use the SecIngestor helper or a local mapping.
            # Since we are in a processor, let's try to find the CIK from the Gold metadata or just search.
            # Searching is slow.
            # Let's use the SecIngestor's mapping file if available.
            # Or just use the `sec_ingestor` instance if we can import it.

            # This is async, but we are in sync code.
            # We can run it in a loop or just read the mapping file directly.
            # The mapping file is usually cached at `sec_ingestor.TICKER_MAP_PATH`?
            # Actually, let's just use a helper function that reads the mapping json.

            # Helper to get CIK (Synchronous)
            cik = None
            try:
                # Try to find CIK from a local mapping cache if possible
                # Or just use the one downloaded by ingestor
                # Path: /tmp/company_tickers.json (or wherever ingestor saves it)
                # Ingestor saves to self.TICKER_MAP_PATH which is likely in memory or temp.
                # Let's assume we can't easily get it without the ingestor.

                # Hack: We can look at the Bronze path for this ticker?
                # Bronze SEC path: bronze/sec/company_facts/equity/.../symbol=TICKER/facts_CIK.json
                # We can glob bronze for the ticker and extract CIK from filename.
                bronze_sec_path = (
                    Path(settings.DATA_DIR)
                    / "bronze"
                    / "sec"
                    / "company_facts"
                    / "equity"
                )
                bronze_files = list(
                    bronze_sec_path.glob(f"**/symbol={symbol}/facts_*.json")
                )
                if bronze_files:
                    # Filename format: facts_{CIK}_{timestamp}.json OR facts_{CIK}.json
                    # e.g. facts_0000320193_190309.json
                    fname = bronze_files[0].name
                    parts = fname.split("_")
                    if len(parts) >= 2:
                        cik_part = parts[1]
                        # Remove extension (handle .json, .metadata, etc.)
                        cik_part = cik_part.split(".")[0]

                        cik = str(int(cik_part))
                        # Silver uses CIK as string, likely without leading zeros if it came from int?
                        # Wait, the directory listing showed `symbol=1738758`.
                        # AAPL CIK is 320193.
                        # Let's try both with and without leading zeros.
            except Exception as e:
                logger.warning(f"Failed to resolve CIK from Bronze for {symbol}: {e}")

            fin_files = []
            if cik:
                # Try exact match first
                silver_path = (
                    Path(settings.DATA_DIR)
                    / "silver"
                    / "sec"
                    / "fundamentals"
                    / "equity"
                )
                fin_files = list(silver_path.glob(f"**/symbol={cik}/*.parquet"))

                # If not found, try with leading zeros?
                if not fin_files:
                    fin_files = list(
                        silver_path.glob(f"**/symbol={int(cik)}/*.parquet")
                    )

            if not fin_files:
                # Fallback: Try to find by scanning (slow but robust for verification)
                # Only if we really need to.
                pass

            if not fin_files:
                # If no financials, we can still generate price features, but metrics will be null.
                logger.info(
                    f"No financials found for {symbol}. Generating price-only features."
                )
                fin_df = pd.DataFrame()
            else:
                fin_df = pd.concat(
                    [pd.read_parquet(f) for f in fin_files], ignore_index=True
                )
                # Sort by Filing Date (Availability)
                fin_df.sort_values("filing_date_utc", inplace=True)

            # 3. Calculate Fundamental Metrics (TTM, etc.)
            # We need to pivot the financials to have concepts as columns
            # Structure: [filing_date_utc, report_date, concept, value]
            # We want a time series of financial states available at 'filing_date_utc'.

            # Pivot is tricky because one filing has multiple concepts.
            # We want one row per filing_date with all latest concepts?
            # Or one row per (filing_date, report_date)?

            # Strategy:
            # 1. Pivot to wide format: Index=[filing_date_utc, report_date], Columns=concept, Values=value
            # 2. Resample/Forward Fill is hard because filings are irregular.

            # Better Strategy:
            # Iterate through price dates (daily).
            # For each day, get the "Latest Known Financials".
            # "Latest Known" means:
            #   - Filter financials where filing_date_utc <= current_date
            #   - Sort by report_date (desc) to get latest fiscal period.
            #   - Take the values.

            # Optimization: `pd.merge_asof`
            # We need a dataframe of "Financial State" indexed by `filing_date_utc`.
            # But a single filing doesn't contain ALL concepts (e.g. 10-Q vs 8-K).
            # We need to maintain a "Current State" that updates with each filing.

            if not fin_df.empty:
                # Pivot to wide format
                # We might have duplicates for (filing_date, concept) if multiple reports filed same day.
                # Take the one with latest report_date.
                fin_df.sort_values(["filing_date_utc", "report_date"], inplace=True)

                # We need to reconstruct the "State of the World" at each filing.
                # This is complex.
                # Simplified approach for MVP:
                # 1. Pivot on (filing_date, concept) -> value. (Ignoring report_date for a moment, assuming latest filing supersedes).
                # Actually, we need report_date to calculate TTM (sum of last 4 quarters).

                # Let's focus on the key concepts needed:
                # Assets, Equity, NetIncome, EPS, Revenue, Cash, Debt.

                # Filter for relevant concepts (Implicit in tag_map below)
                # Map common XBRL tags to these canonical names?
                # SecParser should have done this.
                # Let's assume SecParser output raw tags. We need a mapper.
                # For now, let's assume we have raw tags and we need to map them.
                # Or did SecParser map them?
                # Checking SecParser... it extracts "us-gaap" tags.
                # We need a mapper here.

                # Mapping (Simplified)
                tag_map = {
                    "Assets": "assets",
                    "StockholdersEquity": "equity",
                    "NetIncomeLoss": "net_income",
                    "Revenues": "revenue",
                    "SalesRevenueNet": "revenue",
                    "EarningsPerShareBasic": "eps",
                    "CashAndCashEquivalentsAtCarryingValue": "cash",
                    "LongTermDebt": "debt",
                    "GrossProfit": "gross_profit",
                    "CostOfGoodsAndServicesSold": "cogs",
                }

                fin_df["canonical_concept"] = fin_df["concept"].map(
                    lambda x: tag_map.get(x.split(":")[-1], None)
                )
                fin_df = fin_df.dropna(subset=["canonical_concept"])

                # Pivot: Index=filing_date_utc, Columns=canonical_concept
                # We use merge_asof to join this to prices.
                # But we need to forward fill previous values because a 10-Q might only have NetIncome, not Assets.
                # So we want a cumulative forward fill on the timeline of filings.

                # Create a timeline of all unique filing dates
                unique_filings = fin_df["filing_date_utc"].unique()
                state_df = pd.DataFrame(index=unique_filings)
                state_df.index.name = "timestamp_utc"
                state_df.sort_index(inplace=True)

                for concept in tag_map.values():
                    # Get subset for this concept
                    c_df = fin_df[fin_df["canonical_concept"] == concept]
                    # Sort by filing date
                    c_df = c_df.sort_values("filing_date_utc")
                    # Drop duplicates (keep latest)
                    c_df = c_df.drop_duplicates("filing_date_utc", keep="last")
                    # Set index
                    c_df = c_df.set_index("filing_date_utc")
                    # Join to state
                    state_df[concept] = c_df["value"]

                # Forward fill the state (if a new filing comes in, old values persist until overwritten)
                state_df = state_df.ffill()

                # Now merge_asof state_df to price_df
                # price_df is daily. state_df is sparse.
                # merge_asof(price, state, on='timestamp_utc', direction='backward')

                merged_df = pd.merge_asof(
                    price_df, state_df, on="timestamp_utc", direction="backward"
                )
            else:
                merged_df = price_df
                for col in [
                    "assets",
                    "equity",
                    "net_income",
                    "revenue",
                    "eps",
                    "cash",
                    "debt",
                    "gross_profit",
                    "cogs",
                ]:
                    merged_df[col] = np.nan

            # 4. Calculate Derived Metrics
            # ROE = Net Income / Equity
            merged_df["roe_ttm"] = merged_df["net_income"] / merged_df["equity"]

            # Gross Margin Calculation
            for col in ["gross_profit", "cogs", "revenue"]:
                if col not in merged_df.columns:
                    merged_df[col] = np.nan

            merged_df["gross_margin"] = np.nan
            # Logic: GrossProfit / Revenue
            mask_gp = (
                merged_df["gross_profit"].notna()
                & merged_df["revenue"].notna()
                & (merged_df["revenue"] != 0)
            )
            merged_df.loc[mask_gp, "gross_margin"] = (
                merged_df.loc[mask_gp, "gross_profit"]
                / merged_df.loc[mask_gp, "revenue"]
            )
            # Fallback: (Revenue - COGS) / Revenue
            mask_cogs = (
                merged_df["gross_margin"].isna()
                & merged_df["cogs"].notna()
                & merged_df["revenue"].notna()
                & (merged_df["revenue"] != 0)
            )
            merged_df.loc[mask_cogs, "gross_margin"] = (
                merged_df.loc[mask_cogs, "revenue"] - merged_df.loc[mask_cogs, "cogs"]
            ) / merged_df.loc[mask_cogs, "revenue"]

            # Market Cap = Close * Shares (We need shares!)
            # Shares = NetIncome / EPS (Rough proxy if shares not explicitly in data)
            # Or use 'EntityCommonStockSharesOutstanding' if available.
            # Let's use the proxy for now or add Shares to extraction.
            # Proxy: Shares = NetIncome / EPS
            merged_df["shares_estimated"] = merged_df["net_income"] / merged_df["eps"]
            merged_df["market_cap"] = merged_df["close"] * merged_df["shares_estimated"]

            # PE = Price / EPS
            merged_df["pe_ratio"] = merged_df["close"] / merged_df["eps"]

            # PEG = PE / Growth
            # Need Growth.
            # Calculate YoY growth of EPS.
            # This requires looking back 1 year in the merged dataframe.
            # merged_df['eps_lag_1y'] = merged_df['eps'].shift(252) # Approx 252 trading days
            # merged_df['eps_growth'] = (merged_df['eps'] - merged_df['eps_lag_1y']) / merged_df['eps_lag_1y'].abs()
            # merged_df['peg_ratio'] = merged_df['pe_ratio'] / (merged_df['eps_growth'] * 100)

            # 5. Save to Feature Store
            # Format: Parquet
            # Path: features/model_ready/equity/symbol={symbol}.parquet

            # Filter for schema columns
            # Map to CanonicalFeatureVector fields

            # 5. Save to Feature Store
            final_df = pd.DataFrame(
                {
                    "symbol": symbol,
                    "timestamp_utc": merged_df["timestamp_utc"],
                    "price_close": merged_df["close"],
                    "price_adj_close": merged_df["adj_close"],
                    "volume": merged_df["volume"],
                    "market_cap": merged_df["market_cap"],
                    # Cumulative Split Ratio
                    "pe_ratio": merged_df["pe_ratio"],
                    "roe_ttm": merged_df["roe_ttm"],
                    "debt_to_equity": merged_df["debt"] / merged_df["equity"],
                    "equity": merged_df["equity"],
                    "net_income": merged_df["net_income"],
                    "gross_margin": merged_df["gross_margin"],
                    "revenue": merged_df["revenue"],
                }
            )

            # Clean up Infs/NaNs
            final_df.replace([np.inf, -np.inf], np.nan, inplace=True)

            # Save
            # We need a save_feature method or just use generic save
            # Let's use generic save but we need to respect the path structure.
            # features/model_ready/equity/year=*/...
            # Or just one file per symbol for features?
            # PRD says "Feature Store".
            # Let's save partitioned by year for scalability, or one file for ease.
            # One file per symbol is best for backtesting.

            output_dir = (
                Path(settings.DATA_DIR)
                / "features"
                / "model_ready"
                / "equity"
                / f"symbol={symbol}"
            )
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "features.parquet"

            final_df.to_parquet(output_path, index=False, compression="zstd")
            logger.info(f"Saved Features: {output_path} ({len(final_df)} rows)")

            return str(output_path)

        except Exception as e:
            logger.error(f"Failed to process features for {symbol}: {e}")
            raise


feature_processor = FeatureProcessor()
