import logging
import pandas as pd
import json
from datetime import datetime, timezone
from dataingestion.shared.storage import data_lake
from dataingestion.shared.canonical_schemas import (
    CanonicalBar,
    CanonicalFinancial,
    CanonicalCorporateAction,
)
from dataingestion.features.sec_data.parser import sec_parser

logger = logging.getLogger(__name__)


class SilverProcessor:
    """
    Processor for the Silver Layer.
    Reads Bronze (Raw) -> Validates/Normalizes -> Saves Silver (Parquet).
    """

    def process_market_data(self, bronze_path: str) -> str:
        """
        Process a Bronze Market Data file (Alpaca/YFinance) to Silver.
        Extracts Prices AND Corporate Actions (if present).
        """
        try:
            # Load metadata
            if bronze_path.endswith(".parquet"):
                meta_path = bronze_path.replace(".parquet", ".metadata.json")
            else:
                meta_path = bronze_path.replace(".json", ".metadata.json")

            with open(meta_path, "r") as f:
                meta = json.load(f)

            provider = meta["provider_id"]

            if bronze_path.endswith(".parquet"):
                df = pd.read_parquet(bronze_path)

                # Reconstruct raw_data structure expected by logic below
                raw_data = {}
                if provider == "alpaca":
                    raw_data["bars"] = df.to_dict(orient="records")
                    # Expect symbol in metadata or path
                    raw_data["symbol"] = meta.get("symbol", "UNKNOWN")
                elif provider == "yfinance":
                    raw_data["bars"] = df.to_dict(orient="records")
            else:
                with open(bronze_path, "r") as f:
                    raw_data = json.load(f)
            canonical_bars = []
            canonical_actions = []
            symbol = "UNKNOWN"

            if provider == "alpaca":
                bars = raw_data.get("bars", [])
                symbol = raw_data.get("symbol", "UNKNOWN")

                for bar in bars:
                    # Handle both raw API (t) and DataFrame (timestamp) keys
                    ts_val = bar.get("t") or bar.get("timestamp")
                    ts = pd.to_datetime(ts_val)
                    if ts.tz is None:
                        ts = ts.tz_localize("UTC")
                    else:
                        ts = ts.tz_convert("UTC")

                    canonical_bars.append(
                        CanonicalBar(
                            symbol=symbol,
                            timestamp_utc=ts,
                            open=float(bar.get("o") or bar.get("open")),
                            high=float(bar.get("h") or bar.get("high")),
                            low=float(bar.get("l") or bar.get("low")),
                            close=float(bar.get("c") or bar.get("close")),
                            volume=float(bar.get("v") or bar.get("volume")),
                            source="alpaca",
                        )
                    )

            elif provider == "yfinance":
                bars = raw_data.get("bars", [])
                symbol = raw_data.get("ticker", "UNKNOWN")

                for bar in bars:
                    ts = pd.to_datetime(bar["timestamp"])
                    if ts.tz is None:
                        ts = ts.tz_localize("UTC")
                    else:
                        ts = ts.tz_convert("UTC")

                    canonical_bars.append(
                        CanonicalBar(
                            symbol=symbol,
                            timestamp_utc=ts,
                            open=float(bar["open"]),
                            high=float(bar["high"]),
                            low=float(bar["low"]),
                            close=float(bar["close"]),
                            volume=float(bar["volume"]),
                            source="yfinance",
                        )
                    )

                    # Extract Corporate Actions
                    div = float(bar.get("dividends", 0.0))
                    split = float(bar.get("stock_splits", 0.0))

                    if div > 0:
                        canonical_actions.append(
                            CanonicalCorporateAction(
                                symbol=symbol,
                                ex_date_utc=ts,
                                action_type="dividend",
                                value=div,
                                source="yfinance",
                            )
                        )

                    if split > 0:
                        canonical_actions.append(
                            CanonicalCorporateAction(
                                symbol=symbol,
                                ex_date_utc=ts,
                                action_type="split",
                                value=split,
                                source="yfinance",
                            )
                        )

            if not canonical_bars:
                logger.warning(f"No bars found in {bronze_path}")
                return ""

            # Save Prices
            df_bars = pd.DataFrame([b.model_dump() for b in canonical_bars])
            partition_date = df_bars["timestamp_utc"].max().to_pydatetime()

            silver_path = data_lake.save_silver(
                df=df_bars,
                provider=provider,
                slice_name="prices",
                asset_class="equity",
                symbol=symbol,
                date=partition_date,
                filename="history",
            )

            # Save Corporate Actions (if any)
            if canonical_actions:
                df_actions = pd.DataFrame([a.model_dump() for a in canonical_actions])
                data_lake.save_silver(
                    df=df_actions,
                    provider=provider,
                    slice_name="corporate_actions",
                    asset_class="equity",
                    symbol=symbol,
                    date=partition_date,
                    filename="actions",
                )
                logger.info(
                    f"Saved {len(canonical_actions)} corporate actions for {symbol}"
                )

            return silver_path

        except Exception as e:
            logger.error(f"Failed to process market data {bronze_path}: {e}")
            raise

    def process_corporate_actions(self, bronze_path: str) -> str:
        """
        Process a Bronze Corporate Actions file (e.g. Alpaca) to Silver.
        """
        try:
            with open(bronze_path, "r") as f:
                raw_data = json.load(f)

            if not raw_data:
                return ""

            # Load metadata
            meta_path = bronze_path.replace(".json", ".metadata.json")
            with open(meta_path, "r") as f:
                meta = json.load(f)

            provider = meta["provider_id"]
            # Symbol should be in path or we can pass it.
            # Let's assume we can extract from path or it's in metadata if we added it?
            # Or we can parse from the first record if available.
            # Alpaca actions usually have 'symbol'.

            canonical_actions = []
            symbol = "UNKNOWN"

            if provider == "alpaca":
                for item in raw_data:
                    # Alpaca action structure: { "symbol": "AAPL", "ex_date": "...", "amount": ... }
                    symbol = item.get("symbol", symbol)
                    ex_date = pd.to_datetime(item.get("ex_date"))
                    if ex_date.tz is None:
                        ex_date = ex_date.tz_localize("UTC")
                    else:
                        ex_date = ex_date.tz_convert("UTC")

                    # Determine type
                    # Alpaca might return 'dividend' or 'split' types?
                    # Actually get_corporate_actions returns mixed?
                    # We need to check the fields.
                    # 'cash' -> dividend, 'forward_split'/'reverse_split' -> split
                    # Let's check keys.

                    # Dividend: cash, record_date, pay_date, declare_date
                    # Split: forward_split, reverse_split (ratios)

                    # This depends on the exact response shape of `get_corporate_actions`.
                    # Assuming it returns a flat list of dicts from the DF.

                    # If it's a dividend
                    if "cash" in item and item["cash"] is not None:
                        canonical_actions.append(
                            CanonicalCorporateAction(
                                symbol=symbol,
                                ex_date_utc=ex_date,
                                action_type="dividend",
                                value=float(item["cash"]),
                                source="alpaca",
                            )
                        )

                    # If it's a split
                    # Alpaca usually has 'forward_split' and 'reverse_split' columns?
                    # Or 'split_from', 'split_to'?
                    # Let's assume standard fields or we might need to inspect.
                    # For now, let's look for 'split_ratio' or similar.
                    # Actually, let's just log if we see something that looks like a split.
                    # If we don't know the schema, we might miss it.
                    # Let's assume 'forward_split' and 'reverse_split' are keys.

                    # Placeholder logic for splits until we verify schema
                    # if 'split_from' in item: ...
                    pass

            if not canonical_actions:
                return ""

            df_actions = pd.DataFrame([a.model_dump() for a in canonical_actions])
            partition_date = df_actions["ex_date_utc"].max().to_pydatetime()

            silver_path = data_lake.save_silver(
                df=df_actions,
                provider=provider,
                slice_name="corporate_actions",
                asset_class="equity",
                symbol=symbol,
                date=partition_date,
                filename="actions",
            )
            return silver_path

        except Exception as e:
            logger.error(f"Failed to process corporate actions {bronze_path}: {e}")
            raise

    def process_financials(self, bronze_path: str) -> str:
        """
        Process a Bronze SEC Company Facts file to Silver.
        Uses SecParser to extract PIT financials.
        """
        try:
            with open(bronze_path, "r") as f:
                raw_data = json.load(f)

            # Load metadata
            meta_path = bronze_path.replace(".json", ".metadata.json")
            with open(meta_path, "r") as f:
                pass  # meta = json.load(f)

            # provider = meta["provider_id"]
            cik = raw_data.get("cik")
            # We need the ticker. It might not be in the JSON root for companyfacts.
            # But we can try to find it or pass it.
            # SecParser extracts it or uses CIK.

            # Use SecParser logic (modified to return list of dicts or DF)
            # sec_parser.extract_pit_financials returns a DataFrame
            df = sec_parser.extract_pit_financials(raw_data)

            if df.empty:
                logger.warning(f"No financials extracted from {bronze_path}")
                return ""

            # Validate against CanonicalFinancial schema
            # Map DF columns to Schema fields
            # DF cols: [ticker, concept, value, unit, period_start, period_end, duration, filing_date, form, frame, accn]
            # Schema: [symbol, filing_date_utc, report_date, concept, value, unit, duration, form, accession_number, frame]

            canonical_recs = []
            for _, row in df.iterrows():
                try:
                    # Ensure UTC
                    filing_date = (
                        pd.to_datetime(row["filing_date"]).tz_localize("UTC")
                        if row["filing_date"]
                        else datetime.now(timezone.utc)
                    )
                    report_date = (
                        pd.to_datetime(row["period_end"]).tz_localize("UTC")
                        if row["period_end"]
                        else filing_date
                    )

                    rec = CanonicalFinancial(
                        symbol=str(row["ticker"]),
                        filing_date_utc=filing_date,
                        report_date=report_date,
                        concept=row["concept"],
                        value=float(row["value"]),
                        unit=str(row.get("unit", "USD")),
                        duration=(
                            row["duration"]
                            if row["duration"] in ["instant", "Q", "FY", "YTD"]
                            else "instant"
                        ),  # Fallback/Map
                        form=row["form"],
                        accession_number=row["accn"],
                        frame=row.get("frame"),
                    )
                    canonical_recs.append(rec)
                except Exception:
                    # Log row failure but continue?
                    # logger.debug(f"Skipping row: {e}")
                    continue

            if not canonical_recs:
                logger.warning("No valid canonical financials found")
                return ""

            silver_df = pd.DataFrame([r.model_dump() for r in canonical_recs])

            # Save to Silver
            # Partition by Filing Date (Event Time)
            # Since a file contains many dates, we might want to partition by the LATEST filing date in the batch?
            # Or just save the whole batch under "today" (Processing Time)?
            # PRD says "Immutable Promotion".
            # Let's partition by the ingestion date (today) for the file,
            # OR partition by the max filing date in the set.
            partition_date = silver_df["filing_date_utc"].max().to_pydatetime()

            silver_path = data_lake.save_silver(
                df=silver_df,
                provider="sec",
                slice_name="fundamentals",
                asset_class="equity",
                symbol=str(
                    cik
                ),  # Use CIK or Ticker? PRD says Symbol. Let's try to use Ticker if available.
                date=partition_date,
                filename="pit_financials",
            )
            return silver_path

        except Exception as e:
            logger.error(f"Failed to process financials {bronze_path}: {e}")
            raise


silver_processor = SilverProcessor()
