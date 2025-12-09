import pytest
import pandas as pd
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from pathlib import Path

from dataingestion.features.alpaca_data.ingestor import AlpacaIngestor
from dataingestion.processors.silver_processor import SilverProcessor
from dataingestion.shared.storage import data_lake
from dataingestion.shared.config import get_settings

settings = get_settings()


@pytest.fixture
def mock_alpaca_data():
    """Create a mock DataFrame representing Alpaca bar data."""
    data = {
        "timestamp": [
            datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc),
        ],
        "open": [150.0, 151.0],
        "high": [155.0, 156.0],
        "low": [149.0, 150.5],
        "close": [154.0, 155.0],
        "volume": [1000, 1500],
        "trade_count": [50, 75],
        "vwap": [152.0, 153.0],
    }
    return pd.DataFrame(data)


@pytest.fixture
def temp_data_lake(tmp_path):
    """Override the global data_lake base dir with a temporary path."""
    original_base = data_lake.base_dir
    data_lake.base_dir = tmp_path
    yield data_lake
    data_lake.base_dir = original_base


def test_alpaca_pipeline_end_to_end(mock_alpaca_data, temp_data_lake):
    """
    Test the full flow:
    1. Ingestor fetches data (Mocked API) -> Writes Bronze Parquet + Metadata
    2. Processor reads Bronze Parquet -> Writes Silver Parquet
    """

    # --- Step 1: Ingestion ---
    with patch(
        "dataingestion.features.alpaca_data.ingestor.StockHistoricalDataClient"
    ) as MockClient:
        # Setup Mock
        mock_instance = MockClient.return_value
        mock_response = MagicMock()
        mock_response.df = mock_alpaca_data
        # Mock get_stock_bars to return our response
        mock_instance.get_stock_bars.return_value = mock_response

        # Init Ingestor
        ingestor = AlpacaIngestor()

        # Call Ingest
        start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
        response = ingestor.get_bars("AAPL", "1Hour", start_date)

        # Verify Ingestion Response
        assert response.symbol == "AAPL"
        assert len(response.bars) == 2

        # Verify Bronze File Creation
        # Expected path: bronze/alpaca/market_data/equity/year=2023/month=01/day=01/symbol=AAPL/bars_1Day_20230101.parquet
        # Note: Ingestor logic uses start_date for partitioning
        # Filename in ingestor: f"bars_{timeframe}_{start_str}"

        bronze_dir = (
            temp_data_lake.base_dir
            / "bronze"
            / "alpaca"
            / "market_data"
            / "equity"
            / "year=2023"
            / "month=01"
            / "day=01"
            / "symbol=AAPL"
        )
        assert bronze_dir.exists()

        # Find the .parquet file
        files = list(bronze_dir.glob("*.parquet"))
        assert len(files) == 1
        bronze_file = files[0]

        # Verify Metadata Sidecar
        meta_file = bronze_dir / (
            bronze_file.name.replace(".parquet", ".metadata.json")
        )
        assert meta_file.exists()

        with open(meta_file, "r") as f:
            meta = json.load(f)
            assert meta["provider_id"] == "alpaca"
            assert meta["original_type"] == "<class 'pandas.core.frame.DataFrame'>"

    # --- Step 2: Processing (Silver) ---
    processor = SilverProcessor()

    # Run Processor on the Bronze File
    silver_path_str = processor.process_market_data(str(bronze_file))

    assert silver_path_str is not None
    assert silver_path_str != ""

    silver_path = Path(silver_path_str)
    assert silver_path.exists()

    # Verify Silver Content
    df_silver = pd.read_parquet(silver_path)
    assert len(df_silver) == 2
    assert "timestamp_utc" in df_silver.columns
    assert df_silver.iloc[0]["open"] == 150.0
    assert df_silver.iloc[0]["source"] == "alpaca"

    print(
        f"\nSuccess! Pipeline verified.\nBronze: {bronze_file}\nSilver: {silver_path}"
    )
