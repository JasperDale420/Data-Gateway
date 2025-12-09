import json
import pandas as pd
from typing import Any, Dict, List, Union
from pathlib import Path


class DataWriter:
    BASE_PATH = "/Users/jacobmcmillan/Desktop/data"

    @classmethod
    def write_json(cls, data: Any, path_suffix: str):
        """
        Write data to a JSON file.
        path_suffix: relative path from BASE_PATH (e.g., 'raw/fundamentals/sec_filings/AAPL/123.json')
        """
        full_path = Path(cls.BASE_PATH) / path_suffix
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, "w") as f:
            json.dump(data, f, indent=2, default=str)

        print(f"Saved JSON to {full_path}")

    @classmethod
    def write_parquet(cls, data: Union[List[Dict], pd.DataFrame], path_suffix: str):
        """
        Write data to a Parquet file.
        path_suffix: relative path from BASE_PATH (e.g., 'raw/market_data/alpaca_bars/AAPL_1Day.parquet')
        """
        full_path = Path(cls.BASE_PATH) / path_suffix
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data

        if df.empty:
            print(f"Skipping write to {full_path}: DataFrame is empty")
            return

        # Ensure directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(full_path, index=False)
        print(f"Saved Parquet to {full_path}")
