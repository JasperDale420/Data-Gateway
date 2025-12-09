import logging
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Union, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dataingestion.shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class ForensicDataLake:
    """
    Forensic-Grade Data Lake Storage Engine.
    Implements Bronze/Silver/Gold layers with strict partitioning and metadata.
    """

    def __init__(self):
        self.base_dir = Path(settings.DATA_DIR)

    def _ensure_dir(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

    def _generate_metadata(
        self, data_bytes: bytes, provider: str, extra_meta: Dict = None
    ) -> Dict:
        """Generate mandatory metadata for every file."""
        meta = {
            "provider_id": provider,
            "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "file_hash_sha256": hashlib.sha256(data_bytes).hexdigest(),
            "byte_size": len(data_bytes),
            "schema_version": "1.0",  # Placeholder
        }
        if extra_meta:
            meta.update(extra_meta)
        return meta

    def _get_partition_path(
        self,
        layer: str,
        provider: str,
        slice_name: str,
        asset_class: str,
        date: datetime,
        symbol: str,
    ) -> Path:
        """
        Constructs the PRD-compliant partition path:
        /{layer}/{provider}/{slice}/{asset_class}/year=YYYY/month=MM/day=DD/symbol=SYMBOL/
        """
        symbol = symbol.upper().replace("/", "-")
        return (
            self.base_dir
            / layer
            / provider
            / slice_name
            / asset_class
            / f"year={date.year}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
            / f"symbol={symbol}"
        )

    def save_bronze(
        self,
        data: Union[Dict, List, bytes, str, pd.DataFrame],
        provider: str,
        slice_name: str,  # e.g., 'prices', 'fundamentals'
        asset_class: str,  # e.g., 'equity', 'crypto'
        symbol: str,
        date: datetime,
        filename: str,
        file_extension: Optional[str] = None,
    ) -> str:
        """
        Save raw data to Bronze layer. Immutable.
        Supports Dict/List (JSON), DataFrame (Parquet), or Bytes/Str.
        """
        output_dir = self._get_partition_path(
            "bronze", provider, slice_name, asset_class, date, symbol
        )
        self._ensure_dir(output_dir)

        # Serialize data
        if isinstance(data, pd.DataFrame):
            # Convert DF to Parquet Bytes
            table = pa.Table.from_pandas(data)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink)
            content = sink.getvalue().to_pybytes()
            if not file_extension:
                file_extension = "parquet"
        elif isinstance(data, (dict, list)):
            content = json.dumps(data, default=str).encode("utf-8")
            if not file_extension:
                file_extension = "json"
        elif isinstance(data, str):
            content = data.encode("utf-8")
            if not file_extension:
                file_extension = "txt"
        elif isinstance(data, bytes):
            content = data
            if not file_extension:
                file_extension = "bin"
        else:
            raise ValueError(f"Unsupported data type for Bronze: {type(data)}")

        file_path = output_dir / f"{filename}.{file_extension}"
        meta_path = output_dir / f"{filename}.metadata.json"

        # Immutability Check
        if file_path.exists():
            timestamp = datetime.now().strftime("%H%M%S")
            file_path = output_dir / f"{filename}_{timestamp}.{file_extension}"
            meta_path = output_dir / f"{filename}_{timestamp}.metadata.json"
            logger.info(f"File exists, saving as new version: {file_path}")

        # Write Data
        with open(file_path, "wb") as f:
            f.write(content)

        # Write Metadata
        metadata = self._generate_metadata(
            content, provider, extra_meta={"original_type": str(type(data))}
        )
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved Bronze: {file_path}")
        return str(file_path)

    def save_silver(
        self,
        df: pd.DataFrame,
        provider: str,
        slice_name: str,
        asset_class: str,
        symbol: str,
        date: datetime,
        filename: str,
    ) -> str:
        """
        Save validated data to Silver layer as Parquet.
        """
        output_dir = self._get_partition_path(
            "silver", provider, slice_name, asset_class, date, symbol
        )
        self._ensure_dir(output_dir)

        # file_path = output_dir / f"{filename}.parquet"
        full_path = output_dir / f"{filename}.parquet"

        try:
            # Write Parquet with ZSTD compression (PRD Requirement)
            df.to_parquet(full_path, index=False, compression="zstd")

            logger.info(f"Saved Silver: {full_path} ({len(df)} rows)")
            return str(full_path)
        except Exception as e:
            logger.error(f"Failed to save Silver parquet to {full_path}: {e}")
            raise

    def save_gold(
        self,
        df: pd.DataFrame,
        provider: str,
        slice_name: str,
        asset_class: str,
        symbol: str,
        date: datetime,
        filename: str,
    ) -> str:
        """
        Save Gold (Canonical) data.
        Format: Parquet + ZSTD.
        Path: gold/{provider}/{slice}/{asset_class}/year=YYYY/month=MM/day=DD/symbol={symbol}/{filename}.parquet
        """
        path = self._get_partition_path(
            "gold", provider, slice_name, asset_class, date, symbol
        )
        self._ensure_dir(path)  # Ensure the gold partition path exists
        full_path = path / f"{filename}.parquet"

        try:
            # Save Parquet with ZSTD compression
            df.to_parquet(full_path, index=False, compression="zstd")
            logger.info(f"Saved Gold: {full_path} ({len(df)} rows)")

            return str(full_path)
        except Exception as e:
            logger.error(f"Failed to save Gold parquet to {full_path}: {e}")
            raise

    # Alias for backward compatibility during refactor, but deprecated
    def save_parquet(self, *args, **kwargs):
        logger.warning("Deprecated: save_parquet called. Use save_silver instead.")
        # Map old args to new save_silver if possible, or fail.
        # For now, let's fail to force refactor.
        raise NotImplementedError("Use save_silver() for PRD compliance.")


data_lake = ForensicDataLake()
