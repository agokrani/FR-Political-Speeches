"""Parquet exporter using Polars."""

from pathlib import Path
from typing import Iterator, List

import polars as pl

from ..models import SpeechRecord
from ..utils.logging import get_logger


class ParquetExporter:
    """Exports speech records to Parquet format.

    Uses Polars for efficient columnar storage with zstd compression.
    Parquet is ideal for analytical queries and data science workflows.
    """

    def __init__(self, compression: str = "zstd"):
        """Initialize the exporter.

        Args:
            compression: Compression algorithm (zstd, snappy, gzip, lz4, none)
        """
        self.compression = compression
        self.logger = get_logger()

    def export(
        self,
        records: List[SpeechRecord],
        output_path: Path,
    ) -> int:
        """Export records to a Parquet file.

        Args:
            records: List of speech records
            output_path: Path to output file

        Returns:
            Number of records exported
        """
        if not records:
            self.logger.warning("No records to export to Parquet")
            return 0

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert records to list of dicts
        data = []
        for record in records:
            # Use model_dump to get serializable dict
            d = record.model_dump(mode="json", exclude_none=True)
            data.append(d)

        # Create Polars DataFrame
        df = pl.DataFrame(data)

        # Write to Parquet
        df.write_parquet(
            output_path,
            compression=self.compression,
        )

        self.logger.info(
            f"Exported {len(df)} records to {output_path} "
            f"(compression: {self.compression})"
        )

        return len(df)

    def export_streaming(
        self,
        records: Iterator[SpeechRecord],
        output_path: Path,
        batch_size: int = 10000,
    ) -> int:
        """Export records to Parquet with streaming/batching.

        For very large datasets, processes records in batches to
        manage memory usage.

        Args:
            records: Iterator of speech records
            output_path: Path to output file
            batch_size: Number of records per batch

        Returns:
            Total number of records exported
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        all_data = []
        total_count = 0

        for record in records:
            d = record.model_dump(mode="json", exclude_none=True)
            all_data.append(d)
            total_count += 1

            if len(all_data) >= batch_size:
                self.logger.debug(f"Processed {total_count} records...")

        if not all_data:
            self.logger.warning("No records to export to Parquet")
            return 0

        # Create DataFrame and write
        df = pl.DataFrame(all_data)
        df.write_parquet(
            output_path,
            compression=self.compression,
        )

        self.logger.info(
            f"Exported {total_count} records to {output_path}"
        )

        return total_count


def read_parquet(path: Path) -> pl.DataFrame:
    """Read a Parquet file into a Polars DataFrame.

    Args:
        path: Path to Parquet file

    Returns:
        Polars DataFrame
    """
    return pl.read_parquet(path)


def parquet_to_records(path: Path) -> Iterator[SpeechRecord]:
    """Read Parquet file and yield SpeechRecord objects.

    Args:
        path: Path to Parquet file

    Yields:
        SpeechRecord objects
    """
    df = pl.read_parquet(path)

    for row in df.iter_rows(named=True):
        yield SpeechRecord(**row)
