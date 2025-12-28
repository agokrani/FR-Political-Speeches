"""Collector for Assemblee nationale debates.

NOTE: Structured XML data is only available from 2011+.
For 2000-2010, this collector returns a stub with a warning.
PDF/HTML + OCR/LLM parsing will be implemented in a future iteration.
"""

from pathlib import Path
from typing import Optional

from ..config import Config
from ..utils.http import RateLimitedClient
from ..utils.logging import get_logger
from .base import BaseCollector


class AssembleeCollector(BaseCollector):
    """Collector for Assemblee nationale debate transcripts.

    IMPORTANT: DILA only provides structured XML data from 2011 onwards.
    For the 2000-2010 date range, this collector logs a warning and
    returns empty results. Future versions will implement PDF/HTML parsing.
    """

    def __init__(self, config: Config, output_dir: Path):
        super().__init__(config, output_dir)
        self.source_config = config.sources.assemblee
        self.date_range = config.pipeline.date_range

    def get_source_name(self) -> str:
        return "assemblee"

    def is_enabled(self) -> bool:
        return self.source_config.enabled

    async def collect(self) -> Path:
        """Attempt to collect Assemblee nationale data.

        For 2000-2010, logs a warning about data unavailability.
        For 2011+, would download DILA XML archives (not implemented for MVP).

        Returns:
            Path to the output directory
        """
        if not self.is_enabled():
            self.logger.info("Assemblee collector is disabled")
            return self.output_dir

        # Check if date range overlaps with available data (2011+)
        data_start_year = 2011

        if self.date_range.end.year < data_start_year:
            self.logger.warning(
                f"Assemblee nationale structured data is only available from {data_start_year}. "
                f"Requested range {self.date_range.start} to {self.date_range.end} "
                f"has no structured data available. "
                f"Future versions will implement PDF/HTML + OCR/LLM parsing."
            )
            self._write_stub_notice()
            return self.output_dir

        # For dates >= 2011, we would download DILA XML archives
        # This is a stub for the MVP - implement if date range includes 2011+
        if self.date_range.start.year >= data_start_year:
            self.logger.info(
                f"DILA XML archives available for {self.date_range.start.year}+. "
                f"Downloading is not implemented in MVP."
            )
            await self._collect_dila_archives()
        else:
            self.logger.warning(
                f"Date range spans both unavailable ({self.date_range.start.year}-2010) "
                f"and available (2011-{self.date_range.end.year}) periods. "
                f"Only 2011+ data will be collected."
            )
            await self._collect_dila_archives()

        return self.output_dir

    async def _collect_dila_archives(self) -> None:
        """Download DILA XML archives for available years.

        This is a stub implementation for the MVP.
        Full implementation would:
        1. List available years at base_url
        2. Download .taz archives for each year in range
        3. Extract and store XML files
        """
        self.logger.info(
            f"DILA archive collection is a stub. "
            f"URL pattern: {self.source_config.base_url}{{YEAR}}/AN_YYYYMMDD_###.taz"
        )

        # Create a notice file
        notice_path = self.output_dir / "DILA_COLLECTION_STUB.md"
        notice_path.write_text(
            "# DILA Archive Collection\n\n"
            "This is a stub implementation. Full collection of DILA XML archives\n"
            "for Assemblee nationale debates (2011+) is not yet implemented.\n\n"
            f"Base URL: {self.source_config.base_url}\n"
            f"Date range requested: {self.date_range.start} to {self.date_range.end}\n"
        )

    def _write_stub_notice(self) -> None:
        """Write a notice file explaining the data gap."""
        notice_path = self.output_dir / "DATA_GAP_NOTICE.md"
        notice_path.write_text(
            "# Assemblee Nationale Data Gap\n\n"
            "## Summary\n"
            "Structured XML data from DILA is only available from 2011 onwards.\n"
            f"The requested date range ({self.date_range.start} to {self.date_range.end}) "
            "predates this availability.\n\n"
            "## Future Work\n"
            "Historical data (2000-2010) can be obtained through:\n"
            "1. PDF archives from the Assemblee nationale website\n"
            "2. HTML scraping of historical pages\n"
            "3. OCR processing using tools like docling\n"
            "4. LLM-based text extraction\n\n"
            "This functionality will be implemented in a future iteration.\n"
        )
        self.logger.info(f"Data gap notice written to {notice_path}")
