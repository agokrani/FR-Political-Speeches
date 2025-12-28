"""Collector for Senat comptes rendus."""

import zipfile
from pathlib import Path

from ..config import Config
from ..utils.http import RateLimitedClient
from ..utils.logging import get_logger
from .base import BaseCollector


class SenatCollector(BaseCollector):
    """Collector for Senat debate transcripts.

    Downloads the XML bulk dump from data.senat.fr and extracts it.
    """

    ZIP_FILENAME = "cri.zip"

    def __init__(self, config: Config, output_dir: Path):
        super().__init__(config, output_dir)
        self.source_config = config.sources.senat
        self.extracted_dir = output_dir / "extracted"

    def get_source_name(self) -> str:
        return "senat"

    def is_enabled(self) -> bool:
        return self.source_config.enabled

    async def collect(self) -> Path:
        """Download and extract the Senat XML dump.

        Returns:
            Path to the extracted XML directory
        """
        if not self.is_enabled():
            self.logger.info("Senat collector is disabled")
            return self.output_dir

        async with RateLimitedClient(self.config.pipeline.http) as client:
            # Download ZIP file
            zip_path = await self._download_zip(client)

            # Extract ZIP
            extracted = self._extract_zip(zip_path)

        return extracted

    async def _download_zip(self, client: RateLimitedClient) -> Path:
        """Download the ZIP archive.

        Args:
            client: HTTP client

        Returns:
            Path to downloaded ZIP file
        """
        zip_path = self.get_cache_path(self.ZIP_FILENAME)

        if self.is_cached(self.ZIP_FILENAME):
            self.logger.info(f"Using cached ZIP: {zip_path}")
            return zip_path

        self.logger.info(f"Downloading Senat XML from {self.source_config.xml_url}")
        await client.download_file(self.source_config.xml_url, zip_path)
        self.logger.info(f"ZIP saved to {zip_path}")

        return zip_path

    def _extract_zip(self, zip_path: Path) -> Path:
        """Extract the ZIP archive.

        Args:
            zip_path: Path to ZIP file

        Returns:
            Path to extracted directory
        """
        if self.extracted_dir.exists() and any(self.extracted_dir.iterdir()):
            self.logger.info(f"Using already extracted files in {self.extracted_dir}")
            return self.extracted_dir

        self.logger.info(f"Extracting {zip_path} to {self.extracted_dir}")
        self.extracted_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(self.extracted_dir)

        # Count extracted files
        xml_files = list(self.extracted_dir.rglob("*.xml"))
        self.logger.info(f"Extracted {len(xml_files)} XML files")

        return self.extracted_dir

    def iter_xml_files(self) -> list[Path]:
        """Get list of extracted XML files.

        Returns:
            List of paths to XML files
        """
        if not self.extracted_dir.exists():
            return []
        return list(self.extracted_dir.rglob("*.xml"))
