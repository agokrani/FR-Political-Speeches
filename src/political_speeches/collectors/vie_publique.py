"""Collector for Vie-publique (DILA) discours publics."""

import asyncio
import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Optional

from ..config import Config
from ..utils.http import RateLimitedClient
from ..utils.logging import get_logger
from .base import BaseCollector


class ViePubliqueCollector(BaseCollector):
    """Collector for Vie-publique speeches.

    Downloads the bulk JSON manifest from DILA, filters by date range,
    and crawls individual speech pages for full text.
    """

    MANIFEST_FILENAME = "vp_discours.json"

    def __init__(self, config: Config, output_dir: Path):
        super().__init__(config, output_dir)
        self.source_config = config.sources.vie_publique
        self.date_range = config.pipeline.date_range
        self.pages_dir = output_dir / "pages"
        self.pages_dir.mkdir(parents=True, exist_ok=True)

    def get_source_name(self) -> str:
        return "vie_publique"

    def is_enabled(self) -> bool:
        return self.source_config.enabled

    async def collect(self) -> Path:
        """Download manifest and crawl speech pages.

        Returns:
            Path to the manifest file
        """
        if not self.is_enabled():
            self.logger.info("Vie-publique collector is disabled")
            return self.output_dir

        async with RateLimitedClient(self.config.pipeline.http) as client:
            # Step 1: Download manifest
            manifest_path = await self._download_manifest(client)

            # Step 2: Load and filter manifest
            speeches = self._load_and_filter_manifest(manifest_path)
            self.logger.info(
                f"Found {len(speeches)} speeches in date range "
                f"{self.date_range.start} to {self.date_range.end}"
            )

            # Step 3: Crawl individual pages (if enabled)
            if self.source_config.crawl_full_text:
                await self._crawl_pages(client, speeches)

        return manifest_path

    async def _download_manifest(self, client: RateLimitedClient) -> Path:
        """Download the JSON manifest.

        Args:
            client: HTTP client

        Returns:
            Path to downloaded manifest
        """
        manifest_path = self.get_cache_path(self.MANIFEST_FILENAME)

        if self.is_cached(self.MANIFEST_FILENAME):
            self.logger.info(f"Using cached manifest: {manifest_path}")
            return manifest_path

        self.logger.info(f"Downloading manifest from {self.source_config.manifest_url}")
        await client.download_file(self.source_config.manifest_url, manifest_path)
        self.logger.info(f"Manifest saved to {manifest_path}")

        return manifest_path

    def _load_and_filter_manifest(self, manifest_path: Path) -> list[dict]:
        """Load manifest and filter by date range.

        Args:
            manifest_path: Path to manifest file

        Returns:
            List of speech entries in date range
        """
        self.logger.info("Loading manifest...")

        with open(manifest_path, "r", encoding="utf-8") as f:
            # The manifest is a large JSON array
            data = json.load(f)

        # Handle both array and object formats
        if isinstance(data, dict):
            speeches = data.get("discours", data.get("data", []))
        else:
            speeches = data

        self.logger.info(f"Loaded {len(speeches)} total speeches from manifest")

        # Filter by date range
        filtered = []
        for speech in speeches:
            speech_date = self._parse_date(speech)
            if speech_date and self.date_range.start <= speech_date <= self.date_range.end:
                filtered.append(speech)

        return filtered

    def _parse_date(self, speech: dict) -> Optional[date]:
        """Parse date from speech entry.

        Args:
            speech: Speech dictionary

        Returns:
            Date object or None if unparseable
        """
        # Try various date field names - prononciation is the delivery date in current manifest
        date_str = (
            speech.get("prononciation")
            or speech.get("date")
            or speech.get("dateDiscours")
            or speech.get("date_discours")
            or speech.get("mise_en_ligne")
        )
        if not date_str:
            return None

        try:
            # Try ISO format first
            if "T" in str(date_str):
                return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
            # Try simple date format
            return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    async def _crawl_pages(
        self, client: RateLimitedClient, speeches: list[dict]
    ) -> None:
        """Crawl individual speech pages for full text.

        Args:
            client: HTTP client
            speeches: List of speech entries to crawl
        """
        self.logger.info(f"Crawling {len(speeches)} speech pages...")

        crawled = 0
        skipped = 0
        errors = 0

        for i, speech in enumerate(speeches):
            speech_id = self._get_speech_id(speech)
            if not speech_id:
                errors += 1
                continue

            page_path = self.pages_dir / f"{speech_id}.html"

            # Skip if already cached
            if page_path.exists():
                skipped += 1
                continue

            # Build URL
            url = self._build_speech_url(speech)
            if not url:
                errors += 1
                continue

            try:
                html = await client.get_text(url)
                page_path.write_text(html, encoding="utf-8")
                crawled += 1

                if (crawled + skipped) % 100 == 0:
                    self.logger.info(
                        f"Progress: {crawled + skipped}/{len(speeches)} "
                        f"(crawled: {crawled}, cached: {skipped}, errors: {errors})"
                    )
            except Exception as e:
                self.logger.warning(f"Failed to crawl {url}: {e}")
                errors += 1

        self.logger.info(
            f"Crawling complete: {crawled} new, {skipped} cached, {errors} errors"
        )

    def _get_speech_id(self, speech: dict) -> Optional[str]:
        """Extract speech ID from entry.

        Args:
            speech: Speech dictionary

        Returns:
            Speech ID or None
        """
        # Try various ID fields
        for field in ["id", "identifiant", "uid", "reference"]:
            if field in speech and speech[field]:
                return str(speech[field])

        # Extract from URL if available
        url = speech.get("url") or speech.get("lien")
        if url:
            # Extract ID from URL like /discours/123456-title
            parts = url.rstrip("/").split("/")
            if parts:
                last = parts[-1]
                if "-" in last:
                    return last.split("-")[0]
                return last

        return None

    def _build_speech_url(self, speech: dict) -> Optional[str]:
        """Build full URL for a speech page.

        Args:
            speech: Speech dictionary

        Returns:
            Full URL or None
        """
        # Check for direct URL
        url = speech.get("url") or speech.get("lien")
        if url:
            if url.startswith("http"):
                return url
            return f"{self.source_config.base_url}{url}"

        # Build from ID
        speech_id = self._get_speech_id(speech)
        if speech_id:
            return f"{self.source_config.base_url}/discours/{speech_id}"

        return None

    def iter_cached_pages(self) -> Iterator[tuple[str, Path]]:
        """Iterate over cached HTML pages.

        Yields:
            Tuples of (speech_id, page_path)
        """
        for page_path in self.pages_dir.glob("*.html"):
            speech_id = page_path.stem
            yield speech_id, page_path
