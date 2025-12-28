"""Parser for Vie-publique speeches."""

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Optional

from lxml import html
from selectolax.parser import HTMLParser

from ..config import Config
from ..models import SpeechRecord
from ..utils.logging import get_logger
from .base import BaseParser


class ViePubliqueParser(BaseParser):
    """Parser for Vie-publique discours publics.

    Parses:
    1. JSON manifest for metadata
    2. HTML pages for full text extraction
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self.date_range = config.pipeline.date_range

    def get_source_name(self) -> str:
        return "vie_publique"

    def parse(self, source_path: Path) -> Iterator[SpeechRecord]:
        """Parse Vie-publique data.

        Args:
            source_path: Path to the raw data directory (containing manifest and pages/)

        Yields:
            SpeechRecord objects
        """
        # Determine paths
        if source_path.is_file() and source_path.suffix == ".json":
            manifest_path = source_path
            pages_dir = source_path.parent / "pages"
        else:
            manifest_path = source_path / "vp_discours.json"
            pages_dir = source_path / "pages"

        if not manifest_path.exists():
            self.logger.error(f"Manifest not found: {manifest_path}")
            return

        # Load manifest
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Handle both array and object formats
        if isinstance(data, dict):
            speeches = data.get("discours", data.get("data", []))
        else:
            speeches = data

        self.logger.info(f"Parsing {len(speeches)} speeches from manifest")

        for speech in speeches:
            # Filter by date range
            speech_date = self._parse_date(speech)
            if not speech_date:
                continue
            if not (self.date_range.start <= speech_date <= self.date_range.end):
                continue

            # Extract basic metadata from manifest
            record = self._parse_manifest_entry(speech, speech_date)
            if not record:
                continue

            # Try to get full text from cached HTML page
            if pages_dir.exists():
                page_path = pages_dir / f"{record.source_id}.html"
                if page_path.exists():
                    full_text = self._extract_text_from_html(page_path)
                    if full_text:
                        record = record.model_copy(update={"text": full_text})

            yield record

    def _parse_date(self, speech: dict) -> Optional[date]:
        """Parse date from speech entry."""
        # Try various date field names used in the manifest
        date_str = (
            speech.get("prononciation")  # Delivery date (primary field in current manifest)
            or speech.get("date")
            or speech.get("dateDiscours")
            or speech.get("date_discours")
            or speech.get("mise_en_ligne")  # Publication date as fallback
        )
        if not date_str:
            return None

        try:
            if "T" in str(date_str):
                return datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
            return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    def _parse_manifest_entry(
        self, speech: dict, speech_date: date
    ) -> Optional[SpeechRecord]:
        """Parse a single manifest entry into a SpeechRecord."""
        # Extract ID
        source_id = None
        for field in ["id", "identifiant", "uid", "reference"]:
            if field in speech and speech[field]:
                source_id = str(speech[field])
                break

        if not source_id:
            url = speech.get("url") or speech.get("lien")
            if url:
                parts = url.rstrip("/").split("/")
                if parts:
                    last = parts[-1]
                    source_id = last.split("-")[0] if "-" in last else last

        if not source_id:
            return None

        # Extract title
        title = (
            speech.get("titre")
            or speech.get("title")
            or speech.get("intitule")
            or "Untitled"
        )

        # Extract speaker from intervenants list (current manifest format)
        speaker = "Unknown"
        speaker_role = None
        intervenants = speech.get("intervenants", [])
        if intervenants and isinstance(intervenants, list) and len(intervenants) > 0:
            first = intervenants[0]
            if isinstance(first, dict):
                speaker = first.get("nom") or "Unknown"
                speaker_role = first.get("qualite_long") or first.get("qualite")

        # Fallback to other possible fields
        if speaker == "Unknown":
            speaker = (
                speech.get("auteur")
                or speech.get("orateur")
                or speech.get("speaker")
                or speech.get("intervenant")
                or "Unknown"
            )

        # Get text from manifest (usually just a summary, full text from HTML)
        text = (
            speech.get("texte")
            or speech.get("contenu")
            or speech.get("content")
            or speech.get("resume")
            or speech.get("abstract")
            or ""
        )

        # Build URL
        url = speech.get("url") or speech.get("lien")
        if url and not url.startswith("http"):
            url = f"https://www.vie-publique.fr{url}"

        # Speech type from domaine or type_document
        speech_type = (
            speech.get("type_document")
            or speech.get("domaine")
            or speech.get("type")
            or speech.get("nature")
        )

        # Speaker role fallback
        if not speaker_role:
            speaker_role = (
                speech.get("fonction")
                or speech.get("qualite")
                or speech.get("role")
            )

        return SpeechRecord(
            source="vie_publique",
            source_id=source_id,
            source_url=url,
            date=speech_date,
            speaker=speaker,
            title=title,
            text=text,
            lang="fr",
            license="Licence Ouverte v2.0",
            speaker_role=speaker_role,
            speech_type=speech_type,
        )

    def _extract_text_from_html(self, page_path: Path) -> Optional[str]:
        """Extract full text from an HTML page.

        Uses multiple strategies:
        1. Schema.org JSON-LD
        2. Article body selectors
        3. Main content area

        Args:
            page_path: Path to HTML file

        Returns:
            Extracted text or None
        """
        try:
            html_content = page_path.read_text(encoding="utf-8")
        except Exception as e:
            self.logger.warning(f"Failed to read {page_path}: {e}")
            return None

        # Try Schema.org JSON-LD first
        text = self._extract_from_json_ld(html_content)
        if text and len(text) > 100:
            return text

        # Try CSS selectors
        text = self._extract_from_selectors(html_content)
        if text and len(text) > 100:
            return text

        return None

    def _extract_from_json_ld(self, html_content: str) -> Optional[str]:
        """Extract text from Schema.org JSON-LD."""
        try:
            # Find JSON-LD scripts
            pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
            matches = re.findall(pattern, html_content, re.DOTALL | re.IGNORECASE)

            for match in matches:
                try:
                    data = json.loads(match)

                    # Handle array of objects
                    if isinstance(data, list):
                        for item in data:
                            text = self._extract_text_from_schema(item)
                            if text:
                                return text
                    else:
                        text = self._extract_text_from_schema(data)
                        if text:
                            return text
                except json.JSONDecodeError:
                    continue

        except Exception:
            pass

        return None

    def _extract_text_from_schema(self, data: dict) -> Optional[str]:
        """Extract text from a Schema.org object."""
        # Check for article/speech types
        schema_type = data.get("@type", "")
        if isinstance(schema_type, list):
            schema_type = schema_type[0] if schema_type else ""

        # Look for text content
        for field in ["articleBody", "text", "description", "content"]:
            if field in data and data[field]:
                text = data[field]
                if isinstance(text, str) and len(text) > 100:
                    return text

        return None

    def _extract_from_selectors(self, html_content: str) -> Optional[str]:
        """Extract text using CSS selectors."""
        try:
            parser = HTMLParser(html_content)

            # Selectors for vie-publique.fr speech content
            selectors = [
                ".field--name-field-texte-integral",
                ".layout-content",
                "main",
            ]

            for selector in selectors:
                nodes = parser.css(selector)
                if nodes:
                    text = nodes[0].text(separator=" ", strip=True)
                    if text and len(text) > 100:
                        return text

        except Exception as e:
            self.logger.debug(f"Selector extraction failed: {e}")

        return None
