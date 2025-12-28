"""Text cleaning processor."""

import re
import unicodedata
from datetime import datetime
from typing import Iterator

from lxml import html

from ..config import ProcessingConfig
from ..models import SpeechRecord
from ..utils.logging import get_logger


class TextCleaner:
    """Cleans and normalizes text content in speech records.

    Operations:
    - Unicode normalization (NFC/NFD/NFKC/NFKD)
    - Whitespace normalization
    - HTML stripping
    - Boilerplate removal
    """

    # Common boilerplate patterns to remove
    BOILERPLATE_PATTERNS = [
        # Session markers (match until period or end of sentence)
        r"La séance est ouverte[^.]*\.",
        r"La séance est levée[^.]*\.",
        r"Prochaine séance[^.]*\.",
        r"La séance est suspendue[^.]*\.",
        r"La séance est reprise[^.]*\.",
        # Procedural markers (parenthetical)
        r"\(Applaudissements[^)]*\)",
        r"\(Exclamations[^)]*\)",
        r"\(Rires[^)]*\)",
        r"\(Protestations[^)]*\)",
        r"\(Murmures[^)]*\)",
        # Presidency notes
        r"M\. le président\.?\s*[-–—]?\s*",
        r"Mme la présidente\.?\s*[-–—]?\s*",
        # Page markers
        r"- \d+ -",
        r"page \d+",
    ]

    def __init__(self, config: ProcessingConfig):
        """Initialize the cleaner.

        Args:
            config: Processing configuration
        """
        self.config = config
        self.logger = get_logger()

        # Compile boilerplate patterns
        self._boilerplate_re = [
            re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            for pattern in self.BOILERPLATE_PATTERNS
        ]

    def clean(self, record: SpeechRecord) -> SpeechRecord:
        """Clean a single speech record.

        Args:
            record: Speech record to clean

        Returns:
            Cleaned speech record
        """
        text = record.text

        # Step 1: Unicode normalization
        text = unicodedata.normalize(self.config.unicode_normalize, text)

        # Step 2: Strip HTML if present
        if self.config.strip_html:
            text = self._strip_html(text)

        # Step 3: Whitespace normalization
        text = self._normalize_whitespace(text)

        # Step 4: Remove boilerplate
        if self.config.remove_boilerplate:
            text = self._remove_boilerplate(text)

        # Step 5: Final whitespace cleanup
        text = self._normalize_whitespace(text)

        return record.model_copy(
            update={
                "text": text,
                "cleaned_at": datetime.utcnow(),
            }
        )

    def clean_batch(self, records: Iterator[SpeechRecord]) -> Iterator[SpeechRecord]:
        """Clean a batch of speech records.

        Args:
            records: Iterator of speech records

        Yields:
            Cleaned speech records
        """
        for record in records:
            cleaned = self.clean(record)

            # Skip records that are too short after cleaning
            if len(cleaned.text) >= self.config.min_text_length:
                yield cleaned

    def _strip_html(self, text: str) -> str:
        """Remove HTML tags from text.

        Args:
            text: Text possibly containing HTML

        Returns:
            Plain text
        """
        if "<" not in text:
            return text

        try:
            # Parse as HTML and extract text
            doc = html.fromstring(text)
            return doc.text_content()
        except Exception:
            # Fallback: simple regex removal
            return re.sub(r"<[^>]+>", " ", text)

    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace in text.

        Args:
            text: Input text

        Returns:
            Text with normalized whitespace
        """
        # Replace various whitespace characters with regular space
        text = re.sub(r"[\t\r\f\v]+", " ", text)

        # Collapse multiple spaces
        text = re.sub(r" {2,}", " ", text)

        # Normalize newlines (max 2 consecutive)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Remove leading/trailing whitespace from lines
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)

        # Final strip
        return text.strip()

    def _remove_boilerplate(self, text: str) -> str:
        """Remove common boilerplate patterns.

        Args:
            text: Input text

        Returns:
            Text with boilerplate removed
        """
        for pattern in self._boilerplate_re:
            text = pattern.sub("", text)

        return text

    def clean_speaker(self, speaker: str) -> str:
        """Clean and normalize a speaker name.

        Args:
            speaker: Raw speaker name

        Returns:
            Cleaned speaker name
        """
        if not speaker:
            return "Unknown"

        # Unicode normalize
        speaker = unicodedata.normalize(self.config.unicode_normalize, speaker)

        # Remove titles/prefixes
        prefixes = [
            r"^M\.\s+",
            r"^Mme\.?\s+",
            r"^Mlle\.?\s+",
            r"^Dr\.?\s+",
            r"^Pr\.?\s+",
            r"^Me\.?\s+",
        ]
        for prefix in prefixes:
            speaker = re.sub(prefix, "", speaker, flags=re.IGNORECASE)

        # Remove parenthetical info
        speaker = re.sub(r"\s*\([^)]*\)\s*", " ", speaker)

        # Normalize whitespace
        speaker = " ".join(speaker.split())

        return speaker.strip() or "Unknown"

    def clean_title(self, title: str) -> str:
        """Clean and normalize a speech title.

        Args:
            title: Raw title

        Returns:
            Cleaned title
        """
        if not title:
            return "Untitled"

        # Unicode normalize
        title = unicodedata.normalize(self.config.unicode_normalize, title)

        # Normalize whitespace
        title = " ".join(title.split())

        # Truncate if too long
        if len(title) > 500:
            title = title[:497] + "..."

        return title.strip() or "Untitled"
