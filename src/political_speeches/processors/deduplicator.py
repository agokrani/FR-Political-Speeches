"""Deduplication processor."""

from typing import Iterator, Set

from ..config import ProcessingConfig
from ..models import SpeechRecord
from ..utils.hashing import compute_hash
from ..utils.logging import get_logger


class Deduplicator:
    """Removes duplicate speech records based on text hash.

    Uses xxhash64 (default) or SHA256 to hash the cleaned text
    content and filter out duplicates.
    """

    def __init__(self, config: ProcessingConfig):
        """Initialize the deduplicator.

        Args:
            config: Processing configuration
        """
        self.config = config
        self.logger = get_logger()
        self.seen_hashes: Set[str] = set()
        self.total_seen: int = 0
        self.duplicates_found: int = 0

    def compute_record_hash(self, record: SpeechRecord) -> str:
        """Compute hash for a speech record.

        Args:
            record: Speech record to hash

        Returns:
            Hex-encoded hash string
        """
        # Build content to hash based on configured fields
        parts = []
        for field in self.config.dedupe_fields:
            value = getattr(record, field, None)
            if value:
                parts.append(str(value))

        content = "\n".join(parts)
        return compute_hash(content, self.config.dedupe_hash_algorithm)

    def is_duplicate(self, record: SpeechRecord) -> bool:
        """Check if a record is a duplicate.

        Args:
            record: Speech record to check

        Returns:
            True if duplicate (already seen)
        """
        text_hash = self.compute_record_hash(record)
        return text_hash in self.seen_hashes

    def deduplicate(
        self, records: Iterator[SpeechRecord]
    ) -> Iterator[SpeechRecord]:
        """Deduplicate a stream of speech records.

        Args:
            records: Iterator of speech records

        Yields:
            Unique speech records with text_hash set
        """
        for record in records:
            self.total_seen += 1
            text_hash = self.compute_record_hash(record)

            if text_hash in self.seen_hashes:
                self.duplicates_found += 1
                self.logger.debug(
                    f"Duplicate found: {record.source}/{record.source_id}"
                )
                continue

            self.seen_hashes.add(text_hash)

            # Add hash to record
            yield record.model_copy(update={"text_hash": text_hash})

    def get_stats(self) -> dict:
        """Get deduplication statistics.

        Returns:
            Dictionary with deduplication stats
        """
        return {
            "total_seen": self.total_seen,
            "unique": len(self.seen_hashes),
            "duplicates_found": self.duplicates_found,
            "duplicate_rate": (
                self.duplicates_found / self.total_seen
                if self.total_seen > 0
                else 0
            ),
        }

    def reset(self) -> None:
        """Reset the deduplicator state."""
        self.seen_hashes.clear()
        self.total_seen = 0
        self.duplicates_found = 0


class CrossSourceDeduplicator(Deduplicator):
    """Deduplicator that tracks duplicates across sources.

    Extends the base deduplicator to keep track of which sources
    contributed duplicates.
    """

    def __init__(self, config: ProcessingConfig):
        super().__init__(config)
        self.source_duplicates: dict[str, int] = {}
        self.hash_to_source: dict[str, str] = {}

    def deduplicate(
        self, records: Iterator[SpeechRecord]
    ) -> Iterator[SpeechRecord]:
        """Deduplicate with cross-source tracking.

        Args:
            records: Iterator of speech records

        Yields:
            Unique speech records
        """
        for record in records:
            self.total_seen += 1
            text_hash = self.compute_record_hash(record)

            if text_hash in self.seen_hashes:
                self.duplicates_found += 1

                # Track which source had the duplicate
                original_source = self.hash_to_source.get(text_hash, "unknown")
                self.source_duplicates[record.source] = (
                    self.source_duplicates.get(record.source, 0) + 1
                )

                self.logger.debug(
                    f"Cross-source duplicate: {record.source}/{record.source_id} "
                    f"matches {original_source}"
                )
                continue

            self.seen_hashes.add(text_hash)
            self.hash_to_source[text_hash] = record.source

            yield record.model_copy(update={"text_hash": text_hash})

    def get_stats(self) -> dict:
        """Get extended deduplication statistics."""
        stats = super().get_stats()
        stats["source_duplicates"] = self.source_duplicates
        return stats

    def reset(self) -> None:
        """Reset the deduplicator state."""
        super().reset()
        self.source_duplicates.clear()
        self.hash_to_source.clear()
