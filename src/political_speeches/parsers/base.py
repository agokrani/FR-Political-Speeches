"""Base parser interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator

from ..config import Config
from ..models import SpeechRecord
from ..utils.logging import get_logger


class BaseParser(ABC):
    """Abstract base class for data parsers.

    Parsers are responsible for extracting structured SpeechRecord
    objects from raw collected data.
    """

    def __init__(self, config: Config):
        """Initialize the parser.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.logger = get_logger()

    @abstractmethod
    def parse(self, source_path: Path) -> Iterator[SpeechRecord]:
        """Parse raw data and yield speech records.

        Args:
            source_path: Path to the collected data

        Yields:
            SpeechRecord objects
        """
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the name of this source.

        Returns:
            Source name (e.g., 'vie_publique', 'senat')
        """
        pass

    def count_records(self, source_path: Path) -> int:
        """Count total records without fully parsing.

        Override in subclasses for efficient counting.

        Args:
            source_path: Path to the collected data

        Returns:
            Estimated record count
        """
        return sum(1 for _ in self.parse(source_path))
