"""Base collector interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from ..config import Config
from ..utils.logging import get_logger


class BaseCollector(ABC):
    """Abstract base class for data source collectors.

    Collectors are responsible for downloading raw data from their
    respective sources and storing it locally.
    """

    def __init__(self, config: Config, output_dir: Path):
        """Initialize the collector.

        Args:
            config: Pipeline configuration
            output_dir: Directory to store collected data
        """
        self.config = config
        self.output_dir = output_dir
        self.logger = get_logger()

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    async def collect(self) -> Path:
        """Download raw data to output_dir.

        Returns:
            Path to the collected data (file or directory)
        """
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the name of this source.

        Returns:
            Source name (e.g., 'vie_publique', 'senat')
        """
        pass

    @abstractmethod
    def is_enabled(self) -> bool:
        """Check if this collector is enabled in config.

        Returns:
            True if enabled
        """
        pass

    def get_cache_path(self, filename: str) -> Path:
        """Get path for a cached file.

        Args:
            filename: Name of the file

        Returns:
            Full path to the cache file
        """
        return self.output_dir / filename

    def is_cached(self, filename: str) -> bool:
        """Check if a file is already cached.

        Args:
            filename: Name of the file

        Returns:
            True if file exists in cache
        """
        return self.get_cache_path(filename).exists()

    def get_stats(self) -> dict:
        """Get collection statistics.

        Returns:
            Dictionary with collection stats
        """
        return {
            "source": self.get_source_name(),
            "output_dir": str(self.output_dir),
            "enabled": self.is_enabled(),
        }
