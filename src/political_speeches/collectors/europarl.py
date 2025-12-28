"""Collector for European Parliament verbatim reports.

This collector is DISABLED by default and serves as an expansion module
for future implementation.
"""

from pathlib import Path

from ..config import Config
from ..utils.logging import get_logger
from .base import BaseCollector


class EuroparlCollector(BaseCollector):
    """Collector for European Parliament plenary debates.

    Filters for French MEPs (Members of European Parliament).
    This is an optional expansion module, disabled by default.
    """

    def __init__(self, config: Config, output_dir: Path):
        super().__init__(config, output_dir)
        self.source_config = config.sources.europarl
        self.date_range = config.pipeline.date_range

    def get_source_name(self) -> str:
        return "europarl"

    def is_enabled(self) -> bool:
        return self.source_config.enabled

    async def collect(self) -> Path:
        """Collect European Parliament data.

        This is a stub implementation. When enabled, it would:
        1. Query the EP Open Data portal API
        2. Filter for French MEPs (filter_country: FR)
        3. Download plenary debate transcripts in the date range
        4. Store in RDF/XML format

        Returns:
            Path to the output directory
        """
        if not self.is_enabled():
            self.logger.info(
                "Europarl collector is disabled. "
                "Enable in config with sources.europarl.enabled: true"
            )
            return self.output_dir

        self.logger.info(
            f"Europarl collection for French MEPs "
            f"({self.date_range.start} to {self.date_range.end})"
        )

        # Stub implementation
        await self._collect_ep_data()

        return self.output_dir

    async def _collect_ep_data(self) -> None:
        """Download European Parliament open data.

        Stub implementation - would use the EP Open Data API:
        https://data.europarl.europa.eu/

        Filtering strategy:
        - By MEP nationality (FR for French)
        - By date range
        - Plenary session transcripts only
        """
        self.logger.info(
            f"EP Open Data collection is a stub. "
            f"Portal: {self.source_config.portal_url}"
        )

        # Create a notice file
        notice_path = self.output_dir / "EUROPARL_STUB.md"
        notice_path.write_text(
            "# European Parliament Data Collection\n\n"
            "## Status\n"
            "This is a stub implementation. Full collection is not yet implemented.\n\n"
            "## Data Source\n"
            f"- Portal: {self.source_config.portal_url}\n"
            f"- Filter: {self.source_config.filter_country} (French MEPs)\n"
            f"- Date range: {self.date_range.start} to {self.date_range.end}\n\n"
            "## Implementation Notes\n"
            "When implemented, this collector will:\n"
            "1. Query the EP Open Data SPARQL endpoint\n"
            "2. Filter for French MEP interventions\n"
            "3. Download debate transcripts (RDF/XML or JSON-LD)\n"
            "4. Parse and convert to SpeechRecord format\n"
        )
        self.logger.info(f"Stub notice written to {notice_path}")
