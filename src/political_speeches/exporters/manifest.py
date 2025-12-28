"""Manifest generator for pipeline runs."""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from ..config import Config
from ..models import ManifestRecord, SourceStats
from ..utils.hashing import compute_file_checksum
from ..utils.logging import get_logger


class ManifestGenerator:
    """Generates manifest.json for pipeline runs.

    The manifest provides:
    - Run metadata (ID, timestamp, config hash)
    - Source statistics (records per source)
    - Output file checksums for reproducibility
    - Error tracking
    """

    def __init__(self, config: Config):
        """Initialize the generator.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.logger = get_logger()

    def compute_config_hash(self) -> str:
        """Compute a hash of the configuration.

        Returns:
            Short hex hash of config
        """
        config_json = json.dumps(
            self.config.model_dump(mode="json"),
            sort_keys=True,
            default=str,
        )
        full_hash = hashlib.sha256(config_json.encode()).hexdigest()
        return full_hash[:16]  # Short hash

    def generate(
        self,
        source_stats: dict[str, SourceStats],
        output_dir: Path,
        errors: Optional[list[str]] = None,
    ) -> ManifestRecord:
        """Generate a manifest record.

        Args:
            source_stats: Statistics per source
            output_dir: Directory containing output files
            errors: List of error messages

        Returns:
            ManifestRecord object
        """
        # Compute totals
        total_records = sum(s.parsed for s in source_stats.values())
        deduplicated_records = sum(s.deduplicated for s in source_stats.values())

        # Compute output file checksums
        output_files = {}
        for filename in ["curated.jsonl", "curated.parquet"]:
            filepath = output_dir / filename
            if filepath.exists():
                checksum = compute_file_checksum(filepath, "sha256")
                output_files[filename] = checksum

        manifest = ManifestRecord(
            run_id=f"run_{uuid4().hex[:8]}",
            run_timestamp=datetime.utcnow(),
            config_hash=self.compute_config_hash(),
            date_range_start=self.config.pipeline.date_range.start,
            date_range_end=self.config.pipeline.date_range.end,
            sources_processed=source_stats,
            total_records=total_records,
            deduplicated_records=deduplicated_records,
            output_files=output_files,
            errors=errors or [],
        )

        return manifest

    def write(
        self,
        manifest: ManifestRecord,
        output_path: Path,
    ) -> None:
        """Write manifest to a JSON file.

        Args:
            manifest: Manifest record
            output_path: Path to output file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(manifest.model_dump_json(indent=2))

        self.logger.info(f"Manifest written to {output_path}")

    def generate_and_write(
        self,
        source_stats: dict[str, SourceStats],
        output_dir: Path,
        errors: Optional[list[str]] = None,
    ) -> ManifestRecord:
        """Generate manifest and write to file.

        Args:
            source_stats: Statistics per source
            output_dir: Directory containing output files
            errors: List of error messages

        Returns:
            ManifestRecord object
        """
        manifest = self.generate(source_stats, output_dir, errors)
        self.write(manifest, output_dir / "manifest.json")
        return manifest


class SourcesDocGenerator:
    """Generates SOURCES.md documentation."""

    TEMPLATE = """# Data Sources

This document describes the data sources used in the French Political Speeches dataset.

## Date Range

- **Start**: {start_date}
- **End**: {end_date}

## Sources

### Vie-publique (DILA) - Discours Publics

- **URL**: https://www.vie-publique.fr/discours
- **Bulk Manifest**: https://echanges.dila.gouv.fr/OPENDATA/DISCOURS_PUBLICS/vp_discours.json
- **License**: Licence Ouverte v2.0 (Open License)
- **Description**: Official speeches and public declarations from major French political figures
- **Coverage**: Speeches from 1974 onwards
- **Records in dataset**: {vie_publique_count}

### Senat - Comptes Rendus

- **URL**: https://data.senat.fr/la-base-comptes-rendus/
- **Bulk Data**: https://data.senat.fr/data/debats/cri.zip (XML format)
- **License**: Licence Ouverte (Open License)
- **Description**: Complete transcripts of French Senate sessions
- **Coverage**: Sessions from January 2003 onwards
- **Records in dataset**: {senat_count}

### Assemblee Nationale (DILA/Journal Officiel)

- **URL**: https://echanges.dila.gouv.fr/OPENDATA/Debats/AN/
- **Format**: XML archives
- **License**: Licence Ouverte (Open License)
- **Description**: Debate transcripts from the French National Assembly
- **Coverage**: Structured XML data available from 2011 onwards only
- **Note**: For the 2000-2010 period, structured data is not available. Future versions may implement PDF/HTML + OCR parsing.
- **Records in dataset**: {assemblee_count}

### European Parliament (Optional)

- **URL**: https://data.europarl.europa.eu/
- **Format**: RDF/Turtle, JSON-LD
- **License**: European Parliament Open Data License
- **Description**: Plenary session verbatim reports, filtered for French MEPs
- **Coverage**: 1999 onwards
- **Status**: {europarl_status}
- **Records in dataset**: {europarl_count}

## Processing

- **Unicode Normalization**: NFC
- **Deduplication**: Based on text content hash (xxhash64)
- **Minimum Text Length**: 100 characters

## Citation

If you use this dataset, please cite:

```
French Political Speeches Dataset (2000-2010)
Sources: Vie-publique, Senat, Assemblee Nationale
License: Open License / Licence Ouverte
Generated by: political-speeches pipeline v{version}
```

---

*Generated on {generation_date}*
"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = get_logger()

    def generate(
        self,
        source_stats: dict[str, SourceStats],
        output_path: Path,
    ) -> None:
        """Generate SOURCES.md documentation.

        Args:
            source_stats: Statistics per source
            output_path: Path to output file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        content = self.TEMPLATE.format(
            start_date=self.config.pipeline.date_range.start.isoformat(),
            end_date=self.config.pipeline.date_range.end.isoformat(),
            vie_publique_count=source_stats.get("vie_publique", SourceStats()).deduplicated,
            senat_count=source_stats.get("senat", SourceStats()).deduplicated,
            assemblee_count=source_stats.get("assemblee", SourceStats()).deduplicated,
            europarl_status="Enabled" if self.config.sources.europarl.enabled else "Disabled",
            europarl_count=source_stats.get("europarl", SourceStats()).deduplicated,
            version="0.1.0",
            generation_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        )

        output_path.write_text(content, encoding="utf-8")
        self.logger.info(f"SOURCES.md written to {output_path}")
