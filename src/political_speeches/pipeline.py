"""Main pipeline orchestrator."""

import asyncio
from pathlib import Path
from typing import List, Optional

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .collectors import (
    AssembleeCollector,
    EuroparlCollector,
    SenatCollector,
    ViePubliqueCollector,
)
from .config import Config
from .exporters import JSONLExporter, ParquetExporter
from .exporters.manifest import ManifestGenerator, SourcesDocGenerator
from .models import ManifestRecord, SourceStats, SpeechRecord
from .parsers import (
    AssembleeXMLParser,
    EuroparlParser,
    SenatXMLParser,
    ViePubliqueParser,
)
from .processors import TextCleaner
from .processors.deduplicator import CrossSourceDeduplicator
from .utils.logging import get_console, get_logger, setup_logging


class Pipeline:
    """Main pipeline orchestrator.

    Coordinates the full data processing pipeline:
    1. Collect raw data from sources
    2. Parse into speech records
    3. Clean and normalize text
    4. Deduplicate across sources
    5. Export to output formats
    """

    def __init__(self, config: Config):
        """Initialize the pipeline.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.logger = setup_logging(config.pipeline.log_level)
        self.console = get_console()

        # Setup directories
        self.output_dir = Path(config.pipeline.output_dir)
        self.raw_dir = self.output_dir / "raw"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self._init_collectors()
        self._init_parsers()
        self.cleaner = TextCleaner(config.processing)
        self.deduplicator = CrossSourceDeduplicator(config.processing)

        # Statistics
        self.source_stats: dict[str, SourceStats] = {}
        self.errors: list[str] = []

    def _init_collectors(self) -> None:
        """Initialize data collectors."""
        self.collectors = {
            "vie_publique": ViePubliqueCollector(
                self.config, self.raw_dir / "vie_publique"
            ),
            "senat": SenatCollector(self.config, self.raw_dir / "senat"),
            "assemblee": AssembleeCollector(self.config, self.raw_dir / "assemblee"),
            "europarl": EuroparlCollector(self.config, self.raw_dir / "europarl"),
        }

    def _init_parsers(self) -> None:
        """Initialize data parsers."""
        self.parsers = {
            "vie_publique": ViePubliqueParser(self.config),
            "senat": SenatXMLParser(self.config),
            "assemblee": AssembleeXMLParser(self.config),
            "europarl": EuroparlParser(self.config),
        }

    def run(self, progress: Optional[Progress] = None) -> ManifestRecord:
        """Execute the complete pipeline.

        Args:
            progress: Optional rich Progress instance

        Returns:
            ManifestRecord with run statistics
        """
        if progress is None:
            progress = self._create_progress()

        with progress:
            # Phase 1: Collect
            self.logger.info("Phase 1: Collecting data from sources")
            raw_paths = asyncio.run(self._collect_all(progress))

            # Phase 2: Parse
            self.logger.info("Phase 2: Parsing collected data")
            all_records = self._parse_all(raw_paths, progress)

            # Phase 3: Clean
            self.logger.info("Phase 3: Cleaning text content")
            cleaned_records = self._clean_all(all_records, progress)

            # Phase 4: Deduplicate
            self.logger.info("Phase 4: Deduplicating records")
            unique_records = self._deduplicate(cleaned_records, progress)

            # Phase 5: Export
            self.logger.info("Phase 5: Exporting results")
            manifest = self._export(unique_records, progress)

        self._print_summary(manifest)
        return manifest

    def _create_progress(self) -> Progress:
        """Create a rich Progress instance."""
        return Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=self.console,
        )

    async def _collect_all(self, progress: Progress) -> dict[str, Path]:
        """Collect data from all enabled sources.

        Args:
            progress: Progress tracker

        Returns:
            Dictionary mapping source name to collected data path
        """
        enabled_sources = self.config.get_enabled_sources()
        task_id = progress.add_task("Collecting...", total=len(enabled_sources))

        results = {}
        for source_name in enabled_sources:
            collector = self.collectors[source_name]
            progress.update(task_id, description=f"Collecting {source_name}")

            try:
                path = await collector.collect()
                results[source_name] = path

                # Initialize stats
                self.source_stats[source_name] = SourceStats(collected=1)

            except Exception as e:
                self.logger.error(f"Collection failed for {source_name}: {e}")
                self.errors.append(f"Collection error ({source_name}): {str(e)}")
                self.source_stats[source_name] = SourceStats(errors=1)

            progress.update(task_id, advance=1)

        return results

    def _parse_all(
        self, raw_paths: dict[str, Path], progress: Progress
    ) -> List[SpeechRecord]:
        """Parse all collected data.

        Args:
            raw_paths: Mapping of source to data path
            progress: Progress tracker

        Returns:
            List of all parsed records
        """
        all_records = []
        task_id = progress.add_task("Parsing...", total=len(raw_paths))

        for source_name, path in raw_paths.items():
            parser = self.parsers[source_name]
            progress.update(task_id, description=f"Parsing {source_name}")

            try:
                records = list(parser.parse(path))
                all_records.extend(records)

                if source_name in self.source_stats:
                    self.source_stats[source_name].parsed = len(records)
                else:
                    self.source_stats[source_name] = SourceStats(parsed=len(records))

                self.logger.info(f"Parsed {len(records)} records from {source_name}")

            except Exception as e:
                self.logger.error(f"Parsing failed for {source_name}: {e}")
                self.errors.append(f"Parsing error ({source_name}): {str(e)}")
                if source_name in self.source_stats:
                    self.source_stats[source_name].errors += 1

            progress.update(task_id, advance=1)

        return all_records

    def _clean_all(
        self, records: List[SpeechRecord], progress: Progress
    ) -> List[SpeechRecord]:
        """Clean all records.

        Args:
            records: List of parsed records
            progress: Progress tracker

        Returns:
            List of cleaned records
        """
        task_id = progress.add_task("Cleaning...", total=len(records))
        cleaned = []

        exclude_roles = self.config.processing.exclude_speaker_roles

        for record in records:
            try:
                cleaned_record = self.cleaner.clean(record)

                # Skip if too short after cleaning
                if len(cleaned_record.text) < self.config.processing.min_text_length:
                    continue

                # Skip excluded speaker roles
                if exclude_roles and cleaned_record.speaker_role in exclude_roles:
                    continue

                cleaned.append(cleaned_record)

                # Update stats
                if record.source in self.source_stats:
                    self.source_stats[record.source].cleaned += 1

            except Exception as e:
                self.logger.warning(f"Cleaning failed for {record.source_id}: {e}")

            progress.update(task_id, advance=1)

        return cleaned

    def _deduplicate(
        self, records: List[SpeechRecord], progress: Progress
    ) -> List[SpeechRecord]:
        """Deduplicate records.

        Args:
            records: List of cleaned records
            progress: Progress tracker

        Returns:
            List of unique records
        """
        task_id = progress.add_task("Deduplicating...", total=len(records))
        unique = []

        for record in self.deduplicator.deduplicate(iter(records)):
            unique.append(record)

            # Update stats
            if record.source in self.source_stats:
                self.source_stats[record.source].deduplicated += 1

            progress.update(task_id, advance=1)

        dedupe_stats = self.deduplicator.get_stats()
        self.logger.info(
            f"Deduplication: {dedupe_stats['total_seen']} -> {dedupe_stats['unique']} "
            f"({dedupe_stats['duplicates_found']} duplicates removed)"
        )

        return unique

    def _export(
        self, records: List[SpeechRecord], progress: Progress
    ) -> ManifestRecord:
        """Export records to output formats.

        Args:
            records: List of deduplicated records
            progress: Progress tracker

        Returns:
            ManifestRecord with run statistics
        """
        export_tasks = []
        if self.config.export.jsonl:
            export_tasks.append("jsonl")
        if self.config.export.parquet:
            export_tasks.append("parquet")
        export_tasks.append("manifest")
        if self.config.export.generate_sources_md:
            export_tasks.append("sources_md")

        task_id = progress.add_task("Exporting...", total=len(export_tasks))

        # Export JSONL
        if self.config.export.jsonl:
            progress.update(task_id, description="Exporting JSONL")
            jsonl_exporter = JSONLExporter()
            jsonl_exporter.export(iter(records), self.output_dir / "curated.jsonl")
            progress.update(task_id, advance=1)

        # Export Parquet
        if self.config.export.parquet:
            progress.update(task_id, description="Exporting Parquet")
            parquet_exporter = ParquetExporter()
            parquet_exporter.export(records, self.output_dir / "curated.parquet")
            progress.update(task_id, advance=1)

        # Generate manifest
        progress.update(task_id, description="Generating manifest")
        manifest_gen = ManifestGenerator(self.config)
        manifest = manifest_gen.generate_and_write(
            self.source_stats, self.output_dir, self.errors
        )
        progress.update(task_id, advance=1)

        # Generate SOURCES.md
        if self.config.export.generate_sources_md:
            progress.update(task_id, description="Generating SOURCES.md")
            docs_dir = Path("docs")
            docs_dir.mkdir(exist_ok=True)
            sources_gen = SourcesDocGenerator(self.config)
            sources_gen.generate(self.source_stats, docs_dir / "SOURCES.md")
            progress.update(task_id, advance=1)

        return manifest

    def _print_summary(self, manifest: ManifestRecord) -> None:
        """Print a summary of the pipeline run."""
        self.console.print("\n[bold green]Pipeline completed successfully![/bold green]")
        self.console.print(f"Run ID: {manifest.run_id}")
        self.console.print(f"Config hash: {manifest.config_hash}")
        self.console.print(
            f"Date range: {manifest.date_range_start} to {manifest.date_range_end}"
        )
        self.console.print(f"\nTotal records: {manifest.total_records}")
        self.console.print(f"After deduplication: {manifest.deduplicated_records}")

        self.console.print("\n[bold]Per-source statistics:[/bold]")
        for source, stats in self.source_stats.items():
            self.console.print(
                f"  {source}: {stats.parsed} parsed, "
                f"{stats.cleaned} cleaned, {stats.deduplicated} final"
            )

        self.console.print(f"\n[bold]Output files:[/bold]")
        for filename, checksum in manifest.output_files.items():
            self.console.print(f"  {filename}: {checksum[:16]}...")

        if manifest.errors:
            self.console.print(f"\n[bold red]Errors ({len(manifest.errors)}):[/bold red]")
            for error in manifest.errors[:5]:  # Show first 5
                self.console.print(f"  - {error}")

    async def collect_source(self, source_name: str) -> Path:
        """Collect data from a single source.

        Args:
            source_name: Name of the source

        Returns:
            Path to collected data
        """
        if source_name not in self.collectors:
            raise ValueError(f"Unknown source: {source_name}")

        collector = self.collectors[source_name]
        return await collector.collect()
