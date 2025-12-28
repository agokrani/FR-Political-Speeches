"""Command-line interface for the political speeches pipeline."""

import asyncio
from datetime import date
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

from .config import Config
from .pipeline import Pipeline
from .utils.logging import setup_logging

app = typer.Typer(
    name="political-speeches",
    help="French Political Speeches Data Pipeline (2000-2010)",
    add_completion=False,
)

console = Console()


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        from . import __version__
        console.print(f"political-speeches version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit",
    ),
) -> None:
    """French Political Speeches Data Pipeline.

    Collects, cleans, deduplicates, and exports French political speeches
    from multiple public sources (2000-2010).
    """
    pass


@app.command()
def run(
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to configuration YAML file",
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="Override start date (YYYY-MM-DD)",
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="Override end date (YYYY-MM-DD)",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Override output directory",
    ),
    sources: Optional[List[str]] = typer.Option(
        None,
        "--source",
        "-s",
        help="Enable specific sources (can be repeated)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Run the complete data pipeline.

    Executes all stages: collect, parse, clean, deduplicate, export.
    """
    # Load config
    if config_path:
        if not config_path.exists():
            console.print(f"[red]Config file not found: {config_path}[/red]")
            raise typer.Exit(1)
        config = Config.from_yaml(config_path)
    else:
        # Try default config location
        default_config = Path("config/default.yaml")
        if default_config.exists():
            config = Config.from_yaml(default_config)
        else:
            config = Config.default()

    # Apply overrides
    if start_date:
        try:
            config.pipeline.date_range.start = date.fromisoformat(start_date)
        except ValueError:
            console.print(f"[red]Invalid start date format: {start_date}[/red]")
            raise typer.Exit(1)

    if end_date:
        try:
            config.pipeline.date_range.end = date.fromisoformat(end_date)
        except ValueError:
            console.print(f"[red]Invalid end date format: {end_date}[/red]")
            raise typer.Exit(1)

    if output_dir:
        config.pipeline.output_dir = output_dir

    if sources:
        # Disable all, then enable specified
        config.sources.vie_publique.enabled = "vie_publique" in sources
        config.sources.senat.enabled = "senat" in sources
        config.sources.assemblee.enabled = "assemblee" in sources
        config.sources.europarl.enabled = "europarl" in sources

    if verbose:
        config.pipeline.log_level = "DEBUG"

    # Run pipeline
    console.print("[bold]Starting French Political Speeches Pipeline[/bold]")
    console.print(
        f"Date range: {config.pipeline.date_range.start} to "
        f"{config.pipeline.date_range.end}"
    )
    console.print(f"Enabled sources: {', '.join(config.get_enabled_sources())}")
    console.print()

    try:
        pipeline = Pipeline(config)
        manifest = pipeline.run()

        console.print(f"\n[green]Output: {config.pipeline.output_dir / 'curated.jsonl'}[/green]")
        console.print(f"[green]Manifest: {config.pipeline.output_dir / 'manifest.json'}[/green]")

    except Exception as e:
        console.print(f"[red]Pipeline failed: {e}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1)


@app.command("collect-only")
def collect_only(
    source: str = typer.Argument(
        ...,
        help="Source to collect: vie_publique, senat, assemblee, europarl",
    ),
    config_path: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to configuration YAML file",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Override output directory",
    ),
) -> None:
    """Collect raw data from a single source without processing.

    Useful for testing collection or downloading data incrementally.
    """
    valid_sources = ["vie_publique", "senat", "assemblee", "europarl"]
    if source not in valid_sources:
        console.print(f"[red]Invalid source: {source}[/red]")
        console.print(f"Valid sources: {', '.join(valid_sources)}")
        raise typer.Exit(1)

    # Load config
    if config_path and config_path.exists():
        config = Config.from_yaml(config_path)
    else:
        config = Config.default()

    if output_dir:
        config.pipeline.output_dir = output_dir

    console.print(f"[bold]Collecting data from {source}[/bold]")

    try:
        pipeline = Pipeline(config)
        path = asyncio.run(pipeline.collect_source(source))
        console.print(f"[green]Data collected to: {path}[/green]")
    except Exception as e:
        console.print(f"[red]Collection failed: {e}[/red]")
        raise typer.Exit(1)


@app.command("validate-config")
def validate_config(
    config_path: Path = typer.Argument(
        ...,
        help="Path to configuration file to validate",
    ),
) -> None:
    """Validate a configuration file.

    Checks that the configuration file is valid YAML and conforms
    to the expected schema.
    """
    if not config_path.exists():
        console.print(f"[red]Config file not found: {config_path}[/red]")
        raise typer.Exit(1)

    try:
        config = Config.from_yaml(config_path)

        console.print("[green]Configuration is valid![/green]")
        console.print()

        # Show summary
        table = Table(title="Configuration Summary")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="white")

        table.add_row(
            "Date range",
            f"{config.pipeline.date_range.start} to {config.pipeline.date_range.end}",
        )
        table.add_row("Output directory", str(config.pipeline.output_dir))
        table.add_row("Enabled sources", ", ".join(config.get_enabled_sources()))
        table.add_row("Unicode normalize", config.processing.unicode_normalize)
        table.add_row("Min text length", str(config.processing.min_text_length))
        table.add_row("Export JSONL", str(config.export.jsonl))
        table.add_row("Export Parquet", str(config.export.parquet))

        console.print(table)

    except Exception as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        raise typer.Exit(1)


@app.command("init-config")
def init_config(
    output_path: Path = typer.Argument(
        Path("config/default.yaml"),
        help="Path to write default configuration",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing file",
    ),
) -> None:
    """Generate a default configuration file.

    Creates a new YAML configuration file with default values
    that can be customized for your needs.
    """
    if output_path.exists() and not force:
        console.print(f"[yellow]Config file already exists: {output_path}[/yellow]")
        console.print("Use --force to overwrite")
        raise typer.Exit(1)

    config = Config.default()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    config.to_yaml(output_path)

    console.print(f"[green]Default config written to: {output_path}[/green]")


@app.command()
def info() -> None:
    """Show information about available sources and their status."""
    console.print("[bold]French Political Speeches Pipeline[/bold]")
    console.print()

    table = Table(title="Data Sources")
    table.add_column("Source", style="cyan")
    table.add_column("Description")
    table.add_column("Coverage")
    table.add_column("Status", style="green")

    table.add_row(
        "vie_publique",
        "Discours publics from vie-publique.fr",
        "1974+",
        "Available",
    )
    table.add_row(
        "senat",
        "Senate session transcripts",
        "2003+",
        "Available",
    )
    table.add_row(
        "assemblee",
        "National Assembly debates",
        "2011+ (structured)",
        "Partial (2000-2010 needs OCR)",
    )
    table.add_row(
        "europarl",
        "European Parliament (French MEPs)",
        "1999+",
        "Optional (disabled by default)",
    )

    console.print(table)

    console.print("\n[bold]Quick Start:[/bold]")
    console.print("  political-speeches run                    # Run full pipeline")
    console.print("  political-speeches run -s vie_publique    # Single source")
    console.print("  political-speeches validate-config config/default.yaml")


if __name__ == "__main__":
    app()
