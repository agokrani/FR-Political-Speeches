"""Configuration management for the political speeches pipeline."""

from datetime import date
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class DateRange(BaseModel):
    """Date range for filtering speeches."""

    start: date = date(2000, 1, 1)
    end: date = date(2010, 12, 31)


class HttpConfig(BaseModel):
    """HTTP client configuration."""

    timeout: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_backoff: float = Field(default=2.0, description="Exponential backoff multiplier")
    rate_limit_delay: float = Field(
        default=1.0, description="Delay between requests in seconds"
    )


class ViePubliqueConfig(BaseModel):
    """Vie-publique source configuration."""

    enabled: bool = True
    manifest_url: str = (
        "https://echanges.dila.gouv.fr/OPENDATA/DISCOURS_PUBLICS/vp_discours.json"
    )
    base_url: str = "https://www.vie-publique.fr"
    crawl_full_text: bool = True


class SenatConfig(BaseModel):
    """Senat source configuration."""

    enabled: bool = True
    xml_url: str = "https://data.senat.fr/data/debats/cri.zip"
    # Alternative PostgreSQL dump URL (not used by default)
    dump_url: str = "https://data.senat.fr/data/debats/debats.zip"


class AssembleeConfig(BaseModel):
    """Assemblee nationale source configuration."""

    enabled: bool = True
    base_url: str = "https://echanges.dila.gouv.fr/OPENDATA/Debats/AN/"
    # 2000-2010 requires OCR/LLM parsing (TBD)
    fallback_stub: bool = True


class EuroparlConfig(BaseModel):
    """European Parliament source configuration."""

    enabled: bool = False  # OFF by default
    portal_url: str = "https://data.europarl.europa.eu"
    filter_country: str = "FR"


class SourcesConfig(BaseModel):
    """All data sources configuration."""

    vie_publique: ViePubliqueConfig = Field(default_factory=ViePubliqueConfig)
    senat: SenatConfig = Field(default_factory=SenatConfig)
    assemblee: AssembleeConfig = Field(default_factory=AssembleeConfig)
    europarl: EuroparlConfig = Field(default_factory=EuroparlConfig)


class ProcessingConfig(BaseModel):
    """Text processing configuration."""

    unicode_normalize: Literal["NFC", "NFD", "NFKC", "NFKD"] = "NFC"
    strip_html: bool = True
    remove_boilerplate: bool = True
    min_text_length: int = Field(default=100, description="Minimum text length to keep")
    dedupe_hash_algorithm: Literal["xxhash64", "sha256"] = "xxhash64"
    dedupe_fields: list[str] = Field(
        default_factory=lambda: ["text"], description="Fields to hash for deduplication"
    )
    exclude_speaker_roles: list[str] = Field(
        default_factory=list, description="Speaker roles to exclude from output"
    )


class ExportConfig(BaseModel):
    """Export configuration."""

    jsonl: bool = True
    parquet: bool = True
    generate_sources_md: bool = True
    manifest_checksums: list[str] = Field(default_factory=lambda: ["sha256"])


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""

    date_range: DateRange = Field(default_factory=DateRange)
    output_dir: Path = Path("data")
    log_level: str = "INFO"
    max_workers: int = Field(default=4, description="Max concurrent workers")
    http: HttpConfig = Field(default_factory=HttpConfig)


class Config(BaseSettings):
    """Root configuration combining all settings."""

    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    export: ExportConfig = Field(default_factory=ExportConfig)

    model_config = {"extra": "ignore"}

    @classmethod
    def from_yaml(cls, path: Path) -> "Config":
        """Load configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data) if data else cls()

    @classmethod
    def default(cls) -> "Config":
        """Return default configuration."""
        return cls()

    def to_yaml(self, path: Path) -> None:
        """Save configuration to a YAML file."""
        data = self.model_dump(mode="json")
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def get_enabled_sources(self) -> list[str]:
        """Return list of enabled source names."""
        sources = []
        if self.sources.vie_publique.enabled:
            sources.append("vie_publique")
        if self.sources.senat.enabled:
            sources.append("senat")
        if self.sources.assemblee.enabled:
            sources.append("assemblee")
        if self.sources.europarl.enabled:
            sources.append("europarl")
        return sources
