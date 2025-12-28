"""Data models for the political speeches pipeline."""

import datetime as dt
from typing import Literal, Optional

from pydantic import BaseModel, Field


class SpeechRecord(BaseModel):
    """Unified speech record across all sources."""

    # Required fields
    source: Literal["vie_publique", "senat", "assemblee", "europarl"]
    source_id: str = Field(..., description="Unique identifier within source")
    date: dt.date = Field(..., description="Speech date")
    speaker: str = Field(..., description="Speaker name")
    title: str = Field(..., description="Speech title")
    text: str = Field(..., description="Full text content")

    # Optional URL
    source_url: Optional[str] = Field(None, description="Original URL")

    # Metadata with defaults
    lang: str = Field(default="fr", description="Language code")
    retrieved_at: dt.datetime = Field(
        default_factory=dt.datetime.utcnow, description="When collected"
    )
    license: str = Field(default="Open License", description="Source license")

    # Optional fields
    speaker_role: Optional[str] = Field(None, description="Speaker's role/title")
    speech_type: Optional[str] = Field(
        None, description="Type: discours, intervention, declaration, etc."
    )
    session_id: Optional[str] = Field(None, description="Session identifier if applicable")

    # Processing metadata (added during pipeline)
    text_hash: Optional[str] = Field(None, description="Hash of cleaned text for deduplication")
    cleaned_at: Optional[dt.datetime] = Field(None, description="When text was cleaned")

    model_config = {"extra": "ignore"}


class SourceStats(BaseModel):
    """Statistics for a single source."""

    collected: int = 0
    parsed: int = 0
    cleaned: int = 0
    deduplicated: int = 0
    errors: int = 0


class ManifestRecord(BaseModel):
    """Pipeline run manifest for reproducibility."""

    run_id: str = Field(..., description="Unique run identifier")
    run_timestamp: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    config_hash: str = Field(..., description="Hash of config for reproducibility")

    # Date range processed
    date_range_start: dt.date
    date_range_end: dt.date

    # Statistics
    sources_processed: dict[str, SourceStats] = Field(
        default_factory=dict, description="Per-source statistics"
    )
    total_records: int = 0
    deduplicated_records: int = 0

    # Output files with checksums
    output_files: dict[str, str] = Field(
        default_factory=dict, description="filename -> SHA256 checksum"
    )

    # Errors encountered
    errors: list[str] = Field(default_factory=list)

    # Pipeline version
    pipeline_version: str = "0.1.0"

    model_config = {"extra": "ignore"}
