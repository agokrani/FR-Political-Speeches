# French Political Speeches Pipeline

A reproducible data pipeline for collecting, cleaning, and exporting French political speeches and parliamentary interventions (2000-2010).

## Features

- **Multi-source collection**: Vie-publique, Senat, Assemblée nationale, European Parliament
- **Config-driven**: YAML configuration for date ranges, sources, and processing options
- **Robust HTTP handling**: Rate limiting, retries with exponential backoff
- **Text processing**: Unicode normalization, whitespace cleanup, boilerplate removal
- **Deduplication**: Hash-based duplicate detection across sources
- **Multiple export formats**: JSONL and Parquet with checksums
- **Reproducible**: Manifest with config hash and output checksums

## Installation

```bash
# Clone the repository
git clone https://github.com/your-org/political-data-prep.git
cd political-data-prep

# Install with pip (development mode)
pip install -e ".[dev]"
```

## Quick Start

```bash
# Run the full pipeline with default config
political-speeches run

# Run with custom date range
political-speeches run --start-date 2005-01-01 --end-date 2007-12-31

# Run only specific sources
political-speeches run -s vie_publique -s senat

# Collect data from a single source
political-speeches collect-only vie_publique

# Validate a configuration file
political-speeches validate-config config/custom.yaml

# Show available sources
political-speeches info
```

## Configuration

Create a custom configuration file:

```bash
political-speeches init-config config/custom.yaml
```

Edit the configuration:

```yaml
pipeline:
  date_range:
    start: "2000-01-01"
    end: "2010-12-31"
  output_dir: "data"

sources:
  vie_publique:
    enabled: true
  senat:
    enabled: true
  assemblee:
    enabled: true
  europarl:
    enabled: false  # Optional, disabled by default

processing:
  unicode_normalize: "NFC"
  min_text_length: 100

export:
  jsonl: true
  parquet: true
```

## Data Sources

| Source | Description | Coverage |
|--------|-------------|----------|
| **Vie-publique** | Official speeches from DILA | 1974+ |
| **Senat** | Senate session transcripts | 2003+ |
| **Assemblée nationale** | National Assembly debates | 2011+ (structured XML) |
| **Europarl** | European Parliament (French MEPs) | 1999+ (optional) |

**Note**: Assemblée nationale structured data is only available from 2011. For 2000-2010, future versions will implement PDF/HTML + OCR parsing.

## Output

The pipeline generates:

- `data/curated.jsonl` - Line-delimited JSON records
- `data/curated.parquet` - Columnar format for analytics
- `data/manifest.json` - Run metadata with checksums
- `docs/SOURCES.md` - Data source documentation

### Record Schema

```json
{
  "source": "vie_publique",
  "source_id": "123456",
  "source_url": "https://www.vie-publique.fr/discours/123456",
  "date": "2005-03-15",
  "speaker": "Jacques Chirac",
  "title": "Discours du Président de la République",
  "text": "Mesdames et Messieurs...",
  "lang": "fr",
  "license": "Licence Ouverte v2.0",
  "text_hash": "a1b2c3d4e5f6"
}
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy src/

# Linting
ruff check src/
```

## Storage Requirements

Estimated storage for a full run:

| Component | Size |
|-----------|------|
| Vie-publique manifest | ~220 MB |
| Vie-publique HTML pages | ~1-2 GB |
| Senat XML | ~500 MB - 2 GB |
| Curated output | ~1-3 GB |
| **Total** | **~5-15 GB** |

## License

MIT License - See LICENSE file for details.

Data sources are provided under Licence Ouverte (French Open License).
