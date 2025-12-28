"""JSONL exporter."""

from pathlib import Path
from typing import Iterator, List

from ..models import SpeechRecord
from ..utils.logging import get_logger


class JSONLExporter:
    """Exports speech records to JSONL format.

    Each line is a complete JSON object representing one speech record.
    This format is efficient for streaming and line-by-line processing.
    """

    def __init__(self):
        self.logger = get_logger()

    def export(
        self,
        records: Iterator[SpeechRecord],
        output_path: Path,
    ) -> int:
        """Export records to a JSONL file.

        Args:
            records: Iterator of speech records
            output_path: Path to output file

        Returns:
            Number of records exported
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        count = 0
        with open(output_path, "w", encoding="utf-8") as f:
            for record in records:
                # Convert to JSON with proper date handling
                json_str = record.model_dump_json(exclude_none=True)
                f.write(json_str + "\n")
                count += 1

                if count % 1000 == 0:
                    self.logger.debug(f"Exported {count} records to JSONL")

        self.logger.info(f"Exported {count} records to {output_path}")
        return count

    def export_list(
        self,
        records: List[SpeechRecord],
        output_path: Path,
    ) -> int:
        """Export a list of records to JSONL.

        Args:
            records: List of speech records
            output_path: Path to output file

        Returns:
            Number of records exported
        """
        return self.export(iter(records), output_path)

    def append(
        self,
        records: Iterator[SpeechRecord],
        output_path: Path,
    ) -> int:
        """Append records to an existing JSONL file.

        Args:
            records: Iterator of speech records
            output_path: Path to output file

        Returns:
            Number of records appended
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        count = 0
        with open(output_path, "a", encoding="utf-8") as f:
            for record in records:
                json_str = record.model_dump_json(exclude_none=True)
                f.write(json_str + "\n")
                count += 1

        self.logger.info(f"Appended {count} records to {output_path}")
        return count


def read_jsonl(path: Path) -> Iterator[SpeechRecord]:
    """Read speech records from a JSONL file.

    Args:
        path: Path to JSONL file

    Yields:
        SpeechRecord objects
    """
    import json

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                data = json.loads(line)
                yield SpeechRecord(**data)
