"""Parser for Assemblee nationale XML debates.

NOTE: This is a skeleton implementation for DILA XML archives (2011+).
For 2000-2010, no structured data is available.
"""

from datetime import date
from pathlib import Path
from typing import Iterator, Optional

from lxml import etree

from ..config import Config
from ..models import SpeechRecord
from ..utils.logging import get_logger
from .base import BaseParser


class AssembleeXMLParser(BaseParser):
    """Parser for Assemblee nationale DILA XML files.

    This is a skeleton implementation. Full parsing of DILA XML
    format would be implemented here once data is collected.
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self.date_range = config.pipeline.date_range

    def get_source_name(self) -> str:
        return "assemblee"

    def parse(self, source_path: Path) -> Iterator[SpeechRecord]:
        """Parse Assemblee nationale XML files.

        Args:
            source_path: Path to the XML data directory

        Yields:
            SpeechRecord objects
        """
        if not source_path.exists():
            self.logger.warning(f"Source path does not exist: {source_path}")
            return

        # Check for stub notice
        notice_path = source_path / "DATA_GAP_NOTICE.md"
        if notice_path.exists():
            self.logger.info(
                "Assemblee data gap notice found. "
                "No structured data available for 2000-2010."
            )
            return

        # Find XML files
        if source_path.is_file():
            xml_files = [source_path]
        else:
            xml_files = list(source_path.rglob("*.xml"))

        if not xml_files:
            self.logger.info("No Assemblee XML files found")
            return

        self.logger.info(f"Parsing {len(xml_files)} Assemblee XML files")

        for xml_file in xml_files:
            try:
                yield from self._parse_xml_file(xml_file)
            except Exception as e:
                self.logger.warning(f"Failed to parse {xml_file}: {e}")

    def _parse_xml_file(self, xml_path: Path) -> Iterator[SpeechRecord]:
        """Parse a single DILA XML file.

        The DILA XML format for AN debates uses a specific schema.
        This is a skeleton implementation based on expected structure.

        Args:
            xml_path: Path to XML file

        Yields:
            SpeechRecord objects
        """
        try:
            tree = etree.parse(str(xml_path))
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"XML syntax error in {xml_path}: {e}")
            return

        # DILA XML structure (expected):
        # <debatsAssembleeNationale>
        #   <metadonnees>
        #     <dateSeance>2011-01-15</dateSeance>
        #   </metadonnees>
        #   <contenu>
        #     <paragraphe>
        #       <orateur>...</orateur>
        #       <texte>...</texte>
        #     </paragraphe>
        #   </contenu>
        # </debatsAssembleeNationale>

        # Extract session date
        session_date = self._extract_session_date(root)
        if session_date:
            if not (self.date_range.start <= session_date <= self.date_range.end):
                return

        # Parse interventions (skeleton)
        for intervention in root.iter("paragraphe"):
            record = self._parse_intervention(intervention, session_date, xml_path)
            if record:
                yield record

    def _extract_session_date(self, root: etree._Element) -> Optional[date]:
        """Extract session date from XML."""
        for tag in ["dateSeance", "date", "jour"]:
            elem = root.find(f".//{tag}")
            if elem is not None and elem.text:
                try:
                    from datetime import datetime
                    return datetime.strptime(elem.text.strip()[:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
        return None

    def _parse_intervention(
        self, elem: etree._Element, session_date: Optional[date], xml_path: Path
    ) -> Optional[SpeechRecord]:
        """Parse an intervention element."""
        if not session_date:
            return None

        # Extract speaker
        orateur = elem.find(".//orateur")
        if orateur is None:
            return None

        nom = orateur.find(".//nom")
        speaker = nom.text.strip() if nom is not None and nom.text else None
        if not speaker:
            return None

        # Extract text
        texte = elem.find(".//texte")
        text = ""
        if texte is not None:
            text = " ".join(texte.itertext()).strip()

        if len(text) < self.config.processing.min_text_length:
            return None

        # Build source ID
        line = getattr(elem, "sourceline", 0)
        source_id = f"an_{session_date.isoformat()}_{xml_path.stem}_L{line}"

        # Speaker role
        qualite = orateur.find(".//qualite")
        speaker_role = qualite.text.strip() if qualite is not None and qualite.text else None

        return SpeechRecord(
            source="assemblee",
            source_id=source_id,
            source_url=f"https://www.assemblee-nationale.fr/dyn/debats/{xml_path.stem}",
            date=session_date,
            speaker=speaker,
            title=f"Intervention - Assemblée nationale - {session_date}",
            text=text,
            lang="fr",
            license="Licence Ouverte",
            speaker_role=speaker_role,
            speech_type="intervention",
            session_id=xml_path.stem,
        )
