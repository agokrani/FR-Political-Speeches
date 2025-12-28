"""Parser for Senat XML debate transcripts."""

import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Optional

from lxml import etree

from ..config import Config
from ..models import SpeechRecord
from ..utils.logging import get_logger
from .base import BaseParser

# Senat CRI namespace
CRI_NS = "http://senat.fr/schemas/thb/cri"
XHTML_NS = "http://www.w3.org/1999/xhtml"
NAMESPACES = {
    "cri": CRI_NS,
    "xhtml": XHTML_NS,
}


class SenatXMLParser(BaseParser):
    """Parser for Senat comptes rendus XML files.

    Parses the XML structure to extract individual interventions
    with speaker, date, and text content.

    The Senat XML format uses:
    - cri: namespace for elements like <cri:intervenant>
    - Attributes: nom (name), qua (qualite/role), civ (civility)
    - <p> elements for text content
    """

    def __init__(self, config: Config):
        super().__init__(config)
        self.date_range = config.pipeline.date_range

    def get_source_name(self) -> str:
        return "senat"

    def parse(self, source_path: Path) -> Iterator[SpeechRecord]:
        """Parse Senat XML files.

        Args:
            source_path: Path to the extracted XML directory

        Yields:
            SpeechRecord objects
        """
        if not source_path.exists():
            self.logger.error(f"Source path does not exist: {source_path}")
            return

        # Find all XML files
        if source_path.is_file():
            xml_files = [source_path]
        else:
            xml_files = list(source_path.rglob("*.xml"))

        self.logger.info(f"Parsing {len(xml_files)} Senat XML files")

        parsed_count = 0
        for xml_file in xml_files:
            try:
                for record in self._parse_xml_file(xml_file):
                    parsed_count += 1
                    yield record
            except Exception as e:
                self.logger.warning(f"Failed to parse {xml_file}: {e}")

        self.logger.info(f"Parsed {parsed_count} interventions from Senat XML files")

    def _parse_xml_file(self, xml_path: Path) -> Iterator[SpeechRecord]:
        """Parse a single XML file.

        Args:
            xml_path: Path to XML file

        Yields:
            SpeechRecord objects
        """
        # Use lenient parser to handle malformed XML
        parser = etree.XMLParser(recover=True, encoding="utf-8")

        try:
            tree = etree.parse(str(xml_path), parser)
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            self.logger.warning(f"XML syntax error in {xml_path}: {e}")
            return
        except Exception as e:
            self.logger.warning(f"Failed to parse {xml_path}: {e}")
            return

        # Extract session date from filename (format: d20050127.xml)
        session_date = self._extract_date_from_filename(xml_path)
        if not session_date:
            session_date = self._extract_session_date(xml_path, root)

        if session_date:
            if not (self.date_range.start <= session_date <= self.date_range.end):
                return  # Skip files outside date range
        else:
            # Cannot determine date, skip file
            self.logger.debug(f"Could not determine date for {xml_path}")
            return

        # Primary structure: <cri:intervenant> elements (Senat CRI format)
        # Try with namespace first
        found_intervenants = False
        for intervenant in root.iter(f"{{{CRI_NS}}}intervenant"):
            found_intervenants = True
            record = self._parse_cri_intervenant(intervenant, session_date, xml_path)
            if record:
                yield record

        # If no namespaced elements found, try local name matching
        if not found_intervenants:
            for elem in root.iter():
                local_name = self._get_local_name(elem)
                if local_name == "intervenant":
                    found_intervenants = True
                    record = self._parse_cri_intervenant(elem, session_date, xml_path)
                    if record:
                        yield record

        # Fallback: Try legacy structures
        if not found_intervenants:
            for intervention in root.iter("intervention"):
                record = self._parse_intervention(intervention, session_date, xml_path)
                if record:
                    yield record

    def _get_local_name(self, elem: etree._Element) -> Optional[str]:
        """Get the local name of an element, handling both namespaced and prefixed tags.

        Args:
            elem: XML element

        Returns:
            Local name or None
        """
        tag = elem.tag
        if not isinstance(tag, str):
            return None

        # Handle namespaced tag: {http://...}localname
        if tag.startswith("{"):
            try:
                return etree.QName(tag).localname
            except ValueError:
                return None

        # Handle prefixed tag: prefix:localname
        if ":" in tag:
            return tag.split(":", 1)[1]

        # Plain tag name
        return tag

    def _extract_date_from_filename(self, xml_path: Path) -> Optional[date]:
        """Extract date from Senat filename format (d20050127.xml).

        Args:
            xml_path: Path to XML file

        Returns:
            Date or None
        """
        filename = xml_path.stem
        # Match format like d20050127 or D20050127
        match = re.match(r"[dD](\d{4})(\d{2})(\d{2})", filename)
        if match:
            try:
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                pass
        return None

    def _parse_cri_intervenant(
        self, elem: etree._Element, session_date: date, xml_path: Path
    ) -> Optional[SpeechRecord]:
        """Parse a <cri:intervenant> element.

        Args:
            elem: XML element
            session_date: Session date
            xml_path: Source file path

        Returns:
            SpeechRecord or None
        """
        # Extract speaker name from 'nom' attribute
        speaker = elem.get("nom", "").strip()
        if not speaker:
            return None

        # Extract speaker role from 'qua' attribute (qualite)
        speaker_role = elem.get("qua", "").strip() or None

        # Extract ID
        source_id = elem.get("id", "")
        if not source_id:
            source_id = self._build_source_id(elem, xml_path, session_date)

        # Extract text from <p> elements within the intervenant
        text_parts = []
        for p_elem in elem.iter():
            local_name = self._get_local_name(p_elem)
            if local_name == "p":
                # Get all text content from this paragraph
                p_text = self._get_element_text(p_elem)
                if p_text:
                    text_parts.append(p_text)

        text = " ".join(text_parts).strip()

        # Skip if text is too short
        if len(text) < self.config.processing.min_text_length:
            return None

        # Clean up speaker name (remove trailing periods, normalize case)
        speaker = self._clean_speaker_name(speaker)

        return SpeechRecord(
            source="senat",
            source_id=source_id,
            source_url=f"https://www.senat.fr/seances/{xml_path.stem}",
            date=session_date,
            speaker=speaker,
            title=f"Intervention - Sénat - {session_date}",
            text=text,
            lang="fr",
            license="Licence Ouverte",
            speaker_role=speaker_role,
            speech_type="intervention",
            session_id=xml_path.stem,
        )

    def _get_element_text(self, elem: etree._Element) -> str:
        """Extract all text content from an element, including nested elements.

        Args:
            elem: XML element

        Returns:
            Combined text content
        """
        texts = []
        if elem.text:
            texts.append(elem.text.strip())
        for child in elem:
            child_text = self._get_element_text(child)
            if child_text:
                texts.append(child_text)
            if child.tail:
                texts.append(child.tail.strip())
        return " ".join(filter(None, texts))

    def _clean_speaker_name(self, name: str) -> str:
        """Clean up speaker name.

        Args:
            name: Raw speaker name

        Returns:
            Cleaned name
        """
        # Remove trailing periods and whitespace
        name = name.rstrip(". ").strip()
        # Normalize case (Title Case for names)
        # But preserve all-caps for some French names
        if name.isupper():
            # Convert "JEAN-PAUL EMORINE" to "Jean-Paul Emorine"
            parts = name.split()
            name = " ".join(part.title() if "-" not in part else "-".join(p.title() for p in part.split("-")) for part in parts)
        return name

    def _extract_session_date(
        self, xml_path: Path, root: etree._Element
    ) -> Optional[date]:
        """Extract session date from XML or filename.

        Args:
            xml_path: Path to XML file
            root: XML root element

        Returns:
            Date or None
        """
        # Try XML elements
        for tag in ["date", "dateSeance", "date_seance", "jour"]:
            elem = root.find(f".//{tag}")
            if elem is not None and elem.text:
                parsed = self._parse_date_string(elem.text)
                if parsed:
                    return parsed

        # Try date attribute
        for attr in ["date", "dateSeance"]:
            if attr in root.attrib:
                parsed = self._parse_date_string(root.attrib[attr])
                if parsed:
                    return parsed

        # Try filename pattern (e.g., seance_20050315.xml)
        filename = xml_path.stem
        date_match = re.search(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})", filename)
        if date_match:
            try:
                return date(
                    int(date_match.group(1)),
                    int(date_match.group(2)),
                    int(date_match.group(3)),
                )
            except ValueError:
                pass

        return None

    def _parse_date_string(self, date_str: str) -> Optional[date]:
        """Parse a date string in various formats."""
        if not date_str:
            return None

        date_str = date_str.strip()

        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y%m%d",
            "%d %B %Y",  # French format
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str[:10], fmt).date()
            except ValueError:
                continue

        return None

    def _parse_intervention(
        self, elem: etree._Element, session_date: Optional[date], xml_path: Path
    ) -> Optional[SpeechRecord]:
        """Parse an <intervention> element.

        Args:
            elem: XML element
            session_date: Session date
            xml_path: Source file path

        Returns:
            SpeechRecord or None
        """
        # Extract speaker
        speaker = self._extract_speaker(elem)
        if not speaker:
            return None

        # Extract text
        text = self._extract_text(elem)
        if not text or len(text) < self.config.processing.min_text_length:
            return None

        # Build ID
        source_id = self._build_source_id(elem, xml_path, session_date)

        # Get date (from intervention or session)
        intervention_date = session_date
        date_elem = elem.find(".//date")
        if date_elem is not None and date_elem.text:
            parsed = self._parse_date_string(date_elem.text)
            if parsed:
                intervention_date = parsed

        if not intervention_date:
            return None

        # Speaker role
        speaker_role = None
        for tag in ["qualite", "fonction", "titre"]:
            role_elem = elem.find(f".//{tag}")
            if role_elem is not None and role_elem.text:
                speaker_role = role_elem.text.strip()
                break

        return SpeechRecord(
            source="senat",
            source_id=source_id,
            source_url=f"https://www.senat.fr/seances/{xml_path.stem}",
            date=intervention_date,
            speaker=speaker,
            title=f"Intervention - Sénat - {intervention_date}",
            text=text,
            lang="fr",
            license="Licence Ouverte",
            speaker_role=speaker_role,
            speech_type="intervention",
            session_id=xml_path.stem,
        )

    def _parse_paragraphe(
        self, elem: etree._Element, session_date: Optional[date], xml_path: Path
    ) -> Optional[SpeechRecord]:
        """Parse a <paragraphe> element."""
        # Similar to intervention parsing
        speaker = self._extract_speaker(elem)
        text = self._extract_text(elem)

        if not speaker or not text:
            return None
        if len(text) < self.config.processing.min_text_length:
            return None

        if not session_date:
            return None

        source_id = self._build_source_id(elem, xml_path, session_date)

        return SpeechRecord(
            source="senat",
            source_id=source_id,
            date=session_date,
            speaker=speaker,
            title=f"Intervention - Sénat - {session_date}",
            text=text,
            lang="fr",
            license="Licence Ouverte",
            speech_type="intervention",
            session_id=xml_path.stem,
        )

    def _parse_orateur_block(
        self, elem: etree._Element, session_date: Optional[date], xml_path: Path
    ) -> Optional[SpeechRecord]:
        """Parse an <orateur> block with following text."""
        speaker_elem = elem.find(".//nom")
        if speaker_elem is None or not speaker_elem.text:
            return None

        speaker = speaker_elem.text.strip()

        # Get following text content
        text_parts = []
        for sibling in elem.itersiblings():
            if sibling.tag == "orateur":
                break  # Next speaker
            if sibling.text:
                text_parts.append(sibling.text.strip())

        text = " ".join(text_parts)
        if len(text) < self.config.processing.min_text_length:
            return None

        if not session_date:
            return None

        source_id = self._build_source_id(elem, xml_path, session_date)

        return SpeechRecord(
            source="senat",
            source_id=source_id,
            date=session_date,
            speaker=speaker,
            title=f"Intervention - Sénat - {session_date}",
            text=text,
            lang="fr",
            license="Licence Ouverte",
            speech_type="intervention",
            session_id=xml_path.stem,
        )

    def _extract_speaker(self, elem: etree._Element) -> Optional[str]:
        """Extract speaker name from element."""
        for tag in ["orateur", "intervenant", "auteur", "nom"]:
            speaker_elem = elem.find(f".//{tag}")
            if speaker_elem is not None:
                # Check for nested nom element
                nom = speaker_elem.find(".//nom")
                if nom is not None and nom.text:
                    return nom.text.strip()
                if speaker_elem.text:
                    return speaker_elem.text.strip()

        # Check attribute
        if "orateur" in elem.attrib:
            return elem.attrib["orateur"]

        return None

    def _extract_text(self, elem: etree._Element) -> str:
        """Extract text content from element."""
        # Get all text content, joining paragraphs
        texts = []

        for text_elem in elem.iter():
            if text_elem.text:
                texts.append(text_elem.text.strip())
            if text_elem.tail:
                texts.append(text_elem.tail.strip())

        return " ".join(filter(None, texts))

    def _build_source_id(
        self, elem: etree._Element, xml_path: Path, session_date: Optional[date]
    ) -> str:
        """Build a unique source ID."""
        # Use existing ID if available
        for attr in ["id", "identifiant", "uid"]:
            if attr in elem.attrib:
                return elem.attrib[attr]

        # Generate from filename and position
        base = xml_path.stem
        # Use sourceline if available (lxml feature)
        line = getattr(elem, "sourceline", None)
        if line:
            return f"{base}_L{line}"

        # Fallback to date-based ID
        if session_date:
            return f"{base}_{session_date.isoformat()}"

        return f"{base}_unknown"
