"""Parsers for extracting structured records from raw data."""

from .base import BaseParser
from .vie_publique import ViePubliqueParser
from .senat_xml import SenatXMLParser
from .assemblee_xml import AssembleeXMLParser
from .europarl import EuroparlParser

__all__ = [
    "BaseParser",
    "ViePubliqueParser",
    "SenatXMLParser",
    "AssembleeXMLParser",
    "EuroparlParser",
]
