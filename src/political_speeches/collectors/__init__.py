"""Data collectors for various sources."""

from .base import BaseCollector
from .vie_publique import ViePubliqueCollector
from .senat import SenatCollector
from .assemblee import AssembleeCollector
from .europarl import EuroparlCollector

__all__ = [
    "BaseCollector",
    "ViePubliqueCollector",
    "SenatCollector",
    "AssembleeCollector",
    "EuroparlCollector",
]
