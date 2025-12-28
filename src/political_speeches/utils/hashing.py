"""Hashing utilities for deduplication and checksums."""

import hashlib
from pathlib import Path
from typing import Literal

import xxhash


def compute_hash(
    text: str,
    algorithm: Literal["xxhash64", "sha256"] = "xxhash64",
) -> str:
    """Compute hash of text content.

    Args:
        text: Text to hash
        algorithm: Hash algorithm to use

    Returns:
        Hex-encoded hash string
    """
    encoded = text.encode("utf-8")

    if algorithm == "xxhash64":
        return xxhash.xxh64(encoded).hexdigest()
    elif algorithm == "sha256":
        return hashlib.sha256(encoded).hexdigest()
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


def compute_file_checksum(filepath: Path, algorithm: str = "sha256") -> str:
    """Compute checksum of a file.

    Args:
        filepath: Path to file
        algorithm: Hash algorithm (sha256, md5, etc.)

    Returns:
        Hex-encoded checksum string
    """
    hasher = hashlib.new(algorithm)

    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()
