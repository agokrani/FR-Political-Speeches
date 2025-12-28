"""HTTP client with rate limiting and retry logic."""

import asyncio
from pathlib import Path
from typing import Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..config import HttpConfig
from .logging import get_logger


class RateLimitedClient:
    """HTTP client with rate limiting and automatic retries."""

    def __init__(self, config: Optional[HttpConfig] = None):
        """Initialize the HTTP client.

        Args:
            config: HTTP configuration. Uses defaults if not provided.
        """
        self.config = config or HttpConfig()
        self.logger = get_logger()
        self._last_request_time: float = 0
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.config.timeout),
                follow_redirects=True,
            )
        return self._client

    async def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        loop = asyncio.get_event_loop()
        now = loop.time()
        elapsed = now - self._last_request_time

        if elapsed < self.config.rate_limit_delay:
            delay = self.config.rate_limit_delay - elapsed
            await asyncio.sleep(delay)

        self._last_request_time = loop.time()

    def _create_retry_decorator(self):
        """Create a retry decorator with current config."""
        return retry(
            stop=stop_after_attempt(self.config.max_retries),
            wait=wait_exponential(
                multiplier=self.config.retry_backoff,
                min=1,
                max=60,
            ),
            retry=retry_if_exception_type(
                (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException)
            ),
            before_sleep=lambda retry_state: self.logger.warning(
                f"Retrying request (attempt {retry_state.attempt_number}): "
                f"{retry_state.outcome.exception() if retry_state.outcome else 'unknown'}"
            ),
        )

    async def get(self, url: str) -> httpx.Response:
        """Perform a GET request with rate limiting and retries.

        Args:
            url: URL to fetch

        Returns:
            HTTP response

        Raises:
            httpx.HTTPStatusError: On HTTP error after retries exhausted
        """
        retry_decorator = self._create_retry_decorator()

        @retry_decorator
        async def _do_get() -> httpx.Response:
            await self._rate_limit()
            client = await self._get_client()
            response = await client.get(url)
            response.raise_for_status()
            return response

        return await _do_get()

    async def get_text(self, url: str) -> str:
        """Fetch URL and return text content.

        Args:
            url: URL to fetch

        Returns:
            Response text
        """
        response = await self.get(url)
        return response.text

    async def get_json(self, url: str) -> dict:
        """Fetch URL and return JSON content.

        Args:
            url: URL to fetch

        Returns:
            Parsed JSON data
        """
        response = await self.get(url)
        return response.json()

    async def download_file(self, url: str, dest: Path) -> Path:
        """Download a file with streaming.

        Args:
            url: URL to download
            dest: Destination file path

        Returns:
            Path to downloaded file
        """
        retry_decorator = self._create_retry_decorator()

        @retry_decorator
        async def _do_download() -> Path:
            await self._rate_limit()
            client = await self._get_client()

            async with client.stream("GET", url) as response:
                response.raise_for_status()

                # Ensure parent directory exists
                dest.parent.mkdir(parents=True, exist_ok=True)

                with open(dest, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        f.write(chunk)

            return dest

        return await _do_download()

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "RateLimitedClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()


# Synchronous wrapper for simple use cases
class SyncClient:
    """Synchronous HTTP client wrapper."""

    def __init__(self, config: Optional[HttpConfig] = None):
        self.config = config or HttpConfig()

    def get(self, url: str) -> httpx.Response:
        """Perform a synchronous GET request."""
        with httpx.Client(
            timeout=self.config.timeout,
            follow_redirects=True,
        ) as client:
            response = client.get(url)
            response.raise_for_status()
            return response

    def download_file(self, url: str, dest: Path) -> Path:
        """Download a file synchronously."""
        dest.parent.mkdir(parents=True, exist_ok=True)

        with httpx.Client(
            timeout=self.config.timeout,
            follow_redirects=True,
        ) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

        return dest
