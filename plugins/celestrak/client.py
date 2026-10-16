"""HTTP client for CelesTrak that respects its usage policy.

CelesTrak answers 403 when the same dataset is requested again before it has been
updated (and may block an IP that keeps doing it), so a 403 is never retried: the
caller gets SourceNotUpdated and skips the run. Transient errors (5xx, timeouts)
are retried with backoff.
"""
import json
import logging
import time

import requests

from celestrak import config

log = logging.getLogger(__name__)


class SourceNotUpdated(Exception):
    """CelesTrak refused the request because the data has not changed (HTTP 403)."""


class BadPayload(Exception):
    """The response is not the expected CSV/JSON dataset."""


def fetch(url: str, fmt: str, attempts: int = 3, timeout: int = 120) -> bytes:
    headers = {"User-Agent": config.USER_AGENT}
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            if attempt == attempts:
                raise
            log.warning("Request failed (%s), retry %d/%d", exc, attempt, attempts)
            time.sleep(10 * attempt)
            continue

        if resp.status_code == 403:
            raise SourceNotUpdated(f"403 from CelesTrak: {resp.text[:200].strip()}")
        if resp.status_code >= 500 and attempt < attempts:
            log.warning("HTTP %s, retry %d/%d", resp.status_code, attempt, attempts)
            time.sleep(10 * attempt)
            continue
        resp.raise_for_status()
        validate(resp.content, fmt)
        return resp.content
    raise RuntimeError("unreachable")


def validate(content: bytes, fmt: str) -> None:
    """CelesTrak returns 200 with a plain-text message for bad queries; reject those."""
    if not content.strip():
        raise BadPayload("Empty response")
    if fmt == "json":
        try:
            data = json.loads(content)
        except ValueError as exc:
            raise BadPayload(f"Not JSON: {content[:200]!r}") from exc
        if not isinstance(data, list) or not data:
            raise BadPayload(f"Expected non-empty JSON array, got: {content[:200]!r}")
    elif fmt == "csv":
        header = content.split(b"\n", 1)[0]
        if b"," not in header:
            raise BadPayload(f"Not CSV: {content[:200]!r}")
    else:
        raise ValueError(f"Unknown format {fmt}")
