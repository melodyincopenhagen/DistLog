"""Minimal Python client for the DistLog HTTP API.

Stdlib-only (urllib) so this example runs against any Python 3.8+
without pip install. The client mirrors the server's contract from
docs/DECISIONS.md ADR-009 (backpressure -> 429 + Retry-After) and
ADR-010 (token auth).

Design choices worth knowing:

- One Client object holds the base URL, optional bearer token, and a
  retry policy. Stateless beyond that.
- write() and query() raise typed exceptions for known failure modes
  (BackpressureError, UnauthorizedError, ServerError, ParseError).
  Callers that want fail-fast just don't retry; callers that want
  patience set max_retries > 0.
- Backpressure retry uses Retry-After from the server when present
  (capped at 30s), with exponential backoff fallback. This is the
  client-side half of the engine's stall protocol from Stage E.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional


class DistLogError(Exception):
    """Base for all client-raised errors."""


class UnauthorizedError(DistLogError):
    """401 from the server. Token missing, malformed, or unknown."""


class ParseError(DistLogError):
    """400 from the server. Usually a bad SQL string or invalid body."""


class BackpressureError(DistLogError):
    """429 from the server after retries are exhausted.

    The engine is healthy and rejecting writes only because flush
    cannot keep up. Try again with backoff or reduce write rate.
    """


class ServerError(DistLogError):
    """5xx from the server, or a transport error after retries."""


@dataclass
class RetryPolicy:
    max_retries: int = 5
    initial_backoff_s: float = 0.1
    max_backoff_s: float = 30.0


@dataclass
class QueryRow:
    doc_id: int
    values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    columns: List[str]
    rows: List[QueryRow]


class Client:
    """HTTP client for one DistLog instance.

    Thread-safety: the urllib backend is thread-safe, but the Client
    holds no per-request mutable state, so it's safe to share across
    threads.
    """

    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        retry: RetryPolicy = RetryPolicy(),
        timeout_s: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.retry = retry
        self.timeout_s = timeout_s

    # === Public API ===

    def write(
        self,
        message: str,
        *,
        timestamp: Optional[str] = None,
        source: Optional[str] = None,
        fields: Optional[Mapping[str, str]] = None,
    ) -> int:
        """Ingest one record. Returns the assigned doc_id.

        timestamp accepts an RFC3339 string; omit to let the server
        default to now. tenant_id is determined by the auth token on
        the server; do not pass it from here.
        """
        body: Dict[str, Any] = {"message": message}
        if timestamp is not None:
            body["ts"] = timestamp
        if source is not None:
            body["source"] = source
        if fields:
            body["fields"] = dict(fields)
        resp = self._request_with_retry("POST", "/api/ingest", body)
        return int(resp["doc_id"])

    def write_batch(self, records: Iterable[Mapping[str, Any]]) -> List[int]:
        """Convenience: ingest many records sequentially.

        Returns the doc_ids in input order. The server has no batch
        endpoint; this is just a loop with shared retry state per
        record. Stops on the first non-retryable error.
        """
        out: List[int] = []
        for r in records:
            out.append(
                self.write(
                    r["message"],
                    timestamp=r.get("ts"),
                    source=r.get("source"),
                    fields=r.get("fields"),
                )
            )
        return out

    def get(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single record by doc_id. Returns None on 404."""
        try:
            return self._request_with_retry("GET", f"/api/logs/{doc_id}", None)
        except DistLogError as e:
            if "not found" in str(e).lower():
                return None
            raise

    def query(self, sql: str) -> QueryResult:
        """Run a SQL query, return a typed QueryResult."""
        resp = self._request_with_retry("POST", "/api/query", {"sql": sql})
        return QueryResult(
            columns=list(resp.get("columns", [])),
            rows=[
                QueryRow(doc_id=int(r["doc_id"]), values=dict(r.get("values", {})))
                for r in resp.get("rows", [])
            ],
        )

    def healthz(self) -> Dict[str, Any]:
        """Engine status. Not auth-gated server-side."""
        return self._request_with_retry("GET", "/api/healthz", None, _allow_404=False)

    # === Internals ===

    def _request_with_retry(
        self,
        method: str,
        path: str,
        body: Optional[Any],
        *,
        _allow_404: bool = True,
    ) -> Dict[str, Any]:
        backoff = self.retry.initial_backoff_s
        attempts = self.retry.max_retries + 1
        last_err: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return self._do_request(method, path, body, allow_404=_allow_404)
            except BackpressureError as e:
                last_err = e
                if attempt == attempts - 1:
                    raise
                wait = self._extract_retry_after(e) or backoff
                wait = min(wait, self.retry.max_backoff_s)
                time.sleep(wait)
                backoff = min(backoff * 2, self.retry.max_backoff_s)
            except ServerError as e:
                # 5xx is also retried, but only with exponential backoff —
                # the server didn't tell us how long.
                last_err = e
                if attempt == attempts - 1:
                    raise
                time.sleep(min(backoff, self.retry.max_backoff_s))
                backoff = min(backoff * 2, self.retry.max_backoff_s)
        assert last_err is not None
        raise last_err  # type: ignore[misc]

    def _do_request(
        self,
        method: str,
        path: str,
        body: Optional[Any],
        *,
        allow_404: bool,
    ) -> Dict[str, Any]:
        url = self.base_url + path
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                return _decode_json(resp.read())
        except urllib.error.HTTPError as e:
            # The exception carries status and headers; surface
            # typed errors. We attach the Retry-After header onto
            # BackpressureError so the retry loop can use it.
            body_bytes = e.read() if hasattr(e, "read") else b""
            msg = _extract_error_message(body_bytes) or e.reason
            if e.code == 401:
                raise UnauthorizedError(msg) from None
            if e.code == 400:
                raise ParseError(msg) from None
            if e.code == 404:
                if allow_404:
                    raise DistLogError(f"not found: {msg}") from None
                raise ServerError(f"404 unexpected: {msg}") from None
            if e.code == 429:
                err = BackpressureError(msg)
                err.retry_after = e.headers.get("Retry-After")  # type: ignore[attr-defined]
                raise err from None
            if 500 <= e.code < 600:
                raise ServerError(f"server {e.code}: {msg}") from None
            raise ServerError(f"unexpected status {e.code}: {msg}") from None
        except urllib.error.URLError as e:
            raise ServerError(f"transport error: {e.reason}") from None

    @staticmethod
    def _extract_retry_after(err: BackpressureError) -> Optional[float]:
        v = getattr(err, "retry_after", None)
        if not v:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None


def _decode_json(b: bytes) -> Dict[str, Any]:
    if not b:
        return {}
    try:
        out = json.loads(b)
    except json.JSONDecodeError as e:
        raise ServerError(f"server returned non-JSON: {e}") from None
    if not isinstance(out, dict):
        raise ServerError("server returned non-object JSON")
    return out


def _extract_error_message(b: bytes) -> Optional[str]:
    if not b:
        return None
    try:
        obj = json.loads(b)
    except json.JSONDecodeError:
        return b.decode("utf-8", errors="replace").strip()
    if isinstance(obj, dict) and isinstance(obj.get("error"), str):
        return obj["error"]
    return None
