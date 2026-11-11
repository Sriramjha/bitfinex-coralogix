#!/usr/bin/env python3
"""
Bitfinex account / exchange activity → Coralogix Logs API.

Polls Bitfinex API v2 authenticated history for the last N minutes and ships
events to:

  POST https://ingress.<domain>/logs/v1/singles

Default sources (Read permission only):
  POST /v2/auth/r/movements/hist   deposits (amount > 0) and withdrawals (amount < 0)
  POST /v2/auth/r/ledgers/hist     wallet transfers (category 51)

Optional:
  POST /v2/auth/r/trades/hist
  POST /v2/auth/r/orders/hist      closed/cancelled orders, about 2 weeks

Usage:
  cp .env.example .env
  pip install -r requirements.txt
  python ship_bitfinex_to_coralogix.py --once --lookback-minutes 5   # cron
  python ship_bitfinex_to_coralogix.py                               # loop

API key needs Read permission only. Do not enable trading or withdrawals.
Give this key to one client. Bitfinex nonces are per key and must increase.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv


LOGGER = logging.getLogger("bitfinex-coralogix")

SEEN_ID_LIMIT = 4000
OVERLAP_SECONDS = 2
MAX_PAGES = 30

MOVEMENT_LIMIT = 1000
LEDGER_LIMIT = 2500
TRADE_LIMIT = 1000
ORDER_LIMIT = 2500

DEFAULT_SOURCES = ["deposit", "withdraw", "transfer"]
OPTIONAL_SOURCES = {"trade", "order"}
KNOWN_SOURCES = set(DEFAULT_SOURCES) | OPTIONAL_SOURCES
DEFAULT_TRANSFER_CATEGORIES = ["51"]

# Positional fields from the Bitfinex v2 docs. Null placeholders are omitted.
MOVEMENT_FIELDS = (
    (0, "id"),
    (1, "currency"),
    (2, "currencyName"),
    (5, "mtsStarted"),
    (6, "mtsUpdated"),
    (9, "status"),
    (12, "amount"),
    (13, "fees"),
    (16, "destinationAddress"),
    (17, "paymentId"),
    (20, "transactionId"),
    (21, "withdrawNote"),
)
LEDGER_FIELDS = (
    (0, "id"),
    (1, "currency"),
    (2, "wallet"),
    (3, "mts"),
    (5, "amount"),
    (6, "balance"),
    (8, "description"),
)
TRADE_FIELDS = (
    (0, "id"),
    (1, "symbol"),
    (2, "mts"),
    (3, "orderId"),
    (4, "execAmount"),
    (5, "execPrice"),
    (6, "orderType"),
    (7, "orderPrice"),
    (8, "maker"),
    (9, "fee"),
    (10, "feeCurrency"),
    (11, "cid"),
)
ORDER_FIELDS = (
    (0, "id"),
    (1, "gid"),
    (2, "cid"),
    (3, "symbol"),
    (4, "mtsCreate"),
    (5, "mtsUpdate"),
    (6, "amount"),
    (7, "amountOrig"),
    (8, "orderType"),
    (9, "typePrev"),
    (10, "mtsTif"),
    (12, "flags"),
    (13, "status"),
    (16, "price"),
    (17, "priceAvg"),
    (18, "priceTrailing"),
    (19, "priceAuxLimit"),
    (23, "notify"),
    (24, "hidden"),
    (25, "placedId"),
    (28, "routing"),
    (31, "meta"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_ms() -> int:
    return int(utc_now().timestamp() * 1000)


def parse_ts_to_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        if value < 10_000_000_000:
            return int(value * 1000)
        if value > 10_000_000_000_000:
            return int(value / 1_000_000)
        return int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return parse_ts_to_ms(int(text))
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def env_csv(name: str, default: Optional[List[str]] = None) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return list(default or [])
    return [part.strip() for part in raw.split(",") if part.strip()]


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def amount_sign(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0
    if number > 0:
        return 1
    if number < 0:
        return -1
    return 0


def row_value(row: Sequence[Any], index: int) -> Any:
    if index < 0 or index >= len(row):
        return None
    return row[index]


def map_row(row: Sequence[Any], fields: Sequence[Tuple[int, str]]) -> Dict[str, Any]:
    mapped: Dict[str, Any] = {}
    used = set()
    for index, name in fields:
        used.add(index)
        value = row_value(row, index)
        if value is not None:
            mapped[name] = value
    extra = []
    for index, value in enumerate(row):
        if index in used or value is None:
            continue
        extra.append(value)
    if extra:
        mapped["unmapped"] = extra
    return mapped


def map_movement(row: Sequence[Any]) -> Dict[str, Any]:
    event = map_row(row, MOVEMENT_FIELDS)
    sign = amount_sign(event.get("amount"))
    if sign > 0:
        event["direction"] = "in"
    elif sign < 0:
        event["direction"] = "out"
    return event


def map_ledger(row: Sequence[Any], category: int) -> Dict[str, Any]:
    event = map_row(row, LEDGER_FIELDS)
    event["category"] = category
    sign = amount_sign(event.get("amount"))
    if sign > 0:
        event["direction"] = "in"
    elif sign < 0:
        event["direction"] = "out"
    return event


def map_trade(row: Sequence[Any]) -> Dict[str, Any]:
    event = map_row(row, TRADE_FIELDS)
    event["side"] = "sell" if amount_sign(event.get("execAmount")) < 0 else "buy"
    return event


def map_order(row: Sequence[Any]) -> Dict[str, Any]:
    event = map_row(row, ORDER_FIELDS)
    sign = amount_sign(event.get("amount"))
    if sign == 0:
        sign = amount_sign(event.get("amountOrig"))
    if sign > 0:
        event["side"] = "buy"
    elif sign < 0:
        event["side"] = "sell"
    return event


def normalize_symbol(symbol: str) -> str:
    text = symbol.strip()
    if not text or any(char in text for char in "/?#& "):
        raise SystemExit(f"Invalid BITFINEX_SYMBOLS entry: {symbol!r}")
    if not text.startswith(("t", "f")):
        LOGGER.info("Prefixing Bitfinex trading symbol %s -> t%s", text, text)
        text = "t" + text
    return text


def event_timestamp_ms(event: Dict[str, Any]) -> Optional[int]:
    for key in ("mtsUpdated", "mtsUpdate", "mts", "mtsCreate", "mtsStarted", "timestamp", "time"):
        parsed = parse_ts_to_ms(event.get(key))
        if parsed is not None:
            return parsed
    return None


def event_id_for(source: str, event: Dict[str, Any]) -> str:
    if event.get("cx_event_id"):
        return str(event["cx_event_id"])
    record_id = event.get("id")
    if record_id is None or record_id == "":
        return ""
    if source in {"deposit", "withdraw"}:
        return f"{record_id}:{event.get('status')}:{event.get('mtsUpdated')}"
    if source == "order":
        return f"{record_id}:{event.get('status')}:{event.get('mtsUpdate')}"
    return str(record_id)


def normalize_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    for prefix in ("bitfinex_", "bitfinex-"):
        if text.startswith(prefix):
            return text[len(prefix) :]
    return text


def map_severity(source: str, event: Dict[str, Any]) -> int:
    kind = normalize_source(source) or normalize_source(event.get("event_source"))
    status = str(event.get("status") or "").lower()
    if any(token in status for token in ("error", "fail")):
        return 5
    if status.startswith("executed") or status.startswith("completed") or "fill" in status:
        return 3
    if "cancel" in status or "pending" in status:
        return 4
    if kind == "withdraw":
        return 4
    return 3


def in_window(event: Dict[str, Any], start_ms: int, end_ms: int) -> bool:
    timestamp = event_timestamp_ms(event)
    if timestamp is None:
        return True
    return start_ms <= timestamp <= end_ms


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                self.data.update(loaded)
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("Could not load state file %s: %s", self.path, exc)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self.path)

    def _bucket(self, key: str) -> Dict[str, Any]:
        bucket = self.data.get(key)
        if not isinstance(bucket, dict):
            bucket = {"seen_ids": [], "last_time_ms": None}
            self.data[key] = bucket
        bucket.setdefault("seen_ids", [])
        bucket.setdefault("last_time_ms", None)
        return bucket

    def get_seen_ids(self, key: str) -> Set[str]:
        return {str(item) for item in (self._bucket(key).get("seen_ids") or [])}

    def get_last_time_ms(self, key: str) -> Optional[int]:
        value = self._bucket(key).get("last_time_ms")
        return int(value) if value is not None else None

    def update(self, key: str, event_ids: Iterable[str], last_time_ms: Optional[int]) -> None:
        bucket = self._bucket(key)
        merged = list(dict.fromkeys(list(event_ids) + list(bucket.get("seen_ids") or [])))
        bucket["seen_ids"] = merged[:SEEN_ID_LIMIT]
        if last_time_ms is not None:
            prev = bucket.get("last_time_ms")
            bucket["last_time_ms"] = (
                max(int(prev), last_time_ms) if prev is not None else last_time_ms
            )


class HistPage:
    def __init__(self, rows: List[Dict[str, Any]], complete: bool) -> None:
        self.rows = rows
        self.complete = complete


class BitfinexClient:
    """Bitfinex API v2 authenticated REST client.

    Signature is HMAC-SHA384 over ``/api/{path}{nonce}{body}``.
    Docs: https://docs.bitfinex.com/docs/rest-auth
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        *,
        timeout: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self._last_nonce = 0

    def _nonce(self) -> str:
        # Microseconds stay under JS MAX_SAFE_INTEGER and match the official examples.
        candidate = time.time_ns() // 1000
        if candidate <= self._last_nonce:
            candidate = self._last_nonce + 1
        self._last_nonce = candidate
        return str(candidate)

    def _sign(self, path: str, nonce: str, body: str) -> str:
        message = f"/api/{path}{nonce}{body}"
        return hmac.new(self.api_secret, message.encode("utf-8"), hashlib.sha384).hexdigest()

    def _post(self, path: str, payload: Dict[str, Any], *, max_retries: int = 6) -> Any:
        body = json.dumps(payload, separators=(",", ":"))
        url = f"{self.base_url}/{path}"
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            nonce = self._nonce()
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "bfx-apikey": self.api_key,
                "bfx-nonce": nonce,
                "bfx-signature": self._sign(path, nonce, body),
            }
            try:
                resp = self.session.post(url, headers=headers, data=body, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                sleep_for = min(2**attempt, 30)
                LOGGER.warning("Request error (%s); retrying in %ss", exc, sleep_for)
                time.sleep(sleep_for)
                continue

            try:
                parsed = resp.json() if resp.content else []
            except ValueError:
                parsed = {"raw": resp.text[:500]}

            error = self._error_parts(parsed)
            if error is None and resp.ok:
                return parsed

            code, message = error if error is not None else (resp.status_code, resp.text[:400])
            if self._is_rate_limit(resp.status_code, code, message) and attempt < max_retries - 1:
                sleep_for = 60
                LOGGER.warning("Rate limited by Bitfinex; sleeping %ss", sleep_for)
                time.sleep(sleep_for)
                continue
            if self._is_nonce_error(code, message) and attempt < max_retries - 1:
                LOGGER.warning("Nonce rejected by Bitfinex; retrying with a higher nonce")
                time.sleep(1)
                continue
            if resp.status_code >= 500 and error is None and attempt < max_retries - 1:
                sleep_for = min(2**attempt, 30)
                LOGGER.warning(
                    "Bitfinex %s on %s; retrying in %ss", resp.status_code, path, sleep_for
                )
                time.sleep(sleep_for)
                continue
            raise RuntimeError(self._format_api_error(path, resp.status_code, code, message))

        raise RuntimeError(f"Bitfinex API failed after retries for {path}: {last_error}")

    @staticmethod
    def _error_parts(body: Any) -> Optional[Tuple[Any, str]]:
        if isinstance(body, list) and body and body[0] == "error":
            code = body[1] if len(body) > 1 else None
            message = body[2] if len(body) > 2 else ""
            return code, str(message)
        if isinstance(body, dict) and body.get("error"):
            return body.get("code"), str(body.get("error"))
        return None

    @staticmethod
    def _is_rate_limit(status: int, code: Any, message: str) -> bool:
        text = f"{code} {message}".lower()
        return (
            status == 429
            or ("rate" in text and "limit" in text)
            or "err_rate_limit" in text
        )

    @staticmethod
    def _is_nonce_error(code: Any, message: str) -> bool:
        text = f"{code} {message}".lower()
        return "nonce" in text

    @staticmethod
    def _format_api_error(path: str, status: int, code: Any, message: str) -> str:
        hint = ""
        text = f"{code} {message}".lower()
        if "nonce" in text:
            hint = (
                " Nonces are per API key and must strictly increase. "
                "Use this key only on the poller, or create a new key."
            )
        elif any(token in text for token in ("permission", "apikey", "digest", "auth", "invalid key")):
            hint = (
                " Check BITFINEX_API_KEY / BITFINEX_API_SECRET and that the key "
                "has Read permission for this endpoint."
            )
        elif "rate" in text and "limit" in text:
            hint = " Bitfinex blocked this IP for about 60 seconds."
        return f"Bitfinex API error {status} for {path}: code={code} message={message}.{hint}"

    def _collect(
        self,
        path: str,
        *,
        start_ms: int,
        end_ms: int,
        limit: int,
        time_index: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[List[Any]], bool]:
        """Page newest-first history until ``start_ms`` or a short page."""
        rows: List[List[Any]] = []
        seen: Set[Any] = set()
        cursor_end = end_ms
        complete = False
        for page_no in range(MAX_PAGES):
            payload: Dict[str, Any] = {
                "start": start_ms,
                "end": cursor_end,
                "limit": limit,
            }
            if extra:
                payload.update(extra)
            parsed = self._post(path, payload)
            if not isinstance(parsed, list):
                raise RuntimeError(f"Unexpected Bitfinex payload for {path}: {str(parsed)[:300]}")
            page = [row for row in parsed if isinstance(row, list)]
            oldest: Optional[int] = None
            added = 0
            for row in page:
                record_id = row_value(row, 0)
                key = ("id", record_id) if record_id is not None else ("row", tuple(row))
                timestamp = parse_ts_to_ms(row_value(row, time_index))
                if timestamp is not None:
                    oldest = timestamp if oldest is None else min(oldest, timestamp)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
                added += 1
            if len(page) < limit:
                complete = True
                break
            if oldest is None or oldest > cursor_end or added == 0:
                LOGGER.warning("Bitfinex pagination made no progress on %s", path)
                complete = False
                break
            if oldest < start_ms:
                complete = True
                break
            # Keep the boundary timestamp for one extra page so rows that share it
            # are not dropped, then step behind it.
            if oldest == cursor_end:
                cursor_end = oldest - 1
            else:
                cursor_end = oldest
            if cursor_end < start_ms:
                complete = True
                break
        else:
            LOGGER.error(
                "Stopped after %s pages on %s; checkpoint will not advance",
                MAX_PAGES,
                path,
            )
            complete = False
        return rows, complete

    def fetch_movements(self, start_ms: int, end_ms: int) -> HistPage:
        raw, complete = self._collect(
            "v2/auth/r/movements/hist",
            start_ms=start_ms,
            end_ms=end_ms,
            limit=MOVEMENT_LIMIT,
            time_index=6,
        )
        return HistPage([map_movement(row) for row in raw], complete)

    def fetch_ledgers(self, start_ms: int, end_ms: int, category: int) -> HistPage:
        raw, complete = self._collect(
            "v2/auth/r/ledgers/hist",
            start_ms=start_ms,
            end_ms=end_ms,
            limit=LEDGER_LIMIT,
            time_index=3,
            extra={"category": category},
        )
        return HistPage([map_ledger(row, category) for row in raw], complete)

    def fetch_trades(
        self, start_ms: int, end_ms: int, symbol: Optional[str]
    ) -> HistPage:
        path = "v2/auth/r/trades/hist"
        if symbol:
            path = f"v2/auth/r/trades/{quote(symbol, safe=':')}/hist"
        raw, complete = self._collect(
            path,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=TRADE_LIMIT,
            time_index=2,
        )
        return HistPage([map_trade(row) for row in raw], complete)

    def fetch_orders(
        self, start_ms: int, end_ms: int, symbol: Optional[str]
    ) -> HistPage:
        path = "v2/auth/r/orders/hist"
        if symbol:
            path = f"v2/auth/r/orders/{quote(symbol, safe=':')}/hist"
        # Orders history filters on MTS_CREATE and only keeps about two weeks.
        raw, complete = self._collect(
            path,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=ORDER_LIMIT,
            time_index=4,
        )
        return HistPage([map_order(row) for row in raw], complete)


class CoralogixShipper:
    def __init__(
        self,
        api_key: str,
        domain: str,
        application_name: str,
        subsystem_name: str,
        batch_size: int = 200,
        timeout: float = 60.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        domain = domain.lstrip(".").strip()
        self.url = f"https://ingress.{domain}/logs/v1/singles"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self.application_name = application_name
        self.subsystem_name = subsystem_name
        self.batch_size = max(1, batch_size)
        self.timeout = timeout
        self.session = session or requests.Session()

    def _to_cx_record(self, event: Dict[str, Any], *, category: str) -> Dict[str, Any]:
        timestamp = event_timestamp_ms(event) or utc_now_ms()
        computer = event.get("currency") or event.get("symbol") or event.get("wallet")
        record: Dict[str, Any] = {
            "applicationName": self.application_name,
            "subsystemName": self.subsystem_name,
            "severity": map_severity(event.get("event_source") or event.get("cx_category"), event),
            "category": category,
            "timestamp": timestamp,
            "text": json.dumps(event, separators=(",", ":"), default=str),
        }
        if computer:
            record["computerName"] = str(computer)
        return record

    def ship(self, events: Iterable[Dict[str, Any]], *, category: str) -> int:
        batch: List[Dict[str, Any]] = []
        sent = 0

        def flush() -> None:
            nonlocal sent, batch
            if not batch:
                return
            resp = self.session.post(
                self.url,
                headers=self.headers,
                data=json.dumps(batch, separators=(",", ":")),
                timeout=self.timeout,
            )
            if not resp.ok:
                raise RuntimeError(
                    f"Coralogix ingest error {resp.status_code}: {resp.text[:500]}"
                )
            sent += len(batch)
            LOGGER.info("Shipped %s logs to Coralogix (%s)", len(batch), category)
            batch = []

        for event in events:
            enriched = dict(event)
            enriched.setdefault("cx_source", "bitfinex")
            enriched.setdefault("cx_category", category)
            batch.append(self._to_cx_record(enriched, category=category))
            if len(batch) >= self.batch_size:
                flush()
        flush()
        return sent


def window_start_ms(state: StateStore, key: str, lookback_minutes: int) -> int:
    now_ms = utc_now_ms()
    lookback_ms = lookback_minutes * 60 * 1000
    window_start = now_ms - lookback_ms
    overlap_ms = OVERLAP_SECONDS * 1000
    last_time_ms = state.get_last_time_ms(key)
    if last_time_ms is not None and last_time_ms < window_start:
        LOGGER.info("Catching up %s from last checkpoint (gap larger than lookback)", key)
        return max(0, last_time_ms - overlap_ms)
    return max(0, window_start - overlap_ms)


def filter_new_events(
    source: str,
    events: List[Dict[str, Any]],
    seen_ids: Set[str],
    extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        enriched = dict(event)
        if extra:
            enriched.update(extra)
        enriched["event_source"] = f"bitfinex_{source}"
        event_id = event_id_for(source, enriched)
        if event_id and event_id in seen_ids:
            continue
        if event_id:
            enriched["cx_event_id"] = event_id
        kept.append(enriched)
    return kept


def ship_source(
    source: str,
    events: List[Dict[str, Any]],
    shipper: CoralogixShipper,
    state: StateStore,
    state_key: str,
    *,
    dry_run: bool,
    window_end_ms: Optional[int] = None,
    advance_checkpoint: bool = True,
) -> int:
    new_ids: List[str] = []
    for event in events:
        event_id = str(event.get("cx_event_id") or event_id_for(source, event) or "")
        if event_id:
            new_ids.append(event_id)

    checkpoint = window_end_ms if advance_checkpoint else None
    if not events:
        LOGGER.info("%s: no new events", state_key)
        if not dry_run and checkpoint is not None:
            state.update(state_key, [], checkpoint)
            state.save()
        return 0

    if dry_run:
        LOGGER.info("Dry run: would ship %s %s events", len(events), state_key)
        for event in events[:3]:
            LOGGER.info("Sample %s: %s", state_key, json.dumps(event, default=str)[:400])
        return 0

    sent = shipper.ship(events, category=f"bitfinex-{source}")
    state.update(state_key, new_ids, checkpoint)
    state.save()
    LOGGER.info("%s: shipped %s events", state_key, sent)
    return sent


def _log_scan(label: str, scanned: int, kept: int, start_ms: int, end_ms: int, complete: bool) -> None:
    LOGGER.info(
        "%s scan: scanned=%s kept=%s window=%s .. %s complete=%s",
        label,
        scanned,
        kept,
        ms_to_iso(start_ms),
        ms_to_iso(end_ms),
        complete,
    )


def poll_sources(
    client: BitfinexClient,
    shipper: CoralogixShipper,
    state: StateStore,
    *,
    sources: List[str],
    lookback_minutes: int,
    transfer_categories: List[int],
    symbols: List[str],
    dry_run: bool,
) -> int:
    now_ms = utc_now_ms()
    total = 0
    movement_sources = [name for name in ("deposit", "withdraw") if name in sources]
    if movement_sources:
        starts = {
            name: window_start_ms(state, name, lookback_minutes) for name in movement_sources
        }
        start_ms = min(starts.values())
        page = client.fetch_movements(start_ms, now_ms)
        if not page.complete:
            LOGGER.error("Movement history exceeded the page cap; checkpoints stay put")
        for name in movement_sources:
            selected = []
            for event in page.rows:
                if not in_window(event, starts[name], now_ms):
                    continue
                sign = amount_sign(event.get("amount"))
                if name == "withdraw" and sign >= 0:
                    continue
                if name == "deposit" and sign < 0:
                    continue
                selected.append(event)
            events = filter_new_events(name, selected, state.get_seen_ids(name))
            _log_scan(name.capitalize(), len(selected), len(events), starts[name], now_ms, page.complete)
            total += ship_source(
                name,
                events,
                shipper,
                state,
                name,
                dry_run=dry_run,
                window_end_ms=now_ms,
                advance_checkpoint=page.complete,
            )

    if "transfer" in sources:
        for category in transfer_categories:
            key = f"transfer:{category}"
            start_ms = window_start_ms(state, key, lookback_minutes)
            try:
                page = client.fetch_ledgers(start_ms, now_ms, category)
            except RuntimeError as exc:
                LOGGER.error("Ledger category %s failed: %s", category, exc)
                continue
            if not page.complete:
                LOGGER.error(
                    "Ledger category %s exceeded the page cap; checkpoint stays put",
                    category,
                )
            selected = [event for event in page.rows if in_window(event, start_ms, now_ms)]
            events = filter_new_events("transfer", selected, state.get_seen_ids(key))
            _log_scan(
                f"Transfer {category}",
                len(selected),
                len(events),
                start_ms,
                now_ms,
                page.complete,
            )
            total += ship_source(
                "transfer",
                events,
                shipper,
                state,
                key,
                dry_run=dry_run,
                window_end_ms=now_ms,
                advance_checkpoint=page.complete,
            )

    symbol_targets: List[Optional[str]] = list(symbols) if symbols else [None]
    if "trade" in sources:
        for symbol in symbol_targets:
            key = f"trade:{symbol or '*'}"
            start_ms = window_start_ms(state, key, lookback_minutes)
            page = client.fetch_trades(start_ms, now_ms, symbol)
            if not page.complete:
                LOGGER.error("Trade %s exceeded the page cap; checkpoint stays put", symbol or "*")
            selected = [event for event in page.rows if in_window(event, start_ms, now_ms)]
            events = filter_new_events("trade", selected, state.get_seen_ids(key))
            _log_scan(
                f"Trade {symbol or '*'}",
                len(selected),
                len(events),
                start_ms,
                now_ms,
                page.complete,
            )
            total += ship_source(
                "trade",
                events,
                shipper,
                state,
                key,
                dry_run=dry_run,
                window_end_ms=now_ms,
                advance_checkpoint=page.complete,
            )

    if "order" in sources:
        for symbol in symbol_targets:
            key = f"order:{symbol or '*'}"
            start_ms = window_start_ms(state, key, lookback_minutes)
            page = client.fetch_orders(start_ms, now_ms, symbol)
            if not page.complete:
                LOGGER.error("Order %s exceeded the page cap; checkpoint stays put", symbol or "*")
            selected = []
            for event in page.rows:
                created = parse_ts_to_ms(event.get("mtsCreate"))
                if created is None or start_ms <= created <= now_ms:
                    selected.append(event)
            events = filter_new_events("order", selected, state.get_seen_ids(key))
            _log_scan(
                f"Order {symbol or '*'}",
                len(selected),
                len(events),
                start_ms,
                now_ms,
                page.complete,
            )
            total += ship_source(
                "order",
                events,
                shipper,
                state,
                key,
                dry_run=dry_run,
                window_end_ms=now_ms,
                advance_checkpoint=page.complete,
            )

    return total


def resolve_lookback_minutes(args: argparse.Namespace) -> int:
    if getattr(args, "lookback_minutes", None) is not None:
        return int(args.lookback_minutes)
    if getattr(args, "lookback_hours", None) is not None:
        return int(args.lookback_hours) * 60
    if os.getenv("LOOKBACK_MINUTES"):
        return int(os.getenv("LOOKBACK_MINUTES", "5"))
    if os.getenv("LOOKBACK_HOURS"):
        return int(os.getenv("LOOKBACK_HOURS", "1")) * 60
    return 5


def parse_categories(values: List[str]) -> List[int]:
    categories: List[int] = []
    for value in values:
        try:
            categories.append(int(value))
        except ValueError as exc:
            raise SystemExit(
                f"Invalid BITFINEX_TRANSFER_CATEGORIES entry: {value!r}"
            ) from exc
    return categories or [51]


def run_once(args: argparse.Namespace) -> int:
    sources = [item.lower() for item in env_csv("BITFINEX_SOURCES", DEFAULT_SOURCES)]
    unknown = [item for item in sources if item not in KNOWN_SOURCES]
    if unknown:
        raise SystemExit(f"Unknown BITFINEX_SOURCES: {unknown}. Known: {sorted(KNOWN_SOURCES)}")

    client = BitfinexClient(
        api_key=require_env("BITFINEX_API_KEY"),
        api_secret=require_env("BITFINEX_API_SECRET"),
        base_url=os.getenv("BITFINEX_BASE_URL", "https://api.bitfinex.com").strip(),
        timeout=float(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
    )
    shipper = CoralogixShipper(
        api_key=(
            os.getenv("CORALOGIX_SEND_YOUR_DATA_KEY", "").strip()
            if args.dry_run
            else require_env("CORALOGIX_SEND_YOUR_DATA_KEY")
        )
        or "dry-run",
        domain=os.getenv("CORALOGIX_DOMAIN", "coralogix.com").strip(),
        application_name=os.getenv("CORALOGIX_APPLICATION_NAME", "bitfinex"),
        subsystem_name=os.getenv("CORALOGIX_SUBSYSTEM_NAME", "exchange-logs"),
        batch_size=int(os.getenv("CORALOGIX_BATCH_SIZE", "200")),
        timeout=float(os.getenv("HTTP_TIMEOUT_SECONDS", "60")),
    )
    state = StateStore(Path(os.getenv("STATE_FILE", "./state.json")))
    lookback_minutes = resolve_lookback_minutes(args)
    symbols = [normalize_symbol(item) for item in env_csv("BITFINEX_SYMBOLS")]
    LOGGER.info(
        "Polling Bitfinex lookback=%s minute(s) sources=%s dry_run=%s",
        lookback_minutes,
        ",".join(sources),
        args.dry_run,
    )
    return poll_sources(
        client,
        shipper,
        state,
        sources=sources,
        lookback_minutes=lookback_minutes,
        transfer_categories=parse_categories(
            env_csv("BITFINEX_TRANSFER_CATEGORIES", DEFAULT_TRANSFER_CATEGORIES)
        ),
        symbols=symbols,
        dry_run=args.dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll Bitfinex account activity and ship it to Coralogix"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll cycle and exit (recommended for cron)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch Bitfinex logs but do not send to Coralogix or update state",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=None,
        help="Lookback window in minutes (default: 5)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Lookback window in hours (alternative to --lookback-minutes)",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level (default: INFO)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.once or args.dry_run:
        sent = run_once(args)
        LOGGER.info("Done. Total events shipped: %s", sent)
        return 0

    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
    LOGGER.info("Starting continuous poller (interval=%ss)", interval)
    while True:
        cycle_start = time.time()
        try:
            sent = run_once(args)
            LOGGER.info("Cycle complete. Shipped %s events", sent)
        except Exception:
            LOGGER.exception("Poll cycle failed")
        elapsed = time.time() - cycle_start
        sleep_for = max(1.0, interval - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    sys.exit(main())
