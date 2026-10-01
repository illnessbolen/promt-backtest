"""Market metadata for the live tracker: the current windows of every tracked series, fetched ahead of
time from Gamma by computed slug, plus on-demand lookups by token id for anything else he trades."""

from __future__ import annotations

import logging
import time
from typing import Any

from bosona import parse
from bosona.http import ApiClient
from bosona.live.store import LiveStore
from bosona.windows import slug_for, window_end, window_start

log = logging.getLogger(__name__)


class MarketRegistry:
    def __init__(self, store: LiveStore, client: ApiClient, gamma_url: str, assets: list[str], timeframes: list[str],
                 lead_s: float = 60.0) -> None:
        self.store = store
        self.client = client
        self.gamma_url = gamma_url
        self.assets = assets
        self.timeframes = timeframes
        self.lead_s = lead_s
        self.by_token: dict[str, dict[str, Any]] = {}
        self.by_slug: dict[str, dict[str, Any]] = {}
        self._missing_until: dict[str, float] = {}
        for row in store.markets():
            self._index(row)

    def _index(self, row: dict[str, Any]) -> None:
        self.by_slug[row["slug"]] = row
        for tok in (row.get("up_token_id"), row.get("down_token_id")):
            if tok:
                self.by_token[tok] = row

    def _ingest(self, markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = int(time.time())
        rows = []
        for m in markets:
            events = m.get("events") or []
            row, _ = parse.parse_market(m, events[0] if events else None, now)
            rows.append(row)
            self._index(row)
        self.store.upsert("markets", rows)
        return rows

    def get(self, token: str) -> dict[str, Any] | None:
        return self.by_token.get(token)

    async def lookup(self, token: str) -> dict[str, Any] | None:
        row = self.by_token.get(token)
        if row is not None:
            return row
        for closed in ("false", "true"):
            found = await self.client.get_json(f"{self.gamma_url}/markets", {"clob_token_ids": token, "closed": closed})
            if found:
                self._ingest(found)
                return self.by_token.get(token)
        return None

    def wanted(self, now: float, timeframes: list[str] | None = None) -> list[tuple[str, str, int, str]]:
        """(asset, timeframe, window_start, slug) of the open window of each series, plus the next one when it
        opens within `lead_s`."""
        out = []
        t = int(now)
        for tf in timeframes or self.timeframes:
            ws = window_start(tf, t)
            starts = [ws]
            nxt = window_end(tf, ws)
            if nxt - now <= self.lead_s:
                starts.append(nxt)
            for a in self.assets:
                out.extend((a, tf, s, slug_for(a, tf, s)) for s in starts)
        return out

    async def refresh(self, now: float) -> list[dict[str, Any]]:
        """Make sure the wanted windows are known (Gamma lookup by slug, 50 per request); returns their rows."""
        wanted = self.wanted(now)
        missing = [w[3] for w in wanted if w[3] not in self.by_slug and self._missing_until.get(w[3], 0) <= now]
        for i in range(0, len(missing), 50):
            chunk = missing[i : i + 50]
            try:
                # Gamma returns 20 rows unless told otherwise
                found = await self.client.get_json(f"{self.gamma_url}/markets",
                                                   [("slug", s) for s in chunk] + [("limit", len(chunk))])
            except Exception as exc:  # noqa: BLE001 - retried on the next refresh
                log.warning("markets: Gamma lookup failed: %s", exc)
                continue
            self._ingest(found)
            for s in chunk:
                if s not in self.by_slug:
                    self._missing_until[s] = now + 60  # not listed (yet): do not hammer Gamma
        return [self.by_slug[w[3]] for w in wanted if w[3] in self.by_slug]
