"""Public, bounded REST reads for paper funding; never submits an order."""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

from scripts.funding_arb_accounting import MarketUnavailable, dec

BINANCE = "binance_perpetual"
HYPERLIQUID = "hyperliquid_perpetual"


def request_json(url, payload=None):
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json", "User-Agent": "Hummingbot-funding-paper-audit/2",
    })
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def metadata_snapshot():
    """Rate forecasts, actual symbol-specific intervals, and reference prices.

    Binance fundingInfo contains only adjusted intervals; eight hours is used
    only after a successful full fundingInfo response. HL API funding is hourly.
    These forecasts are never themselves posted as settled payments.
    """
    premiums = request_json("https://fapi.binance.com/fapi/v1/premiumIndex")
    adjustments = request_json("https://fapi.binance.com/fapi/v1/fundingInfo")
    contexts = request_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
    if not isinstance(premiums, list) or not isinstance(adjustments, list) or not isinstance(contexts, list) or len(contexts) != 2:
        raise MarketUnavailable("unexpected public metadata response")
    intervals = {row["symbol"]: int(row["fundingIntervalHours"]) * 3600 for row in adjustments}
    observed = time.time()
    result = {}
    for row in premiums:
        symbol = row.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        pair = symbol[:-4] + "-USDT"
        try:
            result[(BINANCE, pair)] = {
                "rate": dec(row["lastFundingRate"]),
                "interval": intervals.get(symbol, 28800),
                "next_time": int(row["nextFundingTime"]) / 1000,
                "mark_price": dec(row["markPrice"], positive=True),
                "index_price": dec(row["indexPrice"], positive=True),
                "observed_at": observed, "source": "binance_public_metadata",
            }
        except (KeyError, ValueError):
            continue
    for asset, row in zip(contexts[0]["universe"], contexts[1]):
        if asset.get("isDelisted"):
            continue
        pair = asset["name"] + "-USD"
        try:
            result[(HYPERLIQUID, pair)] = {
                "rate": dec(row["funding"]), "interval": 3600,
                "next_time": (int(observed) // 3600 + 1) * 3600,
                "mark_price": dec(row["markPx"], positive=True),
                "index_price": dec(row["oraclePx"], positive=True),
                "observed_at": observed, "source": "hyperliquid_public_metadata",
                "volume_24h": dec(row.get("dayNtlVlm", 0), nonnegative=True),
            }
        except (KeyError, ValueError):
            continue
    return result


def volume_snapshot():
    """One batch read, cached by the universe manager; no per-market polling."""
    rows = request_json('https://fapi.binance.com/fapi/v1/ticker/24hr')
    if not isinstance(rows, list):
        raise MarketUnavailable('unexpected public volume response')
    return {r['symbol'][:-4]+'-USDT': dec(r['quoteVolume'], nonnegative=True)
            for r in rows if r.get('symbol', '').endswith('USDT')}


def funding_history(connector, trading_pair, start_seconds, end_seconds):
    """Read confirmed public rates, including bounded pagination for catch-up."""
    first = int(float(start_seconds) * 1000) + 1  # position must precede settlement
    last = int(float(end_seconds) * 1000)
    if last < first:
        return []
    records = []
    for _ in range(8):
        if connector == BINANCE:
            symbol = trading_pair.replace("-", "")
            query = urllib.parse.urlencode({"symbol": symbol, "startTime": first, "endTime": last, "limit": 1000})
            rows = request_json("https://fapi.binance.com/fapi/v1/fundingRate?" + query)
            time_key = "fundingTime"
        elif connector == HYPERLIQUID:
            rows = request_json("https://api.hyperliquid.xyz/info", {
                "type": "fundingHistory", "coin": trading_pair.rsplit("-", 1)[0],
                "startTime": first, "endTime": last,
            })
            time_key = "time"
        else:
            raise MarketUnavailable("unsupported funding-history connector")
        if not isinstance(rows, list):
            raise MarketUnavailable("unexpected funding history response")
        if not rows:
            break
        latest = first - 1
        for row in rows:
            stamp = int(row[time_key])
            latest = max(latest, stamp)
            if first <= stamp <= last:
                event = {"timestamp": stamp / 1000, "rate": dec(row["fundingRate"]),
                         "source": "venue_funding_history", "rate_confirmed": True}
                if row.get("markPrice"):
                    event["price"] = dec(row["markPrice"], positive=True)
                    event["price_source"] = "venue_settlement_mark"
                records.append(event)
        if latest < first:
            raise MarketUnavailable("funding history pagination did not advance")
        first = latest + 1
        limit = 1000 if connector == BINANCE else 500
        if len(rows) < limit or first > last:
            break
    else:
        raise MarketUnavailable("funding catch-up exceeds bounded history window")
    unique = {row["timestamp"]: row for row in records}
    return [unique[key] for key in sorted(unique)]
