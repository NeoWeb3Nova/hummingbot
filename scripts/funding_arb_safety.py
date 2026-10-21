"""Isolated paper-execution adapter used by v2_funding_rate_arb.

Hummingbot retains ownership of connectors, clocks, MQTT and orchestration.
This adapter owns deterministic paper fills and their persistent ledger only.
Live order submission is deliberately unavailable until fill reconciliation has
its own acceptance tests. No production connector implementation is patched.
"""
from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from collections import Counter, deque
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType

from scripts import funding_arb_public_data as public_data
from scripts.funding_arb_universe import FundingUniverse, entry_rank
from scripts.funding_arb_accounting import (
    MODEL_VERSION, ONE, SCHEMA_VERSION, ZERO, MarketUnavailable, atomic_json,
    dec, fill_base, fill_quote, funding_payment, is_buy, liquidation_estimate,
    json_value, matched_quantities, position_value, restored_position, safe_capacity,
    split_position, valid_levels,
)


class FundingArbSafety:
    def __init__(self, strategy):
        self.s = strategy
        self.metadata = {}
        self.metadata_refresh = 0.0
        self.funding_refresh = 0.0
        self.tasks = set()
        self.metadata_task = None
        self.funding_jobs = set()
        self.semaphore = asyncio.Semaphore(2)
        self.quote_seen = {}
        self.reference_prices = {}
        self.rejections = Counter()
        self.last_rejection = {}
        self.entry_paths = {}
        self.blocked_reason = ""
        self.last_report = None
        self.audit_started_at = None
        self.last_persist = 0.0
        self.restore_failed = False
        self.rules_refresh = {}
        self.rules_jobs = set()
        self.mqtt_status = "waiting"
        self.mqtt_retry_at = 0.0
        self.market_health_at = 0.0
        self.market_health = {}
        self.universe = FundingUniverse(self)
        self.metadata_status = {}

    @property
    def path(self):
        return Path(self.s.config.paper_state_path)

    def reject(self, token, reason):
        self.s.entry_candidate_ticks[token] = 0
        self.entry_paths.pop(token, None)
        self.rejections[reason] += 1
        self.last_rejection[token] = {"reason": reason, "timestamp": self.s.current_timestamp}

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    def restore(self, timestamp):
        self.audit_started_at = timestamp
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("is_dry_run") is not True:
                raise MarketUnavailable("refusing to mix non-paper ledger into paper mode")
            history = data.get("closed_trades", [])
            if not isinstance(history, list):
                raise MarketUnavailable("invalid history schema")
            self.s.stopped_funding_arbitrages = {}
            for row in sorted(history, key=lambda r: (r.get("close_timestamp", 0), r.get("trade_id", ""))):
                for key in ("trading_pnl", "funding_pnl", "total_net_pnl"):
                    dec(row.get(key, 0))
                self.s.stopped_funding_arbitrages.setdefault(row["token"], []).append(copy.deepcopy(row))
                self.s.token_last_closed_timestamp[row["token"]] = max(
                    self.s.token_last_closed_timestamp.get(row["token"], 0), float(row.get("close_timestamp", 0)))
            legacy = data.get("schema_version") != SCHEMA_VERSION
            raw = data.get("raw_positions", data.get("active_positions", []))
            positions = {}
            for record in raw:
                token = record["token"]
                pos = restored_position(record, timestamp, legacy=legacy)
                pos["position_id"] = pos.get("position_id") or uuid.uuid4().hex
                if legacy:
                    # Preserve old cash flows; never backfill them twice or claim
                    # to have recovered original fees/quantities that were lost.
                    pos["funding_history_start"] = timestamp
                    for payment in pos["funding_payments"]:
                        payment.setdefault("source", "legacy_unverified")
                    pos["entry_fee_source"] = "legacy_estimate"
                    try:
                        fees = [self.fee(pos[f"connector_{i}"], self.s.get_trading_pair_for_connector(token, pos[f"connector_{i}"]),
                                         pos[f"amount_{i}"], pos[f"price_{i}"], is_buy(pos["side"]) if i == 1 else not is_buy(pos["side"]), False)
                                for i in (1, 2)]
                        pos["entry_fee_1"], pos["entry_fee_2"] = fees
                        pos["entry_fee_quote"] = sum(fees, ZERO)
                    except Exception:
                        pos["entry_fee_source"] = "legacy_unknown"
                        self.blocked_reason = "legacy entry fees could not be recovered"
                positions[token] = pos
            self.s.active_funding_arbitrages = positions
            # Mount restored positions even if a YAML watchlist was changed.
            # Removed positions remain outside the desired universe so the
            # exit loop can close them, but their books must stay subscribed.
            desired = set(data.get("watchlist", self.s.config.tokens))
            self.s.config.tokens = desired
            for token in desired | set(positions):
                for name, connector in self.s.connectors.items():
                    pair = self.s.get_trading_pair_for_connector(token, name)
                    if pair not in connector.trading_pairs and hasattr(connector, "add_trading_pair"):
                        self._spawn(connector.add_trading_pair(pair))
            self.s.token_permanent_blacklist = set(data.get("permanent_blacklist", []))
            self.s.token_quarantine_ledger = copy.deepcopy(data.get("quarantined_tokens", {}))
            self.s.probationary_tokens = copy.deepcopy(data.get("probationary_tokens", {}))
            for token, value in data.get("post_tp_lockout", {}).items():
                self.s.token_post_tp_lockout[token] = float(value.get("lockout_until", 0) if isinstance(value, dict) else value)
            status = data.get("rotation_status", {})
            self.s.rotation_event_history = status.get("rotation_events", [])
            self.s.last_rotation_timestamp = float(status.get("last_rotation_timestamp", 0))
            universe_state = status.get('discovery', {}).get('state', {})
            self.universe.admitted_at = dict(universe_state.get('admitted_at', {}))
            self.universe.cooldowns = dict(universe_state.get('cooldowns', {}))
            audit = data.get("audit", {})
            self.audit_started_at = audit.get("started_at", timestamp)
            self.rejections.update(audit.get("rejections", {}))
            self.s.logger().info("[AUDIT-RESTORE] Restored %s positions and %s closed records without erasing history.",
                                 len(positions), len(history))
        except Exception as exc:
            self.restore_failed = True
            self.blocked_reason = f"state restore failed: {exc}"
            self.s.logger().error("[AUDIT-BLOCKED] %s; original state will not be overwritten.", self.blocked_reason)

    async def shutdown(self):
        for task in list(self.tasks):
            task.cancel()
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        self.publish()

    def refresh(self):
        now = self.s.current_timestamp
        for name, connector in self.s.connectors.items():
            updater = getattr(connector, "_update_trading_rules", None)
            if updater is not None and name not in self.rules_jobs and now - self.rules_refresh.get(name, 0) >= 30:
                if not getattr(connector, "is_trading_required", False):
                    self.rules_refresh[name] = now
                    self.rules_jobs.add(name)
                    self._spawn(self._refresh_public_rules(name, connector))
        if (self.metadata_task is None or self.metadata_task.done()) and now - self.metadata_refresh >= 30:
            self.metadata_refresh = now
            self.metadata_task = self._spawn(self._refresh_metadata())
        if now - self.funding_refresh >= 30:
            self.funding_refresh = now
            for target in self._funding_targets():
                for i in (1, 2):
                    key = (id(target), i)
                    if key not in self.funding_jobs:
                        self.funding_jobs.add(key)
                        self._spawn(self._sync_funding(target, i, key))

    async def _refresh_public_rules(self, name, connector):
        try:
            # Hummingbot does not run this public loop when trading_required
            # is false. Reuse its venue-specific parser without enabling any
            # account, user-stream, leverage or order polling tasks.
            async with self.semaphore:
                await connector._update_trading_rules()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.s.logger().warning("[PUBLIC-RULES] %s: %s; entries still require a valid symbol rule.", name, exc)
        finally:
            self.rules_jobs.discard(name)

    async def _refresh_metadata(self):
        started = time.monotonic()
        self.metadata_status['last_attempt_at'] = self.s.current_timestamp
        try:
            snapshot = await asyncio.to_thread(public_data.metadata_snapshot)
            self.metadata = snapshot
            self.metadata_status.update(last_success_at=self.s.current_timestamp,
                                        markets=len(snapshot), last_error=None)
            # Discovery needs wide forecasts, not hundreds of unused price deques.
            # Keep settlement references for watched, held and pending positions.
            reference_tokens = set(self.s.config.tokens) | {p['token'] for p in self._funding_targets()}
            reference_keys = {(c,self.s.get_trading_pair_for_connector(t,c))
                              for t in reference_tokens for c in self.s.connectors}
            self.reference_prices = {k:v for k,v in self.reference_prices.items() if k in reference_keys}
            for key, value in snapshot.items():
                if key not in reference_keys:
                    continue
                observations = self.reference_prices.setdefault(key, deque(maxlen=360))
                observations.append((float(value["observed_at"]), value["index_price"], value["mark_price"]))
        except Exception as exc:
            self.metadata_status['last_error'] = str(exc)
            self.s.logger().warning("[PUBLIC-METADATA] %s; stale data cannot permit new entries.", exc)
        finally:
            self.metadata_status['last_duration_seconds'] = time.monotonic()-started

    def funding_info(self, token):
        report = {}
        now = self.s.current_timestamp
        for connector in sorted(self.s.config.connectors):
            pair = self.s.get_trading_pair_for_connector(token, connector)
            row = self.metadata.get((connector, pair))
            if not row or abs(now - float(row["observed_at"])) > self.s.config.max_funding_data_age_seconds:
                raise MarketUnavailable("funding metadata missing or stale")
            if row["interval"] <= 0 or not (0 <= float(row["next_time"]) - now <= row["interval"] + 120):
                raise MarketUnavailable("funding schedule invalid")
            report[connector] = SimpleNamespace(rate=dec(row["rate"]), interval=row["interval"],
                                               next_funding_utc_timestamp=row["next_time"],
                                               mark_price=row["mark_price"], index_price=row["index_price"],
                                               observed_at=row["observed_at"])
        return report

    def _funding_targets(self):
        active = list(self.s.active_funding_arbitrages.values())
        closed = [r for rows in self.s.stopped_funding_arbitrages.values() for r in rows
                  if r.get("model_version") == MODEL_VERSION and r.get("funding_pending", False)]
        return active + closed

    def _still_target(self, target):
        return any(target is p for p in self._funding_targets())

    async def _sync_funding(self, target, leg, key):
        try:
            if not target.get("is_virtual", True):
                return
            connector = target[f"connector_{leg}"]
            pair = self.s.get_trading_pair_for_connector(target["token"], connector)
            opened = float(target.get("funding_history_start", target["open_timestamp"]))
            end = min(float(target.get("close_timestamp", self.s.current_timestamp)), self.s.current_timestamp)
            if end <= opened:
                return
            async with self.semaphore:
                rows = await asyncio.to_thread(public_data.funding_history, connector, pair, opened, end)
            if not self._still_target(target):
                return  # a partial close replaced this object while the request was in flight
            known = {(p.get("connector"), int(float(p["timestamp"]) * 1000)) for p in target.get("funding_payments", [])}
            for row in rows:
                event_key = (connector, int(row["timestamp"] * 1000))
                if event_key in known:
                    continue
                price = row.get("price")
                price_source = row.get("price_source")
                if price is None:
                    candidates = [v for v in self.reference_prices.get((connector, pair), [])
                                  if abs(v[0] - row["timestamp"]) <= self.s.config.max_funding_data_age_seconds]
                    if candidates:
                        observed = min(candidates, key=lambda v: abs(v[0] - row["timestamp"]))
                        price = observed[1]  # HL settles against oracle/index, not mark
                        price_source = "near_boundary_observed_oracle_estimate"
                    else:
                        price = target[f"price_{leg}"]
                        price_source = "entry_price_proxy_estimate"
                side = is_buy(target["side"]) if leg == 1 else not is_buy(target["side"])
                quantity = dec(target[f"amount_{leg}"], positive=True)
                amount = funding_payment(side="BUY" if side else "SELL", quantity=quantity, price=price, rate=row["rate"])
                target.setdefault("funding_payments", []).append({
                    "event_id": f"{connector}:{pair}:{event_key[1]}", "timestamp": row["timestamp"],
                    "connector": connector, "trading_pair": pair, "side": "BUY" if side else "SELL",
                    "rate": str(row["rate"]), "quantity": str(quantity), "reference_price": str(price),
                    "amount": str(amount), "source": row["source"], "rate_confirmed": True,
                    "price_source": price_source, "amount_estimated": price_source != "venue_settlement_mark",
                })
                known.add(event_key)
            interval = int(target.get(f"funding_interval_{leg}", 3600 if connector == public_data.HYPERLIQUID else 28800))
            next_time = float(target.get(f"first_funding_time_{leg}", target.get(f"next_funding_time_{leg}", 0)))
            due = []
            if next_time > opened:
                stamp = next_time
                while stamp <= end and len(due) < 2048:
                    due.append(stamp)
                    stamp += interval
            actual = {int(float(p["timestamp"])) for p in target.get("funding_payments", []) if p.get("connector") == connector}
            missing = [stamp for stamp in due if not any(abs(stamp - a) <= 2 for a in actual)]
            target[f"funding_missing_{leg}"] = missing
            target[f"funding_synced_through_{leg}"] = end
            target.pop(f"funding_error_since_{leg}", None)
            target["funding_pending"] = bool(target.get("funding_missing_1") or target.get("funding_missing_2")
                                             or target.get("funding_error_since_1") or target.get("funding_error_since_2"))
            if "close_timestamp" in target:
                target["funding_pending"] = target["funding_pending"] or any(
                    float(target.get(f"funding_synced_through_{i}", 0)) < end for i in (1, 2))
                funding = sum((dec(p["amount"]) for p in target.get("funding_payments", [])), ZERO)
                target["funding_pnl"] = float(funding)
                target["total_net_pnl"] = float(dec(target["trading_pnl"]) + funding)
                for i in (1, 2):
                    venue = target[f"connector_{i}"]
                    cash = sum((dec(p["amount"]) for p in target.get("funding_payments", []) if p.get("connector") == venue), ZERO)
                    target.setdefault("venue_net_pnl", {})[venue] = str(
                        dec(target[f"leg{i}_pnl"]) - dec(target.get(f"entry_fee_{i}", 0))
                        - dec(target.get(f"exit_fee_{i}", 0)) + cash)
            elif target.get("valuation"):
                values = target["valuation"]
                values["funding_pnl"] = sum((dec(p["amount"]) for p in target.get("funding_payments", [])), ZERO)
                values["total_net_pnl"] = dec(values["trading_pnl"]) + values["funding_pnl"]
            # Cash flows and their deduplication keys must survive a restart together.
            self.persist()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._still_target(target):
                target["funding_pending"] = True
                target.setdefault(f"funding_error_since_{leg}", self.s.current_timestamp)
            self.s.logger().warning("[FUNDING-PENDING] %s leg %s: %s", target.get("token"), leg, exc)
        finally:
            self.funding_jobs.discard(key)

    def books(self, connector, pair):
        conn = self.s.connectors[connector]
        if not conn.ready:
            public = getattr(conn, "status_dict", {})
            if getattr(conn, "is_trading_required", True) or public.get("symbols_mapping_initialized") is not True:
                raise MarketUnavailable("connector public symbol mapping not ready")
        status = getattr(conn, "network_status", None)
        if status is not None and getattr(status, "name", "CONNECTED") != "CONNECTED":
            raise MarketUnavailable("connector disconnected")
        book = self.s.market_data_provider.get_order_book(connector, pair)
        if book is None:
            raise MarketUnavailable("missing order book")
        tracker = getattr(conn, "order_book_tracker", None)
        metrics = getattr(getattr(tracker, "metrics", None), "per_pair_metrics", {})
        metric = metrics.get(pair)
        if metric is not None:
            received = max(metric.last_diff_timestamp, metric.last_snapshot_timestamp)
            if received <= 0 or time.perf_counter() - received > self.s.config.max_orderbook_age_seconds:
                raise MarketUnavailable("tracker reports stale order book")
        identity = (getattr(book, "last_diff_uid", 0), getattr(book, "snapshot_uid", 0))
        key = (connector, pair)
        now = time.monotonic()
        previous = self.quote_seen.get(key)
        if previous is None or previous[0] != identity:
            self.quote_seen[key] = (identity, now)
        elif now - previous[1] > self.s.config.max_orderbook_age_seconds:
            raise MarketUnavailable("order book has not received an update")
        bids, asks = book.snapshot
        bid_rows = valid_levels(zip(bids["price"], bids["amount"]), False)
        ask_rows = valid_levels(zip(asks["price"], asks["amount"]), True)
        if bid_rows[0][0] >= ask_rows[0][0]:
            raise MarketUnavailable("crossed order book")
        return bid_rows, ask_rows

    def quote(self, connector, pair, *, buy, amount=None, notional=None):
        bids, asks = self.books(connector, pair)
        rows = asks if buy else bids
        return fill_base(rows, amount) if amount is not None else fill_quote(rows, notional)

    def fee(self, connector, pair, quantity, price, buy, closing):
        quote = pair.rsplit("-", 1)[1]
        result = self.s.connectors[connector].get_fee(
            base_currency=pair.rsplit("-", 1)[0], quote_currency=quote,
            order_type=OrderType.MARKET, order_side=TradeType.BUY if buy else TradeType.SELL,
            amount=dec(quantity), price=dec(price), is_maker=False,
            position_action=PositionAction.CLOSE if closing else PositionAction.OPEN)
        percent = dec(result.percent, nonnegative=True)
        if getattr(result, "percent_token", None) not in {None, quote}:
            raise MarketUnavailable("non-quote percentage fee requires a conversion model")
        flat = ZERO
        for item in getattr(result, "flat_fees", []):
            if item.token != quote:
                raise MarketUnavailable("non-quote flat fee requires a conversion model")
            flat += dec(item.amount, nonnegative=True)
        return dec(quantity, positive=True) * dec(price, positive=True) * percent + flat

    def capacity(self, token, c1, c2, side, target):
        tier, cap = self.s.get_token_tier(token)
        target = min(dec(target, positive=True), dec(cap, positive=True))
        ratio = min(dec(self.s.config.max_orderbook_depth_ratio), Decimal("0.10")) if tier == 3 else dec(self.s.config.max_orderbook_depth_ratio)
        capacities = []
        for connector in (c1, c2):
            pair = self.s.get_trading_pair_for_connector(token, connector)
            for rows in self.books(connector, pair):  # both entry and exit sides must have capacity
                capacities.append(safe_capacity(rows, ratio, self.s.config.max_leg_price_impact_pct))
        result = min([target] + capacities)
        if not self.s.config.dynamic_position_sizing and result < target:
            raise MarketUnavailable("fixed position size exceeds safe depth capacity")
        if result < dec(self.s.config.min_position_size_quote):
            raise MarketUnavailable("safe depth capacity below minimum; never force minimum size")
        return result

    def entry_position(self, token, c1, c2, side, target, info):
        pairs = [self.s.get_trading_pair_for_connector(token, c) for c in (c1, c2)]
        buys = [is_buy(side), not is_buy(side)]
        prices = [self.quote(c, p, buy=b, notional=target).price for c, p, b in zip((c1, c2), pairs, buys)]
        multipliers = [dec(self.s.get_token_multiplier(token, c), positive=True) for c in (c1, c2)]
        q1, q2 = matched_quantities(target, *prices, *multipliers,
                                   lambda q: self.s.connectors[c1].quantize_order_amount(pairs[0], q),
                                   lambda q: self.s.connectors[c2].quantize_order_amount(pairs[1], q),
                                   self.s.config.max_hedge_mismatch_pct)
        fills, exits, entry_fees, exit_fees = [], [], [], []
        for i, (connector, pair, buy, quantity) in enumerate(zip((c1, c2), pairs, buys, (q1, q2))):
            rule = self.s.connectors[connector].trading_rules.get(pair)
            if rule is None:
                raise MarketUnavailable("missing venue trading rules")
            fill = self.quote(connector, pair, buy=buy, amount=quantity)
            exit_fill = self.quote(connector, pair, buy=not buy, amount=quantity)
            if quantity < dec(rule.min_order_size) or fill.notional < dec(rule.min_notional_size):
                raise MarketUnavailable("quantized order is below venue minimum")
            # Last re-quote must still fit the risk cap and same-unit hedge.
            if fill.notional > target * Decimal("1.001"):
                raise MarketUnavailable("re-quote exceeds notional cap")
            bids, asks = self.books(connector, pair)
            best = (asks if buy else bids)[0][0]
            if abs(fill.worst / best - ONE) > dec(self.s.config.max_leg_price_impact_pct):
                raise MarketUnavailable("per-leg price impact exceeds limit")
            fills.append(fill)
            exits.append(exit_fill)
            entry_fees.append(self.fee(connector, pair, quantity, fill.price, buy, False))
            exit_fees.append(self.fee(connector, pair, quantity, exit_fill.price, not buy, True))
        effective = min(f.notional for f in fills)
        if effective < dec(self.s.config.min_position_size_quote):
            raise MarketUnavailable("balanced order below strategy minimum")
        p1, p2 = fills[0].price / multipliers[0], fills[1].price / multipliers[1]
        divergence = abs(p1 - p2) / ((p1 + p2) / 2)
        if divergence > min(dec(self.s.config.max_allowed_price_divergence), dec(self.s.config.max_entry_spread_pct)):
            raise MarketUnavailable("entry basis exceeds configured funding-strategy limit")
        now = self.s.current_timestamp
        lev = self.s.get_effective_leverage(token, effective)
        if token in self.s.probationary_tokens:
            lev = 1
        pos = {"token": token, "position_id": uuid.uuid4().hex, "model_version": MODEL_VERSION,
               "is_virtual": True, "accounting_quality": "audited_paper", "connector_1": c1, "connector_2": c2,
               "side": "BUY" if buys[0] else "SELL", "price_1": fills[0].price, "price_2": fills[1].price,
               "amount_1": q1, "amount_2": q2, "position_size_quote": effective, "leverage": lev,
               "entry_fee_1": entry_fees[0], "entry_fee_2": entry_fees[1], "entry_fee_quote": sum(entry_fees, ZERO),
               "entry_fee_source": "connector_fee_schedule", "open_timestamp": now,
               "initial_margin_quote": sum((f.notional / lev for f in fills), ZERO), "funding_payments": [],
               "funding_pending": False, "funding_history_start": now, "partial_tp_executed": False,
               "breakeven_active": False, "executors_ids": ["DRY_RUN_SANDBOX"],
               "tier": self.s.get_token_tier(token)[0], "is_probationary_probe": token in self.s.probationary_tokens}
        for i, c in enumerate((c1, c2), 1):
            pos[f"funding_interval_{i}"] = info[c].interval
            pos[f"first_funding_time_{i}"] = info[c].next_funding_utc_timestamp
            pos[f"next_funding_time_{i}"] = info[c].next_funding_utc_timestamp
        carry_per_second = sum(((ONE if not b else -ONE) * f.notional * dec(info[c].rate) / dec(info[c].interval)
                                for c, b, f in zip((c1, c2), buys, fills)), ZERO)
        expected = carry_per_second * 86400 / effective
        immediate = position_value(pos, *exits, *exit_fees)
        # Do not book an entry cross-venue basis as immediate profit. Use all
        # four taker fees and local entry/exit depth; stress adverse convergence.
        long_p, short_p = (p1, p2) if buys[0] else (p2, p1)
        adverse_basis = max(ZERO, long_p - short_p) * min(q1 * multipliers[0], q2 * multipliers[1])
        friction = max(ZERO, -immediate["trading_pnl"]) + adverse_basis
        if carry_per_second <= ZERO:
            raise MarketUnavailable("net funding forecast is not positive at matched quantities")
        payback = friction / (carry_per_second * 3600)
        if payback > min(dec(self.s.config.max_payback_hours), Decimal("12")):
            raise MarketUnavailable("full round-trip payback exceeds limit")
        if friction / effective > dec(self.s.config.max_entry_spread_friction_pct):
            raise MarketUnavailable("full round-trip friction exceeds limit")
        if self.s.config.trade_profitability_condition_to_enter and immediate["trading_pnl"] < ZERO:
            raise MarketUnavailable("immediate round-trip profitability required")
        if expected < dec(self.s.config.min_funding_rate_profitability):
            raise MarketUnavailable("matched-quantity funding spread below threshold")
        pos.update(expected_profitability=expected, entry_profitability_after_fees=immediate["trading_pnl"] / effective,
                   selection_expected_net_quote=carry_per_second*3600*12-friction,
                   selection_horizon_hours=12, selection_forecast_kind='constant_current_rates_after_full_friction',
                   payback_hours=float(payback), entry_friction_pct=float(friction / effective),
                   min_hold_hours=max(float(self.s.config.min_holding_hours_floor), float(payback) * self.s.config.duration_linked_holding_factor),
                   regime_tier="TIER_1_SURFING" if payback <= 8 and expected * 365 >= Decimal("0.8") else "TIER_2_FARMING")
        pos["min_hold_until"] = now + pos["min_hold_hours"] * 3600
        return pos

    def value(self, position):
        fills, fees = [], []
        for i in (1, 2):
            connector = position[f"connector_{i}"]
            pair = self.s.get_trading_pair_for_connector(position["token"], connector)
            buy = not is_buy(position["side"]) if i == 1 else is_buy(position["side"])
            quantity = dec(position[f"amount_{i}"], positive=True)
            fill = self.quote(connector, pair, buy=buy, amount=quantity)
            fills.append(fill)
            fees.append(self.fee(connector, pair, quantity, fill.price, buy, True))
        for i, fill in enumerate(fills, 1):
            position[f"current_price_{i}"] = fill.price
        return position_value(position, *fills, *fees), fills, fees

    def budget(self):
        budget = dec(self.s.config.paper_account_balance_quote, positive=True)
        connectors = sorted(self.s.config.connectors)
        balances = {c: budget / 2 for c in connectors}
        realized = ZERO
        for rows in self.s.stopped_funding_arbitrages.values():
            for record in rows:
                net = dec(record.get("total_net_pnl", 0))
                realized += net
                legs = record.get("venue_net_pnl", {})
                if legs and all(c in legs for c in connectors):
                    for c in connectors:
                        balances[c] += dec(legs[c])
                else:
                    for c in connectors:
                        balances[c] += net / 2  # legacy allocation is explicitly estimated
        margin = {c: ZERO for c in connectors}
        unrealized = ZERO
        for pos in self.s.active_funding_arbitrages.values():
            values = pos.get("valuation", {})
            unrealized += dec(values.get("total_net_pnl", 0))
            for i in (1, 2):
                c = pos[f"connector_{i}"]
                used = dec(pos[f"amount_{i}"]) * dec(pos[f"price_{i}"]) / dec(pos["leverage"])
                margin[c] += used
                leg_pnl = dec(values.get(f"leg{i}_pnl", 0))
                fund = sum((dec(p["amount"]) for p in pos.get("funding_payments", []) if p.get("connector") == c), ZERO)
                balances[c] += min(ZERO, leg_pnl + fund) - dec(pos.get(f"entry_fee_{i}", 0))
        available = {c: balances[c] * (ONE - dec(self.s.config.paper_margin_reserve_pct)) - margin[c] for c in connectors}
        return {"initial_balance": budget, "equity": budget + realized + unrealized,
                "realized": realized, "margin": margin, "available": available}

    def _entry_block(self):
        if self.blocked_reason:
            return self.blocked_reason
        for pos in self.s.active_funding_arbitrages.values():
            if pos.get("valuation_status") != "valid":
                return "existing position cannot be valued"
            missing = pos.get("funding_missing_1", []) + pos.get("funding_missing_2", [])
            if missing and self.s.current_timestamp - min(missing) > 120:
                return "confirmed funding history is overdue"
            if any(self.s.current_timestamp - float(pos.get(f"funding_error_since_{i}", self.s.current_timestamp)) > 120 for i in (1, 2)):
                return "public funding history remains unavailable"
        state = self.budget()
        if state["equity"] <= state["initial_balance"] * (ONE - dec(self.s.config.max_portfolio_drawdown_pct)):
            return "paper portfolio loss budget reached"
        return ""

    def create(self):
        if not self.s.config.dry_run:
            raise RuntimeError("live execution is disabled pending paired-fill reconciliation")
        blocked = self._entry_block()
        candidates = []
        for token in sorted(self.s.config.tokens):
            if token in self.s.active_funding_arbitrages:
                continue
            try:
                if blocked:
                    raise MarketUnavailable(blocked)
                if token in self.s.token_permanent_blacklist and token not in self.s.probationary_tokens:
                    raise MarketUnavailable("permanent blacklist")
                if self.s.token_quarantine_ledger.get(token, {}).get("quarantine_until", 0) > self.s.current_timestamp:
                    raise MarketUnavailable("risk quarantine")
                self.s.token_quarantine_ledger.pop(token, None)
                if self.s.token_post_tp_lockout.get(token, 0) > self.s.current_timestamp:
                    raise MarketUnavailable("post-profit cooldown")
                if self.s.current_timestamp - self.s.token_last_closed_timestamp.get(token, 0) < self.s.config.reentry_cooldown_seconds:
                    raise MarketUnavailable("reentry cooldown")
                info = self.funding_info(token)
                best = self.s.get_most_profitable_combination(info)
                if best is None:
                    raise MarketUnavailable("no valid funding spread")
                c1, c2, side, expected = best
                if expected < dec(self.s.config.min_funding_rate_profitability):
                    raise MarketUnavailable("funding forecast below threshold")
                if not self.universe.signals.ready(token, (c1,c2,is_buy(side)), info,
                        self.s.current_timestamp, samples=self.s.config.funding_signal_min_snapshots,
                        seconds=self.s.config.funding_signal_min_seconds,
                        maximum_age=self.s.config.max_funding_data_age_seconds):
                    raise MarketUnavailable('awaiting independent funding snapshots')
                tier = self.s.get_token_tier(token)[0]
                blackout_applies = tier == 3 if self.s.config.settlement_blackout_tier3_only else tier > 1
                if blackout_applies and any(0 <= v.next_funding_utc_timestamp - self.s.current_timestamp <= self.s.config.settlement_blackout_minutes * 60
                                            for v in info.values() if v.interval > 3600):
                    raise MarketUnavailable("symbol-specific settlement blackout")
                target = dec(self.s.config.position_size_quote, positive=True)
                if token in self.s.probationary_tokens:
                    target = min(target, dec(self.s.config.graduation_probe_size_quote))
                target = self.capacity(token, c1, c2, side, target)
                pos = self.entry_position(token, c1, c2, side, target, info)
                funds = self.budget()
                for i in (1, 2):
                    requirement = pos[f"amount_{i}"] * pos[f"price_{i}"] / pos["leverage"] + pos[f"entry_fee_{i}"]
                    if requirement > funds["available"][pos[f"connector_{i}"]]:
                        raise MarketUnavailable("insufficient per-venue paper collateral after reserve")
                path = (c1, c2, is_buy(side))
                if self.entry_paths.get(token) != path:
                    self.s.entry_candidate_ticks[token] = 0
                self.entry_paths[token] = path
                self.s.entry_candidate_ticks[token] = self.s.entry_candidate_ticks.get(token, 0) + 1
                if self.s.entry_candidate_ticks[token] < self.s.config.entry_persistence_ticks:
                    continue
                candidates.append(pos)
            except (MarketUnavailable, KeyError, ValueError) as exc:
                if str(exc) in ('funding forecast below threshold','no valid funding spread',
                                'funding metadata missing or stale','funding schedule invalid'):
                    self.universe.signals.reset(token)
                self.reject(token, str(exc))
        for proposal in sorted(candidates, key=entry_rank):
            token = proposal['token']
            try:
                if self._entry_block():
                    raise MarketUnavailable(self._entry_block())
                if len(self.s.active_funding_arbitrages) >= self.s.config.max_concurrent_positions:
                    raise MarketUnavailable('position slot limit')
                # Reprice and reserve against the portfolio after every preceding admission.
                pos = self.universe._forecast_position(token)
                values, _, _ = self.value(pos)
                pos['valuation'], pos['valuation_status'] = values, 'valid'
                self.s.active_funding_arbitrages[token] = pos
                try:
                    self.persist()
                except Exception as exc:
                    del self.s.active_funding_arbitrages[token]
                    self.blocked_reason = f'state persistence failed: {exc}'
                    raise MarketUnavailable(self.blocked_reason) from exc
                self.s.entry_candidate_ticks[token] = 0
                if token in self.s.probationary_tokens:
                    self.s.probationary_tokens[token]['probe_executed'] = True
                self.s.logger().info('[AUDITED-PAPER-OPEN] %s ranked after fees; payback %.2fh; no real orders.',token,pos['payback_hours'])
            except (MarketUnavailable, KeyError, ValueError) as exc:
                self.reject(token, str(exc))
        return []

    def liquidation(self, token, pos):
        distances = []
        tier = self.s.get_token_tier(token)[0]
        threshold = max(dec(self.s.config.liquidation_buffer_pct),
                        dec(getattr(self.s.config, f"tier_{tier}_liquidation_buffer")), Decimal("0.08"))
        for i in (1, 2):
            connector = pos[f"connector_{i}"]
            pair = self.s.get_trading_pair_for_connector(token, connector)
            meta = self.metadata.get((connector, pair))
            if not meta or abs(self.s.current_timestamp - meta["observed_at"]) > self.s.config.max_funding_data_age_seconds:
                raise MarketUnavailable("mark price unavailable for illustrative liquidation check")
            fund = sum((dec(p["amount"]) for p in pos.get("funding_payments", []) if p.get("connector") == connector), ZERO)
            liq, distance, breach = liquidation_estimate(pos[f"price_{i}"], meta["mark_price"], pos[f"amount_{i}"],
                                                       pos["leverage"], fund, pos.get(f"entry_fee_{i}", 0),
                                                       self.s.config.paper_maintenance_margin_pct,
                                                       is_buy(pos["side"]) if i == 1 else not is_buy(pos["side"]))
            pos[f"liq_price_{i}"] = liq
            pos[f"liq_distance_{i}_pct"] = distance
            distances.append(ZERO if breach else distance)
        pos["liquidation_buffer_pct"] = threshold
        pos["safety_distance_pct"] = min(distances)
        return min(distances) <= threshold, min(distances), threshold

    def stop(self):
        now = self.s.current_timestamp
        for token, pos in list(self.s.active_funding_arbitrages.items()):
            try:
                values, fills, fees = self.value(pos)
                pos["valuation"], pos["valuation_status"] = values, "valid"
                pos["last_valid_valuation_at"] = now
                ratio = values["total_net_pnl"] / dec(pos["position_size_quote"], positive=True)
                reason = None
                try:
                    liquidate, distance, buffer = self.liquidation(token, pos)
                    if liquidate:
                        reason = f"Paper Liquidation Defense ({float(distance):.2%} <= {float(buffer):.2%}; model estimate)"
                except MarketUnavailable:
                    pos["liquidation_check_unavailable"] = True
                breach = ratio <= -dec(self.s.config.max_basis_loss_pct)
                self.s.basis_divergence_tick_counter[token] = self.s.basis_divergence_tick_counter.get(token, 0) + 1 if breach else 0
                if reason is None and self.s.basis_divergence_tick_counter[token] >= self.s.config.basis_divergence_confirmation_ticks:
                    reason = "Basis Spread Divergence Stop Loss (net after all estimated fees)"
                try:
                    info = self.funding_info(token)
                    c1, c2 = pos["connector_1"], pos["connector_2"]
                    expected = (dec(info[c2].rate) / info[c2].interval - dec(info[c1].rate) / info[c1].interval) * 86400
                    if not is_buy(pos["side"]):
                        expected = -expected
                    inverted = expected <= dec(self.s.config.funding_rate_diff_stop_loss)
                    self.s.inversion_tick_counter[token] = self.s.inversion_tick_counter.get(token, 0) + 1 if inverted else 0
                    if reason is None and self.s.inversion_tick_counter[token] >= self.s.config.rate_inversion_confirmation_ticks:
                        reason = "Rate Inversion Stop Loss"
                except MarketUnavailable:
                    pos["funding_forecast_unavailable"] = True
                if ratio >= dec(self.s.config.breakeven_trailing_activation_pct):
                    pos["breakeven_active"] = True
                if reason is None and token not in self.s.config.tokens:
                    reason = "Manual Rotation: Requested Removal"
                if reason is None and pos.get("breakeven_active") and ratio <= dec(self.s.config.breakeven_trailing_stop_pct):
                    reason = "Breakeven Trailing Stop (execution price can slip below trigger)"
                if reason is None and ratio >= dec(self.s.config.profitability_to_take_profit):
                    reason = "Take Profit"
                if reason is None and self.budget()["equity"] <= dec(self.s.config.paper_account_balance_quote) * (ONE - dec(self.s.config.max_portfolio_drawdown_pct)):
                    reason = "Paper Portfolio Loss Budget"
                if reason:
                    self.close(token, ONE, reason)
                elif self.s.config.scaling_tp_enabled and not pos.get("partial_tp_executed") and ratio >= dec(self.s.config.partial_tp_threshold_pct):
                    self.close(token, dec(self.s.config.partial_tp_ratio), "Partial Take Profit")
                    # Deliberately do not reuse pre-split PnL to close the remainder.
            except (MarketUnavailable, KeyError, ValueError) as exc:
                pos["valuation_status"] = "unavailable"
                pos["valuation_error"] = str(exc)
                self.s.entry_candidate_ticks[token] = 0
        return []

    def close(self, token, fraction, reason):
        original = self.s.active_funding_arbitrages[token]
        piece, remaining = split_position(original, fraction)
        # A partial close must be executable on both venues, including its
        # remainder. Reject dust instead of inventing fractional venue fills.
        if remaining is not None:
            for part in (piece, remaining):
                for i in (1, 2):
                    conn = self.s.connectors[part[f"connector_{i}"]]
                    pair = self.s.get_trading_pair_for_connector(token, part[f"connector_{i}"])
                    quantity = dec(part[f"amount_{i}"])
                    rule = conn.trading_rules[pair]
                    if dec(conn.quantize_order_amount(pair, quantity)) != quantity or quantity < dec(rule.min_order_size):
                        raise MarketUnavailable("partial close would leave untradeable quantity increments")
                    if quantity * dec(part[f"price_{i}"]) < dec(rule.min_notional_size):
                        raise MarketUnavailable("partial close would leave below-minimum notional")
        values, fills, fees = self.value(piece)
        if remaining is not None:
            remaining_values, _, _ = self.value(remaining)
        now = self.s.current_timestamp
        record = copy.deepcopy(piece)
        record.update({key: float(value) for key, value in values.items()})
        record.update(trade_id=uuid.uuid4().hex, model_version=MODEL_VERSION, close_reason=reason,
                      close_timestamp=now, duration_seconds=max(0, int(now - piece["open_timestamp"])),
                      position_size=float(piece["position_size_quote"]),
                      open_price_1=float(piece["price_1"]), open_price_2=float(piece["price_2"]),
                      close_price_1=float(fills[0].price), close_price_2=float(fills[1].price), funding_pending=True)
        record["venue_net_pnl"] = {}
        for i in (1, 2):
            connector = piece[f"connector_{i}"]
            funding = sum((dec(p["amount"]) for p in piece.get("funding_payments", []) if p.get("connector") == connector), ZERO)
            record["venue_net_pnl"][connector] = str(values[f"leg{i}_pnl"] - dec(piece.get(f"entry_fee_{i}", 0)) - fees[i-1] + funding)
            record[f"exit_fee_{i}"] = str(fees[i-1])
        self.s.stopped_funding_arbitrages.setdefault(token, []).append(record)
        if remaining is None:
            del self.s.active_funding_arbitrages[token]
            self.s.token_last_closed_timestamp[token] = now
            self.s.inversion_tick_counter[token] = 0
            self.s.basis_divergence_tick_counter[token] = 0
            whole_net = sum((dec(r["total_net_pnl"]) for r in self.s.stopped_funding_arbitrages[token]
                             if r.get("position_id") == original["position_id"]), ZERO)
            if token in self.s.probationary_tokens:
                self.s._handle_probation_result(token, float(whole_net), reason)
            else:
                self.s._evaluate_token_quarantine(token, float(whole_net))
        else:
            remaining["partial_tp_executed"] = True
            remaining["locked_tp_pnl"] = float(dec(original.get("locked_tp_pnl", 0)) + values["total_net_pnl"])
            remaining["valuation"] = remaining_values
            remaining["valuation_status"] = "valid"
            self.s.active_funding_arbitrages[token] = remaining
        if "Take Profit" in reason:
            self.s.token_post_tp_lockout[token] = now + self.s.config.post_tp_cooldown_hours * 3600
        self.persist()
        self.s.logger().info("[AUDITED-PAPER-CLOSE] %s %s: net %.6f; funding reconciliation may still be pending.", token, reason, values["total_net_pnl"])

    def report(self):
        now = self.s.current_timestamp
        active = []
        raw = []
        for token, pos in sorted(self.s.active_funding_arbitrages.items()):
            raw.append(copy.deepcopy(pos))
            values = pos.get("valuation", {})
            funding = sum((dec(p["amount"]) for p in pos.get("funding_payments", [])), ZERO)
            size = dec(pos["position_size_quote"])
            leverage = int(pos["leverage"])
            row = {"token": token, "position_id": pos["position_id"], "is_virtual": True,
                   "accounting_quality": pos["accounting_quality"], "tier": self.s.get_token_tier(token)[0],
                   "connector_1": pos["connector_1"], "connector_2": pos["connector_2"], "side": pos["side"],
                   "entry_price_1": float(pos["price_1"]), "entry_price_2": float(pos["price_2"]),
                   "amount_1": str(pos["amount_1"]), "amount_2": str(pos["amount_2"]),
                   "position_size": float(size), "leverage": leverage, "long_leverage": leverage, "short_leverage": leverage,
                   "initial_margin_quote": float(sum((dec(pos[f"amount_{i}"]) * dec(pos[f"price_{i}"]) / leverage for i in (1, 2)), ZERO)),
                   "entry_fee_quote": float(dec(pos.get("entry_fee_quote", 0))), "funding_income": float(funding),
                   "settlement_count": len(pos.get("funding_payments", [])), "funding_payments": pos.get("funding_payments", []),
                   "funding_pending": pos.get("funding_pending", False), "valuation_status": pos.get("valuation_status", "unavailable"),
                   "unrealized_trading_pnl": float(dec(values.get("trading_pnl", 0))),
                   "net_pnl": float(dec(values.get("trading_pnl", 0)) + funding),
                   "net_pnl_pct": float((dec(values.get("trading_pnl", 0)) + funding) / size),
                   "open_timestamp": pos["open_timestamp"], "duration_seconds": max(0, int(now - pos["open_timestamp"])),
                   "payback_hours": pos.get("payback_hours", 0), "entry_spread_pct": float(dec(pos.get("entry_profitability_after_fees", 0))),
                   "min_hold_hours": pos.get("min_hold_hours", 0), "min_hold_until": pos.get("min_hold_until", 0),
                   "remaining_hold_hours": max(0, (pos.get("min_hold_until", 0) - now) / 3600),
                   "regime_tier": pos.get("regime_tier", "LEGACY"), "partial_tp_executed": pos.get("partial_tp_executed", False),
                   "locked_tp_pnl": pos.get("locked_tp_pnl", 0), "breakeven_active": pos.get("breakeven_active", False),
                   "is_probationary_probe": pos.get("is_probationary_probe", False),
                   "liquidation_buffer_req_pct": float(dec(pos.get("liquidation_buffer_pct", self.s.config.liquidation_buffer_pct))),
                   "min_liq_distance_pct": float(dec(pos.get("safety_distance_pct", 0))), "liquidation_model": "illustrative_isolated_estimate"}
            for i in (1, 2):
                row[f"current_price_{i}"] = float(dec(pos.get(f"current_price_{i}", pos[f"price_{i}"])))
                row[f"initial_margin_{i}_quote"] = float(dec(pos[f"amount_{i}"]) * dec(pos[f"price_{i}"]) / leverage)
                row[f"liq_price_{i}"] = float(dec(pos.get(f"liq_price_{i}", 0)))
                row[f"liq_distance_{i}_pct"] = float(dec(pos.get(f"liq_distance_{i}_pct", 0)))
                meta = self.metadata.get((pos[f"connector_{i}"], self.s.get_trading_pair_for_connector(token, pos[f"connector_{i}"])), {})
                row[f"next_funding_{i}"] = int(meta.get("next_time", pos.get(f"next_funding_time_{i}", 0)))
            try:
                info = self.funding_info(token)
                daily = ZERO
                for i in (1, 2):
                    c = pos[f"connector_{i}"]
                    buy = is_buy(pos["side"]) if i == 1 else not is_buy(pos["side"])
                    daily += (-ONE if buy else ONE) * dec(pos[f"amount_{i}"]) * dec(info[c].mark_price) * dec(info[c].rate) * 86400 / info[c].interval
                row["projected_daily_funding_quote"] = float(daily)
            except MarketUnavailable:
                row["projected_daily_funding_quote"] = None
            active.append(row)
        closed = sorted((copy.deepcopy(r) for rows in self.s.stopped_funding_arbitrages.values() for r in rows),
                        key=lambda r: (r.get("close_timestamp", 0), r.get("trade_id", "")))
        realized = sum((dec(r.get("total_net_pnl", 0)) for r in closed), ZERO)
        floating = sum((dec(r["net_pnl"]) for r in active), ZERO)
        volume = sum((sum((dec(p[f"amount_{i}"]) * dec(p[f"price_{i}"]) for i in (1, 2)), ZERO) for p in raw), ZERO)
        for row in closed:
            # Legacy publisher discarded per-trade size/entry prices. Preserve
            # its stored volume rather than substituting today's default size.
            if row.get("model_version") == MODEL_VERSION:
                volume += sum((dec(row[f"amount_{i}"]) * (dec(row[f"price_{i}"]) + dec(row[f"close_price_{i}"])) for i in (1, 2)), ZERO)
            else:
                volume += dec(row.get("position_size", row.get("position_size_quote", 0))) * 4
        market = []
        for token in sorted(self.s.config.tokens):
            try:
                info = self.funding_info(token)
                row = {"token": token}
                for c, v in info.items():
                    row[f"{c}_rate_daily"] = float(dec(v.rate) / v.interval * 86400 * 100)
                    row[f"{c}_next_funding"] = int(v.next_funding_utc_timestamp)
                best = self.s.get_most_profitable_combination(info)
                if best:
                    row["best_path"], row["daily_rate_diff_pct"] = f"{best[0]} -> {best[1]}", float(best[3] * 100)
                market.append(row)
            except MarketUnavailable:
                continue
        performance = {"realized_pnl_quote": float(realized), "unrealized_pnl_quote": float(floating),
                       "global_pnl_quote": float(realized + floating), "volume_traded": float(volume),
                       "close_type_counts": dict(Counter(r.get("close_reason", "Unknown") for r in closed))}
        state = self.budget()
        if now - self.market_health_at >= 10:
            self.market_health_at = now
            self.market_health = {}
            for name, conn in self.s.connectors.items():
                good_books, good_rules, errors = [], [], {}
                for token in sorted(self.s.config.tokens):
                    pair = self.s.get_trading_pair_for_connector(token, name)
                    if pair in conn.trading_rules:
                        good_rules.append(token)
                    try:
                        self.books(name, pair)
                        good_books.append(token)
                    except (MarketUnavailable, KeyError, ValueError) as exc:
                        errors[token] = str(exc)
                self.market_health[name] = {"public_only": not getattr(conn, "is_trading_required", False),
                    "configured_pairs": len(self.s.config.tokens), "books_ready": good_books,
                    "rules_ready": good_rules, "book_errors": errors}
        return {"schema_version": SCHEMA_VERSION, "model_version": MODEL_VERSION, "is_dry_run": True,
                "leverage": self.s.config.leverage, "updated_at": now,
                "watchlist": sorted(self.s.config.tokens),
                "scaling_tp_enabled": self.s.config.scaling_tp_enabled,
                "partial_tp_ratio": float(self.s.config.partial_tp_ratio),
                "partial_tp_threshold_pct": float(self.s.config.partial_tp_threshold_pct),
                "profitability_to_take_profit": float(self.s.config.profitability_to_take_profit),
                "min_funding_rate_profitability": float(self.s.config.min_funding_rate_profitability),
                "min_funding_rate_profitability_pct": float(self.s.config.min_funding_rate_profitability * 100),
                "active_positions": active, "raw_positions": raw, "closed_trades": closed, "market_comparison": market,
                "quarantined_tokens": copy.deepcopy(self.s.token_quarantine_ledger),
                "permanent_blacklist": sorted(self.s.token_permanent_blacklist), "probationary_tokens": copy.deepcopy(self.s.probationary_tokens),
                "post_tp_lockout": {t: {"lockout_until": ts, "remaining_seconds": max(0, ts-now)} for t, ts in self.s.token_post_tp_lockout.items()},
                "rotation_status": {"auto_rotation_enabled": self.s.config.auto_rotation_enabled,
                                    "discovery": self.universe.report(),
                                    "rotation_events": self.s.rotation_event_history, "last_rotation_timestamp": self.s.last_rotation_timestamp},
                "graduation_status": {"asset_graduation_enabled": self.s.config.asset_graduation_enabled},
                "performance": performance, "paper_account": state,
                "audit": {"started_at": self.audit_started_at, "blocked_reason": self.blocked_reason,
                          "data_refresh": dict(self.metadata_status),
                          "mqtt_delivery": self.mqtt_status,
                          "public_connectors": copy.deepcopy(self.market_health),
                          "rejections": dict(self.rejections), "last_rejection": copy.deepcopy(self.last_rejection),
                          "valid_valuation": all(p["valuation_status"] == "valid" for p in active),
                          "funding_note": "Confirmed historical rates; non-historical reference prices are explicitly estimated. No real payments.",
                          "legacy_history_preserved": True}}

    def persist(self):
        if self.restore_failed:
            return
        data = self.report()
        atomic_json(self.path, data, owner=(1000, 1000))
        self.last_report = data
        self.last_persist = self.s.current_timestamp

    def publish(self):
        try:
            self.persist()
        except Exception as exc:
            self.blocked_reason = f"state persistence failed: {exc}"
            self.s.logger().error("[PERSIST-BLOCKED] %s", exc)
            return
        data = self.last_report
        if not data:
            return
        if not getattr(self.s, "_pub", None):
            if self.s.current_timestamp < self.mqtt_retry_at:
                return
            self.mqtt_retry_at = self.s.current_timestamp + 5
            try:
                # Headless quickstart may initialize MQTT after strategy.start.
                from hummingbot.remote_iface.mqtt import ETopicPublisher
                self.s._pub = ETopicPublisher("performance", use_bot_prefix=True)
                self.s.mqtt_enabled = True
            except Exception:
                self.mqtt_status = "waiting"
                return
        try:
            payload = {"funding_rate_arb": {"controller_name": "funding_rate_arb", "performance": data["performance"], "custom_info": data}}
            self.s._pub(json.loads(json.dumps(payload, default=json_value, allow_nan=False)))
            self.mqtt_status = "published"
        except Exception as exc:
            self.mqtt_status = "unavailable"
            self.s._pub = None
            self.mqtt_retry_at = self.s.current_timestamp + 5
            self.s.logger().warning("[MQTT-PENDING] %s; local ledger remains authoritative.", exc)

    def tick(self):
        self.refresh()
        # Risk and exits run even when a different token's data is unavailable.
        self.stop()
        if not getattr(self.s, "_is_stop_triggered", False):
            self.s.check_strategy_commands()
            self.s.check_universe_auto_rotation()
            self.s.check_asset_graduation()
            self.create()
        self.publish()
