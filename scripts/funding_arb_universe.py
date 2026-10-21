"""Bounded discovery, independent funding confirmation and safe market rotation.

Connectors keep ownership of subscriptions. This component never places orders.
Wide screening is provisional; admission requires the paper adapter's complete
executable-book, quantity, cost, payback and collateral checks.
"""
from __future__ import annotations

import asyncio
from collections import Counter, deque
from decimal import Decimal
import time

from scripts import funding_arb_public_data as public
from scripts.funding_arb_accounting import MarketUnavailable, ONE, ZERO, dec


class FundingSignals:
    def __init__(self):
        self.rows = {}

    def reset(self, token):
        self.rows.pop(token, None)

    def ready(self, token, path, info, now, *, samples, seconds, maximum_age):
        version = tuple((name, float(row.observed_at)) for name, row in sorted(info.items()))
        if len(version) != 2:
            self.reset(token)
            return False
        # Both venues must provide a new observation, not just a different loop tick.
        observed = min(stamp for _, stamp in version)
        if any(not 0 <= now-stamp <= maximum_age for _, stamp in version):
            self.reset(token)
            return False
        state = self.rows.get(token)
        if state is None or state['path'] != path:
            state = self.rows[token] = dict(path=path, versions=deque(maxlen=samples))
        rows = state['versions']
        if rows and any(stamp <= dict(rows[-1][0])[name] for name, stamp in version):
            if version != rows[-1][0] and any(stamp < dict(rows[-1][0])[name] for name, stamp in version):
                self.reset(token)
                return False
            return len(rows) >= samples and rows[-1][1]-rows[0][1] >= seconds
        if rows and observed-rows[-1][1] > maximum_age:
            rows.clear()
        rows.append((version, observed))
        return len(rows) >= samples and rows[-1][1]-rows[0][1] >= seconds


def entry_rank(position):
    """Higher conservative forecast return, then dollars; deterministic ties."""
    margin = dec(position['initial_margin_quote']) + dec(position['entry_fee_quote'])
    if margin <= ZERO:
        raise MarketUnavailable('invalid candidate margin')
    net = dec(position['selection_expected_net_quote'])
    return (-net/margin, -net, dec(position['payback_hours']), position['token'])


class FundingUniverse:
    def __init__(self, adapter):
        self.a, self.s = adapter, adapter.s
        self.signals = FundingSignals()
        self.last_attempt = 0.0
        self.running = False
        self.volumes, self.volumes_at = {}, 0.0
        self.admitted_at, self.cooldowns = {}, {}
        self.pending_mounts = set()
        self.cleanup_pending = {}
        self.mutation_lock = asyncio.Lock()
        self.status = dict(phase='waiting', last_success_at=None, candidates=[], rejections={}, recent_decisions=[])

    def schedule(self):
        if not self.s.config.auto_rotation_enabled or self.running:
            return
        now = self.s.current_timestamp
        if now-self.last_attempt < float(self.s.config.auto_rotation_interval_minutes)*60:
            return
        self.last_attempt = now
        # Set before scheduling; two ticks cannot launch overlapping scans.
        self.running = True
        self.a._spawn(self.scan(_scheduled=True))

    def eligible(self, token):
        now = self.s.current_timestamp
        return (token not in self.s.token_permanent_blacklist
                and self.s.token_quarantine_ledger.get(token, {}).get('quarantine_until', 0) <= now
                and self.s.token_post_tp_lockout.get(token, 0) <= now
                and self.cooldowns.get(token, 0) <= now)

    def fetch_candidates(self):
        """Pure screen over cached public data; no extra per-token HTTP calls."""
        snapshot, now = self.a.metadata, self.s.current_timestamp
        result, reasons, shared = [], Counter(), 0
        minimum = max(dec(self.s.config.min_funding_rate_profitability),
                      dec(self.s.config.auto_rotation_min_apr)/365)
        for (connector, pair), h in snapshot.items():
            if connector != public.HYPERLIQUID:
                continue
            token = self.s.get_canonical_token(pair.rsplit('-', 1)[0], connector)
            hp = self.s.get_trading_pair_for_connector(token, connector)
            bp = self.s.get_trading_pair_for_connector(token, public.BINANCE)
            b = snapshot.get((public.BINANCE, bp))
            if pair != hp or b is None:
                reasons['unsupported symbol mapping'] += 1
                continue
            shared += 1
            if not self.eligible(token):
                reasons['quarantine, blacklist or cooldown'] += 1
                continue
            try:
                for row in (h, b):
                    if not 0 <= now-float(row['observed_at']) <= self.s.config.max_funding_data_age_seconds:
                        raise MarketUnavailable('stale metadata')
                    if not 0 <= float(row['next_time'])-now <= row['interval']+120:
                        raise MarketUnavailable('invalid settlement schedule')
                if dec(self.volumes.get(bp, 0)) < dec(self.s.config.auto_rotation_min_volume_24h_usd):
                    raise MarketUnavailable('Binance volume below minimum')
                if dec(h.get('volume_24h', 0)) < dec(self.s.config.universe_min_secondary_volume_quote):
                    raise MarketUnavailable('secondary venue volume below minimum')
                ph = dec(h['mark_price'])/dec(self.s.get_token_multiplier(token, connector))
                pb = dec(b['mark_price'])/dec(self.s.get_token_multiplier(token, public.BINANCE))
                if abs(ph-pb)/pb > min(dec(self.s.config.max_allowed_price_divergence),dec(self.s.config.max_entry_spread_pct)):
                    raise MarketUnavailable('mark basis exceeds screen limit')
                daily = abs(dec(h['rate'])/dec(h['interval'])-dec(b['rate'])/dec(b['interval']))*86400
                if daily < minimum:
                    raise MarketUnavailable('funding forecast below admission threshold')
                result.append(dict(token=token, apr=float(daily*365*100), daily_diff=float(daily*100)))
            except (MarketUnavailable, KeyError, ZeroDivisionError) as exc:
                reasons[str(exc)] += 1
        self.status['rejections'] = dict(reasons)
        self.status['shared_markets'] = shared
        return sorted(result, key=lambda r: (-r['daily_diff'], r['token']))

    def _book_ready(self, token):
        for name, connector in self.s.connectors.items():
            pair = self.s.get_trading_pair_for_connector(token, name)
            if pair not in connector.trading_rules:
                raise MarketUnavailable('new market trading rules unavailable')
            self.a.books(name, pair)

    async def _detach(self, token, names):
        if not names:
            return token not in self.cleanup_pending
        errors = []
        for name in reversed(names):
            connector = self.s.connectors[name]
            pair = self.s.get_trading_pair_for_connector(token, name)
            try:
                method = getattr(connector, 'remove_trading_pair', None)
                if method is None or not await asyncio.wait_for(method(pair), self.s.config.universe_mount_timeout_seconds):
                    errors.append(name)
            except Exception:
                errors.append(name)
        if errors:
            self.cleanup_pending[token] = errors
        else:
            self.cleanup_pending.pop(token, None)
        return not errors

    async def mount(self, token):
        """Stage both legs and validate actual books, rollback only new mounts."""
        added = []
        if self.cleanup_pending:
            raise MarketUnavailable('previous market cleanup pending')
        self.pending_mounts.add(token)
        try:
            for name, connector in self.s.connectors.items():
                pair = self.s.get_trading_pair_for_connector(token, name)
                # Trackers can retain a half-mounted pair in trading_pairs.
                try:
                    self.a.books(name, pair)
                    continue
                except (MarketUnavailable, KeyError, ValueError):
                    pass
                method = getattr(connector, 'add_trading_pair', None)
                if method is None:
                    raise MarketUnavailable(f'{name}: dynamic market subscription unsupported')
                added.append(name)  # include a partially completed/failed call in rollback
                if not await asyncio.wait_for(method(pair), self.s.config.universe_mount_timeout_seconds):
                    raise MarketUnavailable(f'{name}: market subscription failed')
            deadline = time.monotonic()+self.s.config.universe_mount_timeout_seconds
            while True:
                try:
                    self._book_ready(token)
                    break
                except (MarketUnavailable, KeyError, ValueError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(.25)
            return added
        except BaseException:
            await self._detach(token, added)
            raise
        finally:
            self.pending_mounts.discard(token)

    async def add(self, token):
        async with self.mutation_lock:
            return await self._add(token)

    async def _add(self, token):
        token = self.s.get_canonical_token(token).upper()
        if token in self.s.config.tokens:
            return True
        added = []
        try:
            added = await self.mount(token)
            self.s.config.tokens = set(self.s.config.tokens) | {token}
            previous_admitted = dict(self.admitted_at)
            self.admitted_at[token] = self.s.current_timestamp
            try:
                self.a.persist()
            except Exception:
                self.s.config.tokens.discard(token)
                self.admitted_at = previous_admitted
                raise
            self.s.stopped_funding_arbitrages.setdefault(token, [])
            return True
        except asyncio.CancelledError:
            await self._detach(token, added)
            raise
        except Exception as exc:
            await self._detach(token, added)
            self.status['last_error'] = str(exc)
            return False

    async def remove(self, token):
        async with self.mutation_lock:
            return await self._remove(token)

    async def _remove(self, token):
        token = self.s.get_canonical_token(token).upper()
        if token in self.s.active_funding_arbitrages:
            return False
        before = set(self.s.config.tokens)
        self.s.config.tokens = before-{token}
        try:
            self.a.persist()
        except Exception:
            self.s.config.tokens = before
            raise
        self.signals.reset(token)
        await self._detach(token, list(self.s.connectors))
        return True

    def _forecast_position(self, token):
        info = self.a.funding_info(token)
        best = self.s.get_most_profitable_combination(info)
        if best is None or dec(best[3]) < dec(self.s.config.min_funding_rate_profitability):
            raise MarketUnavailable('funding forecast below admission threshold')
        c1, c2, side, _ = best
        target = self.a.capacity(token, c1, c2, side, dec(self.s.config.position_size_quote))
        if token in self.s.probationary_tokens:
            target = min(target,dec(self.s.config.graduation_probe_size_quote))
        pos = self.a.entry_position(token, c1, c2, side, target, info)
        for i in (1, 2):
            required = pos[f'amount_{i}']*pos[f'price_{i}']/pos['leverage']+pos[f'entry_fee_{i}']
            if required > self.a.budget()['available'][pos[f'connector_{i}']]:
                raise MarketUnavailable('insufficient per-venue collateral for candidate')
        return pos

    def _retire_candidate(self, candidate):
        now = self.s.current_timestamp
        choices = []
        for token in sorted(self.s.config.tokens):
            if token in self.s.active_funding_arbitrages:
                continue
            if now-self.admitted_at.get(token, 0) < self.s.config.universe_min_residence_seconds:
                continue
            # Unknown data is not a zero-yield score and never justifies eviction.
            try:
                info = self.a.funding_info(token)
                best = self.s.get_most_profitable_combination(info)
                daily = dec(best[3]) if best else ZERO
                apr = daily*365  # Decimal throughout; stored display is percent
                old = None
                if daily >= dec(self.s.config.min_funding_rate_profitability):
                    try:
                        old = self._forecast_position(token)
                    except MarketUnavailable as exc:
                        # Confirmed economic rejection differs from unknown/missing market data.
                        if str(exc) not in ('full round-trip payback exceeds limit',
                                            'full round-trip friction exceeds limit',
                                            'entry basis exceeds configured funding-strategy limit'):
                            raise
                old_score = entry_rank(old)[0] if old else ZERO
            except (MarketUnavailable, KeyError, ValueError):
                continue
            # A lower gross APR can be better after costs. Hysteresis therefore
            # compares annualized 12h net capital return, not the raw rate spread.
            if old is not None:
                new_rank, old_rank = entry_rank(candidate), entry_rank(old)
                if new_rank[:3] >= old_rank[:3]:
                    continue
                advantage = (-new_rank[0]+old_rank[0])*365*2
                if advantage < dec(self.s.config.auto_rotation_min_apr_delta):
                    continue
            choices.append((old_score, -float(apr), token))
        return max(choices)[2] if choices else None

    async def scan(self, force=False, _scheduled=False):
        if self.running and not _scheduled:
            return
        self.running = True
        try:
            async with self.mutation_lock:
                await self._scan(force)
        finally:
            self.running, self.s.is_rotating = False, False

    async def _scan(self, force=False):
        self.running, self.s.is_rotating = True, True
        self.status['phase'] = 'scanning'
        now = self.s.current_timestamp
        try:
            for token,names in list(self.cleanup_pending.items()):
                if token in self.s.config.tokens or token in self.s.active_funding_arbitrages:
                    raise MarketUnavailable('cleanup target belongs to current portfolio')
                await self._detach(token,names)
            if self.cleanup_pending:
                raise MarketUnavailable('previous market cleanup pending')
            if not self.a.metadata:
                raise MarketUnavailable('waiting for public funding metadata')
            if now-self.volumes_at >= self.s.config.universe_volume_refresh_seconds or not self.volumes:
                self.volumes = await asyncio.to_thread(public.volume_snapshot)
                self.volumes_at = now
            candidates = self.fetch_candidates()
            self.status.update(candidate_count=len(candidates), candidates=candidates[:20],
                               public_markets=len(self.a.metadata), last_scan_at=now)
            changes, attempted, admission_errors = 0, 0, Counter()
            for row in candidates:
                token = row['token']
                if token in self.s.config.tokens or not self.eligible(token):
                    continue
                if attempted >= self.s.config.universe_max_probes_per_scan or changes >= self.s.config.universe_max_rotations_per_scan:
                    break
                attempted += 1
                added, committed = [], False
                try:
                    added = await self.mount(token)
                    if not self.eligible(token):
                        raise MarketUnavailable('candidate became ineligible while loading markets')
                    candidate = self._forecast_position(token)
                    before = set(self.s.config.tokens)
                    retired = None
                    if len(before) >= self.s.config.auto_rotation_max_active_tokens:
                        retired = self._retire_candidate(candidate)
                        if retired is None:
                            raise MarketUnavailable('no inferior idle market eligible for replacement')
                    # Recheck after awaits: an existing position must not lose its subscription.
                    if retired in self.s.active_funding_arbitrages:
                        raise MarketUnavailable('retirement target now has a position')
                    self.s.config.tokens = (before-{retired}) | {token}
                    event = dict(timestamp=now, added_token=token, retired_token=retired or 'NONE (Slot Expansion)',
                        added_apr=float(dec(candidate['expected_profitability'])*365*100),
                        expected_net_quote=float(candidate['selection_expected_net_quote']),
                        reason='Book-validated funding universe admission; no order placed')
                    prior_events = list(self.s.rotation_event_history)
                    prior_admitted = dict(self.admitted_at)
                    prior_cooldowns = dict(self.cooldowns)
                    prior_rotation = self.s.last_rotation_timestamp
                    self.s.rotation_event_history = [event]+prior_events[:24]
                    self.admitted_at[token] = now
                    if retired:
                        self.cooldowns[retired] = now+self.s.config.universe_failed_candidate_cooldown_seconds
                    self.s.last_rotation_timestamp = now
                    try:
                        self.a.persist()
                    except Exception:
                        self.s.config.tokens=before
                        self.s.rotation_event_history=prior_events
                        self.admitted_at=prior_admitted
                        self.cooldowns=prior_cooldowns
                        self.s.last_rotation_timestamp=prior_rotation
                        raise
                    committed=True
                    self.s.stopped_funding_arbitrages.setdefault(token,[])
                    if retired:
                        self.signals.reset(retired)
                        await self._detach(retired,list(self.s.connectors))
                    changes += 1
                    self.status['recent_decisions'] = [dict(token=token, decision='admitted', timestamp=now)]+self.status['recent_decisions'][:19]
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    admission_errors[str(exc)] += 1
                    self.cooldowns[token]=now+self.s.config.universe_failed_candidate_cooldown_seconds
                    self.status['recent_decisions'] = [dict(token=token, decision='rejected', reason=str(exc),
                        timestamp=now, cooldown_until=self.cooldowns[token])]+self.status['recent_decisions'][:19]
                finally:
                    if not committed:
                        await self._detach(token,added)
            self.status.update(phase='ready', last_success_at=now, admission_rejections=dict(admission_errors),
                               attempted_probes=attempted, changes=changes, last_error=None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.status.update(phase='unavailable', last_error=str(exc))
        finally:
            self.running, self.s.is_rotating = False, False

    def report(self):
        return dict(self.status, enabled=self.s.config.auto_rotation_enabled,
            interval_seconds=float(self.s.config.auto_rotation_interval_minutes)*60,
            pending_mounts=sorted(self.pending_mounts),
            cleanup_pending={token:list(names) for token,names in self.cleanup_pending.items()},
            state=dict(admitted_at=dict(self.admitted_at), cooldowns=dict(self.cooldowns)))
