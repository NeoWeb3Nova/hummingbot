"""Regression cases for the actual custom strategy adapter; no orders/network.

Run in the pinned Hummingbot image with this checkout's scripts mounted.
The books, venue rules and historical API replies are controlled fixtures;
accounting, lifecycle, configuration and persistence are production code.
"""
import asyncio
import copy
import json
import logging
import tempfile
import time
import unittest
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch, AsyncMock, Mock

import pandas as pd
from pydantic import ValidationError
from hummingbot.core.data_type.common import TradeType
from scripts import funding_arb_public_data as public
from scripts.funding_arb_accounting import (
    Fill, MarketUnavailable, atomic_json, dec, fill_base, fill_quote,
    funding_payment, liquidation_estimate, matched_quantities, position_value,
    safe_capacity, split_position,
)
from scripts.funding_arb_safety import FundingArbSafety
from scripts.v2_funding_rate_arb import FundingRateArbitrage, FundingRateArbitrageConfig

D = Decimal
BN, HL = public.BINANCE, public.HYPERLIQUID
NOW = 1800001000


class Connector:
    def __init__(self, name):
        self.name, self.ready = name, True
        self.trading_pairs = ['BTC-USDT' if name == BN else 'BTC-USD']
        self.trading_rules = {p: NS(min_order_size=D('.001'), min_notional_size=D('5')) for p in self.trading_pairs}
        self.calls = []

    def quantize_order_amount(self, pair, amount):
        return dec(amount).quantize(D('.001'), rounding=ROUND_DOWN)

    def get_fee(self, **kwargs):
        self.calls.append(kwargs)
        return NS(percent=D('.0005'), percent_token=None, flat_fees=[])

    def buy(self, *args, **kwargs):
        raise AssertionError('paper must never submit an order')

    sell = buy


class Book:
    last_diff_uid = 1
    snapshot_uid = 1

    def __init__(self, bid='99.99', ask='100.01', amount='10000'):
        self.bid, self.ask, self.amount = bid, ask, amount

    @property
    def snapshot(self):
        return (pd.DataFrame({'price': [self.bid], 'amount': [self.amount]}),
                pd.DataFrame({'price': [self.ask], 'amount': [self.amount]}))


class Fixture:
    get_trading_pair_for_connector = FundingRateArbitrage.get_trading_pair_for_connector
    get_token_multiplier = FundingRateArbitrage.get_token_multiplier
    get_token_tier = FundingRateArbitrage.get_token_tier
    get_effective_leverage = FundingRateArbitrage.get_effective_leverage
    get_canonical_token = FundingRateArbitrage.get_canonical_token
    get_normalized_funding_rate_in_seconds = FundingRateArbitrage.get_normalized_funding_rate_in_seconds
    get_most_profitable_combination = FundingRateArbitrage.get_most_profitable_combination
    _handle_probation_result = FundingRateArbitrage._handle_probation_result
    _evaluate_token_quarantine = FundingRateArbitrage._evaluate_token_quarantine
    funding_profitability_interval = 86400

    def __init__(self, path, **config):
        self.config = FundingRateArbitrageConfig(tokens={'BTC'}, paper_state_path=str(path),
            position_size_quote=D('1000'), entry_persistence_ticks=2,
            funding_signal_min_snapshots=1, funding_signal_min_seconds=0, **config)
        self.current_timestamp = NOW
        self.connectors = {c: Connector(c) for c in (BN, HL)}
        self.books = {c: Book() for c in (BN, HL)}
        self.market_data_provider = NS(get_order_book=lambda c, p: self.books[c])
        self.active_funding_arbitrages = {}
        self.stopped_funding_arbitrages = {}
        self.token_quarantine_ledger = {}
        self.inversion_tick_counter = {}
        self.token_last_closed_timestamp = {}
        self.entry_candidate_ticks = {}
        self.rotation_event_history = []
        self.last_rotation_timestamp = 0
        self.token_post_tp_lockout = {}
        self.token_permanent_blacklist = set()
        self.probationary_tokens = {}
        self.basis_divergence_tick_counter = {}
        self.audit = FundingArbSafety(self)
        for c in (BN, HL):
            pair = self.get_trading_pair_for_connector('BTC', c)
            self.audit.metadata[(c, pair)] = dict(rate=D('.001') if c == BN else D('0'),
                interval=3600, next_time=NOW+100, observed_at=NOW, mark_price=D('100'), index_price=D('100'))

    def logger(self):
        return logging.getLogger('audit-fixture')

    def open(self):
        self.audit.create()
        self.audit.create()
        return self.active_funding_arbitrages['BTC']


class PrimitiveTests(unittest.TestCase):
    def test_vwap_not_last_price_and_no_invented_liquidity(self):
        rows = [(D('100'), D('1')), (D('110'), D('2'))]
        self.assertEqual(fill_base(rows, D('2')).price, D('105'))
        self.assertEqual(fill_quote(rows, D('210')).amount, D('2'))
        with self.assertRaises(MarketUnavailable):
            fill_base(rows, 4)
        with self.assertRaises(MarketUnavailable):
            fill_quote([], 100)

    def test_invalid_values_rejected(self):
        for value in ('NaN', 'Infinity', '-Infinity', None):
            with self.assertRaises(MarketUnavailable):
                dec(value)

    def test_depth_cap_excludes_remote_levels(self):
        rows = [(D('100'), D('2')), (D('200'), D('100000'))]
        self.assertEqual(safe_capacity(rows, '.15', '.002'), D('30'))
        with self.assertRaises(MarketUnavailable):
            safe_capacity(rows, '.31', '.002')

    def test_base_matching_and_contract_multiplier(self):
        q = lambda v: v.quantize(D('.000001'), rounding=ROUND_DOWN)
        a, b = matched_quantities(1000, 100, 110, 1, 1, q, q, '.001')
        self.assertEqual(a, b)
        self.assertLessEqual(b*110, 1000)
        a, b = matched_quantities(1000, '.01', '10', 1, 1000, q, q, '.001')
        self.assertEqual(a, b*1000)

    def test_four_fees_funding_sign_and_split_conservation(self):
        pos = dict(side='BUY', amount_1=D('10'), amount_2=D('10'), price_1=D('100'), price_2=D('100'),
                   position_size_quote=D('1000'), entry_fee_quote=D('1'), entry_fee_1=D('.5'), entry_fee_2=D('.5'),
                   funding_payments=[dict(amount='2', quantity='10', rate='.002', event_id='history:1')])
        fill = Fill(D('10'), D('1000'), D('100'), D('100'))
        value = position_value(pos, fill, fill, '.5', '.5')
        self.assertEqual(value['trading_pnl'], D('-2'))
        self.assertEqual(value['total_net_pnl'], D('0'))
        self.assertEqual(funding_payment(side='SELL', quantity=10, price=100, rate='.002'), D('2'))
        self.assertEqual(funding_payment(side='BUY', quantity=10, price=100, rate='.002'), D('-2'))
        closed, left = split_position(pos, '.4')
        self.assertEqual(closed['entry_fee_quote']+left['entry_fee_quote'], pos['entry_fee_quote'])
        self.assertEqual(D(closed['funding_payments'][0]['amount'])+D(left['funding_payments'][0]['amount']), D('2'))
        self.assertEqual(closed['funding_payments'][0]['event_id'], 'history:1')

    def test_one_x_short_can_liquidate(self):
        _, distance, breached = liquidation_estimate(100, 205, 10, 1, 0, 0, '.02', False)
        self.assertTrue(breached)
        self.assertLess(distance, 0)


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'state.json'
        self.s = Fixture(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_config_rejects_project_rule_violations(self):
        for invalid in ({'dry_run': False}, {'max_payback_hours': 24}, {'max_orderbook_depth_ratio': '.4'},
                        {'leverage':20}, {'tier_1_liquidation_buffer':'.04'}, {'position_size_quote':'NaN'},
                        {'connectors':{'bybit_perpetual'}}, {'entry_persistence_ticks':0}):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                FundingRateArbitrageConfig(**invalid)

    def test_full_lifecycle_costs_restart_and_history(self):
        pos = self.s.open()
        self.assertEqual(pos['amount_1'], pos['amount_2'])
        self.assertEqual(pos['entry_fee_quote'], pos['entry_fee_1']+pos['entry_fee_2'])
        report = self.s.audit.report()
        self.assertLess(report['performance']['global_pnl_quote'], 0)
        self.assertAlmostEqual(report['active_positions'][0]['net_pnl'], float(pos['valuation']['total_net_pnl']))
        self.s.audit.persist()
        restored = Fixture(self.path)
        restored.audit.restore(NOW+1)
        self.assertFalse(restored.audit.blocked_reason)
        for key in ('amount_1','amount_2','entry_fee_quote','leverage','open_timestamp'):
            self.assertEqual(dec(restored.active_funding_arbitrages['BTC'][key]), dec(pos[key]))
        self.s.audit.close('BTC', D('1'), 'Manual Removal')
        closed = self.s.stopped_funding_arbitrages['BTC'][0]
        self.assertAlmostEqual(closed['total_net_pnl'], float(pos['valuation']['total_net_pnl']))
        self.assertAlmostEqual(sum(map(float, closed['venue_net_pnl'].values())), closed['total_net_pnl'])
        self.assertEqual(len(self.s.audit.report()['closed_trades']), 1)

    def test_mqtt_payload_and_local_state_use_same_serializable_schema(self):
        self.s.open()
        messages = []
        self.s._pub = lambda payload: messages.append(json.loads(json.dumps(payload)))
        self.s.audit.publish()
        self.assertFalse(self.s.audit.blocked_reason)
        self.assertEqual(messages[0]['funding_rate_arb']['custom_info'],json.loads(self.path.read_text()))

    def test_mqtt_initialization_retries_after_headless_start(self):
        messages=[]
        with patch('hummingbot.remote_iface.mqtt.ETopicPublisher',side_effect=[RuntimeError('not ready'),lambda p:messages.append(p)]):
            self.s.audit.publish()
            self.assertFalse(self.s.audit.blocked_reason)
            self.s.current_timestamp+=6
            self.s.audit.publish()
        self.assertEqual(len(messages),1)
        self.assertEqual(self.s.audit.mqtt_status,'published')

    def test_mqtt_disconnect_does_not_masquerade_as_disk_failure(self):
        self.s.open()
        self.s._pub=Mock(side_effect=ConnectionError('broker temporarily unavailable'))
        self.s.audit.publish()
        self.assertTrue(self.path.exists())
        self.assertFalse(self.s.audit.blocked_reason)
        self.assertEqual(self.s.audit.mqtt_status,'unavailable')

    def test_forecast_direction_change_resets_persistence(self):
        self.s.audit.create()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.s.audit.metadata[(BN,'BTC-USDT')]['rate'] = D('-.001')
        self.s.audit.create()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.s.audit.create()
        self.assertTrue(self.s.active_funding_arbitrages)

    def test_missing_leg_depth_does_not_force_minimum_or_fake_price(self):
        self.s.books[HL].amount = '.01'
        self.s.audit.create()
        self.s.audit.create()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.assertIn('safe depth capacity below minimum; never force minimum size', self.s.audit.rejections)
        with self.assertRaises(MarketUnavailable):
            self.s.audit.quote(HL, 'BTC-USD', buy=True, amount=100)

    def test_no_capital_reuse_across_venues(self):
        self.s.config.paper_account_balance_quote = D('200')
        self.s.audit.create()
        self.s.audit.create()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.assertIn('insufficient per-venue paper collateral after reserve', self.s.audit.rejections)

    def test_stale_book_blocks_entry(self):
        self.s.audit.books(BN, 'BTC-USDT')
        self.s.audit.quote_seen[(BN,'BTC-USDT')] = ((1,1), time.monotonic()-100)
        with self.assertRaises(MarketUnavailable):
            self.s.audit.books(BN, 'BTC-USDT')

    def test_partial_close_preserves_fees_and_no_double_close(self):
        # 999.99999 cap yields exactly 9.998 after exchange quantization.
        self.s.config.position_size_quote = D('999.95')
        pos = self.s.open()
        before_fee, before_qty = pos['entry_fee_quote'], pos['amount_1']
        # Use an executable fraction for this venue's quantity increments.
        self.assertEqual(before_qty % D('.002'), 0)
        self.s.audit.close('BTC', D('.5'), 'Partial Take Profit')
        left = self.s.active_funding_arbitrages['BTC']
        closed = self.s.stopped_funding_arbitrages['BTC'][0]
        self.assertEqual(dec(closed['entry_fee_quote'])+left['entry_fee_quote'], before_fee)
        self.assertEqual(dec(closed['amount_1'])+left['amount_1'], before_qty)
        self.assertTrue(left['partial_tp_executed'])
        self.assertEqual(len(self.s.stopped_funding_arbitrages['BTC']), 1)

    def test_partial_close_rejects_dust_without_changing_position(self):
        pos = self.s.open()
        before = copy.deepcopy(pos)
        with self.assertRaises(MarketUnavailable):
            self.s.audit.close('BTC', D('.5'), 'Partial Take Profit')
        self.assertEqual(self.s.active_funding_arbitrages['BTC'], before)
        self.assertFalse(self.s.stopped_funding_arbitrages.get('BTC'))

    def test_persistence_failure_does_not_accept_new_position(self):
        with patch.object(self.s.audit, 'persist', side_effect=OSError('disk full')):
            self.s.audit.create()
            self.s.audit.create()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.assertIn('state persistence failed', self.s.audit.blocked_reason)

    def test_live_ledger_cannot_be_restored_or_overwritten(self):
        original = '{"is_dry_run": false, "closed_trades": []}'
        self.path.write_text(original)
        self.s.audit.restore(NOW)
        self.s.audit.publish()
        self.assertEqual(self.path.read_text(), original)

    def test_legacy_snapshot_migrates_without_rewriting_closed_history(self):
        old_closed = dict(token='BTC',close_timestamp=NOW-50,open_timestamp=NOW-500,
                          trading_pnl=-2.1,funding_pnl=.5,total_net_pnl=-1.6,position_size=700,
                          leverage=3,open_price_1=0.0,open_price_2=0.0,close_reason='Legacy stop')
        legacy = dict(is_dry_run=True,closed_trades=[old_closed],active_positions=[
            dict(token='BTC',connector_1=BN,connector_2=HL,side='SELL',entry_price_1=100,
                 entry_price_2=101,position_size=700,leverage=1,duration_seconds=100,
                 funding_payments=[dict(timestamp=NOW-20,connector=BN,amount=.3)])])
        atomic_json(self.path,legacy)
        self.s.audit.restore(NOW)
        self.assertFalse(self.s.audit.blocked_reason)
        self.s.audit.persist()
        result = json.loads(self.path.read_text())
        self.assertEqual(result['closed_trades'],[old_closed])
        pos = result['raw_positions'][0]
        self.assertEqual(pos['accounting_quality'],'legacy_reconstructed')
        self.assertEqual(pos['funding_history_start'],NOW)
        self.assertEqual(pos['funding_payments'][0]['source'],'legacy_unverified')

    def test_tracking_receipt_age_checked_on_first_quote(self):
        self.s.connectors[BN].order_book_tracker = NS(metrics=NS(per_pair_metrics={
            'BTC-USDT': NS(last_diff_timestamp=time.perf_counter()-100, last_snapshot_timestamp=0)}))
        with self.assertRaises(MarketUnavailable):
            self.s.audit.books(BN, 'BTC-USDT')

    def test_failed_native_funding_listener_does_not_block_valid_public_book(self):
        conn = self.s.connectors[BN]
        conn.ready = False
        conn.is_trading_required = False
        conn.status_dict = dict(symbols_mapping_initialized=True,funding_info=False,order_books_initialized=False)
        self.assertTrue(self.s.audit.books(BN,'BTC-USDT'))
        conn.status_dict['symbols_mapping_initialized'] = False
        with self.assertRaises(MarketUnavailable):
            self.s.audit.books(BN,'BTC-USDT')

    def test_clock_tick_runs_audit_while_global_connector_readiness_is_false(self):
        strategy = NS(_is_stop_triggered=False,on_tick=Mock())
        FundingRateArbitrage.tick(strategy,NOW)
        strategy.on_tick.assert_called_once()
        strategy._is_stop_triggered = True
        FundingRateArbitrage.tick(strategy,NOW+1)
        strategy.on_tick.assert_called_once()

    def test_corrupt_state_preserved_and_entries_blocked(self):
        self.path.write_text('{broken')
        self.s.audit.restore(NOW)
        self.s.audit.publish()
        self.s.audit.create()
        self.assertEqual(self.path.read_text(), '{broken')
        self.assertFalse(self.s.active_funding_arbitrages)

    def test_cooldown_and_risk_ledgers_survive_restart(self):
        self.s.token_post_tp_lockout['BTC'] = NOW+1000
        self.s.token_permanent_blacklist.add('BAD')
        self.s.token_quarantine_ledger['BAD'] = dict(quarantine_until=NOW+2000, reason='loss')
        self.s.audit.persist()
        restored = Fixture(self.path)
        restored.audit.restore(NOW+100)
        self.assertEqual(restored.token_post_tp_lockout['BTC'], NOW+1000)
        self.assertEqual(restored.token_quarantine_ledger['BAD']['quarantine_until'], NOW+2000)
        self.assertIn('BAD', restored.token_permanent_blacklist)

    def test_profitable_reason_cannot_graduate_negative_trade(self):
        self.s.probationary_tokens['BTC'] = {}
        self.s._handle_probation_result('BTC', -1, 'Breakeven Trailing Stop')
        self.assertIn('BTC', self.s.token_permanent_blacklist)

    def test_partial_tranches_are_one_outcome_for_loss_streak(self):
        self.s.stopped_funding_arbitrages['BTC'] = [
            dict(position_id='same',total_net_pnl=-1),dict(position_id='same',total_net_pnl=-1)]
        self.s._evaluate_token_quarantine('BTC',-2)
        self.assertNotIn('BTC',self.s.token_quarantine_ledger)

    def test_manual_removal_ignores_hold_lock(self):
        pos = self.s.open()
        self.assertGreater(pos['min_hold_until'], NOW)
        self.s.config.tokens = set()
        self.s.audit.stop()
        self.assertFalse(self.s.active_funding_arbitrages)
        self.assertIn('Manual Rotation', self.s.stopped_funding_arbitrages['BTC'][0]['close_reason'])

    def test_risk_checks_do_not_depend_on_other_token_readiness(self):
        self.s.open()
        self.s.config.tokens.add('MISSING')
        self.s.books[BN].ask = '105'
        self.s.books[BN].bid = '104.99'
        for _ in range(self.s.config.basis_divergence_confirmation_ticks):
            self.s.audit.stop()
        self.assertFalse(self.s.active_funding_arbitrages)


class FundingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Fixture(Path(self.tmp.name)/'state.json')
        self.pos = self.s.open()
        self.s.current_timestamp += 7201

    async def asyncTearDown(self):
        await self.s.audit.shutdown()
        self.tmp.cleanup()

    async def test_settlement_history_catchup_dedup_and_restart(self):
        def history(c, pair, start, end):
            return [dict(timestamp=NOW+100+i*3600, rate=D('.001'), price=D('101'),
                         price_source='venue_settlement_mark', source='venue_funding_history') for i in range(2)]
        with patch.object(public, 'funding_history', side_effect=history):
            for _ in range(2):
                for leg in (1,2):
                    await self.s.audit._sync_funding(self.pos, leg, ('x',leg))
            self.assertEqual(len(self.pos['funding_payments']), 4)
            self.assertFalse(self.pos['funding_pending'])
            restarted = Fixture(self.s.audit.path)
            restarted.audit.restore(NOW+7300)
            self.assertEqual(len(restarted.active_funding_arbitrages['BTC']['funding_payments']), 4)

    async def test_no_payment_before_boundary_and_missing_history_pending(self):
        self.s.current_timestamp = NOW+10
        with patch.object(public, 'funding_history', return_value=[]):
            await self.s.audit._sync_funding(self.pos,1,('x',1))
            self.assertFalse(self.pos['funding_payments'])
            self.s.current_timestamp = NOW+500
            await self.s.audit._sync_funding(self.pos,1,('x',1))
            self.assertTrue(self.pos['funding_pending'])
            self.assertEqual(self.s.audit._entry_block(), 'confirmed funding history is overdue')

    async def test_late_funding_updates_closed_cash_balances(self):
        self.s.audit.close('BTC', D('1'), 'Manual Removal')
        closed = self.s.stopped_funding_arbitrages['BTC'][0]
        before = closed['total_net_pnl']
        event = dict(timestamp=NOW+100, rate=D('.001'), price=D('100'),
                     price_source='venue_settlement_mark', source='venue_funding_history')
        with patch.object(public, 'funding_history', return_value=[event]):
            await self.s.audit._sync_funding(closed,1,('x',1))
        self.assertNotEqual(closed['total_net_pnl'], before)
        self.assertAlmostEqual(sum(map(float,closed['venue_net_pnl'].values())), closed['total_net_pnl'])

    async def test_real_strategy_constructs_and_stops_with_native_recorder(self):
        from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
        from hummingbot.client.config.config_helpers import ClientConfigAdapter
        from hummingbot.connector.markets_recorder import MarketsRecorder
        from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
        client = ClientConfigAdapter(ClientConfigMap())
        db = SQLConnectionManager(client, SQLConnectionType.TRADE_FILLS, db_path=str(Path(self.tmp.name)/'native.sqlite'))
        with patch.object(MarketsRecorder, '_shared_instance', None):
            MarketsRecorder.get_instance(db, [], 'audit.yml', 'v2_funding_rate_arb', MarketDataCollectionConfigMap())
            config = FundingRateArbitrageConfig(tokens={'BTC'},paper_state_path=str(Path(self.tmp.name)/'native.json'),auto_rotation_enabled=False,asset_graduation_enabled=False)
            strategy = FundingRateArbitrage({}, config)
            self.assertIsInstance(strategy.audit, FundingArbSafety)
            from hummingbot.client.hummingbot_application import HummingbotApplication
            from hummingbot.core.clock import Clock
            from hummingbot.core.clock_mode import ClockMode
            clock = Clock(ClockMode.BACKTEST, start_time=NOW, end_time=NOW+2)
            clock.add_iterator(strategy)
            with patch.object(HummingbotApplication, 'main_application', return_value=NS(_mqtt=None)), patch.object(strategy.audit, 'refresh'):
                clock.backtest_til(NOW+1)
            await strategy.on_stop()
            self.assertTrue(Path(config.paper_state_path).exists())
        db.engine.dispose()

    async def test_public_connectors_ignore_saved_or_supplied_keys(self):
        from hummingbot.core.connector_manager import ConnectorManager
        from hummingbot.core.rate_oracle.rate_oracle import RateOracle
        from hummingbot.client.config.security import Security
        original = ConnectorManager.create_connector
        manager = NS(connectors={},client_config_map=NS(hb_config=NS(
            balance_asset_limit={},rate_limits_share_pct=D('100'),gateway=None)))
        try:
            with patch.object(ConnectorManager,'_funding_paper_public_only',False,create=True), \
                 patch.object(RateOracle,'get_instance',return_value=NS(register_connector=lambda c:None)), \
                 patch.object(Security,'api_keys',side_effect=AssertionError('paper must not load saved keys')):
                FundingRateArbitrage._enable_dry_run_connectors()
                for venue in (BN,HL):
                    conn = ConnectorManager.create_connector(manager,venue,trading_pairs=[],trading_required=True,
                                                            api_keys={'irrelevant_secret':'must-not-be-used'})
                    self.assertFalse(conn._trading_required)
                    if venue == HL:
                        self.assertIsNone(conn.hyperliquid_perpetual_secret_key)
                    else:
                        self.assertIsNone(conn.binance_perpetual_api_key)
        finally:
            ConnectorManager.create_connector = original

    async def test_public_trading_rule_refresh_retries_without_private_tasks(self):
        conn = self.s.connectors[BN]
        conn._update_trading_rules = AsyncMock(side_effect=[OSError('transient'),None])
        await self.s.audit._refresh_public_rules(BN,conn)
        await self.s.audit._refresh_public_rules(BN,conn)
        self.assertEqual(conn._update_trading_rules.await_count,2)
        self.assertNotIn(BN,self.s.audit.rules_jobs)


class UniverseConfigTests(unittest.TestCase):
    def test_dynamic_defaults_enable_bounded_independent_discovery(self):
        config = FundingRateArbitrageConfig()
        self.assertTrue(config.auto_rotation_enabled)
        self.assertEqual(config.auto_rotation_interval_minutes, .5)
        self.assertEqual(config.auto_rotation_max_active_tokens, 12)
        self.assertEqual(config.funding_signal_min_snapshots, 3)
        self.assertLessEqual(config.max_payback_hours, D('12'))

    def test_impossible_confirmation_and_unbounded_refresh_are_rejected(self):
        with self.assertRaises(ValidationError):
            FundingRateArbitrageConfig(funding_signal_min_snapshots=1, funding_signal_min_seconds=30)
        with self.assertRaises(ValidationError):
            FundingRateArbitrageConfig(auto_rotation_interval_minutes=0)


class PublicDataTests(unittest.TestCase):
    def test_symbol_specific_interval_and_confirmed_not_forecast(self):
        premiums = [dict(symbol='BTCUSDT',lastFundingRate='.001',nextFundingTime=NOW*1000,markPrice='100',indexPrice='100')]
        contexts = [dict(universe=[dict(name='BTC')]),[dict(funding='.0001',markPx='100',oraclePx='100')]]
        with patch.object(public,'request_json',side_effect=[premiums,[dict(symbol='BTCUSDT',fundingIntervalHours=4)],contexts]):
            self.assertEqual(public.metadata_snapshot()[(BN,'BTC-USDT')]['interval'],14400)
        with patch.object(public,'request_json',return_value=[dict(fundingTime=(NOW+1)*1000,fundingRate='.003',markPrice='101')]):
            rows = public.funding_history(BN,'BTC-USDT',NOW,NOW+2)
            self.assertEqual(rows[0]['rate'],D('.003'))
            self.assertTrue(rows[0]['rate_confirmed'])


if __name__ == '__main__':
    unittest.main()
