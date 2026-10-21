"""Offline tests of the real selection component, without exchange accounts."""
import asyncio
import ast
import copy
from decimal import Decimal as D
from pathlib import Path
import sys
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch
from collections import deque
import time
import os

REPO = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(os.environ.get('FUNDING_SOURCE_ROOT', str(REPO/'bots' if (REPO/'bots/scripts').is_dir() else REPO)))
sys.path.insert(0,str(SOURCE_ROOT))
from scripts.funding_arb_accounting import MarketUnavailable
from scripts.funding_arb_universe import FundingSignals, FundingUniverse, entry_rank
from scripts import funding_arb_public_data as public

BN, HL = public.BINANCE, public.HYPERLIQUID

def position(token,apr='80',net='1'):
    return dict(token=token,expected_profitability=D(apr)/365/100,
        selection_expected_net_quote=D(net), initial_margin_quote=D('1000'),
        entry_fee_quote=D('1'),payback_hours=4,
        amount_1=D('5'),amount_2=D('5'),price_1=D('100'),price_2=D('100'),
        leverage=1,entry_fee_1=D('.5'),entry_fee_2=D('.5'),connector_1=BN,connector_2=HL)

class Connector:
    def __init__(self,fixture,name):
        self.f,self.name=fixture,name
        self.trading_rules={}
        self.trading_pairs=[]
    async def add_trading_pair(self,pair):
        self.f.calls.append(('add',self.name,pair))
        if self.name==self.f.fail_add:
            return False
        self.trading_pairs.append(pair)
        self.trading_rules[pair]=True
        self.f.books.add((self.name,pair))
        return True
    async def remove_trading_pair(self,pair):
        self.f.calls.append(('remove',self.name,pair))
        if self.f.fail_cleanup:
            return False
        self.f.books.discard((self.name,pair))
        if pair in self.trading_pairs:self.trading_pairs.remove(pair)
        return True

class Fixture:
    def __init__(self):
        self.calls,self.books,self.persisted=[],set(),[]
        self.persisted_states=[]
        self.fail_add=None;self.fail_cleanup=False
        self.fail_persist=False;self.fail_cost=False
        cfg=NS(auto_rotation_enabled=True,auto_rotation_interval_minutes=.5,
            auto_rotation_max_active_tokens=1,auto_rotation_min_apr=.35,auto_rotation_min_apr_delta=.4,
            auto_rotation_min_volume_24h_usd=2000000,universe_min_secondary_volume_quote=500000,
            max_funding_data_age_seconds=120,max_allowed_price_divergence=.01,max_entry_spread_pct=.001,
            min_funding_rate_profitability=D('.0012'),universe_mount_timeout_seconds=.1,
            universe_min_residence_seconds=0,universe_failed_candidate_cooldown_seconds=300,
            universe_max_probes_per_scan=3,universe_max_rotations_per_scan=2,
            universe_volume_refresh_seconds=300,position_size_quote=500,graduation_probe_size_quote=200)
        self.s=NS(config=cfg,current_timestamp=1000,token_permanent_blacklist=set(),
            token_quarantine_ledger={},token_post_tp_lockout={},active_funding_arbitrages={},
            stopped_funding_arbitrages={},rotation_event_history=[],last_rotation_timestamp=0,
            probationary_tokens={},is_rotating=False)
        cfg.tokens={'BTC'}
        self.s.get_canonical_token=lambda token,*args:token.upper()
        self.s.get_trading_pair_for_connector=lambda token,c:token+('-USDT' if c==BN else '-USD')
        self.s.get_token_multiplier=lambda token,c:1
        self.s.get_most_profitable_combination=lambda info:(BN,HL,'BUY',abs(info[BN].rate-info[HL].rate)*24)
        self.s.connectors={c:Connector(self,c) for c in (BN,HL)}
        for c,conn in self.s.connectors.items():
            pair=self.s.get_trading_pair_for_connector('BTC',c)
            conn.trading_pairs.append(pair);conn.trading_rules[pair]=True;self.books.add((c,pair))
        self.metadata={}
        self.set_rate('BTC','3');self.set_rate('NIL','80')
        self.u=FundingUniverse(self)
        self.u.volumes={'BTC-USDT':D('5000000'),'NIL-USDT':D('5000000')}
        self.u.volumes_at=1000
    def set_rate(self,token,apr):
        for c in (BN,HL):
            self.metadata[(c,self.s.get_trading_pair_for_connector(token,c))]=dict(
                rate=D(apr)/365/100/24 if c==BN else D(0),interval=3600,
                next_time=2000,observed_at=1000,mark_price=D(100),volume_24h=D('2000000'))
    def funding_info(self,token):
        return {c:NS(**self.metadata[(c,self.s.get_trading_pair_for_connector(token,c))]) for c in (BN,HL)}
    def books(self,c,pair):
        # Replaced by book() below because the set intentionally tracks subscriptions.
        raise AssertionError
    def book(self,c,pair):
        if (c,pair) not in self.books:raise MarketUnavailable('missing book')
        return True
    def capacity(self,*args):return D(500)
    def entry_position(self,token,*args):
        if self.fail_cost:raise MarketUnavailable('full round-trip payback exceeds limit')
        return position(token, str(abs(self.funding_info(token)[BN].rate)*24*365*100))
    def budget(self):return dict(available={BN:D('5000'),HL:D('5000')})
    def persist(self):
        if self.fail_persist:raise OSError('disk full')
        self.persisted.append(copy.deepcopy(self.s.config.tokens))
        self.persisted_states.append(copy.deepcopy(dict(admitted_at=self.u.admitted_at,cooldowns=self.u.cooldowns)))
    def _spawn(self,coro):
        coro.close()
        self.calls.append(('scheduled',))


class SignalTests(unittest.TestCase):
    def setUp(self):self.signals=FundingSignals()
    def ready(self,a,b=None,now=None,path='long-A'):
        info={BN:NS(observed_at=a),HL:NS(observed_at=a if b is None else b)}
        return self.signals.ready('BTC',path,info,a if now is None else now,
            samples=3,seconds=30,maximum_age=120)
    def test_three_ticks_are_not_three_observations(self):
        for now in range(100,111):self.assertFalse(self.ready(100,now=now))
        self.assertFalse(self.ready(130));self.assertTrue(self.ready(160))
        self.assertTrue(self.ready(160,now=161))
    def test_partial_update_does_not_count_as_independent(self):
        self.ready(100);self.ready(130)
        self.assertFalse(self.ready(160,b=130))
        self.assertTrue(self.ready(160))
    def test_direction_change_needs_new_history(self):
        self.ready(100);self.ready(130);self.assertTrue(self.ready(160))
        self.assertFalse(self.ready(190,path='short-A'))
    def test_clock_regression_and_stale_data_reset(self):
        self.ready(100);self.ready(130);self.assertTrue(self.ready(160))
        self.assertFalse(self.ready(120));self.assertFalse(self.ready(190))
        self.assertFalse(self.ready(190,now=400));self.assertNotIn('BTC',self.signals.rows)
    def test_missing_venue_is_not_confirmed(self):
        self.assertFalse(self.signals.ready('BTC','path',{},100,samples=3,seconds=30,maximum_age=120))

class RankingTests(unittest.TestCase):
    def test_fee_net_candidate_beats_alphabetical_and_raw_apr_order(self):
        candidates=[position('ADA','120','1'),position('ZEC','80','2')]
        self.assertEqual(sorted(candidates,key=entry_rank)[0]['token'],'ZEC')
    def test_capital_efficiency_not_only_gross_dollars(self):
        large=position('ADA',net='3');large['initial_margin_quote']=D(3000)
        small=position('ZEC',net='2')
        self.assertEqual(sorted([large,small],key=entry_rank)[0]['token'],'ZEC')

class UniverseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.f=Fixture()
        # Keep the tracked book set on the fixture, expose the adapter's method separately.
        self.f.u.a=NS(s=self.f.s,metadata=self.f.metadata,funding_info=self.f.funding_info,
            books=self.f.book,capacity=self.f.capacity,entry_position=self.f.entry_position,
            budget=self.f.budget,persist=self.f.persist,_spawn=self.f._spawn)
        self.u=self.f.u
    def test_wide_screen_finds_pool_external_candidate(self):
        self.assertEqual([r['token'] for r in self.u.fetch_candidates()],['NIL'])
        self.assertNotIn('NIL',self.f.s.config.tokens)
    def test_stale_volume_basis_and_symbol_guards(self):
        self.u.volumes['NIL-USDT']=D(0);self.assertFalse(self.u.fetch_candidates())
        self.u.volumes['NIL-USDT']=D('5000000')
        self.f.metadata[(HL,'NIL-USD')]['mark_price']=D(110)
        self.assertFalse(self.u.fetch_candidates())
        self.f.metadata[(HL,'NIL-USD')]['mark_price']=D(100)
        self.f.metadata[(HL,'NIL-USD')]['observed_at']=800
        self.assertFalse(self.u.fetch_candidates())
    def test_blacklisted_candidate_never_admitted(self):
        self.f.s.token_permanent_blacklist.add('NIL')
        self.assertFalse(self.u.fetch_candidates())
    def test_decimal_scoring_does_not_replace_better_idle_market(self):
        self.f.set_rate('BTC','80')
        self.assertIsNone(self.u._retire_candidate(position('NIL','50')))
    def test_decimal_scoring_replaces_worse_idle_market(self):
        self.assertEqual(self.u._retire_candidate(position('NIL','80')),'BTC')
    def test_lower_gross_rate_can_win_on_verified_net_return(self):
        self.f.set_rate('BTC','80')
        self.assertEqual(self.u._retire_candidate(position('NIL','50',net='2')),'BTC')
    def test_confirmed_cost_failure_is_not_unknown_market_data(self):
        self.f.set_rate('BTC','80');self.f.fail_cost=True
        self.assertEqual(self.u._retire_candidate(position('NIL','50')),'BTC')
    def test_missing_book_never_becomes_zero_yield(self):
        self.f.set_rate('BTC','80')
        with patch.object(self.u,'_forecast_position',side_effect=MarketUnavailable('missing order book')):
            self.assertIsNone(self.u._retire_candidate(position('NIL','100')))
    def test_unknown_current_market_is_not_zero_score(self):
        self.f.metadata.pop((BN,'BTC-USDT'))
        self.assertIsNone(self.u._retire_candidate(position('NIL','80')))
    def test_residence_hysteresis_and_held_market_are_protected(self):
        self.u.admitted_at['BTC']=999;self.f.s.config.universe_min_residence_seconds=300
        self.assertIsNone(self.u._retire_candidate(position('NIL','80')))
        self.u.admitted_at.clear();self.f.s.active_funding_arbitrages['BTC']={}
        self.assertIsNone(self.u._retire_candidate(position('NIL','80')))
    async def test_failed_second_leg_preserves_old_watchlist_and_cleans_staging(self):
        self.f.fail_add=HL
        await self.u.scan()
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertFalse(self.f.s.rotation_event_history)
        self.assertNotIn((BN,'NIL-USDT'),self.f.books)
        self.assertIn((BN,'BTC-USDT'),self.f.books)
    async def test_full_cost_failure_rolls_back_before_replacing(self):
        self.f.fail_cost=True
        await self.u.scan()
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertNotIn((BN,'NIL-USDT'),self.f.books)
    async def test_success_admits_new_before_unsubscribing_old(self):
        await self.u.scan()
        self.assertEqual(self.f.s.config.tokens,{'NIL'})
        add_index=self.f.calls.index(('add',HL,'NIL-USD'))
        remove_index=self.f.calls.index(('remove',BN,'BTC-USDT'))
        self.assertLess(add_index,remove_index)
        self.assertEqual(self.f.persisted[-1],{'NIL'})
        self.assertIn('NIL',self.f.persisted_states[-1]['admitted_at'])
        self.assertIn('BTC',self.f.persisted_states[-1]['cooldowns'])
        self.assertEqual(self.f.s.rotation_event_history[0]['added_token'],'NIL')
    async def test_persist_failure_rolls_back_universe_history_and_mounts(self):
        self.f.fail_persist=True
        await self.u.scan()
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertFalse(self.f.s.rotation_event_history)
        self.assertFalse(self.u.admitted_at)
        self.assertNotIn((BN,'NIL-USDT'),self.f.books)
    async def test_active_position_is_never_retired_even_on_forced_scan(self):
        self.f.s.active_funding_arbitrages['BTC']={}
        await self.u.scan(force=True)
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertIn((BN,'BTC-USDT'),self.f.books)
        self.assertFalse(await self.u.remove('BTC'))
    async def test_missing_dynamic_subscription_is_not_silent_success(self):
        self.f.s.connectors[HL]=NS(trading_rules={})
        self.assertFalse(await self.u.add('NIL'))
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertNotIn((BN,'NIL-USDT'),self.f.books)
    async def test_failed_cleanup_blocks_further_expansion(self):
        self.f.fail_add=HL;self.f.fail_cleanup=True
        await self.u.scan()
        self.assertTrue(self.u.cleanup_pending)
        before=len(self.f.calls)
        await self.u.scan()
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertFalse(any(c[0]=='add' for c in self.f.calls[before:]))
    async def test_volume_cache_avoids_duplicate_public_calls(self):
        with patch.object(public,'volume_snapshot',return_value=self.u.volumes) as request:
            await self.u.scan();await self.u.scan()
            request.assert_not_called()
    def test_scheduler_deduplicates_ticks(self):
        self.u.a._spawn=self.f._spawn
        self.u.schedule();self.u.schedule()
        self.assertEqual(self.f.calls,[('scheduled',)])

    async def test_cancellation_cleans_new_pair_but_keeps_original(self):
        entered=asyncio.Event()
        async def delayed(pair):
            entered.set()
            await asyncio.Future()
        self.f.s.connectors[HL].add_trading_pair=delayed
        task=asyncio.create_task(self.u.scan())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(self.f.s.config.tokens,{'BTC'})
        self.assertNotIn((BN,'NIL-USDT'),self.f.books)
        self.assertIn((BN,'BTC-USDT'),self.f.books)


class EntryIntegrationTests(unittest.TestCase):
    """Execute the production create method against controlled execution adapters."""
    def setUp(self):
        path=SOURCE_ROOT/'scripts/funding_arb_safety.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='FundingArbSafety')
        method=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='create')
        module=ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[]))
        from scripts.funding_arb_accounting import dec
        scope=dict(dec=dec,entry_rank=entry_rank,MarketUnavailable=MarketUnavailable,is_buy=lambda side:side=='BUY')
        exec(compile(module,str(path),'exec'),scope)
        self.create=scope['create']
        import logging
        cfg=NS(dry_run=True,tokens={'ADA','ZEC'},max_concurrent_positions=1,
            reentry_cooldown_seconds=0,min_funding_rate_profitability=D('.0012'),
            funding_signal_min_snapshots=1,funding_signal_min_seconds=0,max_funding_data_age_seconds=120,
            settlement_blackout_tier3_only=True,settlement_blackout_minutes=0,position_size_quote=D(500),
            entry_persistence_ticks=1,graduation_probe_size_quote=200)
        s=NS(config=cfg,current_timestamp=1000,active_funding_arbitrages={},
            token_permanent_blacklist=set(),probationary_tokens={},token_quarantine_ledger={},
            token_post_tp_lockout={},token_last_closed_timestamp={},entry_candidate_ticks={},
            get_token_tier=lambda token:(1,500),logger=lambda:logging.getLogger('entry-integration'),
            get_most_profitable_combination=lambda info:(BN,HL,'BUY',D('.003')))
        self.rejected=[]
        a=NS(s=s,_entry_block=lambda:'',entry_paths={},blocked_reason='',
            funding_info=lambda token:{c:NS(observed_at=1000,next_funding_utc_timestamp=2000,interval=3600) for c in (BN,HL)},
            capacity=lambda *args:D(500),
            entry_position=lambda token,*args:position(token,net='1' if token=='ADA' else '2'),
            budget=lambda:dict(available={BN:D(5000),HL:D(5000)}),
            value=lambda pos:({},None,None),persist=lambda:None)
        def reject(token,reason):
            self.rejected.append((token,reason));s.entry_candidate_ticks[token]=0
        a.reject=reject
        a.universe=NS(signals=FundingSignals(),_forecast_position=self.forecast)
        self.a=a;self.opening=[]
    def forecast(self,token):
        self.opening.append(token)
        return self.a.entry_position(token)
    def test_real_entry_loop_chooses_fee_net_winner_for_last_slot(self):
        self.create(self.a)
        self.assertEqual(list(self.a.s.active_funding_arbitrages),['ZEC'])
        self.assertIn(('ADA','position slot limit'),self.rejected)
    def test_reprice_failure_skips_candidate_without_fabricating_position(self):
        def forecast(token):
            if token=='ZEC':raise MarketUnavailable('depth disappeared')
            return self.a.entry_position(token)
        self.a.universe._forecast_position=forecast
        self.create(self.a)
        self.assertEqual(list(self.a.s.active_funding_arbitrages),['ADA'])
        self.assertIn(('ZEC','depth disappeared'),self.rejected)
    def test_actual_entry_loop_does_not_confirm_repeated_snapshot(self):
        self.a.s.config.funding_signal_min_snapshots=3
        self.a.s.config.funding_signal_min_seconds=30
        for _ in range(5):self.create(self.a)
        self.assertFalse(self.a.s.active_funding_arbitrages)
        for stamp in (1030,1060):
            self.a.s.current_timestamp=stamp
            self.a.funding_info=lambda token,stamp=stamp:{c:NS(observed_at=stamp,next_funding_utc_timestamp=2000,interval=3600) for c in (BN,HL)}
            self.create(self.a)
        self.assertEqual(list(self.a.s.active_funding_arbitrages),['ZEC'])


class MetadataIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_wide_discovery_does_not_allocate_unused_reference_histories(self):
        path=SOURCE_ROOT/'scripts/funding_arb_safety.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='FundingArbSafety')
        method=next(n for n in cls.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_refresh_metadata')
        scope=dict(asyncio=asyncio,time=time,public_data=public,deque=deque)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[])),str(path),'exec'),scope)
        snapshot={(c,token+('-USDT' if c==BN else '-USD')):dict(observed_at=1000,index_price=D(100),mark_price=D(100))
                  for c in (BN,HL) for token in ('BTC','ETH','NIL')}
        s=NS(current_timestamp=1000,config=NS(tokens={'BTC'}),connectors={BN:None,HL:None},
            get_trading_pair_for_connector=lambda t,c:t+('-USDT' if c==BN else '-USD'))
        a=NS(s=s,metadata_status={},reference_prices={(BN,'OLD-USDT'):deque()},
             _funding_targets=lambda:[dict(token='ETH')])
        with patch.object(public,'metadata_snapshot',return_value=snapshot):await scope['_refresh_metadata'](a)
        self.assertEqual(len(a.metadata),6)
        self.assertEqual(len(a.reference_prices),4)
        self.assertNotIn((BN,'NIL-USDT'),a.reference_prices)
        self.assertNotIn((BN,'OLD-USDT'),a.reference_prices)
        self.assertEqual(a.metadata_status['markets'],6)

if __name__=='__main__':unittest.main()
