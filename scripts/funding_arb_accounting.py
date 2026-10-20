"""Deterministic accounting primitives for the funding strategy's paper adapter.

This module has no connector, network, or order-submission side effects.  Prices
are fills over a finite order book, not mid/last-price substitutes.
"""
from __future__ import annotations

import copy
import json
import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterable

ZERO = Decimal("0")
ONE = Decimal("1")
SCHEMA_VERSION = 2
MODEL_VERSION = "funding-audit-v2"


class MarketUnavailable(ValueError):
    """The requested fill or valuation cannot be established."""


def dec(value, *, positive=False, nonnegative=False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MarketUnavailable("invalid decimal") from exc
    if not result.is_finite() or (positive and result <= ZERO) or (nonnegative and result < ZERO):
        raise MarketUnavailable("non-finite or out-of-range decimal")
    return result


def is_buy(side) -> bool:
    name = getattr(side, "name", str(side)).upper()
    if name in {"BUY", "TRADETYPE.BUY"}:
        return True
    if name in {"SELL", "TRADETYPE.SELL"}:
        return False
    raise MarketUnavailable("unknown position side")


@dataclass(frozen=True)
class Fill:
    amount: Decimal
    notional: Decimal
    price: Decimal
    worst: Decimal


def valid_levels(rows: Iterable, buy: bool) -> list[tuple[Decimal, Decimal]]:
    levels = [(dec(p, positive=True), dec(q, positive=True)) for p, q in rows]
    if not levels:
        raise MarketUnavailable("empty side of order book")
    if any((b[0] < a[0] if buy else b[0] > a[0]) for a, b in zip(levels, levels[1:])):
        raise MarketUnavailable("unsorted order book")
    return levels


def fill_base(rows: Iterable, amount) -> Fill:
    remaining = dec(amount, positive=True)
    requested, notional, worst = remaining, ZERO, ZERO
    for p, q in rows:
        p, q = dec(p, positive=True), dec(q, positive=True)
        taken = min(q, remaining)
        notional += taken * p
        remaining -= taken
        worst = p
        if remaining <= ZERO:
            return Fill(requested, notional, notional / requested, worst)
    raise MarketUnavailable("insufficient displayed base depth")


def fill_quote(rows: Iterable, budget) -> Fill:
    remaining = dec(budget, positive=True)
    requested, amount, worst = remaining, ZERO, ZERO
    for p, q in rows:
        p, q = dec(p, positive=True), dec(q, positive=True)
        taken = min(q, remaining / p)
        spent = min(remaining, taken * p)
        amount += taken
        remaining -= spent
        worst = p
        if remaining <= Decimal("1e-18") * requested:
            return Fill(amount, requested, requested / amount, worst)
    raise MarketUnavailable("insufficient displayed quote depth")


def safe_capacity(rows, ratio, price_band) -> Decimal:
    ratio = dec(ratio, positive=True)
    if ratio > Decimal("0.30"):
        raise MarketUnavailable("depth ratio exceeds project limit")
    band = dec(price_band, nonnegative=True)
    best = rows[0][0]
    return sum((p * q for p, q in rows[:25] if abs(p / best - ONE) <= band), ZERO) * ratio


def matched_quantities(target, price1, price2, multiplier1, multiplier2,
                       quantize1: Callable, quantize2: Callable, tolerance) -> tuple[Decimal, Decimal]:
    """Match canonical base delta, not equal USD amounts at different prices."""
    target, p1, p2 = dec(target, positive=True), dec(price1, positive=True), dec(price2, positive=True)
    m1, m2 = dec(multiplier1, positive=True), dec(multiplier2, positive=True)
    common = min(target / (p1 / m1), target / (p2 / m2))
    q1 = dec(quantize1(common / m1), positive=True)
    q2 = dec(quantize2(common / m2), positive=True)
    for _ in range(8):
        common = min(q1 * m1, q2 * m2)
        new1 = dec(quantize1(common / m1), positive=True)
        new2 = dec(quantize2(common / m2), positive=True)
        if (new1, new2) == (q1, q2):
            break
        q1, q2 = new1, new2
    delta1, delta2 = q1 * m1, q2 * m2
    if abs(delta1 - delta2) / max(delta1, delta2) > dec(tolerance, nonnegative=True):
        raise MarketUnavailable("quantity increments cannot produce a balanced hedge")
    return q1, q2


def position_value(position, close1: Fill, close2: Fill, exit_fee1, exit_fee2) -> dict:
    q1, q2 = dec(position["amount_1"], positive=True), dec(position["amount_2"], positive=True)
    p1, p2 = dec(position["price_1"], positive=True), dec(position["price_2"], positive=True)
    if close1.amount != q1 or close2.amount != q2:
        raise MarketUnavailable("close must value the existing quantities")
    sign = ONE if is_buy(position["side"]) else -ONE
    leg1 = sign * (close1.price - p1) * q1
    leg2 = -sign * (close2.price - p2) * q2
    entry = dec(position.get("entry_fee_quote", 0), nonnegative=True)
    exit_fee = dec(exit_fee1, nonnegative=True) + dec(exit_fee2, nonnegative=True)
    funding = sum((dec(p["amount"]) for p in position.get("funding_payments", [])), ZERO)
    gross = leg1 + leg2
    trading = gross - entry - exit_fee
    return {"leg1_pnl": leg1, "leg2_pnl": leg2, "gross_trading_pnl": gross,
            "entry_fee_quote": entry, "exit_fee_quote": exit_fee,
            "trading_pnl": trading, "funding_pnl": funding, "total_net_pnl": trading + funding}


def split_position(position: dict, ratio) -> tuple[dict, dict | None]:
    ratio = dec(ratio, positive=True)
    if ratio > ONE:
        raise MarketUnavailable("close fraction must be at most one")
    closed = copy.deepcopy(position)
    remaining = copy.deepcopy(position) if ratio < ONE else None
    for target, fraction in [(closed, ratio)] + ([(remaining, ONE - ratio)] if remaining is not None else []):
        for key in ["amount_1", "amount_2", "position_size_quote", "entry_fee_quote", "entry_fee_1", "entry_fee_2", "initial_margin_quote"]:
            if key in target:
                target[key] = dec(position[key]) * fraction
        for payment in target.get("funding_payments", []):
            payment["amount"] = str(dec(payment["amount"]) * fraction)
            if "quantity" in payment:
                payment["quantity"] = str(dec(payment["quantity"]) * fraction)
    return closed, remaining


def funding_payment(*, side, quantity, price, rate) -> Decimal:
    return (-ONE if is_buy(side) else ONE) * dec(quantity, positive=True) * dec(price, positive=True) * dec(rate)


def liquidation_estimate(entry, mark, quantity, leverage, funding, fees, maintenance_ratio, buy):
    """Illustrative isolated-margin distance; never an exchange liquidation quote."""
    entry, mark, quantity = dec(entry, positive=True), dec(mark, positive=True), dec(quantity, positive=True)
    leverage = dec(leverage, positive=True)
    maintenance = dec(maintenance_ratio, nonnegative=True)
    collateral = quantity * entry / leverage + dec(funding) - dec(fees, nonnegative=True)
    equity = collateral + (ONE if buy else -ONE) * quantity * (mark - entry)
    required = quantity * mark * maintenance
    if buy:
        liq = max(ZERO, (quantity * entry - collateral) / (quantity * (ONE - maintenance)))
        distance = (mark - liq) / mark
    else:
        liq = (quantity * entry + collateral) / (quantity * (ONE + maintenance))
        distance = (liq - mark) / mark
    return liq, distance, equity <= required


def json_value(value):
    if isinstance(value, Decimal):
        return str(dec(value))
    if hasattr(value, "name"):
        return value.name
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"unsupported persistent value: {type(value).__name__}")


def atomic_json(path, payload, *, owner=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, default=json_value, ensure_ascii=False, allow_nan=False, indent=2)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if owner is not None and hasattr(os, "chown") and os.geteuid() == 0:
            os.chown(temporary, *owner)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def restored_position(raw: dict, now, *, legacy=False) -> dict:
    position = copy.deepcopy(raw)
    position["side"] = "BUY" if is_buy(position.get("side", "BUY")) else "SELL"
    for canonical, aliases in {
        "price_1": ["price_1", "entry_price_1"], "price_2": ["price_2", "entry_price_2"],
        "position_size_quote": ["position_size_quote", "position_size"],
    }.items():
        position[canonical] = next((raw[k] for k in aliases if k in raw), None)
        position[canonical] = dec(position[canonical], positive=True)
    for i in (1, 2):
        key = f"amount_{i}"
        position[key] = dec(raw.get(key, position["position_size_quote"] / position[f"price_{i}"]), positive=True)
    position["leverage"] = dec(raw.get("leverage", 1), positive=True)
    position["open_timestamp"] = float(raw.get("open_timestamp", now - float(raw.get("duration_seconds", 0))))
    position["is_virtual"] = bool(raw.get("is_virtual", True))
    if not position["is_virtual"]:
        raise MarketUnavailable("cannot restore live position into paper strategy")
    position["funding_payments"] = copy.deepcopy(raw.get("funding_payments", []))
    position.setdefault("entry_fee_quote", ZERO)
    position["entry_fee_quote"] = dec(position["entry_fee_quote"], nonnegative=True)
    position["accounting_quality"] = "legacy_reconstructed" if legacy else raw.get("accounting_quality", "audited_paper")
    for i in (1, 2):
        position.setdefault(f"next_funding_time_{i}", raw.get(f"next_funding_{i}", 0))
    return position
