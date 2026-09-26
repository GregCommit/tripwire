"""Shared portfolio simulation for Tripwire's "Portfolio test" tab.

Used by both backtest.py (5-year backtest side) and app.py (live side) so the two portfolios
follow exactly the same rulebook:

- Start fully invested: the start value is split equally across the given stocks that trade on
  the start day. The "untouched" line holds exactly that, never trading.
- Each STRONG signal moves a slice (default 10% of current total value) into the signalled
  stock, funded by trimming every other base holding in proportion to its size. Slices are kept
  separate from the base holdings: they are never trimmed or topped up while open, so a stock
  that was only ever bought through a signal is back to zero once its slice closes.
- A repeat signal on an already-boosted stock only pushes its exit out; at most `max_open`
  slices at once, further signals are skipped.
- At the close of the `hold`-th trading day after the (latest) signal day the slice's shares
  are sold and the proceeds paid back to the holdings that funded it, in the same proportions.
- Every traded amount pays `cost` (0.1%) on each buy and each sell. No new money, no borrowing,
  no shorting. A missing price carries the last one forward; trades wait for the next bar.

Pure Python (no pandas) so the Flask app can import it without extra dependencies.
"""

DEFAULTS = {"start_value": 1000.0, "slice": 0.10, "max_open": 10, "hold": 5, "cost": 0.001}


def simulate(bars, spy, calendar, start_symbols, signals, cfg=None):
    """Run the rulebook day by day.

    bars:          {sym: {date: (open, close)}}  — date strings "YYYY-MM-DD"
    spy:           {date: close}
    calendar:      sorted trading dates to simulate (first = start day)
    start_symbols: stocks eligible for the starting split (those without a bar on day 0 get none)
    signals:       [{"date", "sym", "label", "price"}] — price None = buy at next day's open
                   (backtest); a price = buy at that price on the signal day (live alert price)
    """
    c = dict(DEFAULTS, **(cfg or {}))
    cost, start_value = c["cost"], c["start_value"]
    if not calendar:
        return None
    idx = {d: i for i, d in enumerate(calendar)}
    day0 = calendar[0]

    last_close = {}

    def close_on(sym, d):
        bar = bars.get(sym, {}).get(d)
        if bar and bar[1]:
            last_close[sym] = bar[1]
        return last_close.get(sym)

    base = [s for s in sorted(set(start_symbols)) if bars.get(s, {}).get(day0)]
    shares, untouched = {}, {}
    invested, taken = {}, {}
    for s in base:
        px = bars[s][day0][1]
        last_close[s] = px
        n = (start_value / len(base)) / px
        shares[s] = untouched[s] = n
        invested[s] = start_value / len(base)
    cash = 0.0 if base else start_value
    spy0 = spy.get(day0)
    spy_units = start_value / spy0 if spy0 else 0.0

    def value(prices):
        base_v = sum(n * prices[s] for s, n in shares.items() if n and prices.get(s))
        slice_v = sum(sl["shares"] * prices[s] for s, sl in open_slices.items() if prices.get(s))
        return cash + base_v + slice_v

    def current_prices(d, use_open=False):
        p = dict(last_close)
        if use_open:
            for s in bars:
                bar = bars[s].get(d)
                if bar and bar[0]:
                    p[s] = bar[0]
        return p

    def take_from_others(exclude, amount, prices):
        """Sell `amount` worth across the other base holdings in proportion to size.
        Returns (net cash, {stock: share of the funding}) so the slice can be paid back the same way."""
        others = {s: n * prices[s] for s, n in shares.items() if s != exclude and n > 1e-12 and prices.get(s)}
        total = sum(others.values())
        if total <= 0:
            return 0.0, {}
        amount = min(amount, total)
        frac = amount / total
        for s, v in others.items():
            shares[s] -= shares[s] * frac
            taken[s] = taken.get(s, 0.0) + v * frac * (1 - cost)
        return amount * (1 - cost), {s: v / total for s, v in others.items()}

    def pay_back(weights, money, prices):
        """Return a closed slice's proceeds to the holdings that funded it, in the same proportions.
        (Spreading by *current* holdings instead lets a crash that fills all slots drain every
        holding but one, which then absorbs everything and never gives it back.)"""
        nonlocal cash
        live = {s: w for s, w in weights.items() if prices.get(s)}
        total = sum(live.values())
        if total <= 0:
            cash += money
            return
        for s, w in live.items():
            part = money * w / total
            shares[s] = shares.get(s, 0.0) + part * (1 - cost) / prices[s]
            invested[s] = invested.get(s, 0.0) + part

    open_slices = {}   # sym -> {"shares", "exit_i", "signal_date", "entry_date", "entry_price", "funded", "label"}
    pending = []       # backtest entries waiting for the next available open
    trades, skipped, extended = [], [], 0
    by_day = {}
    for sg in signals:
        if sg["date"] in idx:
            by_day.setdefault(sg["date"], []).append(sg)

    def open_slice(sym, px, i_signal, d_entry, label, prices):
        funded, weights = take_from_others(sym, c["slice"] * value(prices), prices)
        if funded <= 0:
            return False
        invested[sym] = invested.get(sym, 0.0) + funded
        last_close.setdefault(sym, px)
        open_slices[sym] = {"shares": funded * (1 - cost) / px, "exit_i": i_signal + c["hold"],
                            "signal_date": calendar[i_signal], "entry_date": d_entry, "entry_price": px,
                            "funded": funded, "label": label, "from": weights}
        return True

    equity = []
    for i, d in enumerate(calendar):
        # 1. backtest entries at today's open (signal was yesterday or earlier, bar missing since)
        still = []
        for p in pending:
            bar = bars.get(p["sym"], {}).get(d)
            if not bar or not bar[0]:
                still.append(p)
                continue
            if p["i_signal"] + c["hold"] <= i:      # window already over before a bar appeared
                continue
            if not open_slice(p["sym"], bar[0], p["i_signal"], d, p["label"], current_prices(d, use_open=True)):
                skipped.append({"date": calendar[p["i_signal"]], "sym": p["sym"], "reason": "nothing to fund it from"})
        pending = still

        # 2. today's signals: live ones execute now at the alert price, backtest ones queue for tomorrow
        for sg in sorted(by_day.get(d, []), key=lambda x: (x.get("order", 0), x["sym"])):
            sym = sg["sym"]
            if sym in open_slices:
                open_slices[sym]["exit_i"] = i + c["hold"]
                extended += 1
                continue
            queued = next((p for p in pending if p["sym"] == sym), None)
            if queued:
                queued["i_signal"] = i
                extended += 1
                continue
            if len(open_slices) + len(pending) >= c["max_open"]:
                skipped.append({"date": d, "sym": sym, "reason": f"all {c['max_open']} slots busy"})
                continue
            if sg.get("price"):
                if not open_slice(sym, sg["price"], i, d, sg.get("label", ""), current_prices(d)):
                    skipped.append({"date": d, "sym": sym, "reason": "nothing to fund it from"})
            else:
                pending.append({"sym": sym, "i_signal": i, "label": sg.get("label", "")})

        # 3. today's closes, then exits due at today's close
        for s in list(bars):
            close_on(s, d)
        prices = dict(last_close)
        for sym in [s for s, sl in open_slices.items() if sl["exit_i"] <= i]:
            bar = bars.get(sym, {}).get(d)
            if not bar or not bar[1]:
                continue                              # wait for the next available close
            sl = open_slices.pop(sym)
            proceeds = sl["shares"] * bar[1] * (1 - cost)
            taken[sym] = taken.get(sym, 0.0) + proceeds
            pay_back(sl["from"], proceeds, prices)
            trades.append({"sym": sym, "label": sl["label"], "signal_date": sl["signal_date"],
                           "entry_date": sl["entry_date"], "entry_price": round(sl["entry_price"], 4),
                           "exit_date": d, "exit_price": round(bar[1], 4), "invested": round(sl["funded"], 2),
                           "pnl": round(proceeds - sl["funded"], 2),
                           "pnl_pct": round((proceeds / sl["funded"] - 1) * 100, 2)})

        # 4. mark to market
        strat = value(prices)
        unt = sum(n * prices[s] for s, n in untouched.items() if prices.get(s))
        sp = spy_units * spy[d] if spy.get(d) else (equity[-1]["spy"] if equity else start_value)
        equity.append({"date": d, "strategy": round(strat, 2), "untouched": round(unt, 2), "spy": round(sp, 2)})

    prices = dict(last_close)
    per_stock = []
    for s in sorted(set(shares) | set(invested)):
        held = shares.get(s, 0.0) + (open_slices[s]["shares"] if s in open_slices else 0.0)
        end_val = held * prices.get(s, 0.0)
        sl_trades = [t for t in trades if t["sym"] == s]
        start_px = bars.get(s, {}).get(day0, (None, None))[1]
        per_stock.append({
            "sym": s,
            "untouched_pct": round((prices[s] / start_px - 1) * 100, 1) if start_px and s in base and prices.get(s) else None,
            "slices": len(sl_trades),
            "slice_pnl": round(sum(t["pnl"] for t in sl_trades), 2),
            "total_pnl": round(end_val + taken.get(s, 0.0) - invested.get(s, 0.0), 2),
            "end_value": round(end_val, 2),
        })
    per_stock.sort(key=lambda x: -x["total_pnl"])
    return {
        "start_date": day0, "end_date": calendar[-1], "start_value": start_value,
        "equity": equity, "trades": trades, "skipped": skipped, "extended": extended,
        "open_slices": [{"sym": s, **{k: v for k, v in sl.items() if k not in ("exit_i", "from")},
                         "exit_date": calendar[sl["exit_i"]] if sl["exit_i"] < len(calendar) else None}
                        for s, sl in open_slices.items()],
        "per_stock": per_stock, "base_symbols": base, "cfg": c,
        "summary": summarize(equity, start_value),
    }


def summarize(equity, start_value=None, from_date=None):
    """Totals for each line, optionally rebased from `from_date` (the backtest's fair-test stretch)."""
    rows = [e for e in equity if from_date is None or e["date"] >= from_date]
    if not rows:
        return None
    first, last = rows[0], rows[-1]
    days = _days_between(first["date"], last["date"])
    out = {"from": first["date"], "to": last["date"], "days": days}
    for line in ("strategy", "untouched", "spy"):
        base = start_value if (from_date is None and start_value) else first[line]
        end = last[line] * ((start_value or base) / base) if from_date else last[line]
        s0 = start_value or base
        total = end / s0 - 1 if s0 else 0.0
        peak, worst = 0.0, 0.0
        for e in rows:
            v = e[line]
            peak = max(peak, v)
            if peak:
                worst = min(worst, v / peak - 1)
        out[line] = {
            "end": round(end, 2), "change": round(end - s0, 2), "total_pct": round(total * 100, 2),
            "cagr_pct": round(((1 + total) ** (365.25 / days) - 1) * 100, 2) if days >= 90 and total > -1 else None,
            "worst_drop_pct": round(worst * 100, 1),
        }
    return out


def _days_between(a, b):
    from datetime import date
    y1, m1, d1 = map(int, a.split("-"))
    y2, m2, d2 = map(int, b.split("-"))
    return (date(y2, m2, d2) - date(y1, m1, d1)).days
