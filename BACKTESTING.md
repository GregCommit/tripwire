# Tripwire Rule Backtesting

`backtest.py` is a standalone tool that pressure-tests Tripwire's 7 technical rules
against ~5 years of daily price data, tells you which rules actually predict
short-term (1–5 trading-day) moves and at what threshold levels, and can write
validated per-ticker thresholds back into the app.

It is deliberately **standalone** — it does not import `app.py` (that would boot the
Flask server). It re-implements the rule math in vectorized form and includes a
`parity` command that verifies, against the real `evaluate_rules()` source, that the
two produce identical triggers.

## What it measures

For every rule trigger ("event"), it measures the **signal-direction excess return
vs SPY** at 1, 3, and 5 trading days:

- For a **BUY** signal, a positive forward return is good; for **SELL**, a negative
  forward return is good (both expressed so that higher = the rule was "right").
- "Excess" = the stock's return minus SPY's over the same window, so a rule isn't
  rewarded just for firing during a broad market rally.
- The **headline metric** is the mean 5-day excess return, and a threshold is only
  considered effective if the 3-day figure agrees in sign.

Events are **de-clustered to episode starts** (the day a condition first becomes
true), mirroring the app's 24-hour alert cooldown — a condition that stays true for
a week is one alert in production, not five.

## Commands

```
python backtest.py fetch      # download + cache prices to backtest_data/ (run once)
python backtest.py parity     # verify the rule math matches app.py (should be 100%)
python backtest.py grid       # Phase B: test every rule across its threshold grid
python backtest.py combo      # Phase C: rule combinations + ablation
python backtest.py select     # write backtest_results/recommended_params.json
python backtest.py report     # build backtest_results/report.html
python backtest.py            # run the whole pipeline end to end
python backtest.py apply      # write recommendations into the app DB (makes a backup)
python backtest.py undo       # restore the most recent params backup
```

Flags: `--refresh` (re-download data), `--years N` (window, default 5),
`--min-n N` (minimum episodes to adopt a per-ticker threshold, default 30).

## Reading the report

`backtest_results/report.html` (self-contained, open in any browser) has:

1. **Rule effectiveness** — the best threshold for each rule pooled across all
   tickers, with N, hit rate, t-statistic, and max adverse excursion (MAE = the
   worst intra-window drawdown if you acted on the signal).
2. **Threshold sensitivity** — a bar per grid point (green = positive 5-day edge);
   a broad band of green means the rule is robust, a lone green spike is likely noise.
3. **Per-ticker recommendations** — for each ticker/rule, whether the tool
   recommends a ticker-specific threshold, a category-level fallback, or disabling
   the rule, with the evidence behind each.
4. **Combinations** — how the app's multi-rule agreement signals perform, and each
   rule's marginal contribution (ablation).

## How thresholds are chosen (and why it won't overfit blindly)

A **per-ticker** threshold is adopted only if it has **N ≥ 30 episodes**, a positive
and sign-consistent short-term edge, and **positive out-of-sample** performance
(optimized on the first 70% of the window, validated on the last 30%). Otherwise the
tool falls back to the best **category-level** threshold; if a rule shows no edge
even pooled, it recommends **disabling** that rule for that ticker. Many thresholds
are tested, so single-ticker results with small N are treated as suggestive, not
conclusive — prefer the pooled and out-of-sample-validated numbers.

## Applying results

`python backtest.py apply` merges `recommended_params.json` into the app's
`rule_params` table (same schema the Settings/AI edits use), after saving the current
values to `backtest_results/params_backup_<timestamp>.json`. Restart or let the app
re-check to see the new thresholds in each stock's Edit panel. `python backtest.py
undo` restores the latest backup.

## Data & limitations

- Data is free daily OHLCV from yfinance (`period=max`, split/dividend-adjusted),
  cached on disk; Stooq is an automatic fallback if Yahoo throttles. yfinance has
  hourly rate limits, so `fetch` runs once and everything else reads the cache — use
  `--refresh` sparingly.
- **ARM** (IPO Sep 2023) and **SNDK** (spun off Feb 2025) have short histories; they
  appear in the report flagged as short-history and lean on category-pooled results.
- This is an **event study**, not a trading simulation — no transaction costs,
  slippage, or position sizing. It answers "does the signal have predictive edge,"
  which is the right question for an alerting tool. Past performance does not
  guarantee future results.
