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
python backtest.py fetch          # download + cache prices to backtest_data/ (run once)
python backtest.py parity         # verify the rule math matches app.py (should be 100%)
python backtest.py grid           # Phase B: test every rule across its threshold grid
python backtest.py combo          # Phase C: rule combinations + ablation (equal-vote ensemble)
python backtest.py combo-weighted # Phase C variant: doubles the vote of the strongest rules
python backtest.py weight-check   # runs combo + combo-weighted, then compares the two
python backtest.py select         # write backtest_results/recommended_params.json + rule_stats.json
python backtest.py report         # build backtest_results/report.html
python backtest.py                # run the whole pipeline end to end
python backtest.py apply          # write recommendations + calibrated_at into the app DB (makes a backup)
python backtest.py undo           # restore the most recent params backup
```

Flags: `--refresh` (re-download data), `--years N` (window, default 5),
`--min-n N` (minimum episodes to adopt a per-ticker threshold, default 30).

### `weight-check`: does boosting the "best" rules help the ensemble?

A natural-seeming idea is to weight the multi-rule ensemble vote by each rule's individual
edge — e.g. double-count support/resistance and volatility since they test strongest solo.
`weight-check` tests this directly: it runs the equal-vote combo (`combo`) and a
weighted variant (`combo-weighted`, breakout/volatility rules at 2x) side by side and
writes `backtest_results/combo_weighting_comparison.csv`.

**Result: up-weighting regressed; down-weighting noise won.** The 2x-weighted ensemble's
pooled mean 5-day excess return was *worse* than equal-vote (about -0.07pp), because an
ensemble's value comes from independent confirmation — letting one "best" rule dominate
the vote undermines that. A follow-up pass tested the opposite lever: instead of
amplifying the strongest rules, *exclude the near-zero-edge rules from the vote
entirely*. Removing `consecutive_down`, `gap`, and `ma_cross` (the "core four" vote:
volatility, support/resistance, volume, RSI) beat plain equal-vote decisively:

| vote scheme | pooled 5d excess | hit rate | STRONG-events only |
|---|---|---|---|
| equal vote, all rules (ma_cross 0) | +0.59% | 54.5% | +0.93% / 55% hit |
| 2x strongest rules | +0.53% | 54.5% | — (regressed) |
| core four (consec/gap/ma excluded) | **+1.11%** | **58.2%** | **+1.89% / 61% hit** |

The STRONG-only column matters most because STRONG is what gates outbound
notifications (`notify_strong_only`). Improvement held on 7/11 tickers (2 flat, 3
small declines), held with short-history tickers (ARM/SNDK) excluded, and matched
what the per-ticker ablations had already predicted (consec diluted EVR/LLY/MU; gap
diluted ARM/AMAT) — i.e., a confirmed prior hypothesis, not scheme-shopping. Expected
STRONG cadence under this vote: roughly one notification per week across an 11-ticker
watchlist.

The app's ensemble (`RULE_SIGNAL_WEIGHT` in both `app.py`'s `compute_signal()` and the
dashboard JS's `computeSignal()`) reflects this: weight 1 for volatility,
support_resistance, volume, and rsi; weight 0 for consecutive_down, gap, and ma_cross.
Excluded rules still fire, log, and display their own alerts — they just don't vote
toward STRONG/TRENDING badges or notifications. Do not change the vote weights without
re-running `weight-check` and seeing an improvement, not a regression.

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
values to `backtest_results/params_backup_<timestamp>.json`. It also stamps a
`calibrated_at` (today's date) into the app's `settings` table, which the app reads
fresh on every `/api/status` call and shows on the action-window banner — turning
amber once it's more than 90 days old, as a signal the thresholds may need
re-calibrating. Restart or let the app re-check to see the new thresholds in each
stock's Edit panel. `python backtest.py undo` restores the latest backup.

## Evidence in the app UI: `rule_stats.json`

`select` also writes `backtest_results/rule_stats.json`, a compact per-symbol,
per-rule summary of the backtested edge behind whichever threshold was actually
adopted (ticker-level or category-fallback — a "disable" decision has no live
threshold and so has no entry here). Shape:

```json
{
  "NVDA": {
    "volatility": {"exc5": 3.7, "hit5": 61.2, "n": 41, "mae": -2.8,
                    "level": "category", "short_history": false},
    "...": "..."
  }
}
```

- `exc5` — mean 5-day signal-direction excess return vs SPY, in percent.
- `hit5` — percent of episodes where that excess return was positive.
- `n` — episode count behind the number (more is more trustworthy).
- `mae` — mean adverse excursion (worst intra-window drawdown), in percent.
- `level` — `"ticker"` (per-symbol validated) or `"category"` (pooled fallback).
- `short_history` — true for tickers with a short trading history (ARM, SNDK as of
  this writing) where even a large `n` should be read with extra caution.

The app loads this file at startup (`app.py`'s `RULE_STATS`; missing file = the
evidence-line feature is silently off, no error) and serves it at
`GET /api/rule-stats`. The dashboard fetches it once, caches it client-side, and
renders a compact line — e.g. "Backtested: +3.2% avg vs SPY over 5d · 62% hit ·
n=41 · worst case -2.8%" — on every rule detail panel and alert entry, with a "low
confidence" badge when `short_history` is true or `n < 30`. The same numbers are
folded into the AI assistant's system prompt and the automatic news-synthesis
prompt, so both cite the actual backtested edge instead of describing a rule
generically. Re-run `select` (or the full pipeline) after any threshold change to
keep this file in sync with what is actually live in the app.

## Two-tier alerts & notifications

Retuning thresholds to their validated (stricter) values necessarily means fewer
alerts — correct, but it also removes the day-to-day "something is moving" signal
casual users had gotten used to. Two independent mechanisms address this without
diluting the validated signal tier itself:

- **Info tier** (`show_info_tier` setting, default on): after the normal
  signal-tier `evaluate_rules()` call, `app.py` runs a second, purely in-memory pass
  using `INFO_RULES` — the pre-retune (v3-era), looser category thresholds, mirroring
  `CATEGORY_DEFAULTS` in this file. No extra network calls (same quote/history
  already fetched). Any rule that crosses the info threshold but not the signal
  threshold becomes a transient "activity" event, shown as a single muted line per
  stock card. These are never persisted to the `alerts` table, never trigger
  `notify_alert`, and never carry a BUY/SELL claim — awareness only.
- **`notify_strong_only`** (setting, default **on** — a deliberate behavior change):
  every rule trigger still logs to the DB/UI regardless of this setting, but outbound
  push/email/WhatsApp notifications only fire when the symbol's multi-rule ensemble
  signal for that check cycle is STRONG (at least 2 weighted votes, ≥2/3 agreement —
  see `compute_signal()` in `app.py`, which mirrors the dashboard JS's
  `computeSignal()` exactly). Turn it off to get notified on every individual rule
  trigger, matching pre-v5 behavior.

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
- The calibration window was **bull-heavy**. The app computes SPY's regime (below vs
  above its 200-day SMA) and exposes it as `regime` on `/api/status`; when it reads
  `"bear"`, the dashboard shows a BEAR REGIME pill in the topbar and bounce-watch
  (SELL-side) alert details, rule cards, the AI assistant prompt, and the automatic
  news-synthesis prompt all get a short caveat that the backtested bounce statistics
  are less trustworthy evidence right now. Outside a bear regime this annotation is
  silent — it never appears for BUY signals or in a bull regime.
- Alerts are only valid inside their calibrated **4-trading-day action window**. The
  Alerts tab shows a per-alert "window: Nd left" / "window closed" chip (a simple
  Mon-Fri trading-day count, no holiday calendar), and a symbol group's aggregate
  signal badge only counts alerts still inside that window — an alert from weeks ago
  can no longer contribute to a "STRONG BOUNCE WATCH" badge.

## v6 app changes driven by the backtest

The v6 dashboard release turns several backtest findings into product behavior:

- **Vote membership (`weight-check`)** — only the four rules with a proven solo edge
  (volatility, support/resistance, volume, RSI) vote toward STRONG/TRENDING signals
  and outbound notifications. `consecutive_down`, `gap`, and `ma_cross` have weight 0.
- **`consecutive_down` demoted to context-only** — it had near-zero edge yet was the
  single largest alert generator (~1/4 of all alerts), so it now shows on the card for
  context but never logs an alert, sends a notification, or triggers a news-synthesis
  call (`CONTEXT_ONLY_RULES` in `app.py`). This cuts alert noise and API spend.
- **Self-scoring / outcome tracking** — every alert is scored ~5 trading days after it
  fires (`resolve_outcomes`): the stock's realized return and its signal-direction
  excess vs SPY are stored on the alert row. The Analytics tab shows live hit rate and
  average excess **per rule, next to the backtested figure**, so calibration drift is
  visible without re-running the backtest. This is the app auditing its own predictions.
- **Sensitivity dial** — a per-stock Conservative / Calibrated / Sensitive control
  (`_sensitivity` in `rule_params`, `_apply_sensitivity` in `app.py`) is the everyday
  way to trade alert volume for conviction; raw per-rule thresholds remain available as
  advanced overrides. "Calibrated" is the backtest baseline; "Sensitive" adopts the
  looser info-tier thresholds; "Conservative" tightens them ~30%.
- **One-click recalibration** — Settings → Recalibrate runs this backtester
  (`--refresh` → grid → combo → select) as a subprocess, previews the threshold changes
  vs what's live, and applies/undoes them (`/api/recalibrate/*`). It's the GUI wrapper
  around `select` + `apply`/`undo`, so the quarterly re-tune is a button, not a CLI ritual.
- **Daily digest** — an optional once-a-day summary (email + WhatsApp) that replaces
  instant pings, matching the 1–5 day signal horizon.
