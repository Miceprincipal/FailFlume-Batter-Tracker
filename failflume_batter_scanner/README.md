# FAILFLUME Batter Scanner v1.7.2.4

Local MLB batter scanner. Python standard library only; browser UI; canonical Savant history plus a short-lived MLB StatsAPI D−1 overlay.






## v1.7.2.4 — conservative bookmaker-compatible parlay export

v1.7.2.4 changes only parlay JSON construction. **Different games only** is enabled by default: generated standard multiples contain at most one selection from any MLB game, avoiding same-event/related-contingency combinations that generally require bookmaker-specific Bet Builder / Same Game Parlay handling. The control can be disabled explicitly for intentional SGP/Bet Builder workflows. JSON schema `failflume.parlays.v5` records the compatibility profile and number of candidate combinations rejected by the rule. Ranking, scoring, calibration, history and model versions are unchanged.

## v1.7.2.3 — deep-grid follow-goal / lazy scroll / cached oddities

v1.7.2.3 extends the presentation-only deep-grid workspace. With **follow goal ranking** enabled (default), changing GOAL type/target immediately re-ranks and refills the loaded cards from the already-loaded scanner state; no extra network/model fit is required. The selected layout now defines the lazy-load batch size (for example 2×2 loads four cards at a time, 3×3 loads nine), while a scrollable workspace appends further ranked cards near the bottom or via **LOAD MORE**. Disabling follow-goal preserves manually selected players.

The card views add **Oddities**, exposing only quantities already present or directly countable in the cached PA/pitch history: BB/PA, HBP/PA, combined free-pass rate, recent walk/HBP rate/counts, K−BB gap, whiff/swing, AB/PA, xBA−AVG and raw swing/whiff counts. Full cards and the player drawer also surface the main walk/HBP fields. These are display diagnostics only; `SCORING_VERSION` and `DEEP_MODEL_VERSION` remain the frozen v1.7.1 model and none of the added values feed ranking, tuning or calibration.

## v1.7.2.2 — configurable player deep-grid workspace

v1.7.2.2 adds a presentation-only player workspace. The browser can display 1×1, 2×2, 2×3, 3×3, 4×2 or 4×4 reusable player cards; each slot has independent player/view selectors, plus global view and rank-fill controls. Initial views are Goal, Contact, Power, HR, Trending, Matchup, Splits and Full. Workspace state is stored in browser localStorage. No scoring, calibration, feature, data-ingestion or ranking logic changes.

## v1.7.2.1 — recent-date reconciliation retry fix

v1.7.2.1 fixes a sync-selector bug in the v1.7.2 D−1 bridge. Dates inside the provisional lookback are now deliberately reprocessed on `SYNC DATA` even if an older run/database row already marked the date `ok`. This lets upgraded v1.7.1 databases recover from a prematurely successful date marker and lets later syncs deterministically retire provisional rows when canonical Savant catches up. Older successful dates remain skipped. Predictive/scoring logic is unchanged.

## v1.7.2 — D−1 provisional bridge (data pipeline only)

v1.7.2 changes **data availability, not predictive logic**. The v1.7.1 scoring/model/tuning families are unchanged. The purpose is to remove the silent D−2 blind spot when Baseball Savant has not yet published yesterday.

`SYNC DATA` still asks Savant first. For recent completed dates (default: the last two calendar days), if Savant is empty, unavailable, or missing completed games, the scanner reads MLB StatsAPI completed-game feeds and writes a `statsapi_provisional` overlay containing PA outcomes, batter/pitcher IDs and handedness, pitch descriptions, and any available launch speed / launch angle / distance. StatsAPI does **not** supply Savant xBA/barrel here, so those fields remain unknown; provisional rows do not update the deep xBA-quality state and missing barrel/xBA values are never treated as zero.

When Savant publishes the date, canonical rows replace the provisional overlay **game by game**. If a provisional game exists, replacement is allowed only when the Savant game has at least as many completed PAs as the provisional feed; an obviously partial Savant publication therefore cannot erase a complete D−1 game. Games absent from the current Savant response keep their provisional rows and remain retryable. Raw Savant CSV and MLB JSON snapshots remain immutable.

The UI now distinguishes `history through` from `canonical Savant through` and visibly flags `PROVISIONAL D−1`. Deep hyperparameter fitting plus Backtest Lab / Historical Replication stay canonical-only; the provisional overlay updates live pregame player, pitcher, hand, bullpen/park and opportunity state but cannot silently retune the frozen model from incomplete quality fields.

Because this bridge was designed after the Sep 14 provisional board had already been inspected, **Sep 14 is a shadow comparison for the bridge itself; clean prospective use of the new ingestion path begins Sep 15.** The v1.7.1 model-family boundary remains Sep 14.


## v1.7.1 — final simplicity gate / Sep 14 freeze candidate

v1.7.1 deliberately adds **no new predictive feature**. It is the final pre-prospective pruning pass before the Sep 14+ clean run.

The v1.7 total-history PA-reliability experiment is fixed **OFF** (`batter_reliability_prior_pa = 0`). On the Aug 14–Sep 12 development block it changed log loss by only about two hundred-thousandths, and the three historical development recheck windows all re-selected `k = 0`. The compatibility key remains in old-fit scoring, but v1.7.1 does not tune it.

After the normal dual-panel tuner fits the remaining FULL model, v1.7.1 creates exactly one explicit **LEAN challenger** on the same deterministic folds. LEAN removes the generative feature families most likely to be polishing noise — recent-form outcome adjustment, xBA contact quality, bullpen quality and park environment — while retaining player history, handedness, pitcher effect, starter exposure, lineup/PA opportunity and within-game rho. The retained LEAN knobs are locally re-optimized before comparison.

The parsimony rule is predetermined: **LEAN replaces FULL only if its primary dual-panel game log loss is within paired 1-SE of FULL and its top-decile log loss is no worse.** Otherwise FULL remains selected. This is intentionally asymmetric: complexity must justify itself; the stripped model does not need to manufacture a nominal win to survive. The chosen family is then calibrated normally. Backtest Lab reports FULL LL, LEAN LL, paired 1-SE, both tail LLs, and the selected family.

The v1.6 RHP-specific calibration slope remains retired after 0/3 historical selection. Fine prior-PA bins remain diagnostics only. Historical audit/recheck windows are development-spent; they can report whether FULL or LEAN would have been selected historically, but they are not independent validation of v1.7.1.

**Freeze this build before Sep 14 games. Do not use Sep 14+ outcomes for any further model/family choice until the prospective sample is intentionally opened.**

In v1.7.1 the data/cache contract was unchanged; v1.7.2 later adds the explicitly replaceable provisional D−1 overlay described above.

## v1.7 — continuous low-history reliability gate (retired in v1.7.1)

v1.7 tested one narrow model change motivated by the v1.6 replication audit: a continuous total-history batter reliability gate `prior_PA / (prior_PA + k)` with `k=0` as exact v1.6 behavior. It attenuated player/hand/recent/xBA deviations only. The experiment produced negligible development improvement and re-selected OFF in all three historical development rechecks, so v1.7.1 freezes it at zero rather than carrying it into the prospective test.

## v1.6 — RHP calibration audition + pseudo-prospective replication carousel

v1.6 makes two deliberately narrow changes after the persistent development residuals in v1.5.1.

First, calibration gets **one earned interaction only**: an RHP-specific raw-logit slope. The simpler main-effects family remains a candidate and `RHP slope = OFF` is explicit. Chronological blocked OOF log loss must prefer the interaction over both identity and the simpler calibrator before it survives. There is still no general interaction lattice.

Second, Backtest Lab adds a **Historical Replication Audit**. This is not another tuner target. It mechanically selects 2–5 deterministic, non-overlapping historical windows before the repeatedly-inspected Aug–Sep development block. For each window, the model is fit using only dates before that window starts, parameters are frozen for the whole window, and each game date is predicted before that date is revealed. Later audit windows may use earlier-window outcomes because those outcomes would have been known by then; no window sees itself or the future. The audit reports the same calibration/ranking/slice diagnostics, including fine prior-PA bins, but never auto-applies the results back into the current live fit.

The audit defaults to **3 windows × 21 game dates** because a full 2026 local season usually leaves enough pre-August history for three genuinely separate blocks after the 55-date minimum training runway. The UI will reduce the count only if the local history cannot support the requested number without overlap. Identical model/data/audit settings reuse the append-only stored result. Historical fitting/scoring is entirely local and makes no MLB/Savant calls.

A report-only **fine prior-PA diagnostic** (`12–24`, `25–49`, `50–99`, `100–199`, `200–399`, `400+`) remains shown in both the normal development backtest and the historical recheck. v1.7 did not insert a MID-band patch or choose knots from those windows; it tested only the continuous reliability family described above. v1.7.1 retires that family after it failed to earn robust contribution.

The clean prospective boundary remains **Sep 14+**. The original v1.6 audit established that the sample-size residual recurred; because that result directly motivated v1.7 and then v1.7.1 family pruning, those same blocks are development evidence rather than pristine external validation.

## v1.5.1 — opener / bulk-role correctness patch

v1.5.1 keeps the v1.5 local-feature experiment intact but fixes the historical pitcher-role proxy used by both the bullpen state and oracle-context backtest. Completed team-games are now classified structurally: the first pitcher remains the functional starter unless he faces **9 or fewer batters** and a later pitcher faces **12 or more**, in which case that later bulk arm becomes the functional starter. The opener's PAs therefore enter bullpen history and the bulk follower is excluded from it. Historical backtest context also uses the inferred bulk arm/hand rather than pretending the opener was the full starter.

The inference is performed only after a completed prior game when updating historical state, so it cannot leak the game currently being predicted. Backtest reports expose the count of opener-proxy team-games. The normal non-opener path is regression-compatible with v1.5; opener games intentionally differ, so a whole-dataset floating-point identity check against v1.4.1 is no longer a valid test after this correctness fix.

No new model feature, search weight, API source, or interaction is introduced. xBA, bullpen and park effects still include exact zero candidates; network throttling is unchanged; the clean prospective boundary remains **Sep 14**.

## v1.5 — blind local feature expansion

v1.5 adds three feature families that can be reconstructed strictly from information already present **before** the game being predicted. They enter the same deterministic expanding/fixed-width nested-blind tuner as the existing model and each has an exact zero-effect candidate, so none is retained merely because it sounds baseball-smart.

- **Statcast contact quality:** rolling expected hits from `estimated_ba` (xBA) on prior plate appearances. Non-batted-ball PAs contribute zero expected hits, keeping the signal on a per-PA scale. The tuner selects both shrinkage (`contact_quality_prior_pa`) and influence (`contact_quality_effect`).
- **Opponent bullpen quality:** prior relief-pitcher PA outcomes for the opposing team. The first pitcher faced by each batting side is excluded as the starter; later pitchers feed the bullpen posterior. `bullpen_prior_pa` and `bullpen_effect` are blind-tuned.
- **Park environment:** the home franchise's prior home-game outcome distribution is compared with the same franchise's road-game environment, then shrunk toward league rates. `park_prior_pa` and `park_effect` are blind-tuned.

The order is: stable player → handedness → recent outcomes → xBA contact-quality correction → starter/bullpen mixture → park adjustment → coherent game-market probabilities → existing cross-fitted slice calibration. The tuner also records explicit final-config ablations for the three new effects so the report shows the dual-panel log-loss cost of forcing each feature back to zero.

Pitch-family matchup and weather are deliberately **not** added yet. The current SQLite history does not contain a complete pitch-family feature column for the already-cached season, and historical weather is not stored as timestamped pregame forecasts. Adding either retroactively would create an avoidable provenance/leakage problem.

Because Sep 13 outcomes were already being observed while these v1.5 feature choices were discussed, **v1.5's clean prospective external validation begins Sep 14**. Sep 13 can still be scored/recorded, but it is not claimed as a pristine external test of this new architecture.

Network behaviour is unchanged: historical fitting/backtesting is local; Savant/MLB access is only for backfill/current-slate metadata and remains globally throttled/cached.

## v1.4.1 — opportunity-boundary check

v1.4.1 is a deliberately narrow development build. The v1.4 backtest selected both `starter_share_scale = 1.25` and `lineup_slot_strength = 1.25`, which were the upper bounds of their search grids. This build changes **only the search range for those two opportunity components** so the tuner can determine whether 1.25 was a real optimum or merely the edge of the cage.

Both grids are now:

`0.00, 0.25, 0.50, 0.75, 1.00, 1.25, 1.50, 1.75`

All dual-panel folds, sqrt(date-N) primary scoring, paired 1-SE admission rule, top-decile proper-score tie-break, calibration policy, rho treatment, network limits and the then-current prospective-holdout discipline are unchanged. Tuning/model/backtest version IDs are bumped so v1.4 results cannot be silently reused for this boundary check.


## v1.4 — dual rolling truth-hunt / paired 1-SE tail guard

v1.4 replaces the single inner development window with **two deterministic chronological panels that score the same test blocks**:

- **expanding history** — every fold may use all earlier dates;
- **fixed-width history** — every fold uses the same number of immediately preceding game dates as the first expanding fold.

The fixed-width panel exists specifically to stop us confusing "this signal changes by calendar regime" with "this signal only became stable after the model had more history." Every parameter candidate sees the exact same folds. Rerunning the same model/data/config does not draw a friendlier random sample.

Each panel contains up to five non-overlapping four-game-date test blocks spread across the available pre-holdout history. Within every block, all player-games are predicted before that date is revealed. The fixed training width is derived mechanically from the available history (the first expanding fold's training span), not hand-selected after seeing results.

### Predetermined two-stage model selection

The primary objective is game-level binary log loss across supported H / TB / HR / XBH targets. A date contributes with weight `sqrt(N_date)`, interpolating between "every date has equal weight" and "every player-game has equal weight." Expanding and fixed-width panel scores then receive equal weight.

For each coordinate search step:

1. find the candidate with the lowest dual-panel primary log loss;
2. compare every other candidate to that winner using paired date-level loss differences;
3. any candidate within **one paired standard error** remains statistically admissible;
4. among the admissible set, select the lowest **top-decile log loss**; top-decile Brier and absolute calibration error break remaining ties;
5. AUC and Top-N lift remain report-only diagnostics.

The top decile is defined **within each game date** from each candidate's own frozen pre-outcome probabilities, then pooled for scoring. Tail scores are calculated independently per supported market/target and those cells receive equal weight, so common `1+ hit` outcomes cannot drown out rarer targets. There are still no hand-set `lambda` / `mu` robustness weights.

This also changes the treatment of `rho`: within-game clustering is **not** killed merely because `rho=0` wins a tiny global-mean advantage. Any rho value that remains within paired 1-SE of the global winner is allowed to survive if it materially improves the proper tail score. The same two-stage rule applies to every tunable contribution; nothing is sacred, but nothing is executed by a metric structurally insensitive to its job.

The search trace records expanding-panel and fixed-width winners plus per-fold winners for each coordinate. Panel agreement is therefore inspectable rather than inferred from one aggregate number.

### Calibration and holdout discipline

After the generative model is chosen, additive hand / lineup-band / prior-PA-band / recent-form-band calibration is still main-effects-only and L2-shrunk. Calibration is learned from the expanding-panel out-of-fold predictions only; ridge strength remains selected by chronological blocked CV and identity/no-calibration remains an explicit candidate. No interaction lattice is added.

Historical data through Sep 12 is development-spent once inspected. The Backtest Lab remains useful as a development benchmark, but **for v1.5, clean prospective validation begins Sep 14 because Sep 13 outcomes were observed during feature design.**

Network behaviour is unchanged from v1.3.2: Savant request starts are globally spaced by at least 3 seconds, MLB StatsAPI starts by at least 0.5 seconds, metadata workers are capped at two, fresh endpoint snapshots are reused, and shared 429 cooldown/backoff remains enforced. Historical model fitting itself is local and makes no API calls.


## v1.3.2 — unseen-pitcher fix + network politeness guard

Keeps the v1.3.1 `starter_share` unseen-pitcher hotfix and adds a shared host-level request limiter so UI refreshes, concurrent MLB metadata fetches, retries, and historical backfill cannot independently burst the providers. Defaults are deliberately conservative: Baseball Savant requests start at least **3 seconds apart**, MLB StatsAPI request starts at least **0.5 seconds apart**, MLB metadata uses at most **2 workers**, and even a manual FORCE refresh reuses any endpoint snapshot younger than **30 seconds**. HTTP 429 now creates a shared 15/30/60s-or-`Retry-After` cooldown across every worker instead of only sleeping the request that happened to receive the throttle.

Backfill remains one Savant date request at a time, skips successfully cached dates, preserves immutable raw responses, and normal daily use reads historical scoring entirely from the local database. These limits can be made *slower* with environment variables; the code clamps the defaults so accidental aggressive settings do not silently remove the safety floor.

## v1.3.1 — unseen-pitcher hotfix

Fixes an `UnboundLocalError` in live scoring when tonight's probable starter has zero historical PA in the local cache. `starter_share` is now defined from the fitted opportunity model regardless of pitcher-history availability; an unseen pitcher correctly falls back to the pre-pitcher batter distribution while preserving diagnostics. No model/tuning/scoring version is changed, so existing v1.3 fitted parameters and append-only cache remain reusable.


## v1.3 — blind auto-balance / nothing sacred

v1.3 changes the deep-model fitting objective from "fit the per-PA engine, then inspect game probabilities" to a nested chronological game-level tuner. The existing v1.1 PA fit may be used as a leakage-safe **starting point only**; game-level blind folds can move away from it.

The inner tuner can rebalance or suppress all major contributions:

- player shrinkage (`player_prior_pa`) and player-skill influence (`player_effect`)
- handedness shrinkage and handedness influence
- recency memory, recency prior strength and recency influence
- opposing-pitcher shrinkage and pitcher influence
- empirical starter PA share via a tunable scale
- batting-order PA opportunity via a tunable strength
- within-game hit overdispersion via a tunable rho scale

Where zero is coherent, **zero is an explicit candidate**. A model in which recent form, handedness, pitcher context, lineup position or hit clustering does not improve blind game-level log loss can turn that contribution off rather than preserving it by design. The second coordinate pass revisits shrinkage/effects after the first rebalance so early choices are not frozen.

### Predetermined decision rule

There are no hand-picked slice-robustness `lambda` / `mu` weights in this build. Hyperparameter selection uses one rule declared before evaluation:

1. lowest mean chronological-fold **game-level binary log loss** across sufficiently populated H / TB / HR / XBH target cells;
2. Brier score breaks an exact log-loss tie;
3. AUC and Top-N lift are diagnostics only and never override probability accuracy.

Rare cells with too few positive/negative events are excluded from the shared tuning objective so a handful of 3-HR/4-HR games cannot steer the entire model. They remain visible in the final reports.

### Slice calibration stays blind

After the raw generative model is selected, v1.3 fits additive logit calibration main effects for:

- opposing starter hand;
- lineup band (1–3 / 4–6 / 7–9);
- prior-PA band;
- recent-form band.

No interaction lattice is fitted in v1.3. Slice boundaries are derived from the **earlier training block available to that fold**, not from future outcomes or future feature distributions. Slice offsets are L2-shrunk toward zero. Ridge strength is chosen by rolling blocked CV, and raw identity/no-calibration competes explicitly; if calibration does not improve blind log loss for a market/target, it is disabled.

Independent target calibrators are projected back onto the mathematical constraint `P(1+) >= P(2+) >= P(3+) >= P(4+)`, so calibration cannot produce incoherent threshold probabilities.

### Holdout status

The historical Backtest Lab still freezes each scored day before revealing that day's results. However, after inspecting the Aug 14–Sep 12 v1.2 report, that historical period is **development-spent** for future model-design claims. v1.3 labels historical outer runs as development benchmarks. **For v1.5, clean prospective validation begins Sep 14 because Sep 13 outcomes were observed during feature design.**

The first v1.3 fit is heavier than v1.2 because it evaluates multiple blind game-level alternatives. Fits remain append-only and are reused until the local historical coverage signature changes.



## v1.2 — Backtest Lab / locked game-level holdout

v1.2 adds a local **BACKTEST LAB** for the deep generative model. It does not change the existing v1.0 PA model or its auto-fit mathematics; it evaluates the model at the game-market level over a large historical block.

Click **BACKTEST LAB** and choose:

- **Holdout game dates** — default 30 final game dates in the local Statcast database.
- **Min prior PA** — default 12, matching the scanner's ordinary minimum-history filter.
- **RUN / REUSE LOCKED TEST** — one pass computes every 1+/2+/3+/4+ Hits, Total Bases, HR and XBH target.

The same model/data/config combination is immutable: rerunning it reuses the existing append-only result instead of recomputing the same holdout. Changing the market/target dropdown only changes which already-generated result is displayed; it does not touch the holdout again.

### Leakage boundary

The holdout split is chronological. Deep hyperparameters are auto-fit with `before_day = holdout_start`, so the locked block is unavailable to parameter selection. For every date inside the holdout:

1. all starters on that date are scored using sufficient statistics containing only earlier dates;
2. every prediction for the date is frozen;
3. only then are that date's plate appearances added to batter, handedness, pitcher, recent-state and league history;
4. the loop advances to the next date.

This is the same one-way time boundary used by the existing PA-level tuner, now applied to final game-market probabilities.

### Historical context mode

The first historical evaluator is deliberately labelled **oracle-context**. The completed game's terminal PA rows are used only to reconstruct two facts that existed before play:

- batting order = first nine distinct batters for each team-game;
- opposing starter = first pitcher actually faced by that team.

The hitter/pitcher performance model still uses only prior dates. This cleanly tests the mathematics given the correct actual lineup/starter, but it is not presented as a reconstruction of exactly what a pregame feed knew at a particular timestamp. New append-only MLB snapshots can support a separate true pregame-context test as that archive grows.

### Metrics

Every target reports:

- player-games, actual successes and expected successes;
- actual rate and mean predicted probability;
- Brier score and a flat-model-mean Brier benchmark;
- Brier skill versus that benchmark;
- binary log loss;
- pooled AUC;
- logistic calibration intercept and slope;
- expected-minus-observed z diagnostic (explicitly only an independence diagnostic);
- equal-count calibration deciles;
- Top-5 / Top-10 / Top-20 per-slate lift;
- slices by opposing starter hand, lineup band, prior-PA sample size and early/middle/late holdout block.

The Backtest Lab is an **evaluator**, not a game-level auto-tuner. It does not search weights against the locked holdout. Model changes should be made using development/rolling-CV data; a new model version then earns one new locked evaluation.

### Append-only storage

Backtest output is stored locally in new append-only tables:

- `backtest_runs`
- `backtest_predictions`

Canonical Statcast rows, raw Savant snapshots, MLB response snapshots, model fits and previous backtest runs are never overwritten. The only intentionally replaceable database rows are `statsapi_provisional` D−1 rows, which are retired when the matching canonical Savant game is complete.

CLI equivalent:

```bat
python app.py --backtest --holdout-days 30 --backtest-min-pa 12
```

## Upgrade without losing data

The ZIP contains **no SQLite database and no cached API responses**. Extract/merge `failflume_batter_scanner` over the existing folder and keep the existing `data/` directory.

Historical canonical data remains append-only, with one explicit provisional overlay:

- Savant raw CSV snapshots are content-addressed and never replaced.
- Canonical Savant pitch rows remain `INSERT OR IGNORE` on `(game_pk, at_bat_number, pitch_number)`.
- `statsapi_provisional` rows for recent dates may be deleted/replaced for the same game when canonical Savant reaches at least the same completed-PA count.
- MLB JSON refreshes append immutable snapshots.
- Derived state snapshots append under `data/state_cache/YYYY-MM-DD/`.
- Deep-model fits append to the `model_fits` table; a new data coverage signature creates a new fit rather than replacing an old one.
- Old derived scoring is disposable/versioned and is recomputed from the immutable Statcast store.

## Run

Windows: double-click `run.bat`.

The launcher prefers `%LOCALAPPDATA%\Programs\Python\Python312\python.exe`, clears `PYTHONHOME`/`PYTHONPATH`, starts the local server and opens:

`http://127.0.0.1:8765`

macOS/Linux: `./run.sh`.

## v1.0 — deep model now tunes itself from the local data

Click **RUN DEEP ANALYSIS**. The first deep run after the historical database changes performs a rolling out-of-sample fit; later runs reuse that append-only fit until new data changes the coverage signature.

Deep mode no longer uses the UI recency slider or the old fixed values for:

- player prior strength
- handedness prior strength
- recent-state prior strength
- recent-state memory/decay
- pitcher prior strength

Those values are selected by the local history using **future-date holdout prediction** and multinomial per-PA log loss. The validation date is scored using only information from earlier dates; that day's PA are added only after every prediction for the date has been frozen.

The model also measures rather than hand-enters:

- the historical fraction of team PA actually faced against the first opposing pitcher in the game (`starter_share`)
- the full PA-count distribution for batting-order slots 1–9 from local team-game PA totals

The current quick-model `Recency decay` control is disabled while deep mode is active. Deep mode displays its learned recency half-life in PA and approximate games.

### Auto-fit objective

The tuner works on one coherent PA outcome model:

`OUT / BB-HBP / 1B / 2B / 3B / HR`

For historical validation PA it builds, strictly from prior dates:

1. player posterior vs local league outcome distribution
2. handedness posterior vs the corresponding local league L/R environment
3. exponentially weighted recent-state posterior
4. opposing-pitcher allowed posterior

It then minimizes out-of-sample multinomial log loss. Brier score is recorded as a secondary diagnostic.

The drawer exposes the selected parameter values and the validation log-loss improvement versus the old fallback defaults.

### When auto-fit cannot run

If local history is shorter than 35 game dates or 12,000 terminal PA, deep mode uses conservative fallback parameters and labels itself `fallback`. Once enough data exists, it fits automatically.

For a 90-day MLB cache the first fit can take tens of seconds on a normal desktop. It is not repeated unless the historical coverage changes or the tuning version changes.

## One personalised generative model per hitter

Every deep-mode batter receives one per-PA outcome distribution over:

`OUT / BB-HBP / 1B / 2B / 3B / HR`

All markets are derived from that same distribution:

- `P(1+ / 2+ / 3+ / 4+ Hits)`
- `P(1+ / 2+ / 3+ / 4+ Total Bases)`
- `P(1+ / 2+ / 3+ / 4+ HR)`
- `P(1+ / 2+ / 3+ / 4+ XBH)`

Changing **Number** or **Type** therefore queries a different tail of the same player model; it does not switch to a separately hand-weighted equation.

## Personal shape, handedness and recent state

The stable hitter distribution is empirical-Bayes/Dirichlet shrinkage toward the local league distribution. The shrink strength is auto-fit.

Tonight's probable starter hand selects actual PA-level Statcast `p_throws` history. Player L/R evidence is shrunk toward the player's stable shape plus the local league hand environment, with the shrink strength auto-fit.

Recent form is a continuously decaying per-PA outcome distribution. Both its half-life and its shrinkage toward stable skill are auto-fit from historical holdout prediction.

## Pitcher context

The opposing probable starter gets an allowed outcome posterior, with pitcher shrinkage auto-fit from historical PA prediction.

The starter's adjustment is applied only for the historically observed starter PA share. The remaining expected PA effectively revert toward the non-starter environment instead of pretending the probable starter controls the entire game.

## PA opportunity

Confirmed batting order now uses an empirical PA-count distribution learned directly from local team-game history for that lineup slot. It is no longer based on a manually typed `4.7 / 4.6 / ...` opportunity table in deep mode.

If lineup order is unknown, deep mode uses the empirically observed all-slot PA distribution.

## Within-game clustering now propagates to every market

The existing player hit-overdispersion estimate remains deliberately conservative and cross-game streaks remain diagnostic-only.

v1.0 now propagates the same estimated within-game production state through all market tails:

1. number of hits in a game is drawn from the player's beta-binomial hit distribution
2. those hits are allocated among `1B / 2B / 3B / HR` using the player's personalised hit composition
3. Hits, TB, XBH and HR targets are all calculated from that shared game state

This avoids fitting separate, extremely noisy HR/XBH clustering parameters while also avoiding the old asymmetry where only multi-hit tails received clustering information.

## What the UI shows

For the selected target every row shows:

`base probability · recent-state delta · starter-matchup delta · tonight model probability`

The player drawer includes:

- full 4×4 market probability matrix
- stable hit composition
- final per-PA outcome distribution
- expected PA distribution
- observed vs model multi-hit shape
- hit overdispersion / streak diagnostics
- local sample sizes
- **AUTO-FIT MODEL PARAMETERS** with learned prior strengths, recency half-life, starter share, validation PA and log-loss improvement

These percentages are model estimates. Prospective calibration against realized game outcomes remains a separate validation layer.

## Parlay JSON

Deep-mode JSON exports the selected leg `model_probability_pct`, the deep-model/tuning version and the learned recency/prior parameters used for the slate.

Parlay generation still prioritizes automatic diversification across players, games and repeated pairs. It does **not** multiply leg probabilities into a joint parlay probability because cross-leg correlation is not modeled.

## QUICK mode

QUICK mode remains as the lightweight heuristic Contact / HR / Trending / GOAL scanner. Its recency slider still works there. Deep mode deliberately ignores it.

Current scoring identifier: `1.0-generative-autofit-v1`  
Deep model identifier: `1.0-pa-dirichlet-autofit-v1`  
Auto-tuning identifier: `1.0-rolling-pa-logloss-v1`

## Backfill / sync

An empty database automatically starts a conservative 90-day backfill. Savant is fetched one game date at a time with a default 3-second delay; canonical-complete dates are skipped later. Recent incomplete dates may be temporarily filled from MLB StatsAPI and are retried until Savant becomes canonical.

For deep analysis, **season backfill is preferable**: more history improves empirical-Bayes shrinkage, L/R splits, pitcher estimates and holdout tuning.

`SYNC DATA` requests missing/retryable canonical dates through yesterday. For the recent provisional window it falls back to completed MLB StatsAPI game feeds whenever Savant is late or incomplete, then automatically retires those provisional games on a later successful Savant sync.

Environment override:

```bat
set FAILFLUME_SAVANT_DELAY=3.0
set FAILFLUME_PROVISIONAL_DAYS=2
run.bat
```

## Diagnostics

- `/api/health`
- `/api/backfill_status`
- `/api/config`

## v1.1 — standalone JSON parlay tracker

`FAILFLUME_PARLAY_TRACKER.html` is a separate standalone tracker and does not replace `static/index.html`.

- Open `FAILFLUME_PARLAY_TRACKER.html` directly in a browser.
- Use **IMPORT JSON(S)** to load one or more parlay JSON files generated by the scanner.
- Imported batches are normalized and stored locally in browser localStorage.
- Each imported JSON has its own **DELETE** button; deleting a batch also removes that batch's manual leg overrides.
- The tracker polls MLB StatsAPI every 30 seconds and tracks H, TB, HR, and XBH targets from live/final box scores.
- Manual leg override cycles through automatic → WON → LOST → VOID → automatic.
- Multiple slate dates can coexist in the tracker; the MLB schedule is fetched once per imported date per refresh.
- Generated JSON does not contain bookmaker odds/stakes, so imported generated tickets display model metadata rather than pretending to know a payout.
