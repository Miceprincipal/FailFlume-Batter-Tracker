# FAILFLUME — Early Test Package

## IMPORTANT: statistical research tool only

**FAILFLUME is a clanker-built experimental statistical/research tool in early testing. It is not a betting system, not betting advice, and should not be relied upon to decide whether to place a bet or how much to stake.**

The rankings, probabilities, quality scores, diagnostics and generated parlay combinations can be wrong, incomplete, stale, miscalibrated or affected by data/API issues. Historical performance does not establish future performance. The parlay exporter uses conservative structural compatibility rules, but **it does not guarantee that a bookmaker will accept a combination or that the combination complies with every bookmaker's rules.**

Use it to inspect/test the model and interface. Treat anything betting-related as test output only.

---

## What is in this repository

- `failflume_batter_scanner/` — local Python MLB batter scanner and browser UI.
- `FAILFLUME_PARLAY_TRACKER_v1.1.html` — standalone browser tracker for JSON parlay exports from the scanner.
- `SHA256SUMS.txt` — hashes for the files in this package.

The scanner is currently **app v1.7.2.4** with the predictive/scoring model still frozen at the v1.7.1 model family.

## What the software does

The scanner keeps a local MLB history cache, scores current batters for selectable targets such as hits / total bases / home runs, and exposes diagnostic views including Goal, Contact, Power, HR, Trending, Matchup, Splits, Full and Oddities.

Recent completed games can temporarily use an MLB StatsAPI D−1 overlay when Baseball Savant is late; later canonical Savant data replaces that provisional overlay.

The parlay exporter can generate test combinations from ranked players. **“Different games only” is enabled by default** as a conservative compatibility rule for standard multiples. This is not a universal bookmaker-rule validator.

## Network / privacy behaviour

The application runs locally on your computer. The browser UI is served at:

`http://127.0.0.1:8765`

The scanner contacts public MLB/Baseball data endpoints when it needs current/history data, principally:

- Baseball Savant / Statcast
- MLB StatsAPI

The standalone parlay tracker also contacts MLB StatsAPI to update game/box-score status for imported selections.

Scanner history/database/cache files are stored locally under `failflume_batter_scanner/data/` after use. **No database or personal data is included in this repository.** The tracker stores imported batches and manual overrides in browser `localStorage`.

If you want to inspect before running it, the main scanner code is `failflume_batter_scanner/app.py` and the UI is `failflume_batter_scanner/static/index.html`. The project uses the Python standard library only.

---

# Simple walkthrough

## 1. Get it from GitHub

Prefer a GitHub repository/release rather than receiving a loose executable or archive from somebody directly.

From GitHub you can either:

1. open the repository;
2. choose **Code → Download ZIP**; or
3. download the package from the repository's **Releases** page if one is provided.

Nothing here requires installing a custom executable.

### Optional integrity check

Compare the downloaded release ZIP's SHA-256 with the hash published on the GitHub release page. Inside the repository, `SHA256SUMS.txt` contains hashes for the packaged source files.

Windows PowerShell:

```powershell
Get-FileHash .\FAILFLUME_TESTER_PACKAGE_v1.7.2.4.zip -Algorithm SHA256
```

macOS/Linux:

```bash
shasum -a 256 FAILFLUME_TESTER_PACKAGE_v1.7.2.4.zip
```

## 2. Extract the ZIP

Extract it to a normal folder you can write to. Do not run it from inside the compressed ZIP viewer.

## 3. Start the batter scanner

### Windows

Open:

`failflume_batter_scanner`

Then double-click:

`run.bat`

A browser should open automatically at:

`http://127.0.0.1:8765`

If Python is not available, install a normal current Python 3 build from python.org and retry.

### macOS / Linux

Open a terminal in `failflume_batter_scanner` and run:

```bash
chmod +x run.sh
./run.sh
```

Then open `http://127.0.0.1:8765` if it does not open automatically.

## 4. Load/sync baseball data

On first use, the scanner may need to build local history.

1. Click **SYNC DATA**.
2. Let the status panel finish its current fetch/reconciliation work.
3. The scanner may show a provisional recent-history marker if yesterday's full Savant data is not yet available.
4. A later sync can replace provisional MLB data with canonical Savant data automatically.

An empty database starts with a conservative historical backfill. More history takes longer but gives the model more local history to work from.

## 5. Pick what you want to inspect

Use the **GOAL** controls to select the target, for example:

- 1+ hit
- 2+ hits
- total bases
- home run

Changing the goal refreshes/re-ranks the deep-grid view when **follow goal ranking** is enabled.

The large configurable player grid can be changed between layouts such as 1×1, 2×2, 3×3 and 4×4. Each card can show a different diagnostic view.

Useful views:

- **GOAL** — target ranking/probability/quality summary.
- **CONTACT** — batting/contact-oriented cached statistics.
- **POWER / HR** — power/home-run-oriented diagnostics.
- **TREND** — recent cached result pattern.
- **MATCHUP** — current opponent/starter context.
- **SPLITS** — handedness-specific history, with sample-size shrinkage.
- **ODDITIES** — descriptive cached metrics such as BB/PA, HBP/PA, K/PA, whiff/swing, AB/PA and xBA−AVG.
- **FULL** — broader combined display.

These diagnostic displays are not separate betting signals and should not be treated as such.

## 6. Optional: export test parlay combinations

The scanner can export JSON combinations for the standalone tracker.

For ordinary test multiples, leave:

**Different games only** ✓

This prevents the generator from putting two selections from the same MLB game into a standard combination. If the compatibility rule leaves too few valid combinations, the scanner outputs fewer tickets rather than forcing incompatible legs together.

Again: **an exported combination is not a recommendation and is not guaranteed to be accepted by a bookmaker.** Bookmaker-specific same-game, correlated-market, stake, market-availability and account rules are outside the scanner's control.

## 7. Open the parlay tracker

You do not need to install anything for the tracker.

Double-click:

`FAILFLUME_PARLAY_TRACKER_v1.1.html`

It opens directly in your browser.

Then:

1. click **IMPORT JSON(S)**;
2. select one or more JSON files exported by the scanner;
3. the tracker will try to match the relevant MLB games and update results;
4. use **override** on a leg only if an automatic result needs manual correction;
5. use **DELETE TICKET** to remove one parlay without deleting the whole imported batch;
6. use the batch **DELETE** button only when you want to remove the entire imported JSON batch.

## 8. Stop the scanner

Close the terminal/command window that started the scanner, or press `Ctrl+C` in that window.

The local browser tracker can simply be closed like any other page.

---

# A few things to know while testing

- This project is under active development. UI, data handling and diagnostics can change between versions.
- Data feeds can be late or incomplete. The UI distinguishes provisional recent data from canonical Savant history where possible.
- The model is being evaluated prospectively; short runs can look much better or worse than the longer-run model quality.
- Do not infer that a high rank or displayed probability is an instruction to bet.
- Do not use generated parlay structure as evidence that the underlying selections are profitable.
- If something looks odd, save the exact board/export and report the version/date rather than assuming the output is correct.
