# Bio Species Rule Optimization — Analysis Notes

## Overview

Systematic optimization of exobiology species prediction rules using galaxy-wide
ground truth from Spansh landmark data. The goal is to improve precision (reduce
false positives) while maintaining recall (not missing real species).

## Ground Truth Dataset

- **Source**: Spansh API `landmark_subtype` endpoint (species-level confirmed scans)
- **Download**: `scripts/download_spansh_landmarks.py` — fetches up to 10K bodies per species
- **Species covered**: 72 species total (18 high-value targets + ~54 same-genus competitors)
- **Total bodies**: 616,835 unique bodies with confirmed landmark data
- **Merge**: `scripts/merge_landmarks_to_parquet.py` adds `landmark_species`, `landmark_genuses`, `landmark_count` columns to every body in the Parquet DB
- **Limitation**: For species with >10K confirmed bodies, we only have the 10K closest to Sol. This creates near-bubble sampling bias. For precision validation this is acceptable (body properties don't correlate with distance). For recall, region-constrained species may be underrepresented.
- **Stratum Frigeas**: Legitimately has 0 confirmed landmarks on Spansh.

### Spansh API Pagination Gotcha

The Spansh `from` parameter does NOT work for pagination — it returns the same
results regardless of offset. We use **distance-based cursor pagination**: sort
by distance ascending, then set `distance.min` just past the last result's
distance for the next page. This produces zero duplicates.

## Validation Methodology

### Classification

For bodies that have landmark data (someone landed and scanned):

- **TP (True Positive)**: We predict species X AND landmark confirms species X is present
- **FP_wrong_species**: We predict species X but a different species from the same genus was confirmed
- **FP_other_genus**: We predict species X but only species from different genera were confirmed
- **FP_no_landmark**: We predict species X but the body has no landmark data at all (unvisited — excluded from precision calculation)
- **FN (False Negative)**: Landmark confirms species X but we didn't predict it

### Metrics

- **Precision (with data)** = TP / (TP + FP_wrong_species + FP_other_genus) — only bodies with confirmed ground truth
- **Recall** = TP / (TP + FN) = TP / total_landmarks

From 2026-10-16 the analysis uses a stricter ground truth that separates definitely-wrong from
unverified predictions; see "Revised Validation Methodology" in the SrvSurvey section.

## Galaxy-Wide Validation Results

### Baseline (2026-10-15, post Tectonicas fix only)

| Species | TP | FP | FN | Precision | Recall | Value |
|---|---:|---:|---:|---:|---:|---:|
| Fonticulua Fluctus | 521 | 13 | 129 | 97.6% | 80.2% | 20.0M |
| Fonticulua Segmentatus | 1,655 | 0 | 357 | 100.0% | 82.3% | 19.0M |
| Stratum Tectonicas | 30,459 | 18,946 | 2,880 | 61.7% | 91.4% | 19.0M |
| Tussock Stigmasis | 3,330 | 931 | 197 | 78.2% | 94.4% | 19.0M |
| Concha Biconcavis | 862 | 3 | 97 | 99.7% | 89.9% | 16.8M |
| Cactoida Vermis | 10,183 | 4,970 | 1,207 | 67.2% | 89.4% | 16.2M |
| Fumerola Extremus | 1,760 | 97 | 732 | 94.8% | 70.6% | 16.2M |
| Recepta Deltahedronix | 2,014 | 11,157 | 684 | 15.3% | 74.6% | 16.2M |
| Stratum Cucumisis | 10,066 | 8,407 | 15 | 54.5% | 99.9% | 16.2M |
| Clypeus Speculumi | 5,903 | 18,712 | 688 | 24.0% | 89.6% | 16.2M |
| Recepta Conditivus | 4,391 | 13,472 | 26 | 24.6% | 99.4% | 14.3M |
| Tussock Virgam | 9,788 | 2,535 | 1,346 | 79.4% | 87.9% | 14.3M |
| Recepta Umbrux | 7,522 | 10,352 | 47 | 42.1% | 99.4% | 12.9M |
| Aleoida Gravis | 10,110 | 14,053 | 1 | 41.8% | 100.0% | 12.9M |
| Osseus Discus | 9,450 | 8,602 | 1,141 | 52.3% | 89.2% | 12.9M |
| Clypeus Margaritus | 9,497 | 3,389 | 1,023 | 73.7% | 90.3% | 11.9M |
| Tubus Cavas | 10,313 | 3,660 | 1 | 73.8% | 100.0% | 11.9M |
| Frutexa Flammasis | 11,392 | 9,270 | 2 | 55.1% | 100.0% | 10.3M |

**Totals**: 139,216 TP / 10,573 FN (before Tectonicas fix: 122,454 TP / 27,335 FN)

### Current (2026-10-16, post all fixes — Batch 1 + Batch 2)

| Species | Value | TP | FP | FN | Precision | FP Rate | Recall | FN Rate | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Fonticulua Fluctus | 20.0M | 649 | 16 | 1 | 97.6% | 2.4% | 99.8% | 0.2% | Fixed (B1) |
| Fonticulua Segmentatus | 19.0M | 1,990 | 0 | 22 | 100.0% | 0.0% | 98.9% | 1.1% | Fixed (B1) |
| Stratum Tectonicas | 19.0M | 30,459 | 18,946 | 2,880 | 61.7% | 38.3% | 91.4% | 8.6% | Fixed (prior) |
| Tussock Stigmasis | 19.0M | 3,521 | 1,015 | 6 | 77.6% | 22.4% | 99.8% | 0.2% | Fixed (B1) |
| Concha Biconcavis | 16.8M | 959 | 3 | 0 | 99.7% | 0.3% | 100.0% | 0.0% | Fixed (B1) |
| Cactoida Vermis | 16.2M | 10,183 | 4,970 | 1,207 | 67.2% | 32.8% | 89.4% | 10.6% | No viable fix |
| Fumerola Extremus | 16.2M | 2,437 | 127 | 55 | 95.0% | 5.0% | 97.8% | 2.2% | Fixed (B2) |
| Stratum Cucumisis | 16.2M | 10,066 | 8,407 | 15 | 54.5% | 45.5% | 99.9% | 0.1% | — |
| Clypeus Speculumi | 16.2M | 5,903 | 18,712 | 688 | 24.0% | 76.0% | 89.6% | 10.4% | — |
| Recepta Conditivus | 14.3M | 4,391 | 13,472 | 26 | 24.6% | 75.4% | 99.4% | 0.6% | — |
| Tussock Virgam | 14.3M | 10,798 | 2,910 | 336 | 78.8% | 21.2% | 97.0% | 3.0% | Fixed (B2) |
| Recepta Deltahedronix | 12.9M | 2,014 | 11,157 | 684 | 15.3% | 84.7% | 74.6% | 25.4% | No viable fix |
| Osseus Discus | 12.9M | 9,450 | 8,602 | 1,141 | 52.3% | 47.6% | 89.2% | 10.8% | No viable fix |
| Recepta Umbrux | 12.9M | 7,522 | 10,352 | 47 | 42.1% | 57.9% | 99.4% | 0.6% | — |
| Aleoida Gravis | 12.9M | 10,110 | 14,053 | 1 | 41.8% | 58.2% | 100.0% | 0.0% | — |
| Clypeus Margaritus | 11.9M | 10,519 | 3,731 | 1 | 73.8% | 26.2% | 100.0% | 0.0% | Fixed (B1) |
| Tubus Cavas | 11.9M | 10,313 | 3,660 | 1 | 73.8% | 26.2% | 100.0% | 0.0% | — |
| Frutexa Flammasis | 10.3M | 11,392 | 9,270 | 2 | 55.1% | 44.9% | 100.0% | 0.0% | — |

**Totals**: 142,856 TP / 7,113 FN (+3,640 TP, -3,460 FN vs baseline)

**11 of 18 species now above 97% recall.** 3 species investigated and closed (no viable fix).
4 species uninvestigated (already high recall or precision-limited by within-genus confusion).

## Sweep Optimization

### Approach

Script: `experiments/bio_rule_optimization/sweep_all_species.py`

1. **Data collection**: Parallel scan of all 11,965 sectors using 12 workers. Only evaluates bodies with landmark data. Classifies TP/FP per-species AND per-ruleset.
2. **Parameter sweep**: For each species + ruleset, independently sweeps:
   - Tightening existing min/max parameters (gravity, temperature, pressure) at TP percentiles p5/p10/p15/p20/p25
   - Adding new min/max constraints where none exist
   - Excluding individual star classes
   - Combined top-2 and top-3 single-param improvements (greedy stacking)
3. **Pareto analysis**: Identifies variants where no other has both higher precision AND higher recall
4. **Recommendation**: Highest-precision Pareto variant with <=10% TP loss

### Sweep Results Summary (2026-10-15)

| Species | Baseline | Best | Delta | TP Lost | FP Removed | Best Variant |
|---|---:|---:|---:|---:|---:|---|
| Fonticulua Fluctus | 97.6% | 99.4% | +1.8 | 52 | 10 | rs0_max_temperature_p90 |
| Stratum Tectonicas | 59.7% | 62.0% | +2.2 | 432 | 1,091 | rs3_excl_star_K |
| Tussock Stigmasis | 78.2% | 83.8% | +5.6 | 52 | 297 | rs0_excl_star_A |
| Cactoida Vermis | 67.2% | 71.3% | +4.1 | 161 | 945 | rs1_excl_star_B |
| Recepta Deltahedronix | 15.3% | 21.5% | +6.2 | 202 | 4,533 | rs0_max_pressure_add_p90 |
| Stratum Cucumisis | 54.5% | 62.6% | +8.1 | 270 | 2,561 | rs3_excl_star_A |
| Clypeus Speculumi | 24.0% | 27.3% | +3.3 | 18 | 3,066 | rs1_excl_star_F |
| Recepta Conditivus | 24.6% | 28.8% | +4.2 | 37 | 2,715 | rs0_max_pressure_add_p75 |
| Recepta Umbrux | 42.1% | 47.1% | +5.0 | 238 | 2,177 | rs0_max_pressure_add_p75 |
| Aleoida Gravis | 41.8% | 49.2% | +7.4 | 311 | 3,947 | rs0_excl_star_G |
| Osseus Discus | 52.4% | 58.0% | +5.6 | 945 | 2,441 | rs0_max_gravity_p90 |
| Tubus Cavas | 73.8% | 80.3% | +6.5 | 1,023 | 1,379 | rs0_combined_top3 |
| Frutexa Flammasis | 55.1% | 62.4% | +7.3 | 307 | 2,602 | rs0_excl_star_K |

### Problem Species

**Recepta genus**: All three species (Deltahedronix, Conditivus, Umbrux) share nearly identical
environmental requirements. Cross-species confusion dominates FP — a body matching Recepta conditions
could host any of the three. Pressure may be the best discriminator.

**Clypeus Speculumi**: Confused heavily with Clypeus Lacrimam (14,818 wrong-species FP).

## Stratum Tectonicas Deep Dive

### Per-Ruleset Breakdown (Pre-Fix)

| # | Atmosphere | TP | FP | Precision | Notes |
|---|---|---:|---:|---:|---|
| 0 | Ammonia | 8,517 | 3,796 | 69.2% | Main contributor, decent precision |
| 1 | CarbonDioxide | 4,591 | 3,827 | 54.5% | SO2 constraint at 0.9901 missed 0.990099 bodies |
| 2 | SulphurDioxide | ~0 | ~0 | — | Gravity range 0.265–0.275 captured zero actual bodies |
| 3 | Water | 589 | 1,611 | 26.8% | M-star exclusion + volcanism req were counterproductive |

Already excludes star classes G and A at species level.

### False Negative Analysis (COMPLETE)

With 19,642 FN out of 33,339 landmarks (59% miss rate), Tectonicas had the worst
recall of any species. Galaxy-wide scan of all 11,965 sectors identified root causes:

**FN Atmosphere Distribution:**

| Atmosphere | FN | TP | FN% |
|---|---:|---:|---:|
| Thin Carbon dioxide | 13,631 | 4,591 | 69.4% |
| Thin Sulphur dioxide | 3,648 | 0 | 18.6% |
| Thin Ammonia | 1,323 | 8,517 | 6.7% |
| Thin Water | 893 | 589 | 4.5% |
| Other (CO2-rich, O2, Ar) | 147 | 0 | 0.7% |

**Root Causes (ranked by FN count):**

1. **SO2 constraint threshold too high (57.1% of all FN = ~11,217 bodies)**: rs1 required
   `atm_Sulphur_dioxide >= 0.9901` but the actual data value for SO2-rich CO2 atmospheres
   is `0.990099`. Threshold was 0.0001 too high — a precision error, not a real discriminator.

2. **SO2 atmosphere ruleset (rs2) too narrow (18.6% = 3,648 bodies)**: rs2 gravity range was
   0.265–0.275 but confirmed SO2 Tectonicas bodies have gravity 0.29–0.60 (median 0.40).
   rs2 captured zero actual bodies. SO2 atmosphere is itself a near-perfect discriminator
   (100% precision when widened).

3. **Ammonia gravity range too tight (6.7% = 1,323 bodies)**: Confirmed Ammonia Tectonicas
   bodies go up to gravity 0.37, but our max was 0.275. Widening to 0.37 adds TP with zero FP.

4. **G-star exclusion (8.0% = 1,577 bodies)**: Confirmed Tectonicas on G stars, but precision
   is only 24% on G stars — exclusion justified for precision.

5. **Water M-star exclusion + volcanism constraint (4.5% = 893 bodies)**: 789 of 893 Water FN
   are on M stars. Allowing M stars improves precision from 27% to 30%. Volcanism constraint
   had no effect.

6. **A-star exclusion (2.2% = 437 bodies)**: Only 20% precision — exclusion justified.

### Applied Rule Changes (2026-10-15)

Based on the FN analysis, applied four changes to `species_rules.py`:

| Ruleset | Change | Impact |
|---|---|---|
| rs0 (Ammonia) | max_gravity 0.275 → 0.37 | +TP, +0 FP (pure win) |
| rs1 (CO2) | SO2 min 0.9901 → 0.99, max_gravity 0.275 → 0.50 | Recovers ~11,217 FN, same precision |
| rs2 (SO2) | gravity 0.265–0.275 → 0.265–0.60, temp 210 → 450 | Recovers ~3,648 FN at 100% precision |
| rs3 (Water) | Remove M-star excl + volcanism req, add temp 390–455 | Precision 27% → 30%, +45 TP |

**Not changed**: G-star and A-star species-level exclusions retained (low precision if removed).

### Post-Fix Validation (2026-10-15)

| Metric | Before | After | Change |
|---|---:|---:|---|
| TP | 13,697 | 30,459 | +16,762 (+122%) |
| FN | 19,642 | 2,880 | -16,762 (-85%) |
| FP (other genus) | 9,234 | 18,946 | +9,712 |
| Precision | 59.7% | 61.7% | +2.0pp |
| Recall | 41.1% | 91.4% | +50.3pp |

Remaining 2,880 FN breakdown: ~2,014 on excluded G/A stars, ~147 on uncovered
atmospheres (CO2-rich, Oxygen, Argon-rich), ~719 other edge cases.

## Batch 1 Fixes: FN-Driven Improvements (2026-10-16)

After the Stratum Tectonicas fix, analyzed false negatives for all 18 species using
galaxy-wide FN analysis (`scratchpad/analyze_all_species_fn.py`). Identified root causes
for each species with significant FN, then precision-checked each proposed fix against
all landmark-bearing bodies (`scratchpad/precision_check_fixes.py`).

### Applied Fixes

| Species | Change | New Match Prec | dPrecision | dRecall | FN Before→After |
|---|---|---:|---:|---:|---|
| Fonticulua Fluctus | min_gravity 0.248→0.23 | 97.7% | +0.0pp | +19.7pp | 129→1 |
| Fonticulua Segmentatus | max_gravity 0.2744→0.276, remove distanceToArrival | 100% | +0.0pp | +16.7pp | 357→22 |
| Concha Biconcavis | max_temperature 49.7→53.0, max_pressure 0.0047→0.01 | 100% | +0.0pp | +10.1pp | 97→0 |
| Clypeus Margaritus | max_temperature 195.0→196.0, min_pressure 0.0657→0.050 | 74.8% | +0.1pp | +9.7pp | 1023→1 |
| Tussock Stigmasis | max_gravity 0.2733→0.276 | 69.5% | -0.5pp | +5.4pp | 197→6 |

### Rejected Fixes

| Species | Proposed Fix | New Match Prec | Why Rejected |
|---|---|---:|---|
| Osseus Discus | Add Methane/Ammonia/Argon rulesets | 4.9% | 19,840 new FP, precision halved (52→27%) |
| Tussock Virgam | min_gravity 0.0463→0.04 | 48.0% | Initial test showed -1.8pp precision (later revised, see Batch 2) |
| Cactoida Vermis | Remove K-star exclusion | — | Zero effect in precision check |
| Fumerola Extremus | Remove star_solar_radius constraint | — | Zero effect (constraint already bypassed for null data) |

### Post-Batch-1 Validation (2026-10-16)

| Species | Prec Before | Prec After | Recall Before | Recall After |
|---|---:|---:|---:|---:|
| Fonticulua Fluctus | 97.6% | 97.6% | 80.2% | **99.9%** |
| Fonticulua Segmentatus | 100.0% | 100.0% | 82.3% | **98.9%** |
| Concha Biconcavis | 99.7% | 99.7% | 89.9% | **100.0%** |
| Clypeus Margaritus | 73.7% | 73.8% | 90.3% | **100.0%** |
| Tussock Stigmasis | 78.2% | 77.6% | 94.4% | **99.8%** |
| All others | unchanged | unchanged | unchanged | unchanged |

## Batch 2 Fixes: Tussock Virgam + Fumerola Extremus (2026-10-16)

### Tussock Virgam — Gravity Sweep

Re-evaluated Tussock Virgam with a finer gravity sweep (`scratchpad/tussock_virgam_sweep.py`).
The initial Batch 1 precision check methodology had overcounted FP; the refined sweep across
all 12,102 sectors showed new match precision of **73%** consistently across all thresholds.

| Threshold | TP | FP_other | FN | Precision | Recall | New Match Prec |
|---:|---:|---:|---:|---:|---:|---:|
| 0.0463 (old) | 9,788 | 2,535 | 1,013 | 79.4% | 90.6% | — |
| 0.0450 | 10,381 | 2,750 | 420 | 79.1% | 96.1% | 73.4% |
| 0.0440 | 10,575 | 2,821 | 226 | 78.9% | 97.9% | 73.3% |
| **0.0400** (applied) | **10,798** | **2,910** | **3** | **78.8%** | **100.0%** | **72.9%** |

Applied: rs0 min_gravity 0.0463 → 0.04. Impact: +1,010 TP, +375 FP, -0.6pp precision,
+9.4pp recall. FN drops from 1,013 to 3.

### Fumerola Extremus — Solar Radius Constraint Removal

Root cause: `star_solar_radius >= 0.605` constraint on Methane rs2 blocked cool dwarf stars
(H/Y/T/M class) where Fumerola Extremus legitimately grows.

The Batch 1 precision check showed "no effect" because of a **column name bug**: the script
used `isMainStar` instead of `mainStar` for the Parquet column, so star data was never loaded
and the constraint was silently bypassed in both baseline and fixed evaluation. Parameter-only
fixes (gravity, temperature, pressure) were unaffected by this bug.

Fix: Removed the `star_solar_radius >= 0.605` constraint from Methane rs2 entirely.

| Metric | Before | After | Change |
|---|---:|---:|---|
| TP | 1,760 | 2,437 | +677 |
| FP (other genus) | 97 | 127 | +24 (3.4% of new matches are FP) |
| FN | 732 | 55 | -677 |
| Precision | 94.8% | 95.0% | +0.2pp |
| Recall | 70.6% | 97.8% | +27.2pp |

All 677 new TP are on cool dwarf stars: H (285), Y (147), T (93), M (80), L (72).
New match precision: 96.6%.

### Post-Batch-2 Validation (2026-10-16)

| Species | Prec Before | Prec After | Recall Before | Recall After |
|---|---:|---:|---:|---:|
| Tussock Virgam | 79.4% | 78.8% | 87.9% | **97.0%** |
| Fumerola Extremus | 94.8% | 95.0% | 70.6% | **97.8%** |
| All others | unchanged | unchanged | unchanged | unchanged |

## SrvSurvey Comparison, Precision Sweep and Star Age (2026-10-16)

Compared MGST's rules with [SrvSurvey](https://github.com/njthomson/SrvSurvey)'s bio criteria
(cloned at `/mnt/z/SrvSurvey`), traced every disagreement to a rule and condition, swept every
ruleset for precision, and tested main-star age as a discriminator. Rule changes from this work
were committed in `3cdd52c`.

### Revised Validation Methodology

Ground truth is the 391,870 bodies in the Parquet database that carry landmark data
(`extract_landmark_bodies.py` builds `output/landmark_bodies/landmark_bodies.parquet`, with each
body's main-star class and age and the class of the star the body orbits).

Every (body, species) pair is classified as:

| Outcome | Meaning |
|---|---|
| Confirmed (TP) | the species is recorded on the body |
| Definitely wrong (FP) | another species of the same genus is confirmed (a body hosts at most one species per genus), or the body's records are complete and exclude the species: confirmed species count equals the bio signal count, or the genus list is known, consistent with the confirmed species, and lacks the genus |
| Unverified (UP) | neither: some bio signals are unidentified and the genus may be present. Excluded from FP and from the rates |
| Missed (FN) | confirmed, not predicted |
| Correctly rejected (TN) | definitely absent, not predicted |

Rates: **FNR** = FN / (TP + FN); **FDR** = FP / (TP + FP) (= 1 − precision);
**FPR** = FP / (FP + TN). Thresholds are chosen on two thirds of sectors and checked on the
held-out third (`crc32(sector) % 3 == 0`).

`scripts/validate_species_galaxy.py` remains the independent cross-check. It is stricter: it
counts a body with only other genera recorded as wrong even when its records are incomplete.

**Lesson:** the first SrvSurvey comparison counted every unscanned body as a false positive and
reported "3.5M extra FP" for Stratum Tectonicas. Predictions on bodies without landmark data are
unknown, never false positives.

### SrvSurvey Comparison

`srvsurv_parser.py` turns SrvSurvey's JSON criteria trees into an evaluator with the same
interface as `score_body()` (114 species; all 18 targets). On landmark bodies, SrvSurvey found
confirmed bodies that MGST missed for Stratum Tectonicas (+2,838), Osseus Discus (+1,126),
Cactoida Vermis (+1,071), Clypeus Speculumi (+688) and Recepta Deltahedronix (+684), and avoided
7,907 wrong Recepta Deltahedronix predictions. MGST was better for Tussock Virgam (SrvSurvey's
extra predictions there are right only 7.5% of the time).

`attribute_differences.py` attributed each disagreement to a rule and blocking condition:

- **Star exclusions apply to the star a body orbits, not the main star.** With the main star
  in an excluded class but the body orbiting a different star, Stratum Tectonicas has 1,965
  confirmed bodies (92% hit rate). Where the body's own star is the excluded class there are
  0 of 3,332. SrvSurvey has no star exclusions; its `star` clauses only choose colour variants,
  but a species appears only around star classes that have a colour for it.
- **Clypeus Speculumi's "2,000 ly from Sol" limit was wrong.** Distance from Sol has no effect
  (hit rate 23–25% in every 500 ly band, confirmed sightings 48 ly from Sol). Distance from the
  arrival star has a sharp edge near 2,500 ls.
- **SrvSurvey's per-body-type branches** recover missed Stratum Tectonicas (carbon dioxide-rich,
  oxygen on HMC) and Osseus Discus (Ammonia on Rocky, Rocky Ice and HMC; Argon on Rocky Ice)
  bodies, and its sulphur dioxide split removes most wrong Recepta Deltahedronix predictions.
  The translated rulesets match SrvSurvey's branches on all 1.57M bodies tested
  (`check_translations.py`).

### Applied Rule Changes (commit 3cdd52c)

Each change measured on its own against landmark ground truth:

| Change | TP | FN | FP | FNR | FDR |
|---|---:|---:|---:|---:|---:|
| Original rules | 142,676 | 7,113 | 78,938 | 4.75% | 35.62% |
| Own-star exclusions for Stratum Tectonicas (G, A) and Cactoida Vermis (K) | 145,342 | 4,447 | 79,210 | 2.97% | 35.27% |
| Stratum Tectonicas: carbon dioxide-rich and oxygen on HMC (SrvSurvey) | 142,779 | 7,010 | 78,938 | 4.68% | 35.60% |
| Osseus Discus: Ammonia and Argon by body type; Rocky Water from 390 K | 142,980 | 6,809 | 79,035 | 4.55% | 35.60% |
| Recepta Deltahedronix: sulphur dioxide split by body type and volcanism | 142,650 | 7,139 | 71,033 | 4.77% | 33.24% |
| **First four together** | **145,728** | **4,061** | **71,402** | **2.71%** | **32.88%** |
| + own-star colour-table exclusions (Aleoida G; Frutexa K, A; Cactoida B, C; Stratum Cucumisis A, G, B, N; Tussock Stigmasis A) | 145,712 | 4,077 | 63,632 | 2.72% | 30.40% |
| + Clypeus Speculumi ≥ 2,500 ls from arrival star (Sol limit removed) | 146,324 | 3,465 | 60,298 | 2.31% | 29.18% |
| + Clypeus Margaritus T exclusion on own star | 145,664 | 4,125 | 70,806 | 2.75% | 32.71% |
| + Recepta Umbrux / Conditivus carbon dioxide rules need ≥ 1.10% sulphur dioxide | 145,705 | 4,084 | 70,523 | 2.73% | 32.62% |
| + Recepta Conditivus carbon dioxide rule removed (3,679 of its 3,687 wrong bodies stay flagged as Umbrux) | 145,579 | 4,210 | 67,715 | 2.81% | 31.75% |
| **All nine (committed)** | **146,074** | **3,715** | **47,812** | **2.48%** | **24.66%** |
| + Osseus Discus Water on HMC: main-star age ≥ 9,000 My | 146,048 | 3,741 | 45,348 | 2.50% | 23.69% |
| + Cactoida Vermis Water on HMC: main-star age ≤ 9,500 My | 145,500 | 4,289 | 46,805 | 2.86% | 24.34% |
| **All eleven (live rules, age rules applied 2026-10-16)** | **145,474** | **4,315** | **44,341** | **2.88%** | **23.36%** |

FPR went from 1.506% to 0.912% with the nine committed changes, and to 0.846% with the two age rules. The existing validator agrees: its precision went from 52.44%
to 54.19% after the first four and 60.98% with all nine, with identical TP and FN.

Not applied: own-star exclusions for Tussock Virgam (worse: −89 TP, +244 FP); a main-star
fallback for bodies orbiting barycentres (−400 TP for −1,070 FP); Tubus Cavas gravity
0.152 → 0.148 (−36 TP, −168 FP); region cuts for Stratum Cucumisis (likely overfitting).

### Current Results (live rules)

| Species | Value | Correct | Missed | Wrong | Unverified | FNR | FDR | FPR |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Fonticulua Fluctus | 20.0M | 649 | 1 | 3 | 13 | 0.15% | 0.46% | 0.001% |
| Fonticulua Segmentatus | 19.0M | 1,990 | 22 | 0 | 0 | 1.09% | 0.00% | 0.000% |
| Stratum Tectonicas | 19.0M | 32,532 | 807 | 2,893 | 17,373 | 2.42% | 8.17% | 1.099% |
| Tussock Stigmasis | 19.0M | 3,520 | 7 | 150 | 707 | 0.20% | 4.09% | 0.048% |
| Concha Biconcavis | 16.8M | 959 | 0 | 0 | 3 | 0.00% | 0.00% | 0.000% |
| Cactoida Vermis | 16.2M | 10,310 | 1,080 | 618 | 2,419 | 9.48% | 5.66% | 0.209% |
| Fumerola Extremus | 16.2M | 2,437 | 55 | 5 | 122 | 2.21% | 0.20% | 0.002% |
| Recepta Deltahedronix | 16.2M | 1,988 | 710 | 2,911 | 339 | 26.32% | 59.42% | 0.999% |
| Stratum Cucumisis | 16.2M | 10,061 | 20 | 3,958 | 1,295 | 0.20% | 28.23% | 1.381% |
| Clypeus Speculumi | 16.2M | 6,499 | 92 | 3,808 | 1,597 | 1.40% | 36.95% | 1.306% |
| Recepta Conditivus | 14.3M | 4,242 | 175 | 8,597 | 341 | 3.96% | 66.96% | 2.967% |
| Tussock Virgam | 14.3M | 10,798 | 336 | 142 | 2,768 | 3.02% | 1.30% | 0.046% |
| Recepta Umbrux | 12.9M | 7,501 | 68 | 8,726 | 1,017 | 0.90% | 53.77% | 3.045% |
| Aleoida Gravis | 12.9M | 10,110 | 1 | 7,232 | 3,957 | 0.01% | 41.70% | 2.431% |
| Clypeus Margaritus | 11.9M | 10,455 | 65 | 3 | 3,023 | 0.62% | 0.03% | 0.001% |
| Osseus Discus | 12.9M | 9,728 | 863 | 2,015 | 3,781 | 8.15% | 17.16% | 0.707% |
| Tubus Cavas | 11.9M | 10,313 | 1 | 2,666 | 994 | 0.01% | 20.54% | 0.913% |
| Frutexa Flammasis | 10.3M | 11,382 | 12 | 614 | 5,083 | 0.10% | 5.12% | 0.210% |
| **Total** | | **145,474** | **4,315** | **44,341** | **44,832** | **2.88%** | **23.36%** | **0.846%** |

Per-ruleset statistics: `output/species_rules_eval/ruleset_breakdown.md`
(`ruleset_breakdown.py` + `breakdown_report.py`).

### Water-on-HMC Investigation

The two weakest rulesets both cover thin-Water HMC bodies (gravity ≤ 0.063, 390–455 K).
`extract_body_features.py` built 18,200 thin-Water HMC and Rocky landmark bodies with 98
properties (orbit, rotation, mass, radius, materials, composition, both stars' properties,
region); `explore_ruleset.py` ranked properties by ROC AUC and fitted shallow decision trees on
training sectors.

**Stratum Tectonicas, Water on HMC (rs3): no static discriminator.** Every property has an AUC
of 0.46–0.54, the tree scores 0.517 on held-out sectors, and the hit rate is 31–47% in every
star age, star class, luminosity, region and orbit group. Wrong and confirmed bodies host the
same species at the same rates (Tussock Virgam, Clypeus Margaritus, Concha Renibus). Only the bio
signal count separates them (AUC 0.865), and it is known only after a visit. Of the species'
17,373 unverified predictions, 12,387 have a Stratum genus signal with no species logged; since
every logged Stratum species on a predicted body is Tectonicas, these are almost certainly
unlogged sightings, which puts the species' true FDR near 6.6%. Dropping rs3 would take FDR from
8.17% to 2.09% but FNR from 2.42% to 6.87% (−1,482 confirmed). Kept.

System context and location do not help either (`explore_system_context.py`,
`output/species_rules_eval/tectonicas_system_context.txt`): counts of stars, planets, landable,
HMC, thin-atmosphere and Water bodies, distance rank, body ID, siblings and the procedural mass
code all have AUC 0.48–0.52 (mass codes b, c, d: 40%, 43%, 35% right), and a tree on them scores
0.499 on held-out sectors. The hit rate of a body's 5, 10 or 25 nearest checkable neighbours in
other systems has AUC 0.52, so there are no local patches. Two Water HMC bodies in the same system
agree no more than chance (45% of pairs vs 52% if independent, 80 systems). Within the data
available before a visit, Stratum Tectonicas on these bodies behaves like an independent ~40–50%
draw per body.

**Osseus Discus, Water on HMC (rs1): main-star age.** AUC 0.981. Below 8,000 My there are 14
confirmed and 2,341 wrong; above 12,000 My all 795 checkable predictions are right. Stratum
Tectonicas on the same bodies shows no age effect (AUC 0.506), and Osseus Discus on Rocky Water
is good at every age, so this is species- and body-specific. Cactoida Vermis is the mirror
image: 97–99% right around young stars, 32–48% around stars older than ~9,000 My.

### Star Age Sweep (all rulesets)

`sweep_star_age.py` tested main-star age on all 41 rulesets
(`output/species_rules_eval/star_age_sweep.txt`). Age is missing for 0.8% of landmark bodies.

| Ruleset | Age AUC | Finding |
|---|---:|---|
| Osseus Discus Water on HMC | 0.981 | Old stars only; clean cut at 9,000 My |
| Cactoida Vermis Water | 0.113 | Young stars; drop-off above ~9,000 My (HMC) / ~8,500 My (Rocky) |
| Osseus Discus Ammonia on Rocky | 0.739 | 52% right under 4,000 My, 93% above (small ruleset) |
| Tussock Stigmasis | 0.683 | Wrong predictions concentrated under 4,000 My, but so are most confirmed |
| Stratum Cucumisis sulphur dioxide | 0.640 | 78% right under 4,000 My, 92% above |
| Clypeus Speculumi Water on Rocky | 0.376 | 22% right at 1,000–2,000 My vs 52–78% elsewhere; holds on held-out sectors and across 237 sectors, no known mechanism |
| Recepta (all), Tubus Cavas, Frutexa Flammasis, Stratum Tectonicas, Aleoida Gravis | 0.45–0.56 | No useful age effect |

Candidates tested with full statistics (`test_age_candidates.py`,
`output/species_rules_eval/age_candidates.txt`), each on top of the committed rules:

| Candidate | Species TP / FN / FP | Species FNR | Species FDR | All-species FNR / FDR / FPR | Held-out species FDR |
|---|---|---:|---:|---|---:|
| Committed rules | | | | 2.48% / 24.66% / 0.912% | |
| A. Osseus Discus Water on HMC: age ≥ 9,000 My | 9,728 / 863 / 2,015 (was 9,754 / 837 / 4,479) | 8.15% (7.90%) | 17.16% (31.47%) | 2.50% / 23.69% / 0.865% | 18.02% (31.54%) |
| B. Cactoida Vermis Water on HMC: age ≤ 9,500 My | 10,310 / 1,080 / 618 (was 10,884 / 506 / 1,625) | 9.48% (4.44%) | 5.66% (12.99%) | 2.86% / 24.34% / 0.893% | 5.38% (12.85%) |
| C. Osseus Discus Ammonia on Rocky: age ≥ 4,000 My | 9,682 / 909 / 4,413 | 8.58% | 31.31% | 2.53% / 24.64% / 0.911% | 31.29% |
| D. Clypeus Speculumi Water: skip ages 1,000–2,000 My | 6,412 / 179 / 3,498 (was 6,499 / 92 / 3,808) | 2.72% (1.40%) | 35.30% (36.95%) | 2.54% / 24.55% / 0.906% | 36.39% (37.79%) |
| A + B | | | | 2.88% / 23.36% / 0.846% | |
| A + B + C + D | | | | 2.99% / 23.23% / 0.839% | |

**Status:** A (−26 TP, −2,464 FP) and B (−574 TP, −1,007 FP, a recall-for-precision tradeoff)
were applied on 2026-10-16; the live rules reproduce the A + B result exactly. C is not worth it.
D is a small gain with no known mechanism and was not applied. Callers now set `star_age` (main
star's age in My) on each body row: `find_high_value_systems.py`, `validate_species_galaxy.py`,
`compare_engines.py`; a missing age never excludes a body.

## Species Investigation Outcomes

### Osseus Discus (REOPENED 2026-10-16)
> Superseded: splitting by body type (SrvSurvey branches) made Ammonia on Rocky Ice and HMC and Argon on
> Rocky Ice clean enough to apply; Methane is still rejected. Main-star age fixes the Water-on-HMC
> ruleset (applied: main-star age ≥ 9,000 My). See the SrvSurvey section above.

Current: 52.3% precision, 89.2% recall. 1,141 FN.
FN are on Methane (800), Ammonia (150 confirmed), Argon (1 confirmed), Water (80).

Deep analysis of Methane/Ammonia/Argon atmospheres (`scratchpad/osseus_analysis.py`,
`scratchpad/osseus_new_rulesets_test.py`):

**Methane** (800 TP, 1,787 FP_wrong_species, 15,575 FP_other_genus):
- Key discriminators: body_type=Rocky (100% TP vs 96% FP=Icy), volcanism=silicate/metallic/rocky (100% TP)
- Even with Rocky body + volcanism filter: 791 TP, 819 FP_wrong_species, 531 FP_other → 36.9% precision
- **Problem**: Other Osseus species (Cornibus, Pellebantus, etc.) share the same Methane niche
- Combined precision drops from 52.3% → 50.7%. **Not worth applying.**

**Ammonia** (150 TP, 52,057 FP_other_genus):
- TP at gravity 0.04-0.058, temp 170-177K, all with volcanism
- With tight constraints: 150 TP, 28 FP_ws, 103 FP_og → 53.4% precision
- But only +150 TP. Combined precision: 52.4% (neutral). **Marginal value.**

**Argon**: 1 confirmed TP in the entire galaxy. **No ruleset possible.**

### Cactoida Vermis (REOPENED 2026-10-16)
> Superseded: the K exclusion now applies to the star the body orbits, which recovers 701 confirmed
> bodies at unchanged precision, and B and C are excluded too. On HMC it is limited to stars up to
> 9,500 My (applied; trades 574 confirmed for 1,007 fewer wrong). See the SrvSurvey section above.

Current: 67.2% precision, 89.4% recall. 1,207 FN.
65.5% of FN from K-star exclusion.

The Batch 1 precision check showed "no effect" due to the `isMainStar` column name bug.
Proper testing with correct column names (`scratchpad/cactoida_kstar_test.py`) shows:
removing K-star exclusion adds +721 TP but +1,472 FP (32.9% new match precision).
Precision drops 5.2pp (71.9% → 66.7%). **Not worth it.**

### Recepta Deltahedronix (REOPENED 2026-10-16)
> Superseded: SrvSurvey's per-body-type sulphur dioxide rules cut FDR from 84% to 59% (−7,905 wrong,
> −26 confirmed). The remaining wrong predictions are mostly bodies with Conditivus or Umbrux
> confirmed instead. See the SrvSurvey section above.

Current: 15.3% precision, 74.6% recall. 684 FN.
99.4% of FN on Thin CO2 atmosphere (only SO2 ruleset exists). Adding CO2 ruleset
would improve recall but precision is already terrible — within-genus confusion with
Recepta Umbrux (6,566 FP) and Recepta Conditivus (4,239 FP) dominates.

### Uninvestigated Species
> Superseded on 2026-10-16: the SrvSurvey comparison and precision sweep changed rules for several of
> these. The figures below are the pre-2026-10-16 measurements.

The following species were not investigated for FN reduction because they already have
near-perfect recall (>=99.4%) or are primarily limited by within-genus FP confusion:

| Species | Precision | Recall | Primary FP Source |
|---|---:|---:|---|
| Stratum Cucumisis | 54.5% | 99.9% | Stratum Araneamus (3,577), Paleas (1,673) |
| Clypeus Speculumi | 24.0% | 89.6% | Clypeus Lacrimam (14,818) |
| Recepta Conditivus | 24.6% | 99.4% | Recepta Umbrux (7,522), Deltahedronix (2,691) |
| Recepta Umbrux | 42.1% | 99.4% | Recepta Conditivus (4,390), Deltahedronix (2,690) |
| Aleoida Gravis | 41.8% | 100.0% | Other genus (14,053) |
| Tubus Cavas | 73.8% | 100.0% | Tubus Compagibus (1,536), Conifer (208) |
| Frutexa Flammasis | 55.1% | 100.0% | Other genus (9,270) |

The sweep (`experiments/bio_rule_optimization/results/pareto_summary.txt`) has
precision-tightening recommendations for several of these (e.g., Cactoida Vermis exclude
B-star +4.1pp, Aleoida Gravis exclude G-star +7.4pp, Frutexa Flammasis exclude K-star
+7.3pp). These trade TP for lower FP and have not been applied.

## Overall Progress Summary

### All Applied Rule Changes

| # | Session | Species | Change | dPrecision | dRecall |
|---|---|---|---|---:|---:|
| 1 | Tectonicas | Stratum Tectonicas | 4 ruleset fixes (ammonia gravity, CO2 SO2 threshold, SO2 range, Water M-star) | +2.0pp | +50.3pp |
| 2 | Batch 1 | Fonticulua Fluctus | min_gravity 0.248 → 0.23 | +0.0pp | +19.7pp |
| 3 | Batch 1 | Fonticulua Segmentatus | max_gravity 0.2744 → 0.276, remove distanceToArrival | +0.0pp | +16.7pp |
| 4 | Batch 1 | Concha Biconcavis | max_temperature 49.7 → 53.0, max_pressure 0.0047 → 0.01 | +0.0pp | +10.1pp |
| 5 | Batch 1 | Clypeus Margaritus | max_temperature 195.0 → 196.0, min_pressure 0.0657 → 0.050 | +0.1pp | +9.7pp |
| 6 | Batch 1 | Tussock Stigmasis | max_gravity 0.2733 → 0.276 | -0.5pp | +5.4pp |
| 7 | Batch 2 | Tussock Virgam | rs0 min_gravity 0.0463 → 0.04 | -0.6pp | +9.1pp |
| 8 | Batch 2 | Fumerola Extremus | Remove star_solar_radius >= 0.605 from Methane rs2 | +0.2pp | +27.2pp |
| 9 | SrvSurvey | Stratum Tectonicas, Cactoida Vermis | Star exclusions on the star the body orbits | see SrvSurvey section | |
| 10 | SrvSurvey | Stratum Tectonicas | Carbon dioxide-rich and oxygen rulesets on HMC | | |
| 11 | SrvSurvey | Osseus Discus | Ammonia and Argon rulesets by body type; Rocky Water from 390 K | | |
| 12 | SrvSurvey | Recepta Deltahedronix | Sulphur dioxide rule split by body type and volcanism | | |
| 13 | Sweep | Aleoida, Frutexa, Cactoida, Stratum Cucumisis, Tussock Stigmasis | Own-star colour-table exclusions | | |
| 14 | Sweep | Clypeus Speculumi | ≥ 2,500 ls from arrival star replaces ≥ 2,000 ly from Sol | | |
| 15 | Sweep | Clypeus Margaritus | T exclusion on own star | | |
| 16 | Sweep | Recepta Umbrux / Conditivus | Carbon dioxide rules need ≥ 1.10% sulphur dioxide | | |
| 17 | Sweep | Recepta Conditivus | Carbon dioxide ruleset removed | | |
| 18 | Star age | Osseus Discus | Water on HMC needs main-star age ≥ 9,000 My | | |
| 19 | Star age | Cactoida Vermis | Water on HMC needs main-star age ≤ 9,500 My | | |

### Aggregate Impact

| Metric | Pre-optimization | Post-optimization | Change |
|---|---:|---:|---|
| Total TP | 122,454 | 142,856 | +20,402 (+16.7%) |
| Total FN | 27,335 | 7,113 | -20,222 (-74.0%) |
| Species ≥97% recall | 4 of 18 | 11 of 18 | +7 |
| Species ≥95% precision | 2 of 18 | 3 of 18 | +1 |
| Weighted avg recall | ~82% | ~95% | +13pp |

After changes 9–17 (revised methodology, see the SrvSurvey section): 146,074 TP, 3,715 FN
(FNR 2.48%), 47,812 definitely-wrong FP (FDR 24.66%, FPR 0.912%), up from 142,676 TP, 7,113 FN,
78,938 FP (FNR 4.75%, FDR 35.62%, FPR 1.506%). With 18–19: 145,474 TP, 4,315 FN (FNR 2.88%),
44,341 FP (FDR 23.36%, FPR 0.846%).

### Known Limitations

- **Within-genus confusion** is the dominant FP source for Recepta (3 species), Clypeus (2 species),
  Stratum (2 species), Osseus, and Tubus. Environmental parameters alone cannot distinguish
  congeners. Species-level discrimination would require additional signals not in the database
  (e.g., visual identification, exact location within a body).
- **Landmark sampling bias**: Spansh data is capped at 10K bodies per species, biased toward
  bodies near Sol. Region-constrained species may be underrepresented in validation.
- **Unvisited bodies**: The vast majority of predictions are on bodies with no landmark data
  (FP_no_landmark), excluded from precision calculation. True galaxy-wide precision may differ.
- **Incomplete landmark records**: commanders often log only some species on a body. About 46,000
  predictions are unverified (some signals unidentified); they are excluded from FP and the rates.
- **Apparent randomness**: Stratum Tectonicas on Water HMC bodies appears on roughly half of the
  eligible bodies with no stored property that predicts which (only the post-visit bio signal count does).

## Species Ruleset Reference

Generated from `src/mgst/bio/species_rules.py` by `render_ruleset_reference.py` (regenerate after rule
changes). Gravity in g, temperature in K, pressure in atm, gas shares in percent, `star_age` in My.

### Fonticulua Fluctus (20.0M cr) — 1 ruleset

| Field | rs0 (Oxygen) |
|---|---|
| Body type | Icy |
| Volcanism | — |
| Gravity (g) | 0.23 – 0.276 |
| Temperature (K) | 143 – 200 |
| Pressure (atm) | ≥ 0.012 |
| Other | — |

### Fonticulua Segmentatus (19.0M cr) — 1 ruleset

| Field | rs0 (Neon+NeonRich) |
|---|---|
| Body type | Icy |
| Volcanism | none |
| Gravity (g) | 0.25 – 0.276 |
| Temperature (K) | 50 – 75 |
| Pressure (atm) | ≤ 0.006 |
| Other | — |

### Stratum Tectonicas (19.0M cr) — 6 rulesets

Own-star exclusions (star the body orbits): **G, A**

| Field | rs0 (Ammonia) | rs1 (CarbonDioxide) | rs2 (SulphurDioxide) | rs3 (Water) | rs4 (CarbonDioxideRich) | rs5 (Oxygen) |
|---|---|---|---|---|---|---|
| Body type | HMC | HMC | HMC | HMC | HMC | HMC |
| Volcanism | — | — | — | — | none | none, Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| Gravity (g) | 0.045 – 0.37 | 0.045 – 0.5 | 0.265 – 0.6 | 0.045 – 0.063 | ≤ 0.61 | ≤ 0.52 |
| Temperature (K) | 165 – 177 | 165 – 197 | 165 – 450 | 390 – 455 | 165 – 246 | ≥ 165 |
| Pressure (atm) | — | — | — | — | ≥ 0.007 | ≥ 0.02 |
| Other | — | atm_Sulphur_dioxide ≥ 0.99 | — | — | any of: Oxygen ≥ 57%, Argon ≥ 50%, Sulphur dioxide ≥ 0.9% | Sulphur dioxide ≥ 0.8% |

### Tussock Stigmasis (19.0M cr) — 1 ruleset

Own-star exclusions (star the body orbits): **A**

| Field | rs0 (SulphurDioxide) |
|---|---|
| Body type | Rocky, HMC |
| Volcanism | — |
| Gravity (g) | 0.04 – 0.276 |
| Temperature (K) | 132 – 180 |
| Pressure (atm) | ≤ 0.01 |
| Other | — |

### Concha Biconcavis (16.8M cr) — 1 ruleset

| Field | rs0 (Nitrogen) |
|---|---|
| Body type | Rocky, HMC |
| Volcanism | none |
| Gravity (g) | 0.053 – 0.275 |
| Temperature (K) | 42 – 53 |
| Pressure (atm) | ≤ 0.01 |
| Other | — |

### Cactoida Vermis (16.2M cr) — 5 rulesets

Own-star exclusions (star the body orbits): **K, B, C**

| Field | rs0 (SulphurDioxide) | rs1 (Water) | rs2 (Water) | rs3 (Water) | rs4 (Water) |
|---|---|---|---|---|---|
| Body type | Rocky | Rocky | HMC | Rocky | HMC |
| Volcanism | none | none | none | water | water |
| Gravity (g) | 0.265 – 0.276 | 0.04 – 0.0612 | 0.04 – 0.0612 | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature (K) | 160 – 210 | — | — | — | — |
| Pressure (atm) | ≤ 0.005 | — | — | — | — |
| Other | — | — | star_age ≤ 9500 | — | star_age ≤ 9500 |

### Fumerola Extremus (16.2M cr) — 4 rulesets

| Field | rs0 (Ammonia) | rs1 (Argon) | rs2 (Methane) | rs3 (SulphurDioxide) |
|---|---|---|---|---|
| Body type | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice |
| Volcanism | silicate, metallic, rocky | silicate, metallic, rocky | silicate, metallic, rocky | silicate, metallic, rocky |
| Gravity (g) | 0.04 – 0.09 | 0.07 – 0.276 | 0.025 – 0.127 | 0.2656 – 0.276 |
| Temperature (K) | 161 – 177 | 50 – 121 | 77 – 109 | 54 – 210 |
| Pressure (atm) | ≤ 0.0135 | — | ≥ 0.01 | — |
| Other | — | — | — | — |

### Recepta Deltahedronix (16.2M cr) — 4 rulesets

| Field | rs0 (SulphurDioxide) | rs1 (SulphurDioxide) | rs2 (SulphurDioxide) | rs3 (SulphurDioxide) |
|---|---|---|---|---|
| Body type | Icy | Rocky | Rocky Ice | HMC |
| Volcanism | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Geysers, Water Magma | none, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Metallic Magma, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Water Geysers | none |
| Gravity (g) | 0.188 – 0.276 | ≤ 0.276 | 0.22 – 0.276 | ≤ 0.276 |
| Temperature (K) | ≤ 272 | 132 – 207 | ≤ 231 | 132 – 135 |
| Pressure (atm) | — | ≤ 0.06 | — | ≤ 0.0014 |
| Other | Sulphur dioxide ≥ 100% | Sulphur dioxide ≥ 100% | Sulphur dioxide ≥ 100% | Sulphur dioxide ≥ 100% |

### Stratum Cucumisis (16.2M cr) — 4 rulesets

Own-star exclusions (star the body orbits): **A, G, B, N**; Region: **sagittarius-carina**

| Field | rs0 (CarbonDioxide) | rs1 (CarbonDioxideRich) | rs2 (Oxygen) | rs3 (SulphurDioxide) |
|---|---|---|---|---|
| Body type | Rocky | Rocky | Rocky | Rocky |
| Volcanism | none | none | — | — |
| Gravity (g) | 0.04 – 0.6 | 0.44 – 0.56 | 0.4 – 0.6 | 0.26 – 0.55 |
| Temperature (K) | 191 – 371 | 210 – 246 | 200 – 250 | 191 – 373 |
| Pressure (atm) | — | ≥ 0.01 | ≥ 0.01 | — |
| Other | — | — | — | — |

### Clypeus Speculumi (16.2M cr) — 3 rulesets

| Field | rs0 (CarbonDioxide) | rs1 (Water) | rs2 (Water) |
|---|---|---|---|
| Body type | Rocky | Rocky | Rocky |
| Volcanism | none | none | water |
| Gravity (g) | 0.04 – 0.276 | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature (K) | 190 – 197 | — | — |
| Pressure (atm) | ≥ 0.055 | — | — |
| Other | distanceToArrival ≥ 2500 (required) | distanceToArrival ≥ 2500 (required) | distanceToArrival ≥ 2500 (required) |

### Recepta Conditivus (14.3M cr) — 1 ruleset

| Field | rs0 (SulphurDioxide) |
|---|---|
| Body type | any |
| Volcanism | — |
| Gravity (g) | 0.04 – 0.276 |
| Temperature (K) | 132 – 275 |
| Pressure (atm) | — |
| Other | Sulphur dioxide ≥ 1.05% |

### Tussock Virgam (14.3M cr) — 2 rulesets

Main-star exclusions: **A, T, B**

| Field | rs0 (Water) | rs1 (Water) |
|---|---|---|
| Body type | Rocky, HMC | Rocky, HMC |
| Volcanism | none | water |
| Gravity (g) | 0.04 – 0.065 | 0.04 – 0.065 |
| Temperature (K) | — | — |
| Pressure (atm) | — | — |
| Other | — | — |

### Recepta Umbrux (12.9M cr) — 2 rulesets

| Field | rs0 (CarbonDioxide) | rs1 (SulphurDioxide) |
|---|---|---|
| Body type | any | any |
| Volcanism | — | — |
| Gravity (g) | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature (K) | 151 – 200 | 132 – 273 |
| Pressure (atm) | — | — |
| Other | Sulphur dioxide ≥ 1.1% | Sulphur dioxide ≥ 1.05% |

### Aleoida Gravis (12.9M cr) — 1 ruleset

Own-star exclusions (star the body orbits): **G**

| Field | rs0 (CarbonDioxide) |
|---|---|
| Body type | Rocky, HMC |
| Volcanism | none |
| Gravity (g) | 0.04 – 0.276 |
| Temperature (K) | 190 – 197 |
| Pressure (atm) | ≥ 0.054 |
| Other | — |

### Clypeus Margaritus (11.9M cr) — 2 rulesets

Own-star exclusions (star the body orbits): **T**

| Field | rs0 (CarbonDioxide) | rs1 (Water) |
|---|---|---|
| Body type | HMC | HMC |
| Volcanism | none | none |
| Gravity (g) | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature (K) | 190 – 196 | — |
| Pressure (atm) | ≥ 0.05 | — |
| Other | — | — |

### Osseus Discus (12.9M cr) — 6 rulesets

| Field | rs0 (Water) | rs1 (Water) | rs2 (Ammonia) | rs3 (Ammonia) | rs4 (Ammonia) | rs5 (Argon) |
|---|---|---|---|---|---|---|
| Body type | Rocky | HMC | Rocky | Rocky Ice | HMC | Rocky Ice |
| Volcanism | — | — | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma, Rocky Magma, Water Magma | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Silicate Vapour Geysers, Water Geysers |
| Gravity (g) | 0.04 – 0.063 | 0.04 – 0.063 | ≤ 0.058 | ≤ 0.09 | 0.046 – 0.054 | 0.22 – 0.276 |
| Temperature (K) | 390 – 455 | 400 – 455 | ≥ 169 | ≥ 160.7 | ≥ 174 | ≤ 119 |
| Pressure (atm) | — | — | ≥ 0.006 | — | ≥ 0.009 | — |
| Other | — | star_age ≥ 9000 | Ammonia ≥ 100% | Ammonia ≥ 100% | Ammonia ≥ 100% | Argon ≥ 100% |

### Tubus Cavas (11.9M cr) — 1 ruleset

Region: **scutum-centaurus**

| Field | rs0 (CarbonDioxide) |
|---|---|
| Body type | Rocky |
| Volcanism | none |
| Gravity (g) | 0.04 – 0.152 |
| Temperature (K) | 160 – 197 |
| Pressure (atm) | ≥ 0.003 |
| Other | — |

### Frutexa Flammasis (10.3M cr) — 1 ruleset

Own-star exclusions (star the body orbits): **K, A**; Region: **scutum-centaurus**

| Field | rs0 (Ammonia) |
|---|---|
| Body type | Rocky |
| Volcanism | — |
| Gravity (g) | 0.04 – 0.276 |
| Temperature (K) | 152 – 177 |
| Pressure (atm) | ≤ 0.0135 |
| Other | — |


### Matching Engine Notes

- **Atmosphere matching** uses substring matching with exclusion. "CarbonDioxide" matches
  "Thin Carbon dioxide" but not "Thin Carbon dioxide-rich". Only "Thin" prefix atmospheres
  are accepted (bio requires thin atmospheres).
- **Volcanism** constraints: `"None"` = must have no volcanism, `["water"]` = volcanismType
  must contain "water" (case-insensitive substring), `["silicate", "metallic", "rocky"]` =
  must contain at least one keyword, absent = no constraint. A `"None"` entry inside a list
  also accepts bodies without volcanism.
- **Body type** maps BioScan names to Parquet subType values (e.g., "High metal content body"
  → "High metal content world"). Abbreviated as **HMC** in tables above.
- **Star class**: `exclude_star_classes` checks `star_class`, the first letter of the main
  star's `spectralClass`. `exclude_own_star_classes` checks `parent_star_class`, the class of
  the first star up the body's `parent_N_*` chain (`parent_star_class()`); bodies orbiting only
  barycentres have none and are never excluded by it. The first letter merges subclasses (e.g.
  `A` includes Herbig Ae/Be stars, `T` includes T Tauri).
- **Composition**: `atmosphere_component` requires every listed gas share; `atmosphere_component_any`
  requires at least one.
- **Constraints** (`{"field", "min"/"max"}`) read any body-row field; a missing value passes
  unless the constraint has `"required": True` (used for Clypeus Speculumi's `distanceToArrival`).
  `star_age` (main star's age, My) is set by callers and used by the Osseus Discus and Cactoida
  Vermis Water-on-HMC rulesets.
- **Region** is computed from system (x, y, z) coordinates using galactic region geometry.
- **`atm_SO2 ≥ 1.05`** (atmosphere_component) checks the `atm_Sulphur_dioxide` column, which
  is a percentage value (1.05 = 1.05% SO2 content). Used by all three Recepta species as
  a discriminator.

## Key Scripts

| Script | Purpose |
|---|---|
| `scripts/download_spansh_landmarks.py` | Download species landmark data from Spansh |
| `scripts/merge_landmarks_to_parquet.py` | Add landmark columns to Parquet DB |
| `scripts/validate_species_galaxy.py` | Galaxy-wide validation (precision/recall per species) |
| `experiments/bio_rule_optimization/sweep_all_species.py` | Parameter sweep optimization |
| `src/mgst/bio/species_rules.py` | Live species rule definitions |
| `experiments/bio_rule_optimization/extract_landmark_bodies.py` | Build the landmark-body ground-truth extract (star class, own star, star age) |
| `experiments/bio_rule_optimization/ruleset_breakdown.py` / `breakdown_report.py` | Per-species and per-ruleset TP/FN/FP/TN/UP with FNR/FDR/FPR |
| `experiments/bio_rule_optimization/srvsurv_parser.py` | SrvSurvey bio-criteria evaluator |
| `experiments/bio_rule_optimization/compare_engines.py` / `merge_analysis.py` | Galaxy-wide MGST vs SrvSurvey comparison |
| `experiments/bio_rule_optimization/attribute_differences.py` | Attribute disagreements to rules and conditions |
| `experiments/bio_rule_optimization/check_translations.py` | Verify SrvSurvey-derived rulesets match their branches |
| `experiments/bio_rule_optimization/sweep_precision.py` | Threshold and category sweep for false positives |
| `experiments/bio_rule_optimization/extract_body_features.py` / `explore_ruleset.py` | Full-property extract and per-ruleset discriminator search |
| `experiments/bio_rule_optimization/sweep_star_age.py` / `test_age_candidates.py` / `test_water_hmc_candidates.py` | Star age sweep and candidate tests |
| `experiments/bio_rule_optimization/explore_system_context.py` | System-context and position features for one ruleset |
| `experiments/bio_rule_optimization/render_ruleset_reference.py` | Regenerate the ruleset reference above |

## File Locations

- Landmark data: `Databases/spansh_landmarks/` (per-species Parquet + merged `all_landmarks.parquet`)
- Baseline validation: `output/validation_galaxy/results.json`
- Post-Tectonicas validation: `output/validation_post_tectonicas_fix/results.tsv`
- Post-Batch-1 validation: `output/validation_post_batch1_fix/results.tsv`
- Post-Batch-2 validation: `output/validation_post_batch2_fix/results.tsv`
- Sweep results: `experiments/bio_rule_optimization/results/`
- Pareto summary: `experiments/bio_rule_optimization/results/pareto_summary.txt`
- Landmark ground truth extract: `output/landmark_bodies/landmark_bodies.parquet`
- Committed-rule statistics: `output/species_rules_eval/ruleset_breakdown.md`
- Pre-commit comparison: `output/rule_changes_eval/ruleset_breakdown_before_fold.md`
- SrvSurvey comparison: `output/engine_comparison_landmark_only_20261016/`, `output/landmark_bodies/attribution_report.txt`
- Precision sweep: `output/precision_sweep/`
- Star age: `output/species_rules_eval/star_age_sweep.txt`, `age_candidates.txt`, `water_hmc_candidates.txt`
- Validator runs: `output/validation_rule_changes_20261016/`
