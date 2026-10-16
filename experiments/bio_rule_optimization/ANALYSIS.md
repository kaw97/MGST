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

## Species Investigation Outcomes

### Osseus Discus (CLOSED — No viable fix)
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

### Cactoida Vermis (CLOSED — Not viable)
Current: 67.2% precision, 89.4% recall. 1,207 FN.
65.5% of FN from K-star exclusion.

The Batch 1 precision check showed "no effect" due to the `isMainStar` column name bug.
Proper testing with correct column names (`scratchpad/cactoida_kstar_test.py`) shows:
removing K-star exclusion adds +721 TP but +1,472 FP (32.9% new match precision).
Precision drops 5.2pp (71.9% → 66.7%). **Not worth it.**

### Recepta Deltahedronix (CLOSED — Not viable)
Current: 15.3% precision, 74.6% recall. 684 FN.
99.4% of FN on Thin CO2 atmosphere (only SO2 ruleset exists). Adding CO2 ruleset
would improve recall but precision is already terrible — within-genus confusion with
Recepta Umbrux (6,566 FP) and Recepta Conditivus (4,239 FP) dominates.

### Uninvestigated Species

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

### Aggregate Impact

| Metric | Pre-optimization | Post-optimization | Change |
|---|---:|---:|---|
| Total TP | 122,454 | 142,856 | +20,402 (+16.7%) |
| Total FN | 27,335 | 7,113 | -20,222 (-74.0%) |
| Species ≥97% recall | 4 of 18 | 11 of 18 | +7 |
| Species ≥95% precision | 2 of 18 | 3 of 18 | +1 |
| Weighted avg recall | ~82% | ~95% | +13pp |

### Known Limitations

- **Within-genus confusion** is the dominant FP source for Recepta (3 species), Clypeus (2 species),
  Stratum (2 species), Osseus, and Tubus. Environmental parameters alone cannot distinguish
  congeners. Species-level discrimination would require additional signals not in the database
  (e.g., visual identification, exact location within a body).
- **Landmark sampling bias**: Spansh data is capped at 10K bodies per species, biased toward
  bodies near Sol. Region-constrained species may be underrepresented in validation.
- **Unvisited bodies**: The vast majority of predictions are on bodies with no landmark data
  (FP_no_landmark), excluded from precision calculation. True galaxy-wide precision may differ.

## Species Ruleset Reference

Detailed breakdown of every ruleset for each high-value species, as defined in
`src/mgst/bio/species_rules.py`. All gravity is in G, temperature in K, pressure in atm.

### Fonticulua Fluctus (20.0M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Icy body |
| Atmosphere | Oxygen |
| Volcanism | — |
| Gravity | 0.23 – 0.276 |
| Temperature | 143 – 200 K |
| Pressure | ≥ 0.012 |
| Star exclusions | — |
| Region/distance | — |

### Fonticulua Segmentatus (19.0M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Icy body |
| Atmosphere | Neon, NeonRich |
| Volcanism | None (required absent) |
| Gravity | 0.25 – 0.276 |
| Temperature | 50 – 75 K |
| Pressure | ≤ 0.006 |
| Star exclusions | — |
| Region/distance | — |

### Stratum Tectonicas (19.0M cr) — 4 rulesets

Species-level star exclusions: **G, A**

| Field | rs0 (Ammonia) | rs1 (CO2) | rs2 (SO2) | rs3 (Water) |
|---|---|---|---|---|
| Body type | HMC | HMC | HMC | HMC |
| Atmosphere | Ammonia | CarbonDioxide | SulphurDioxide | Water |
| Volcanism | — | — | — | — |
| Gravity | 0.045 – 0.37 | 0.045 – 0.50 | 0.265 – 0.60 | 0.045 – 0.063 |
| Temperature | 165 – 177 K | 165 – 197 K | 165 – 450 K | 390 – 455 K |
| Pressure | — | — | — | — |
| Extra | — | atm_SO2 ≥ 0.99 | — | — |

### Tussock Stigmasis (19.0M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Rocky body, HMC |
| Atmosphere | SulphurDioxide |
| Volcanism | — |
| Gravity | 0.04 – 0.276 |
| Temperature | 132 – 180 K |
| Pressure | ≤ 0.01 |
| Star exclusions | — |
| Region/distance | — |

### Concha Biconcavis (16.8M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Rocky body, HMC |
| Atmosphere | Nitrogen |
| Volcanism | None (required absent) |
| Gravity | 0.053 – 0.275 |
| Temperature | 42 – 53 K |
| Pressure | ≤ 0.01 |
| Star exclusions | — |
| Region/distance | — |

### Cactoida Vermis (16.2M cr) — 3 rulesets

Species-level star exclusions: **K**

| Field | rs0 (SO2) | rs1 (Water/low-g) | rs2 (Water/volcanism) |
|---|---|---|---|
| Body type | Rocky body | Rocky body, HMC | Rocky body, HMC |
| Atmosphere | SulphurDioxide | Water | Water |
| Volcanism | None | None | water (substring) |
| Gravity | 0.265 – 0.276 | 0.04 – 0.0612 | 0.04 – 0.276 |
| Temperature | 160 – 210 K | — | — |
| Pressure | ≤ 0.005 | — | — |

### Fumerola Extremus (16.2M cr) — 4 rulesets

All rulesets require active volcanism (silicate, metallic, or rocky).

| Field | rs0 (Ammonia) | rs1 (Argon) | rs2 (Methane) | rs3 (SO2) |
|---|---|---|---|---|
| Body type | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice, HMC | Rocky, Rocky Ice |
| Atmosphere | Ammonia | Argon | Methane | SulphurDioxide |
| Volcanism | silicate/metallic/rocky | silicate/metallic/rocky | silicate/metallic/rocky | silicate/metallic/rocky |
| Gravity | 0.04 – 0.09 | 0.07 – 0.276 | 0.025 – 0.127 | 0.2656 – 0.276 |
| Temperature | 161 – 177 K | 50 – 121 K | 77 – 109 K | 54 – 210 K |
| Pressure | ≤ 0.0135 | — | ≥ 0.01 | — |

### Recepta Deltahedronix (16.2M cr / min 12.9M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | — (any) |
| Atmosphere | SulphurDioxide |
| Volcanism | — |
| Gravity | 0.04 – 0.276 |
| Temperature | 132 – 272 K |
| Pressure | — |
| Extra | atm_SO2 ≥ 1.05 |
| Star exclusions | — |

### Stratum Cucumisis (16.2M cr) — 4 rulesets

Region constraint: **Sagittarius-Carina** arm only.

| Field | rs0 (CO2) | rs1 (CO2-rich) | rs2 (Oxygen) | rs3 (SO2) |
|---|---|---|---|---|
| Body type | Rocky body | Rocky body | Rocky body | Rocky body |
| Atmosphere | CarbonDioxide | CarbonDioxideRich | Oxygen | SulphurDioxide |
| Volcanism | None | None | — | — |
| Gravity | 0.04 – 0.6 | 0.44 – 0.56 | 0.4 – 0.6 | 0.26 – 0.55 |
| Temperature | 191 – 371 K | 210 – 246 K | 200 – 250 K | 191 – 373 K |
| Pressure | — | ≥ 0.01 | ≥ 0.01 | — |

### Clypeus Speculumi (16.2M cr) — 3 rulesets

Distance constraint: **≥ 2,000 ly from Sol**.

| Field | rs0 (CO2) | rs1 (Water/no volc) | rs2 (Water/water volc) |
|---|---|---|---|
| Body type | Rocky body | Rocky body | Rocky body |
| Atmosphere | CarbonDioxide | Water | Water |
| Volcanism | None | None | water (substring) |
| Gravity | 0.04 – 0.276 | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature | 190 – 197 K | — | — |
| Pressure | ≥ 0.055 | — | — |

### Recepta Conditivus (14.3M cr) — 2 rulesets

| Field | rs0 (CO2) | rs1 (SO2) |
|---|---|---|
| Body type | Icy, Rocky, HMC | — (any) |
| Atmosphere | CarbonDioxide, CO2-Rich | SulphurDioxide |
| Volcanism | None | — |
| Gravity | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature | 150 – 195 K | 132 – 275 K |
| Pressure | — | — |
| Extra | atm_SO2 ≥ 1.05 | atm_SO2 ≥ 1.05 |

### Tussock Virgam (14.3M cr) — 2 rulesets

Species-level star exclusions: **A, T, B**

| Field | rs0 (Water/no volc) | rs1 (Water/water volc) |
|---|---|---|
| Body type | Rocky body, HMC | Rocky body, HMC |
| Atmosphere | Water | Water |
| Volcanism | None | water (substring) |
| Gravity | 0.04 – 0.065 | 0.04 – 0.065 |
| Temperature | — | — |
| Pressure | — | — |

### Recepta Umbrux (12.9M cr) — 2 rulesets

| Field | rs0 (CO2) | rs1 (SO2) |
|---|---|---|
| Body type | — (any) | — (any) |
| Atmosphere | CarbonDioxide | SulphurDioxide |
| Volcanism | — | — |
| Gravity | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature | 151 – 200 K | 132 – 273 K |
| Pressure | — | — |
| Extra | atm_SO2 ≥ 1.05 | atm_SO2 ≥ 1.05 |

### Aleoida Gravis (12.9M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Rocky body, HMC |
| Atmosphere | CarbonDioxide |
| Volcanism | None (required absent) |
| Gravity | 0.04 – 0.276 |
| Temperature | 190 – 197 K |
| Pressure | ≥ 0.054 |
| Star exclusions | — |
| Region/distance | — |

### Clypeus Margaritus (11.9M cr) — 2 rulesets

Species-level star exclusions: **T**

| Field | rs0 (CO2) | rs1 (Water) |
|---|---|---|
| Body type | HMC | HMC |
| Atmosphere | CarbonDioxide | Water |
| Volcanism | None | None |
| Gravity | 0.04 – 0.276 | 0.04 – 0.276 |
| Temperature | 190 – 196 K | — |
| Pressure | ≥ 0.050 | — |

### Osseus Discus (12.9M cr) — 1 ruleset

| Field | rs0 |
|---|---|
| Body type | Rocky body, HMC |
| Atmosphere | Water |
| Volcanism | — |
| Gravity | 0.04 – 0.063 |
| Temperature | 400 – 455 K |
| Pressure | — |
| Star exclusions | — |
| Region/distance | — |

### Tubus Cavas (11.9M cr) — 1 ruleset

Region constraint: **Scutum-Centaurus** arm only.

| Field | rs0 |
|---|---|
| Body type | Rocky body |
| Atmosphere | CarbonDioxide |
| Volcanism | None (required absent) |
| Gravity | 0.04 – 0.152 |
| Temperature | 160 – 197 K |
| Pressure | ≥ 0.003 |

### Frutexa Flammasis (10.3M cr) — 1 ruleset

Region constraint: **Scutum-Centaurus** arm only.

| Field | rs0 |
|---|---|
| Body type | Rocky body |
| Atmosphere | Ammonia |
| Volcanism | — |
| Gravity | 0.04 – 0.276 |
| Temperature | 152 – 177 K |
| Pressure | ≤ 0.0135 |

### Matching Engine Notes

- **Atmosphere matching** uses substring matching with exclusion. "CarbonDioxide" matches
  "Thin Carbon dioxide" but not "Thin Carbon dioxide-rich". Only "Thin" prefix atmospheres
  are accepted (bio requires thin atmospheres).
- **Volcanism** constraints: `"None"` = must have no volcanism, `["water"]` = volcanismType
  must contain "water" (case-insensitive substring), `["silicate", "metallic", "rocky"]` =
  must contain at least one keyword, absent = no constraint.
- **Body type** maps BioScan names to Parquet subType values (e.g., "High metal content body"
  → "High metal content world"). Abbreviated as **HMC** in tables above.
- **Star class** is derived from `spectralClass[0]` of the main star in the system (the body
  in bodies.parquet with `type == "Star"` and `mainStar == True`).
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

## File Locations

- Landmark data: `Databases/spansh_landmarks/` (per-species Parquet + merged `all_landmarks.parquet`)
- Baseline validation: `output/validation_galaxy/results.json`
- Post-Tectonicas validation: `output/validation_post_tectonicas_fix/results.tsv`
- Post-Batch-1 validation: `output/validation_post_batch1_fix/results.tsv`
- Post-Batch-2 validation: `output/validation_post_batch2_fix/results.tsv`
- Sweep results: `experiments/bio_rule_optimization/results/`
- Pareto summary: `experiments/bio_rule_optimization/results/pareto_summary.txt`
