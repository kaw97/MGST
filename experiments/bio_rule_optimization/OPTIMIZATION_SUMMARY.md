# Species Rule Optimization — Summary & BioScan Comparison

## Overview

This document summarizes the optimization work performed on MGST's exobiology species
prediction rules (`src/mgst/bio/species_rules.py`) and compares the resulting rules
against the upstream BioScan reference rulesets (`EDMC-BioScan/src/bio_scan/bio_data/rulesets/`).

MGST's rules were originally derived from BioScan, then empirically tuned using
galaxy-wide validation against 616,835 Spansh landmark bodies (confirmed species scans).
The optimization focused on reducing false negatives (missed species) while preserving
or improving precision.

## Results at a Glance

**8 rule changes** were applied across 7 species over three optimization sessions.

| Metric | Before | After | Change |
|---|---:|---:|---|
| Total true positives | 122,454 | 142,856 | +20,402 (+16.7%) |
| Total false negatives | 27,335 | 7,113 | -20,222 (-74.0%) |
| Species with ≥97% recall | 4 / 18 | 11 / 18 | +7 |
| Species with ≥95% precision | 2 / 18 | 3 / 18 | +1 |

## Applied Changes

### 1. Stratum Tectonicas — 4 ruleset fixes

| Ruleset | Change | Rationale |
|---|---|---|
| rs0 (Ammonia) | max_gravity 0.275 → 0.37 | Confirmed TP bodies extend to 0.37G |
| rs1 (CO2) | atm_SO2 min 0.9901 → 0.99, max_gravity 0.275 → 0.50 | Threshold 0.0001 too high (precision artifact), TP extend to 0.50G |
| rs2 (SO2) | gravity 0.265–0.275 → 0.265–0.60, max_temp 210 → 450 | Original range captured zero actual bodies; SO2 atmosphere is near-perfect discriminator |
| rs3 (Water) | Remove M-star exclusion + volcanism req, add temp 390–455K | M-star exclusion was counterproductive; temp range tightens specificity |

**Impact**: Precision 59.7% → 61.7% (+2.0pp), Recall 41.1% → 91.4% (+50.3pp)

### 2. Fonticulua Fluctus — gravity floor

min_gravity 0.248 → 0.23. TP bodies confirmed down to 0.23G.

**Impact**: Precision unchanged (97.6%), Recall 80.2% → 99.8% (+19.7pp)

### 3. Fonticulua Segmentatus — gravity ceiling + constraint removal

max_gravity 0.2744 → 0.276. Removed distanceToArrival constraint (not a real discriminator).

**Impact**: Precision unchanged (100.0%), Recall 82.3% → 98.9% (+16.7pp)

### 4. Concha Biconcavis — temperature and pressure ceilings

max_temperature 49.7 → 53.0K, max_pressure 0.0047 → 0.01 atm.

**Impact**: Precision unchanged (99.7%), Recall 89.9% → 100.0% (+10.1pp)

### 5. Clypeus Margaritus — temperature ceiling + pressure floor

max_temperature 195.0 → 196.0K, min_pressure 0.0657 → 0.050 atm.

**Impact**: Precision 73.7% → 73.8% (+0.1pp), Recall 90.3% → 100.0% (+9.7pp)

### 6. Tussock Stigmasis — gravity ceiling

max_gravity 0.2733 → 0.276.

**Impact**: Precision 78.2% → 77.6% (-0.5pp), Recall 94.4% → 99.8% (+5.4pp)

### 7. Tussock Virgam — gravity floor

rs0 min_gravity 0.0463 → 0.04.

**Impact**: Precision 79.4% → 78.8% (-0.6pp), Recall 87.9% → 97.0% (+9.1pp)

### 8. Fumerola Extremus — constraint removal

Removed `star_solar_radius >= 0.605` from Methane rs2. This constraint blocked
cool dwarf stars (H/Y/T/M class) where the species legitimately grows.

**Impact**: Precision 94.8% → 95.0% (+0.2pp), Recall 70.6% → 97.8% (+27.2pp)

## Species Investigated Without Viable Fix

| Species | Issue | Finding |
|---|---|---|
| Osseus Discus | 1,141 FN on Methane/Ammonia/Argon | Within-genus confusion (other Osseus species share same niche). Adding Methane ruleset: 800 TP but 819 FP_wrong_species. Drops precision 52.3% → 50.7%. |
| Cactoida Vermis | 1,207 FN, 65% from K-star exclusion | Removing K-star exclusion: +721 TP but +1,472 FP (32.9% new match precision). Precision drops 5.2pp. |
| Recepta Deltahedronix | 684 FN, 99% on Thin CO2 | Only SO2 ruleset exists. CO2 ruleset would worsen already-terrible 15.3% precision. |

## Current Performance — All 18 Species

| Species | Value | TP | FP | FN | Precision | FP Rate | Recall | FN Rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Fonticulua Fluctus | 20.0M | 649 | 16 | 1 | 97.6% | 2.4% | 99.8% | 0.2% |
| Fonticulua Segmentatus | 19.0M | 1,990 | 0 | 22 | 100.0% | 0.0% | 98.9% | 1.1% |
| Stratum Tectonicas | 19.0M | 30,459 | 18,946 | 2,880 | 61.7% | 38.3% | 91.4% | 8.6% |
| Tussock Stigmasis | 19.0M | 3,521 | 1,015 | 6 | 77.6% | 22.4% | 99.8% | 0.2% |
| Concha Biconcavis | 16.8M | 959 | 3 | 0 | 99.7% | 0.3% | 100.0% | 0.0% |
| Cactoida Vermis | 16.2M | 10,183 | 4,970 | 1,207 | 67.2% | 32.8% | 89.4% | 10.6% |
| Fumerola Extremus | 16.2M | 2,437 | 127 | 55 | 95.0% | 5.0% | 97.8% | 2.2% |
| Stratum Cucumisis | 16.2M | 10,066 | 8,407 | 15 | 54.5% | 45.5% | 99.9% | 0.1% |
| Clypeus Speculumi | 16.2M | 5,903 | 18,712 | 688 | 24.0% | 76.0% | 89.6% | 10.4% |
| Recepta Conditivus | 14.3M | 4,391 | 13,472 | 26 | 24.6% | 75.4% | 99.4% | 0.6% |
| Tussock Virgam | 14.3M | 10,798 | 2,910 | 336 | 78.8% | 21.2% | 97.0% | 3.0% |
| Recepta Deltahedronix | 12.9M | 2,014 | 11,157 | 684 | 15.3% | 84.7% | 74.6% | 25.4% |
| Osseus Discus | 12.9M | 9,450 | 8,602 | 1,141 | 52.3% | 47.6% | 89.2% | 10.8% |
| Recepta Umbrux | 12.9M | 7,522 | 10,352 | 47 | 42.1% | 57.9% | 99.4% | 0.6% |
| Aleoida Gravis | 12.9M | 10,110 | 14,053 | 1 | 41.8% | 58.2% | 100.0% | 0.0% |
| Clypeus Margaritus | 11.9M | 10,519 | 3,731 | 1 | 73.8% | 26.2% | 100.0% | 0.0% |
| Tubus Cavas | 11.9M | 10,313 | 3,660 | 1 | 73.8% | 26.2% | 100.0% | 0.0% |
| Frutexa Flammasis | 10.3M | 11,392 | 9,270 | 2 | 55.1% | 44.9% | 100.0% | 0.0% |

---

## BioScan Comparison

MGST rules were derived from BioScan (`EDMC-BioScan/src/bio_scan/bio_data/rulesets/`)
and then diverged through empirical tuning. This section documents every difference.

### Structural Differences

MGST adds several constraint types that BioScan does not use:

| Constraint | MGST | BioScan |
|---|---|---|
| Star class exclusions | Yes (5 species) | No |
| Star solar radius | Removed during optimization (was on Fumerola) | No |
| distanceToArrival | Removed during optimization (was on Font. Seg.) | No |
| Region constraints | Yes (3 species) | Yes (3 species, identical) |
| Distance from Sol | Yes (Clypeus Speculumi ≥ 2000 ly) | Yes (identical) |

BioScan covers more atmospheres for some species (additional rulesets), while MGST
sometimes uses tighter parameter ranges with star class exclusions as a substitute
for missing atmosphere-level discrimination.

### Per-Species Comparison

#### Fonticulua Fluctus

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| min_gravity | 0.235 | **0.23** | MGST wider (−0.005G) |
| max_gravity | 0.276 | 0.276 | Same |
| min_temperature | 143 | 143 | Same |
| max_temperature | 200 | 200 | Same |
| min_pressure | 0.012 | 0.012 | Same |
| Body/Atm/Volc | Icy / Oxygen / — | Icy / Oxygen / — | Same |

MGST lowered the gravity floor based on confirmed TP at 0.23G. BioScan's 0.235 floor
misses these bodies. No other differences.

#### Fonticulua Segmentatus

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| Gravity | 0.25–0.276 | 0.25–0.276 | Same |
| Temperature | 50–75 K | 50–75 K | Same |
| Pressure | ≤ 0.006 | ≤ 0.006 | Same |
| distanceToArrival | — | — | Same (MGST removed its old constraint) |
| Body/Atm/Volc | Icy / Neon,NeonRich / None | Icy / Neon,NeonRich / None | Same |

Now identical. MGST previously had a distanceToArrival constraint and tighter max_gravity
(0.2744) that BioScan never had; both were removed during optimization.

#### Stratum Tectonicas

**BioScan has 7 rulesets; MGST has 4.** BioScan covers Argon/ArgonRich, CarbonDioxideRich,
and Oxygen atmospheres that MGST does not. BioScan also has no star class exclusions.

| Ruleset | Parameter | BioScan | MGST | Difference |
|---|---|---|---|---|
| Ammonia | max_gravity | 0.38 | **0.37** | MGST tighter (−0.01G) |
| CO2 | max_gravity | 0.61 | **0.50** | MGST tighter (−0.11G) |
| CO2 | atm_SO2 min | — | **0.99** | MGST adds SO2 component filter |
| SO2 | min_gravity | 0.29 | **0.265** | MGST wider (−0.025G) |
| SO2 | max_gravity | 0.62 | **0.60** | MGST tighter (−0.02G) |
| Water | volcanism | None | — | MGST removed volcanism=None req |
| Water | max_gravity | 0.063 | 0.063 | Same |
| Water | temperature | — | **390–455 K** | MGST adds temperature range |
| Star exclusions | — | — | **G, A** | MGST only |

Missing from MGST:
- Argon/ArgonRich ruleset (gravity 0.485–0.54, temp 167–199K, volcanism None)
- CarbonDioxideRich ruleset (gravity 0.035–0.61, temp 165–260K)
- Oxygen ruleset (gravity 0.4–0.52, temp 165–246K)

These atmospheres account for ~147 of MGST's 2,880 remaining FN ("Other" atmospheres).
They were not added because BioScan's rules for them have not been validated.

#### Tussock Stigmasis

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| Gravity | 0.04–0.276 | 0.04–0.276 | Same |
| Temperature | 132–180 K | 132–180 K | Same |
| Pressure | ≤ 0.01 | ≤ 0.01 | Same |
| Body/Atm/Volc | Rocky,HMC / SO2 / — | Rocky,HMC / SO2 / — | Same |

Identical.

#### Concha Biconcavis

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| max_temperature | **52** | **53** | MGST wider (+1K) |
| max_pressure | **0.0047** | **0.01** | MGST wider (+0.0053 atm) |
| Gravity | 0.053–0.275 | 0.053–0.275 | Same |
| Body/Atm/Volc | Rocky,HMC / Nitrogen / None | Rocky,HMC / Nitrogen / None | Same |

MGST widened both ceilings based on confirmed TP bodies. BioScan's tighter bounds cause
97 false negatives (bodies with confirmed Concha Biconcavis at 52–53K or pressure up to 0.01).

#### Cactoida Vermis

| Ruleset | Parameter | BioScan | MGST | Difference |
|---|---|---|---|---|
| rs0 (SO2) | — | Identical | Identical | Same |
| rs1 (Water/no-volc) | max_gravity | **0.276** | **0.0612** | MGST much tighter |
| rs2 (Water/water-volc) | — | Identical | Identical | Same |
| Star exclusions | — | — | **K** | MGST only |

Two key MGST additions vs BioScan:
1. **K-star exclusion**: Reduces FP at cost of 1,207 FN. Galaxy-wide testing showed
   removing it adds +721 TP but +1,472 FP (precision −5.2pp). Kept as a precision filter.
2. **rs1 max_gravity 0.0612**: BioScan's Water/no-volcanism ruleset has no upper gravity
   limit beyond the genus-level 0.276G. MGST tightens to 0.0612G to distinguish Vermis
   from other Cactoida species on Water worlds. This is a precision optimization.

#### Fumerola Extremus

| Ruleset | Parameter | BioScan | MGST | Difference |
|---|---|---|---|---|
| rs0 (Ammonia) | — | Identical | Identical | Same |
| rs1 (Argon) | — | Identical | Identical | Same |
| rs2 (Methane) | star_solar_radius | — | — | Same (MGST removed its old ≥0.605 constraint) |
| rs3 (SO2) | min_gravity | **0.07** | **0.2656** | MGST much tighter |
| rs3 (SO2) | Body types | Rocky, Rocky Ice, HMC | **Rocky, Rocky Ice** | MGST excludes HMC |

MGST's SO2 ruleset is significantly tighter than BioScan's. The gravity floor 0.2656 vs
0.07 and exclusion of HMC bodies are precision filters. MGST previously had a
`star_solar_radius >= 0.605` constraint on the Methane ruleset that BioScan never had;
it was removed during optimization because it blocked legitimate cool-dwarf-star TP.

#### Recepta Deltahedronix

**BioScan has 3 rulesets; MGST has 1.**

| Ruleset | BioScan | MGST |
|---|---|---|
| CO2 / no-volcanism | Yes (temp 150–195K, atm_SO2 ≥ 1.05) | **No** |
| CO2 / water-volcanism (Icy, Rocky Ice) | Yes (temp 150–195K, atm_SO2 ≥ 1.05) | **No** |
| SO2 | Yes (temp 132–272K, atm_SO2 ≥ 1.05) | Yes (identical) |

MGST omits the two CO2 rulesets. BioScan's CO2 rulesets for Deltahedronix are identical
to its CO2 rules for Recepta Conditivus, making them indistinguishable. Given Deltahedronix's
already-terrible precision (15.3%), adding more rulesets would only increase FP. The SO2
ruleset is identical between BioScan and MGST.

#### Stratum Cucumisis

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| All 4 rulesets | Identical | Identical | Same |
| Region | sagittarius-carina | sagittarius-carina | Same |

Identical.

#### Clypeus Speculumi

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| All 3 rulesets | Identical | Identical | Same |
| Distance | ≥ 2000 ly | ≥ 2000 ly | Same |

Identical.

#### Recepta Conditivus

**BioScan has 4 rulesets; MGST has 2.**

| Ruleset | BioScan | MGST |
|---|---|---|
| CO2/CO2-Rich / no-volcanism | Yes | Yes (identical) |
| Oxygen / no-volcanism (Icy body) | Yes (grav 0.23–0.276, temp 154–175K, pres ≥ 0.01, atm_SO2 ≥ 1.05) | **No** |
| Oxygen / water-volcanism (Icy body) | Yes (same params) | **No** |
| SO2 | Yes | Yes (identical) |

MGST omits the Oxygen rulesets. These are shared with Recepta Umbrux and would increase
within-genus confusion (already 75.4% FP rate).

#### Tussock Virgam

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| min_gravity | 0.04 | 0.04 | Same (MGST matched after optimization) |
| max_gravity | 0.065 | 0.065 | Same |
| Body/Atm/Volc | Rocky,HMC / Water / None + water | Rocky,HMC / Water / None + water | Same |
| Star exclusions | — | **A, T, B** | MGST only |

MGST's min_gravity was originally 0.0463; optimization lowered it to match BioScan's 0.04.
MGST adds A/T/B star class exclusions that BioScan does not have, as precision filters.

#### Recepta Umbrux

**BioScan has 4 rulesets; MGST has 2.**

| Ruleset | BioScan | MGST |
|---|---|---|
| CO2 | Yes | Yes (identical) |
| Oxygen / no-volcanism (Icy body) | Yes (grav 0.23–0.276, temp 154–175K, pres ≥ 0.01, atm_SO2 ≥ 1.05) | **No** |
| Oxygen / water-volcanism (Icy body) | Yes (same params) | **No** |
| SO2 | Yes | Yes (identical) |

Same situation as Recepta Conditivus — Oxygen rulesets omitted to avoid within-genus confusion.

#### Aleoida Gravis

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| All parameters | Identical | Identical | Same |

Identical.

#### Clypeus Margaritus

| Ruleset | Parameter | BioScan | MGST | Difference |
|---|---|---|---|---|
| rs0 (CO2) | min_pressure | **0.054** | **0.050** | MGST wider (−0.004 atm) |
| rs0 (CO2) | max_temperature | **197** | **196** | MGST tighter (−1K) |
| Star exclusions | — | — | **T** | MGST only |

MGST lowered the pressure floor (recovering FN bodies near 0.050 atm) and slightly
tightened the temperature ceiling. T-star exclusion is a precision filter not in BioScan.

#### Osseus Discus

**BioScan has 4 rulesets; MGST has 1.**

| Ruleset | BioScan | MGST |
|---|---|---|
| Ammonia | Yes (Rocky, Rocky Ice, HMC / volcanism Any / grav 0.04–0.088, temp 161–177K, pres ≤ 0.0135) | **No** |
| Argon | Yes (Rocky Ice / volcanism Any / grav 0.2–0.276, temp 65–120K) | **No** |
| Methane | Yes (Rocky / volcanism Any / grav 0.04–0.127, temp 80–110K, pres ≥ 0.012) | **No** |
| Water | grav 0.04–**0.055**, temp — | grav 0.04–**0.063**, temp **400–455K** | See below |

Key differences:
1. **Ammonia/Argon/Methane rulesets omitted**: Galaxy-wide analysis showed within-genus
   confusion makes these unviable. Methane: 800 TP but 819 FP_wrong_species even with
   tight filters (other Osseus species share the niche). Ammonia: only 150 TP. Argon:
   1 confirmed TP in the galaxy.
2. **Water max_gravity widened**: 0.055 → 0.063. Confirmed TP bodies exist up to 0.063G.
3. **Water temperature range added**: 400–455K. BioScan has no temperature constraint;
   MGST adds one as a precision filter.

#### Tubus Cavas

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| All parameters | Identical | Identical | Same |
| Region | scutum-centaurus | scutum-centaurus | Same |

Identical.

#### Frutexa Flammasis

| Parameter | BioScan | MGST | Difference |
|---|---|---|---|
| All parameters | Identical | Identical | Same |
| Region | scutum-centaurus | scutum-centaurus | Same |

Identical.

### Summary of Differences

**MGST wider than BioScan** (recovering false negatives):

| Species | Parameter | BioScan | MGST | FN Recovered |
|---|---|---|---|---:|
| Fonticulua Fluctus | min_gravity | 0.235 | 0.23 | ~128 |
| Concha Biconcavis | max_temperature | 52 | 53 | ~97 |
| Concha Biconcavis | max_pressure | 0.0047 | 0.01 | (included above) |
| Clypeus Margaritus | min_pressure | 0.054 | 0.050 | ~1,022 |
| Osseus Discus | max_gravity (Water) | 0.055 | 0.063 | ~200 (est) |

**MGST tighter than BioScan** (precision filters):

| Species | Parameter | BioScan | MGST | Purpose |
|---|---|---|---|---|
| Stratum Tectonicas | star classes | — | Exclude G, A | +2pp precision |
| Cactoida Vermis | star classes | — | Exclude K | +5pp precision |
| Cactoida Vermis | rs1 max_gravity | 0.276 | 0.0612 | Genus discrimination |
| Tussock Virgam | star classes | — | Exclude A, T, B | Precision filter |
| Clypeus Margaritus | star classes | — | Exclude T | Precision filter |
| Clypeus Margaritus | max_temperature | 197 | 196 | −1K tighter |
| Fumerola Extremus | rs3 min_gravity | 0.07 | 0.2656 | Precision filter |
| Fumerola Extremus | rs3 body types | +HMC | No HMC | Precision filter |
| Osseus Discus | Water temperature | — | 400–455K | Precision filter |
| Stratum Tectonicas | CO2 max_gravity | 0.61 | 0.50 | Precision filter |

**MGST omits rulesets BioScan includes**:

| Species | Missing Rulesets | Reason |
|---|---|---|
| Stratum Tectonicas | Argon, ArgonRich, CO2-Rich, Oxygen (3 rulesets) | Unvalidated; ~147 FN |
| Recepta Deltahedronix | CO2 no-volc, CO2 water-volc (2 rulesets) | Identical to Conditivus rules; precision already 15.3% |
| Recepta Conditivus | Oxygen no-volc, Oxygen water-volc (2 rulesets) | Within-genus confusion with Umbrux |
| Recepta Umbrux | Oxygen no-volc, Oxygen water-volc (2 rulesets) | Within-genus confusion with Conditivus |
| Osseus Discus | Ammonia, Argon, Methane (3 rulesets) | Within-genus confusion (36.9% new match precision at best) |

**Constraints MGST removed** (were unique to MGST, never in BioScan):

| Species | Removed Constraint | Reason |
|---|---|---|
| Fonticulua Segmentatus | distanceToArrival | Not a real discriminator |
| Fumerola Extremus | star_solar_radius ≥ 0.605 | Blocked legitimate cool-dwarf TP |

## Conclusions

1. **Parameter widening works when data supports it.** Five species gained significant
   recall by widening gravity, temperature, or pressure bounds to match confirmed TP
   distributions, with negligible precision cost. BioScan's tighter bounds for these
   species appear to be conservative estimates that exclude real edge cases.

2. **Star class exclusions are effective precision filters.** MGST's star class exclusions
   (absent from BioScan) provide 2–5pp precision improvement per species. These exploit
   the fact that certain species strongly prefer specific stellar environments, even though
   they technically *can* appear around other star types.

3. **Within-genus confusion is the precision ceiling.** For Recepta, Clypeus, Osseus, and
   Stratum, the dominant FP source is prediction of the wrong species within the correct
   genus. Environmental parameters alone cannot distinguish congeners — they share the
   same atmospheres, gravity ranges, and temperatures. This is a fundamental limitation
   of parameter-based prediction.

4. **BioScan's broader rulesets are not always better.** BioScan includes Ammonia/Argon/Methane
   rulesets for Osseus Discus and CO2 rulesets for Recepta Deltahedronix that our validation
   shows would harm precision without meaningful recall gain. MGST's omission of these rulesets
   is a deliberate precision-preserving choice backed by galaxy-wide data.

5. **BioScan's missing atmosphere rulesets for Stratum Tectonicas are a real gap.** BioScan
   covers Argon, CO2-Rich, and Oxygen atmospheres for Tectonicas that MGST does not.
   These account for ~147 of our remaining FN. Adding them with appropriate validation
   would be the next logical improvement.
