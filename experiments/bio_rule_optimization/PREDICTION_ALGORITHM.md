# Exobiology Prediction Algorithm

What predicts each exobiology species and colour on a body, how SrvSurvey's
predictor performs on the full Spansh dataset, where it fails, and what we
changed. Written 2026-10-31, updated 2026-11-01.

## 1. Summary

The best predictor we found is **SrvSurvey's criteria with corrections to
45 species and an orbit-averaged star in place of SrvSurvey's brightest star**.

Scores on 524,839 complete bodies (every genus on the body has a confirmed
species). P is precision and R is recall.

| Method | Species P | Species R | Species wrong | Species missed | Colour P | Colour R | Colour wrong | Colour missed |
|---|---|---|---:|---:|---|---|---:|---:|
| SrvSurvey as is | 69.11% | 99.27% | 493,080 | 8,126 | 62.12% | 99.00% | 667,990 | 11,058 |
| SrvSurvey criteria, orbit star | 69.13% | 99.31% | 492,822 | 7,681 | 62.26% | 99.23% | 665,619 | 8,500 |
| Corrected criteria, SrvSurvey star | 72.45% | 99.61% | 420,868 | 4,336 | 65.19% | 99.34% | 586,881 | 7,335 |
| **Corrected criteria, orbit star (now the engine default)** | **72.47%** | **99.65%** | **420,641** | **3,892** | **65.33%** | **99.57%** | **584,523** | **4,760** |

Against SrvSurvey as is, the final engine misses 52% fewer species (8,126 to
3,892) and 57% fewer species and colour pairs (11,058 to 4,760). It makes 14.7%
fewer wrong species predictions (493,080 to 420,641).

The age, distance and temperature and gravity changes are a deliberate trade. Before
them this engine had 450,704 wrong and 2,719 missed, so they remove 30,063 wrong
predictions for 1,173 more misses on complete bodies. Without them recall was 99.76%.

This table covers the 15 genera whose records carry a colour field. Six more
life forms found mainly on airless bodies are scored separately in section 7.7,
because our Spansh walk didn't collect them.

Three conclusions matter most:

- **SrvSurvey's logic is broadly right.** Body traits decide presence and the
  star or materials decide colour. Testing every trait family found no new
  general mechanism, and the failures are mostly specific and fixable.
- **The corrections and the star fix different things.** The corrections account
  for almost all of the fall in wrong species and most of the fall in missed
  ones. The star changes few predictions overall, but where it
  does, it is right far more often: 6,320 deciding records to 588.
- **Sharp temperature and gravity edges were the largest new gain.** Testing
  every species for a temperature and gravity interaction found almost none that
  a rule should use, but it found hard limits on one axis in 31 species
  (section 5.6).
- **Precision stays near 72% because of a limit we couldn't break.** Rules
  usually leave several species per genus, and no measured trait separates them
  cleanly. Section 7 sets out this and the other limits.

The engine is the research code in `srvsurv_engine.py`. **The main search,
`scripts/find_high_value_systems.py`, doesn't use it.** That script uses the
18-species rules in `src/mgst/bio/species_rules.py`, where the uncommitted changes
are Tubus Cavas's gravity limit (0.152 to 0.1515 g) and the Clypeus Speculumi
parent-chain distance (section 5.7). None of this work is committed. Those rules also hold a committed Cactoida Vermis age rule that this
work finds costly (section 5.5).

## 2. Data and scoring

| Item | Value |
|---|---|
| Analysis database | `/data/Databases_v3/bio_analysis` |
| Bodies with a biological signal | 3,916,224 |
| Bodies with predictions | 3,880,306 |
| Complete bodies | 524,839 |
| Final score output | `output/rule_corrections_score_final_v2_20261101/` |
| Star comparison output | `output/star_model_verdict_fixed_20261031/report.txt` |
| Trait test output | `output/trait_families_v2_20261031/trait_families.tsv` |
| Decision tree output | `output/species_colour_trees_20261031/report.txt` |

How predictions are scored:

- **Right:** the species is predicted and confirmed on the body.
- **Wrong:** the species is predicted but is certainly absent.
- **Unknown:** the body hasn't been scanned enough to tell. These never count
  as wrong.

The tables use two scopes, so their numbers don't add up directly:

- **Complete bodies** give precision and recall (sections 1 and 7).
- **All bodies with known truth** give the per-correction counts (section 5).
  A prediction counts as known if its genus is confirmed on the body or the
  body's genus list excludes that genus. This scope is larger, so for example
  the section 5.2 vetoes remove 187,417 known-wrong predictions, while complete-body
  wrong predictions fall by 72,181 from every correction together.

The database holds bodies with a biological signal or a genus list, so nothing
here measures how well the rules predict *whether a body has any life at all*.

## 3. The prediction algorithm

For each planet in a system, the engine does the following.

### Step 1: convert the body's values

SrvSurvey's criteria were fitted to in-game journal values, so Spansh values
are converted first: gravity × 0.980665, pressure × 1.01325, plus journal names
for planet class, atmosphere and volcanism. A body with no parent star gets no
predictions. The species tables in Appendix A are in Spansh units.

### Step 2: choose the star

The star is the one delivering the most orbit-averaged light, among the stars in the
body's parent chain and the barycentres on it (section 6.6):

```
flux(star) = L × ⟨ 1 / d² ⟩        L = R² (T / 5772 K)⁴
```

The average runs over Kepler orbits, with 5,000 samples per orbit. Binary
partners share one phase, so they stay on opposite sides of their barycentre.
Section 6.1 gives the full method: the data, the Kepler simulation, the estimator,
checks against exact results and known defects, each tied to the code.

Its flattened class (for example, all white dwarfs become `D`) and its surface
temperature feed the next steps.

### Step 3: test presence

Walk SrvSurvey's criteria tree for each genus and species. A species qualifies
if any one of its rulesets passes. Rulesets test:

- planet class, atmosphere type and composition
- temperature, gravity and pressure
- volcanism
- galactic region, for regional species
- distance from a nebula, or the Guardian bubble, for a few species

### Step 4: apply the corrections

- **Regions:** seven Tussock species gain regions 8, 16, 17 and 35, and lose
  region 9 (section 5.1).
- **Vetoes:** twelve species drop out under the conditions in section 5.2.

### Step 5: choose the colour

The colour comes from the star class or from materials, depending on the
species (section 4). A few species also use the body type.

- **Star species:** one colour, from the star in step 2.
- **Material species:** a colour from the rare materials on the body. Half of
  these species still have two possible colours (section 7.3).
- **Anemones, Brain Trees and Sinuous Tubers:** the colour is part of the name
  (Roseum Brain Tree), so SrvSurvey lists each colour as its own species.

### Step 6: filter by known genera (optional)

Once an FSS scan has given the body's genus list, only those genera are kept.

### Code

```python
system = engine.SystemContext(bodies)
props = engine.body_props(body, system, (x, y, z))        # orbit star by default
predictions = engine.predict_props(props, known_genera)   # corrections on by default
```

`star_model="srvsurvey"` and `corrections=False` give the baseline.

## 4. What decides presence and colour

We tested whether each family of traits predicts each species' presence and
colour, using AUC or accuracy with folds grouped by sector. The family list was body
traits, star, system, materials and galactic position.

**Presence.** Body traits were the best group for 87 of 91 species.
SrvSurvey's use of body traits is confirmed. Among bodies the rules already
predict, all traits together reach a median AUC of 0.934, against 0.733 for
body traits alone. So other traits do carry information about which predicted
species are really there, but no clean rule emerged (section 7.1).

**Colour.** This matched SrvSurvey with no exceptions:

| Colour decided by | Species | Accuracy using that family |
|---|---:|---|
| Star class | 63 | about 99.7% |
| Materials, one candidate left | 13 | about 99.7% |
| Materials, two candidates left | 13 | about 50% |

**New findings:**

- The star affects one species' presence: Stratum Araneamus needs a star above
  7,500 K (section 5.2).
- Galactic position doesn't explain the two regional coin flips in section 7.2.
- Which material colour appears isn't explained by the material percentages,
  body ID or other species on the same body (section 7.3).

## 5. Where SrvSurvey's rules fail, and the fixes

### 5.1 Tussock regions (the largest source of missed species)

Seven Tussock species are found across the Orion-Cygnus arm, but `tussock.json`
allows only regions 7 (Izanami) and 18 (Inner Orion Spur) of it. These are
missing:

- 8 Inner Orion-Perseus Conflux
- 16 Orion-Cygnus Arm
- 17 Temple
- 35 Outer Orion Spur

The same species are never found in region 9 (Inner Scutum-Centaurus Arm), which
the criteria allow as part of their Sagittarius-Carina group.

| Species | Added: right | Added: wrong | Added: unknown | Region 9 removed: right | Region 9 removed: wrong | Region 9 removed: unknown |
|---|---:|---:|---:|---:|---:|---:|
| Tussock Albata | 3,225 | 3 | 4,636 | 0 | 2,548 | 7,224 |
| Tussock Caputus | 3,804 | 1 | 6,389 | 0 | 3,131 | 7,500 |
| Tussock Ignis | 4,869 | 5 | 9,239 | 0 | 5,555 | 18,439 |
| Tussock Pennata | 1,044 | 0 | 1,713 | 0 | 594 | 2,037 |
| Tussock Serrati | 2,140 | 3 | 2,767 | 0 | 1,748 | 5,292 |
| Tussock Triticum | 1,317 | 0 | 1,071 | 0 | 774 | 1,674 |
| Tussock Ventusa | 1,892 | 2 | 3,210 | 0 | 1,093 | 4,256 |
| **Total** | **18,291** | **14** | **29,025** | **0** | **15,443** | **46,422** |

Counts are under the orbit star, on all bodies with known truth.

### 5.2 Criteria that are too loose

Each veto drops a species past a clean threshold in the data.

| Species | Dropped when | Right lost | Wrong removed | Unknown removed |
|---|---|---:|---:|---:|
| Clypeus Speculumi | under 2,500 ls from the first star or binary up the parent chain | 8 | 19,213 | 22,460 |
| Electricae Radialem | Methane atmosphere | 1 | 2,813 | 527 |
| Fonticulua Lapida | gravity above 0.2753 g | 1 | 5,991 | 829 |
| Frutexa Flabellum | atmosphere isn't Ammonia | 0 | 17,008 | 70,246 |
| Frutexa Metallicum | gravity above 0.2753 g | 0 | 2,742 | 241 |
| Fumerola Nitris | gravity above 0.2743 g | 0 | 5,215 | 923 |
| Stratum Araneamus | star at or below 7,500 K | 52 | 30,341 | 13,862 |
| Stratum Laminamus | temperature at or below 165 K | 9 | 30,286 | 5,494 |
| Tubus Cavas | gravity above 0.1515 g | 3 | 2,992 | 474 |
| Tubus Compagibus | gravity above 0.1515 g | 11 | 14,512 | 2,520 |
| Tubus Rosarium | gravity above 0.1515 g | 33 | 27,938 | 5,360 |
| Tubus Sororibus | gravity above 0.1515 g | 18 | 7,669 | 944 |
| Fonticulua Campestris | Rocky Ice body, Argon, star older than 10,000 My | 159 | 15,319 | 3,528 |
| Osseus Discus | High metal content body, Water, star younger than 8,500 My | 17 | 5,378 | 963 |
| **Total** | | **312** | **187,417** | **128,371** |

These thresholds were read from the same data that scores them (section 7.8).

### 5.3 Star choice

SrvSurvey's star is wrong in a specific kind of binary (section 6). Because the
star sets most colours and some species' presence, one wrong star usually gets
every prediction on that body wrong.

### 5.4 Deliberately not changed

| Candidate | Result | Decision |
|---|---|---|
| Veto Tela at or below 260 K, in every atmosphere | Trade-off wasn't clean | Rejected; a limit on CO2 atmospheres alone is clean (section 5.6) |
| Widen Electricae Radialem | Trade-off wasn't clean | Rejected |
| Widen Stratum Tectonicas to non-Rocky bodies | 869 right, 3 wrong held out; 495 right, 23 wrong far from Sol; includes 595 High Metal Content bodies with Oxygen | Promising, not applied |
| Soft vetoes from decision trees | 74.9% precision, 99.2% recall (mainly Concha Labiata and Aureolas) | Not applied: costs recall and is harder to read |

### 5.5 Star age

Each species was tested for an age rule of its own, not given another species'
cut-off. Each was split by body type and atmosphere, a minimum, maximum or band
was fitted on half the sectors, and it was scored on the other half. The main
star's age is used, because stars in a system almost always share one.

Of the 90 predicted species, 84 had enough bodies, and 4 show a pattern that
passes on held-out sectors. The other 80 show none.

| Species and group | Fitted limit | Right lost, held out | Wrong removed, held out | Decision |
|---|---|---:|---:|---|
| Fonticulua Campestris, Rocky Ice, Argon | at most 11,998 My | 30 | 3,354 | Applied at 10,000 My (section 5.7) |
| Osseus Discus, High metal content, Water | at least 8,126 My | 5 | 2,453 | Applied at 8,500 My |
| Cactoida Cortexum, Rocky, CO2 | at most 11,832 My | 97 | 343 | Not applied |
| Cactoida Vermis, Rocky, Water | at most 11,706 My | 38 | 212 | Not applied |

Cortexum and Vermis pass the test's thresholds, but 15–22% of what they remove is
real life, so they stay soft patterns. The production rule for Cactoida Vermis on
High metal content Water bodies (at most 9,500 My) removes 2,359 wrong for 761
right, which is well short of the applied rules. Results are in
`output/star_age_rules_20261031`.

#### By dominant star class

This test was repeated with the age of the **dominant star**, the one giving the
body the most orbit-averaged light (section 6.1), in place of the main star's. Each
species was also split by the dominant star's class. Predictions are the final
engine's, with every veto and temperature and gravity limit except the two age
rules, so those were refitted from scratch. A rule fitted for each class was
compared with one rule for the whole group, on held-out sectors in both directions.
A per-class rule counts as useful under the same bar as section 5.6.

**Which star's age is used barely matters.** On 5,193,252 known predictions, the
dominant star's age differs from the main star's on only 2,864 (0.06%), although
its class differs on 18.7%. So stars in a system share an age, and the two applied
rules remove the same bodies under either age: Campestris 158 right and 14,366
wrong, Discus 17 right and 5,378 wrong.

**Age requirements do differ by class, mainly for Fonticulua Campestris.** Of 264
per-class comparisons, 10 beat one pooled rule. Most of the gain is Campestris:

![Dominant star age by class for Fonticulua Campestris on Rocky Ice Argon bodies](star_age_class_figures/Fonticulua_Campestris_Rocky_Ice_world_Argon.png)

On Rocky Ice Argon bodies, where the 10,000 My rule applies, the wrong predictions sit
above about 9,500 My in every class. The right ones around M, L and T dwarfs stop just
below that: their 99th percentiles are 9,731, 9,386 and 9,338 My. Around G and K stars,
some right bodies are older. With the current rule in place, a rule per class adds
nothing, since 1,120 wrong removed held out compares with 1,127 for a single refitted
limit. That refitted limit (about 9,400 My) would still remove 1,127 more wrong for
70 right, so the 10,000 My limit is conservative.

![Dominant star age by class for Fonticulua Campestris, all bodies](star_age_class_figures/Fonticulua_Campestris.png)

On Icy Argon bodies, which the current rule doesn't cover, the class matters. Around
brown dwarfs the cut-off is as sharp as on Rocky Ice bodies, while around K and M stars
it isn't:

| Dominant class, Icy Argon | Right | Wrong | Wrong above 9,500 My | Right above 9,500 My |
|---|---:|---:|---:|---:|
| L | 7,223 | 6,030 | 5,931 | 184 |
| T | 2,602 | 1,849 | 1,830 | 28 |
| M | 60,574 | 38,628 | 34,822 | 7,696 |
| K | 25,503 | 13,759 | 12,519 | 2,877 |
| G | 6,078 | 1,429 | 1,315 | 293 |

Held out on Icy Argon, per-class rules remove 9,666 wrong for 1,115 right, against
5,397 wrong for 1,077 right with one pooled rule. That's 4,269 more wrong for 38 more
right, almost all from L and T dwarfs. A rule for Campestris on Icy Argon bodies
around L or T dwarfs older than about 9,500 My would remove about 7,760 wrong for 212
right on known bodies. It hasn't been applied.

![Dominant star age by class for Osseus Discus on High metal content Water bodies](star_age_class_figures/Osseus_Discus_High_metal_content_world_Water.png)

Osseus Discus needs an old star in every class: right bodies start near 9,000 My
(fitted minimums 8,914 for K, 9,082 for M and 9,262 for L). Wrong ones go up to about
11,300 My, which is the gradual rise described above. Beyond the current rule,
per-class limits add 20 wrong removed (177 against 157 for one limit).

The other per-class gains are small: Concha Labiata on High metal content CO2
bodies (653 more wrong for 12 more right), Recepta Conditivus (302 for 10), Aleoida
Arcus (268 for 23), Recepta Umbrux (189 for 11) and Stratum Laminamus on Rocky
Ammonia bodies (188 for 9). Some per-class maximums are just the oldest stars of
that class, such as F stars at about 8,000 My, so a per-class age limit isn't
necessarily a life requirement. Results are in `output/star_age_by_class_20261103`,
with figures for every species in `output/star_age_by_class_20261103_figures`.

### 5.6 Temperature and gravity

Each species was tested for an interaction between temperature and gravity, in
every split by body type and atmosphere. Three rule shapes were compared:
separate limits on each axis, a limit on a slanted combination, and grid pockets.
What a rule removes had to be almost all wrong, and it had to hold on held-out
sectors in both directions. Of the 90 species, 76 had enough bodies.

**There is little true interaction.** The slanted shapes in the figures are mostly
the outline of where bodies exist, since hot bodies of a given type and atmosphere
also tend to have a particular gravity. Inside those shapes the right share usually
changes gradually, with no line to cut along.

**Tussock Virgam shows this well.** Its wrong predictions cluster near 450 K, near
0.0525 g and above 0.06 g, but those are all one group:

| Temperature | Known bodies | Right |
|---|---:|---:|
| up to 450 K | 12,396 | 12,391 |
| above 450 K | 92 | 0 |

The gravity clusters are the same hot bodies, because hot Rocky bodies sit near
0.053 g and hot High metal content bodies near 0.062 g.

**Concha Labiata on Rocky CO2 bodies is the clearest gradual case.** Its right
share is 96% below 0.05 g, 44% at 0.055–0.06 g and 2% at 0.15–0.25 g, and it falls
from 42% to 3% as pressure rises from under 0.01 to 0.1 atm. That helps rank
predictions, but no veto can use it without losing real finds. We didn't test
whether pressure explains the gravity pattern.

**Where a slanted line or pocket removes more.** A line beat separate limits by at
least 25% in 9 groups and pockets did in 29, across 12 species. Examples are
Recepta Conditivus (5,894 wrong removed, against 3,505) and Bacterium Tela in CO2
(9,429, against 7,173). Pockets need up to 114 grid cells, many groups mix body
types or atmospheres, and we inspected only some of them, so none are applied. They're listed in
`output/temp_gravity_rules_v7_20261101/report.txt`.

**One-axis limits are applied.** They cover 31 species, listed here with counts on
all known bodies. A later limit counts only what earlier ones didn't remove.

| Species | Scope | Dropped when | Right lost | Wrong removed |
|---|---|---|---:|---:|
| Aleoida Spica | High metal content world | above 176 K, below 0.0518 g, above 0.2744 g | 20 | 909 |
| Bacterium Tela | CarbonDioxide | below 275 K, below 0.4658 g, above 0.6098 g | 20 | 8,923 |
| Bacterium Tela | High metal content world CarbonDioxide | below 300 K, below 0.5009 g, above 0.6098 g | 20 | 1,343 |
| Cactoida Lapis | all | above 177 K, above 0.2737 g | 105 | 2,030 |
| Cactoida Peperatis | all | above 0.2739 g | 35 | 827 |
| Cactoida Vermis | Rocky body | above 450 K, above 0.2736 g | 1 | 106 |
| Concha Labiata | all | above 192 K, above 0.2092 g | 496 | 52,584 |
| Concha Labiata | Rocky body | above 192 K, above 0.2067 g | 23 | 1,791 |
| Concha Renibus | all | below 80 K, above 0.2536 g | 204 | 9,574 |
| Concha Renibus | Methane | below 80 K, above 103 K, above 0.1116 g | 0 | 106 |
| Fonticulua Campestris | all | above 0.2742 g | 281 | 4,857 |
| Frutexa Collum | all | above 167 K, above 0.2747 g | 9 | 328 |
| Frutexa Flabellum | all | above 0.2745 g | 11 | 252 |
| Frutexa Flammasis | all | above 0.2744 g | 3 | 110 |
| Frutexa Metallicum | all | above 400 K, above 0.2749 g | 7 | 909 |
| Fumerola Extremus | all | above 0.2745 g | 2 | 75 |
| Fungoida Bullarum | all | above 133 K, above 0.2746 g | 77 | 2,440 |
| Fungoida Setisis | all | above 175 K, above 0.2736 g | 708 | 25,557 |
| Fungoida Setisis | High metal content world | above 175 K, below 0.1474 g, above 0.2743 g | 185 | 9,104 |
| Osseus Pumice | all | above 133 K, above 0.2746 g | 131 | 2,498 |
| Osseus Spiralis | all | above 177 K, above 0.2741 g | 159 | 3,053 |
| Recepta Conditivus | all | below 132 K, below 0.0510 g, above 0.2746 g | 44 | 3,022 |
| Recepta Deltahedronix | all | below 0.0449 g, above 0.2748 g | 25 | 898 |
| Recepta Umbrux | SulphurDioxide | above 273 K, below 0.0407 g, above 0.2746 g | 31 | 808 |
| Tubus Cavas | all | above 0.1472 g | 120 | 3,131 |
| Tubus Compagibus | all | above 0.1470 g | 267 | 6,073 |
| Tubus Conifer | all | above 195 K, below 0.0410 g, above 0.1471 g | 93 | 3,328 |
| Tubus Rosarium | all | above 0.1500 g | 93 | 2,873 |
| Tubus Sororibus | all | above 0.1497 g | 65 | 2,847 |
| Tubus Sororibus | CarbonDioxide | above 0.1481 g | 35 | 1,127 |
| Tussock Capillum | all | above 0.2747 g | 61 | 2,089 |
| Tussock Catena | all | above 0.2744 g | 16 | 817 |
| Tussock Cultro | all | above 0.2743 g | 93 | 2,401 |
| Tussock Divisa | all | above 0.2744 g | 24 | 913 |
| Tussock Stigmasis | all | above 0.2748 g | 17 | 445 |
| Tussock Virgam | all | above 450 K | 0 | 93 |

In total they remove 158,241 wrong and 3,481 right on known bodies. The Fonticulua
Campestris row counts what the 10,000 My age limit leaves. Before that limit moved, they
removed 26,360 wrong and 1,122 right on complete bodies, and fitted on half the sectors
and scored on the other half, they removed 158,688 wrong and 3,596 right.

Many of these species stop near 0.274 g, whereas right predictions above 0.2753 g
are led by Bacterium and Stratum. Some share temperature ceilings, such as about
133 K for Fungoida Bullarum and Osseus Pumice and about 175 K for Fungoida Setisis
and Aleoida Spica. Each limit was still fitted for its own species.

These heatmaps show how to read each species. Colour is the share of known bodies
that are right, from orange (all wrong) through grey (even) to blue (all right).
Blank cells have fewer than 5 known bodies, and unknown bodies aren't counted. The
curves above and beside the map give the right share along each axis, and dashed
lines mark the limits. All of them show predictions before these limits are applied.

Tussock Virgam has a true cliff at 450 K, and the orange cells at the two gravity
clusters both sit past that line:

![Right share across temperature and gravity for Tussock Virgam](temp_gravity_figures/Tussock_Virgam.png)

Fungoida Setisis has limits on both axes, and its right share also drops with
temperature before the 175 K line, so that limit removes only the part that is
almost all wrong:

![Right share across temperature and gravity for Fungoida Setisis](temp_gravity_figures/Fungoida_Setisis.png)

Concha Labiata shows the gradual case. Only its low-gravity strip is mostly right,
and most of the rest stays below a 50% right share, with no clean line to cut along:

![Right share across temperature and gravity for Concha Labiata](temp_gravity_figures/Concha_Labiata.png)

Every species with at least 10 right predictions has its heatmap in Appendix A.

#### Does the range depend on body type, atmosphere, composition, pressure or volcanism?

The heatmaps pool every body type and atmosphere, and the clusters in them come
from that mix. Each species was tested again by splitting it and fitting separate
temperature and gravity limits in each part. Those were compared with one set of
limits for the whole parent group, on held-out sectors in both directions. A split
counts as useful when its limits remove at least 1.25 times the pooled limits'
wrong predictions and 50 more, lose at most 2% of the right ones, and remove
almost all wrong (90% or more) beyond what the pooled limits remove.

- **Across a whole species,** the splits are body type, atmosphere, and both together.
- **Within each body type and atmosphere,** the splits are:
  - the atmosphere mix: the main gas alone, or which second gas
  - the second gas's share: none, under 5%, 5–20%, or 20% or more
  - pressure, in tertiles
  - volcanism

**The clusters are body type, atmosphere and atmosphere mix.** Each colour below is
one body type and atmosphere:

![Right and wrong Stratum Tectonicas predictions by body type and atmosphere](temp_gravity_group_figures/Stratum_Tectonicas.png)

The separate diagonal bands within one colour follow composition and pressure.
Among High metal content CO2 bodies above 275 K and 0.35 g where Stratum Tectonicas
is predicted (9,806 bodies in the first 20 database parts), the bands from low to
high gravity run in two repeating families. Across each family, the CO2 share falls
from about 90% to 63%, the SO2 share rises, and pressure falls by about four times.
Volcanism, being a moon and the star class don't separate the bands.

![Right and wrong Bacterium Aurasus predictions by body type and atmosphere](temp_gravity_group_figures/Bacterium_Aurasus.png)

**Body type and atmosphere were already covered.** Before the current limits, 20 of
681 splits were useful. The largest were the ones the scoped limits in the table
above already handle: Bacterium Tela by atmosphere and by body type and atmosphere,
Fungoida Setisis by body type, and Recepta Umbrux. After those limits, 27 of 642
splits are useful. Volcanism is never useful. Counted once per species and group, at
the best split for each, the remaining gains on held-out sectors are these:

| Species | Group | Best split | More wrong removed | More right lost |
|---|---|---|---:|---:|
| Concha Labiata | High metal content, CO2 | atmosphere mix | 7,037 | 3 |
| Concha Aureolas | Rocky, Ammonia | pressure | 1,013 | 30 |
| Fungoida Stabitis | all | body type | 458 | 20 |
| Stratum Tectonicas | High metal content, CO2 | pressure | 438 | 46 |
| Stratum Tectonicas | High metal content, SO2 | pressure | 425 | 33 |
| Recepta Deltahedronix | all | body type | 389 | 33 |
| Tubus Sororibus | High metal content, CO2 | atmosphere mix | 347 | 29 |
| Recepta Umbrux | all | body type | 319 | 23 |
| Tubus Cavas | Rocky, CO2 | atmosphere mix | 264 | 24 |
| Tubus Compagibus | Rocky, CO2 | atmosphere mix | 136 | 11 |
| Tubus Conifer | Rocky, CO2 | atmosphere mix | 70 | 6 |
| Fonticulua Digitos | all | body type | 52 | 5 |

Apart from Concha Labiata, these are small. Together they remove about 3,900 more
wrong for about 260 right. For Stratum Tectonicas, pressure and mix narrow the
gravity range a little in each part, but the gain is under 1% of its wrong
predictions.

**Composition matters most where it decides presence outright.** For Concha Labiata on
High metal content CO2 bodies, the mix split isn't really a range shift:

| Concha Labiata, High metal content CO2 | Right | Wrong |
|---|---:|---:|
| Pure CO2 | 1,303 | 13,107 |
| CO2 with SO2 | 84 | 41,211 |

SrvSurvey's criterion is "CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99%", and on
this body type the SO2 branch is almost always wrong. A veto on SO2 there would remove
41,211 wrong for 84 right. Within pure CO2, temperature and gravity limits (at most
182 K and 0.085 g) would remove another 10,570 wrong for 7 right, in sample.

A scan of every species, body type and atmosphere, and part found one more group with
almost no life, regardless of mix. Concha Aureolas on High metal content Ammonia bodies
is right on 162 of 94,188 known predictions, with each half of the sectors under 2%.

None of these changes are applied. Results are in `output/temp_gravity_by_group_20261103`
(before the current limits) and `output/temp_gravity_by_group_after_limits_20261103`
(after them), each with a scatter for every species in `figures/`.

#### Heatmaps by genus

Each figure below collects the genus's species heatmaps, one lettered panel per species, in alphabetical order. The caption under each figure names the species and gives its counts.

![Temperature and gravity heatmaps for the Aleoida species](temp_gravity_genus_figures/Aleoida.png)

![Temperature and gravity heatmaps for the Bacterium species](temp_gravity_genus_figures/Bacterium.png)

![Temperature and gravity heatmaps for the Cactoida species](temp_gravity_genus_figures/Cactoida.png)

![Temperature and gravity heatmaps for the Clypeus species](temp_gravity_genus_figures/Clypeus.png)

![Temperature and gravity heatmaps for the Concha species](temp_gravity_genus_figures/Concha.png)

![Temperature and gravity heatmaps for the Electricae species](temp_gravity_genus_figures/Electricae.png)

![Temperature and gravity heatmaps for the Fonticulua species](temp_gravity_genus_figures/Fonticulua.png)

![Temperature and gravity heatmaps for the Frutexa species](temp_gravity_genus_figures/Frutexa.png)

![Temperature and gravity heatmaps for the Fumerola species](temp_gravity_genus_figures/Fumerola.png)

![Temperature and gravity heatmaps for the Fungoida species](temp_gravity_genus_figures/Fungoida.png)

![Temperature and gravity heatmaps for the Osseus species](temp_gravity_genus_figures/Osseus.png)

![Temperature and gravity heatmaps for the Recepta species](temp_gravity_genus_figures/Recepta.png)

![Temperature and gravity heatmaps for the Stratum species](temp_gravity_genus_figures/Stratum.png)

![Temperature and gravity heatmaps for the Tubus species](temp_gravity_genus_figures/Tubus.png)

![Temperature and gravity heatmaps for the Tussock species](temp_gravity_genus_figures/Tussock.png)

### 5.7 Impact of the age, distance and star temperature rules

These figures show each rule over the bodies it applies to. The grey area is what
the rule removes.

![Impact of the star age rules](rule_impact_figures/star_age.png)

**Fonticulua Campestris moved from 12,000 to 10,000 My.** The figure shows the real
cliff near 9,500–10,000 My, which the earlier limit sat well past. On all known bodies
the limit now removes 15,319 wrong for 159 right, against 7,501 wrong for 76 right
before. Both halves of the sectors agree (8,526 and 6,793 wrong for 90 and 69 right).
The limit was read from the figure, not fitted. Osseus Discus has no such gap: below
8,500 My almost everything is wrong, and above it the right share climbs gradually, so
moving the limit up costs more right than it's worth.

![Impact of the star temperature rule](rule_impact_figures/star_temperature.png)

![Impact of the Clypeus Speculumi distance rule](rule_impact_figures/speculumi_distance.png)

**Clypeus Speculumi follows the parent chain, not the arrival star.** The limit stays
at 2,500 ls, and each option below measures from somewhere different, on all 6,557
right and 19,291 wrong predictions. Where an orbit on the way isn't on record, the
last two options use arrival distance.

| Measured from | Right lost | Wrong removed | Wrong left |
|---|---:|---:|---:|
| Arrival star (the earlier rule) | 86 | 15,018 | 4,273 |
| Dominant star under the light model, without the fallback | 258 | 17,246 | 2,045 |
| Dominant star under the light model | 23 | 17,205 | 2,086 |
| Orbit sizes added up the chain to the first star or binary (applied) | **8** | **19,213** | **78** |

Bodies with every orbit on record give the cleanest view: the chain distance loses 1
of 6,288 right and removes 18,721 of 18,758 wrong. The two halves of the sectors
agree (0 and 1 right lost; 10,388 of 10,410 and 8,333 of 8,348 wrong removed).

The dominant star does worse because it is often far away while the body's own star
is close. All 2,045 wrong it leaves are under 2,500 ls by the chain distance, and in
1,985 the dominant star is an ancestor (1,294) or a partner (691). The species
therefore follows a different star from the colours.
The limit was not fitted to this measure, but the comparison uses the same data
that suggested it. On complete bodies, Clypeus Speculumi precision rises from 62.0% to
99.6% and recall from 99.0% to 100%. Results are in
`output/speculumi_distance_v2_20261101`.

## 6. Orbital models compared

### 6.1 The two models

Both models pick the star delivering the most light to the body. They differ in
how they estimate it. This section defines both fully enough to reimplement them.
Code references are to `src/mgst/bio/star_model.py` unless another file is named.

| | SrvSurvey | Orbit-averaged |
|---|---|---|
| Score | $R^2T^4 / (D_b + D_s)$ | $L \cdot \langle 1/d^2 \rangle$ |
| Distance | Squared orbit sizes summed up both chains to the shared parent | Real positions over full Kepler orbits |
| Binary partners | Treated as unrelated, so placed too close | Share a phase, on opposite sides of the barycentre |
| Eccentricity and inclination | Ignored | Used |
| Candidates | Stars in the body's parent chain and the barycentres on it | The same (the comparison runs in 6.3–6.5 let every star compete; section 6.6) |
| Cost | A formula | Monte Carlo; 5,000 samples per orbit (same pick on 598 of 600 bodies against 200,000 samples) |

#### 6.1.1 Data

Each Spansh body row gives an id within its system (`bodyId`), a parent chain and,
for every body except the system root, an orbit about its immediate parent:

| Symbol | Column | Unit |
|---|---|---|
| $a$ | `semiMajorAxis` | AU |
| $e$ | `orbitalEccentricity` | – |
| $i$ | `orbitalInclination` | degrees |
| $\Omega$ | `ascendingNode` | degrees |
| $\omega$ | `argOfPeriapsis` | degrees |
| $P$ | `orbitalPeriod` | days (only used to detect binary partners) |
| $R$ | `solarRadius` (stars) | solar radii |
| $T$ | `surfaceTemperature` (stars) | K |

The chain is `parent_0_type`/`parent_0_id`, `parent_1_…`, up to the root, where
the type is `Star`, `Planet` or `Null` (a barycentre). Barycentres have rows of
type `Barycentre` with their own orbit but **no parent columns**; the root
barycentre, usually id 0, has no row. The rows also carry `meanAnomaly`, the
body's place at the moment it was scanned. The model doesn't use it, since a
time average doesn't depend on a snapshot, but it confirms the partner rule in
6.1.5.

Example, Blau Eur NI-T d3-40 (used throughout this section):

| Body | id | Type | Parents | $a$ (AU) | $e$ | $i$ | $\Omega$ | $\omega$ | $P$ (d) | `meanAnomaly` |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| A | 1 | Neutron star | Null 0 | 295.44 | 0.1809 | 127.04 | −31.01 | 149.90 | 6,155,140 | 265.12 |
| B | 2 | K star | Null 0 | 405.95 | 0.1809 | 127.04 | −31.01 | 329.90 | 6,155,140 | 265.12 |
| B 6 | 14 | Planet | Star 2, Null 0 | 3.755 | 0.0107 | −0.13 | 153.80 | 123.96 | 3,715.5 | 279.98 |
| barycentre 11 | 11 | Barycentre | (none on record) | 2.715 | 0.0151 | −3.63 | −61.89 | 65.53 | 2,283.7 | 53.48 |
| B 4 | 12 | Planet | Null 11, Star 2 | 0.001115 | 0.0234 | 2.27 | −119.35 | 16.37 | 254.29 | 310.17 |
| B 5 | 13 | Planet | Null 11, Star 2 | 0.001790 | 0.0234 | 2.27 | −119.35 | 196.37 | 254.29 | 310.15 |

#### 6.1.2 The quantity

A body's exobiology is fixed when the system is generated, so it cannot depend on
where the bodies happen to be at any moment. We take the light a body receives
from a star averaged over all time:

$$
F_s(b) \;=\; L_s \,\Big\langle \frac{1}{\lvert \mathbf r_b(t) - \mathbf r_s(t) \rvert^{2}} \Big\rangle_t \qquad (1)
$$

and choose the candidate with the largest flux:

$$
s^{*}(b) \;=\; \operatorname*{arg\,max}_{s \in C(b)} F_s(b). \qquad (2)
$$

The luminosity is bolometric, from the Stefan–Boltzmann law, in solar units:

$$
L = R^{2}\,(T / 5772\ \mathrm{K})^{4}. \qquad (3)
$$

`luminosity()` (lines 94–98). A star missing $R$ or $T$ gets $L = 0$ and can't win;
black holes are recorded at 0 K (or with no temperature), so they never win. Only ratios of $F$
between stars matter, so the units of $a$ and $R$ cancel. Absolute magnitude isn't
used.

#### 6.1.3 Rebuilding the orbit tree

The model needs each node's parent, including barycentres, which have none on
record. For every body with chain $(p_0, p_1, \dots, p_k)$, the sequence
$(b, p_0, p_1, \dots, p_k)$ gives the edges $b \to p_0$, $p_0 \to p_1$, and so on.
The parent of barycentre 11 above is therefore read off B 4's chain as star 2. The
first edge seen for a node is kept (`OrbitModel.__init__`, lines 131–136).
`_parents()` reads at most 10 levels (lines 184–191).

This step matters. Without it, a barycentre's own orbit is dropped and every star
beneath it is misplaced. An early version had this bug and placed Slegi's neutron
star at 10.8 AU instead of 23.9 AU (`STAR_MODEL.md`, section 3.3).

#### 6.1.4 Position of one orbit (Kepler)

The position of a node relative to its parent at mean anomaly $M$
(`orbit_position()`, lines 101–119) takes three steps.

**Kepler's equation.** Solve for the eccentric anomaly $E$:

$$
M = E - e \sin E. \qquad (4)
$$

The code uses Newton's method, taking exactly 30 steps on every sample at once.
It starts at $E_0 = M$ when $e \le 0.8$ and at $E_0 = \pi$ when $e > 0.8$:

$$
E_{n+1} = E_n - \frac{E_n - e\sin E_n - M}{1 - e\cos E_n}. \qquad (5)
$$

Eccentricity is capped at 0.999. Starting at $E_0 = M$, Newton's method fails to
converge for some $M$ once $e \ge 0.975$ (14 of 5,000 samples at 0.975, 126 at 0.99).
Those samples still land on the ellipse, but at the wrong times. Starting at
$E_0 = \pi$ converges for every $M$ and every $e < 1$. The residual of (4) is below
$10^{-9}$ at $e$ = 0.975, 0.99 and 0.999 (`tests/test_bio/test_species_rules.py`,
`TestKeplerOrbits`). Below $e = 0.975$ both starts reach the same root, so the
switch changes only orbits more eccentric than that. Until 2026-11-03 the code
always started at $M$. In every 25th sector (476 sectors, 22.1 million orbits),
1,399 orbits have $e \ge 0.975$, all of them planets. The stored predictions and the
results in this document come from that earlier code, so any change is confined
to bodies on those orbits.

**Position in the orbital plane.** With periapsis on the $x'$ axis:

$$
x' = a(\cos E - e), \qquad y' = a\sqrt{1-e^{2}}\,\sin E. \qquad (6)
$$

**Rotation into the system frame.** The standard 3-1-3 rotation
$\mathbf r = R_z(\Omega)\,R_x(i)\,R_z(\omega)\,(x', y', 0)^{\mathsf T}$, written out:

$$
\begin{aligned}
x &= (\cos\Omega\cos\omega - \sin\Omega\sin\omega\cos i)\,x' + (-\cos\Omega\sin\omega - \sin\Omega\cos\omega\cos i)\,y' \\
y &= (\sin\Omega\cos\omega + \cos\Omega\sin\omega\cos i)\,x' + (-\sin\Omega\sin\omega + \cos\Omega\cos\omega\cos i)\,y' \\
z &= (\sin\omega\sin i)\,x' + (\cos\omega\sin i)\,y'
\end{aligned} \qquad (7)
$$

Angles are converted from degrees. A missing $e$, $i$, $\Omega$ or $\omega$ is 0
(circular, in the reference plane), and a missing $a$ is 0 (the body sits on its
parent). One reference plane is assumed for the whole system. The game doesn't
document this, but it only matters when the orbits separating two bodies are of
similar size.

**Why sample in mean anomaly.** The mean anomaly advances uniformly in time:
$M(t) = n(t - \tau)$, with $n = 2\pi/P$. A uniform draw of $M$ on $[0, 2\pi)$ is a
uniform draw of time over one period. So equations (4)–(7) already place the body
where it spends its time: it lingers near apoapsis and races through periapsis
(Kepler's second law). No further weighting is needed.

#### 6.1.5 Phases: which orbits move together

A position in the system is the vector sum of orbits up the tree:

$$
\mathbf r_b(t) \;=\; \sum_{k \in \mathrm{path}(b \to \mathrm{root})} \boldsymbol\rho_k\big(M_k(t)\big), \qquad (8)
$$

where $\boldsymbol\rho_k$ is node $k$'s orbit about its parent, from equations
(4)–(7). The average in (1) then runs over the joint phases $(M_1, \dots, M_K)$.

**Unrelated orbits are independent.** If the periods are incommensurate, the phase
vector $(M_1(t), \dots, M_K(t)) \bmod 2\pi$ fills the torus $[0, 2\pi)^K$ uniformly
over long times (Weyl's equidistribution theorem). The time average in (1) then
equals an expectation over independent, uniform phases:

$$
\Big\langle f\big(\mathbf r(t)\big)\Big\rangle_t \;=\; \frac{1}{(2\pi)^{K}}\int_{[0,2\pi)^K} f\big(\mathbf r(M_1,\dots,M_K)\big)\,dM_1\cdots dM_K. \qquad (9)
$$

Resonances, which would break this, are ignored.

**Binary partners share one phase.** Two bodies orbiting their common barycentre
are one two-body orbit seen from two ends. With relative orbit
$\mathbf r_{\mathrm{rel}} = \mathbf r_B - \mathbf r_A$ and total mass $m = m_A + m_B$,

$$
\mathbf r_A = -\frac{m_B}{m}\,\mathbf r_{\mathrm{rel}}, \qquad \mathbf r_B = \frac{m_A}{m}\,\mathbf r_{\mathrm{rel}}. \qquad (10)
$$

So the partners have the same $P$, $e$, $i$, $\Omega$ and $M$; their arguments of
periapsis differ by exactly $180°$; and $a_A / a_B = m_B / m_A$. Spansh's data
follows this exactly. Blau Eur A and B above share $P$, $e$, $i$, $\Omega$ and
`meanAnomaly`, with $\omega$ = 149.90° and 329.90°, as do planets B 4 and B 5. So
giving partners the same $M$, while keeping each one's own $\omega$, keeps them on
opposite sides of their barycentre at every sample. The model doesn't impose
the 180°; it comes from the data.

In code (`_phase_key()`, lines 141–151): a node shares a phase with any other child
of the same parent whose period matches within 1 part in $10^6$. The shared key is
`("pair", parent, round(P, 3))`; every other node gets `("solo", bodyId)`. Each key
draws one array of $N$ uniform mean anomalies from a NumPy `default_rng(seed=0)`
generator the first time it is needed (`_mean_anomaly()`, lines 153–157). A planet
circling a binary (Slegi BC 2) has a different period from its stars, so it moves
independently of them, as it should.

#### 6.1.6 Estimator

Index $j = 1 \dots N$ labels one instant: one joint draw of every phase. Positions
are built recursively from the root, $\mathbf r_k = \boldsymbol\rho_k + \mathbf r_{\mathrm{parent}(k)}$,
with $\mathbf r_{\mathrm{root}} = \mathbf 0$. A node with no row, such as the root or
a barycentre missing from the data, adds no orbit of its own but passes on its
parent's position (`_position_of()`, lines 159–170). Each node's $3 \times N$
position array is computed once per model and cached.

The estimate of (1) is

$$
\widehat{F}_s(b) \;=\; L_s \cdot \frac{1}{N}\sum_{j=1}^{N} \frac{1}{\max\!\big(\lvert \mathbf r_b^{(j)} - \mathbf r_s^{(j)}\rvert^{2},\,10^{-30}\big)} \qquad (11)
$$

(`mean_inverse_square()` and `flux()`, lines 176–181). Orbits that the body and
the star share, such as the barycentre both sit under, add the *same cached array*
to both positions. They cancel exactly in the difference, so only the orbits that
separate the body from the star affect the result. $N = 5{,}000$
(`ORBIT_SAMPLES`, line 24); the analysis database was built with the same number.

The standard error of (11) is $\sigma(1/d^2) / \sqrt N$. For Blau Eur B 6 it is
0.02% of the mean for star B and 0.37% for star A. Across seeds 0–2 the winner's
share moved by at most 0.14 percentage points at $N = 5{,}000$ and 0.05 at
$N = 200{,}000$. The distribution of $1/d^2$ has a long tail when the separating
orbits are eccentric or approach each other, so convergence is slower there (see
6.1.9).

#### 6.1.7 Candidates and choice

$C(b)$ follows SrvSurvey's `getParentStars` (`SystemStars.parent_stars()`, lines
214–227). For each entry of the body's chain:

- `Star` $p$: star $p$.
- `Null` $p$ (a barycentre): every star whose own chain reaches $p$ through
  barycentres only (`_has_parent()`, lines 205–212). These are the stars under that
  barycentre, directly or through further barycentres. Stars with no chain on
  record are skipped here.

Stars without a `subType` are then dropped. With no candidates the body has no star.
With one, it is the answer. Otherwise the flux (11) is computed for each, and the
largest wins. If every flux is 0 (no radius or temperature on record), the first
candidate is used (`dominant_star()`, lines 230–242). The winner's class comes from
`star_class()` (lines 80–82): Spansh's `subType` is mapped to the journal star type,
then flattened as SrvSurvey does (`D*` → `D`, `W*` → `W`, `C*` → `C`, giants
`X_…` → `X`, `AeBe` → `Ae`).

For B 6, the candidates are B (its own star) and A (B's partner under barycentre 0).

#### 6.1.8 Where the code runs

- **Research engine.** `orbit_star()` in `srvsurv_engine.py` (line 597) applies
  the same rule. It builds one `OrbitModel` per system and reuses it for every body.
  `body_props()` (line 608) still requires SrvSurvey's own star to exist, so that a
  body SrvSurvey would skip is still skipped.
- **Search.** `SectorStars.score()` (`src/mgst/bio/sector_scoring.py`, lines
  94–128) scores each body with no star first. It runs the orbit model only when a
  predicted species is allowed around some of the body's candidate stars and not
  others. In that case it reads the whole system's orbit columns and calls
  `dominant_star()`, which builds a fresh `OrbitModel` (seed 0) for each body.
  Otherwise the answer can't change, and it records the first candidate's class.
  The `parent_star_class` written for such a body is not necessarily the dominant
  star's class.

Results are deterministic for a given call order. Different callers can draw
different samples for the same body, so a body sitting right at 50% can come out
differently in the engine and the search.

#### 6.1.9 Checks against exact results

The simulation was checked against cases with closed-form averages:

| Case | Exact $\langle 1/d^2 \rangle$ | Simulated | SrvSurvey's $1/(D_b + D_s)$ |
|---|---|---|---|
| One orbit, $a = 2$, $e = 0$ | $1/(a^2\sqrt{1-e^2})$ = 0.2500 | 0.2500 | 0.2500 |
| Same, $e = 0.7$ | 0.3501 | 0.3536 ($N$ = 5,000); 0.3505 ($N$ = 10⁶) | 0.2500 |
| Same, $e = 0.9$ | 0.5735 | 0.5944 ($N$ = 5,000); 0.5760 ($N$ = 10⁶) | 0.2500 |
| Planet at 3 around a binary, star at 1 from the centre (circular, coplanar) | $1/(a_p^2 - a_s^2)$ = 0.1250 | 0.1252 | 0.1000 |
| Planet at 1 around A; A and B at 5 and 5 from their barycentre | $1/(D^2 - a_p^2)$, $D = a_A + a_B$: 0.01010 | 0.01010 | 0.01961 |
| Same, A and B at 3 and 7 | 0.01010 | 0.01010 | 0.01695 |

The first formula follows from Kepler's second law: $r^2\dot\nu = h = na^2\sqrt{1-e^2}$,
so $\langle r^{-2}\rangle = \frac{1}{P}\oint \frac{d\nu}{h} = \frac{1}{a^2\sqrt{1-e^2}}$.
The others follow from averaging $d^2 = a_1^2 + a_2^2 - 2a_1a_2\cos\varphi$ over a
uniform relative angle $\varphi$, which gives $\langle d^{-2}\rangle = 1/\lvert a_1^2 - a_2^2\rvert$.

With independent phases for the partners instead of a shared one, the 5-and-5 case
gives 0.467, 46 times too high, because the stars then pass through each other.
The shared phase is essential.

**Blau Eur B 6**, from the real rows:

| Star | $L$ | $\langle 1/d^2\rangle^{-1/2}$ (AU) | SrvSurvey distance (AU) | Orbit share | SrvSurvey share |
|---|---:|---:|---:|---:|---:|
| B (own) | 0.0875 | 3.76 | 3.76 | **56.6%** | 40.5% |
| A (partner) | 2,300 | 695.9 | 502.1 | 43.4% | **59.5%** |

SrvSurvey's distance to A is $\sqrt{3.755^2 + 405.95^2 + 295.44^2}$ = 502.1 AU. The
simulated 695.9 AU can be checked by hand. B 6's orbit is tiny next to the binary,
so its distance to A is close to the A–B separation. That separation is an orbit
with $a = 295.44 + 405.95 = 701.39$ AU and $e = 0.1809$. By the first formula its
effective distance is $a(1-e^2)^{1/4}$ = 695.6 AU. The confirmed colours on B 6
follow B (section 6.4 lists similar cases).

#### 6.1.10 Known defects in the simulation

- **Heavy tails.** When the separating orbits cross, as with two coplanar orbits
  that intersect, the true $\langle 1/d^2\rangle$ diverges logarithmically. The
  estimate is then finite but dominated by a few samples. Bodies and their
  candidate stars are rarely arranged this way, and the $10^{-30}$ floor only
  prevents division by zero.
- **Partner detection by period.** A barycentre with three or more children of
  equal period would put them all on one phase. This hasn't been seen.
- **Bolometric luminosity.** If the game weights visible light, hot, small stars
  such as neutron stars and white dwarfs count for less than equation (3) gives.

#### 6.1.11 SrvSurvey's score, and its two flaws

SrvSurvey (`SystemBody.getRelativeBrightness`, ported as `relative_brightness()`
in `srvsurv_engine.py`, line 527) first finds the *common parent*: the first
parent of the body that is the star itself or appears in the star's own chain.
It then sums squared semi-major axes up each side to that parent:

$$
\mathrm{score}_s(b) = \frac{(R\,T^{2})^{2}}{D_b + D_s}, \qquad D_x = \sum_{k \in \mathrm{path}(x \to \mathrm{common})} a_k^{2}. \qquad (12)
$$

The common parent's own orbit is excluded (`_euclidian()`, line 517). For circular
orbits with independent phases, $D_b + D_s$ is exactly $\langle d^2\rangle$, because
every cross term $2\,\boldsymbol\rho_j\cdot\boldsymbol\rho_k$ averages to zero. So
the score is $L / \langle d^2\rangle$, where the model uses
$L\,\langle 1/d^2\rangle$. That gives two flaws.

**Flaw 1: partners placed too close.** The cross term between two binary partners
doesn't average to zero. By (10) they are always on opposite sides, so their
separation is $a_A + a_B$, and SrvSurvey uses $\sqrt{a_A^2 + a_B^2}$. The missing
$2a_Aa_B$ term overstates the partner's light by $(a_A + a_B)^2 / (a_A^2 + a_B^2)$:
up to 2× when $a_A = a_B$, and 1.9× in Blau Eur.

**Flaw 2: wrong average.** By Jensen's inequality, $1/\langle d^2\rangle \le \langle 1/d^2\rangle$,
with the gap growing as the orbits approach each other in size. For coplanar circular
orbits the exact value is $1/\lvert a_1^2 - a_2^2\rvert$, where SrvSurvey uses
$1/(a_1^2 + a_2^2)$. A planet circling a binary at three times the stars' distance
from the centre gets 20% too little light from each star. This flaw usually affects
both candidates in a similar way, so it changes fewer picks than Flaw 1.

### 6.2 Worked example: Kyloasly AA-Q d5-271 B 2 a

A moon of a planet of star B (an M dwarf), whose partner A is a neutron star.
B is 238.6 AU from the barycentre and A is 154.9 AU from it.

| | SrvSurvey | Orbit-averaged |
|---|---|---|
| Distance to A (the neutron star) | √(154.9² + 238.6²) = 284 AU | 391 AU |
| Light from A, L = 1,118 | 0.0138 | 0.0073 |
| Light from B, L = 0.0301, at 1.92 AU | 0.0081 | 0.0082 |
| **Pick** | **A, neutron (62.9%)** | **B, M dwarf (52.7%)** |

The two estimates of B's light agree. All of the change comes from placing A
correctly, and that alone reverses the choice.

### 6.3 Full-dataset comparison

The two models pick a different star on 11,728 bodies, a different class on
11,330, and produce different predictions on 8,943. That is 0.23% of predicted
bodies, so the overall scores barely move, but those bodies are nearly always
wrong under one model.

| Where predictions differ | SrvSurvey | Orbit-averaged |
|---|---:|---:|
| Deciding records (only this model is right) | 588 | **6,320** (91.5%) |
| Systems those records come from | 277 | **2,896** |
| Bodies where this model gets all records right and the other none | 208 | **2,360** |
| Complete bodies (1,391): species and colour recall | 22.8% | **95.8%** |
| Complete bodies (1,391): species and colour precision | 15.1% | **61.2%** |

The comparison report covers 523,485 complete bodies, slightly fewer than the
524,839 in section 1, because it needs at least one record with a known colour.

By species, the star matters most for Stratum Tectonicas (1,182 deciding records to 94)
and Bacterium Aurasus (732 to 61). Of the 64 species where the two stars differ, only
Electricae Pluma favours SrvSurvey (9 to 4).

![How often each star names the confirmed colour, by species](star_model_figures/species_colours.png)

Where the two stars have different classes and both predict the species (7,474 records),
SrvSurvey's star names the confirmed colour on 2,222 and the orbit-averaged star on 6,976.

The split between Clypeus Lacrimam and Clypeus Speculumi depends on distance rather than
on which star is chosen, since the two stars differ on only 172 of these bodies. Measuring
up the parent chain instead of from the arrival star separates them: at 2,500 ls, 106 of
19,396 Lacrimam bodies pass rather than 4,298, and 6,584 of 6,592 Speculumi bodies are kept.

![Arrival distance against parent-chain distance for Clypeus Lacrimam and Speculumi](star_model_figures/clypeus_distance.png)

By the class pair (SrvSurvey's star → orbit star), deciding records:

| Pair | SrvSurvey right | Orbit right | | Pair | SrvSurvey right | Orbit right |
|---|---:|---:|---|---|---:|---:|
| M → T | 0 | 985 | | G → M | 6 | 379 |
| N → M | 0 | 984 | | A → M | 0 | 277 |
| K → L | 10 | 781 | | N → K | 0 | 208 |
| M → L | 4 | 674 | | N → L | 1 | 198 |
| F → M | 14 | 665 | | A → K | 0 | 104 |
| K → M | 10 | 483 | | L → T | 7 | 101 |
| N → T | 0 | 88 | | **L → M** | **94** | **0** |
| | | | | **M → K** | **88** | **1** |

By the orbit star's share of the light:

| Share | SrvSurvey right | Orbit right |
|---|---:|---:|
| Under 50% | 90 | 616 |
| 50–60% | 366 | 5,142 |
| 60–70% | 99 | 562 |
| 70–80% | 21 | 0 |
| 90–100% | 12 | 0 |

The decisions are close, and most disputes sit near 50%. Above a 70% share the
orbit model has no wins in this set, though only 33 records are involved.

### 6.4 Where SrvSurvey fails and the orbit model is right

All bodies are complete. "New model" is the final engine. "SrvSurvey" uses its
own star and uncorrected criteria, and "—" means it doesn't predict the species.

Each star table lists the two deciding stars. The "average distance" is the
orbit model's, in AU.

#### Kyloasly AA-Q d5-271 B 2 a: neutron star → M dwarf

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| B (own) | M | 0.0301 | 37.1% | 1.92 | **52.7%** |
| A (partner) | Neutron | 1,118 | **62.9%** | 391.3 | 47.3% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Aleoida Arcus | Emerald | Ocher | Emerald |
| Bacterium Aurasus | Teal | Indigo | Teal |
| Stratum Paleas | Green | — | Green |
| Tubus Cavas | Teal | Amethyst | Teal |
| Tussock Propagito | Emerald | Yellow | Emerald |

#### Scheau Blou DA-L b49-0 B 6: M dwarf → T dwarf

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| B (own) | T | 0.00011 | 46.1% | 0.1105 | **57.0%** |
| A (partner) | M | 0.0127 | **53.9%** | 1.369 | 43.0% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Aleoida Gravis | Sage | Emerald | Sage |
| Bacterium Aurasus | Red | Teal | Red |
| Cactoida Cortexum | Orange | Amethyst | Orange |
| Osseus Cornibus | Emerald | — | Emerald |
| Stratum Tectonicas | Grey | Green | Grey |
| Tussock Triticum | Teal | Emerald | Teal |

#### Swoiwns FB-W d2-16 C 4: K dwarf → L dwarf

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| C (own) | L | 0.00156 | 41.7% | 0.532 | **51.0%** |
| B (partner) | K | 0.2234 | **58.3%** | 6.511 | 49.0% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Bacterium Aurasus | Sage | Green | Sage |
| Frutexa Metallicum | Teal | — | Teal |
| Stratum Tectonicas | Turquoise | Lime | Turquoise |
| Tubus Sororibus | Turquoise | Maroon | Turquoise |
| Tussock Ignis | Sage | Green | Sage |

#### Prooe Drye YZ-W d2-16 B 7 a: F star → M dwarf

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| B (own) | M | 0.0467 | 41.5% | 2.973 | **52.8%** |
| A (partner) | F | 3.935 | **58.2%** | 28.89 | 47.0% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Aleoida Arcus | Emerald | Teal | Emerald |
| Bacterium Aurasus | Teal | Lime | Teal |
| Frutexa Acus | Grey | Green | Grey |
| Stratum Paleas | Green | Emerald | Green |
| Tubus Compagibus | Teal | Grey | Teal |
| Tussock Albata | Emerald | Yellow | Emerald |

#### Byooe Auscs LT-M c9-0 B 8: M dwarf → L dwarf

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| B (own) | L | 0.00059 | 45.9% | 0.278 | **56.6%** |
| A (partner) | M | 0.0556 | **54.0%** | 3.076 | 43.4% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Aleoida Arcus | Lime | Emerald | Lime |
| Bacterium Aurasus | Sage | Teal | Sage |
| Frutexa Metallicum | Teal | Grey | Teal |
| Stratum Tectonicas | Turquoise | Green | Turquoise |
| Tubus Sororibus | Turquoise | Teal | Turquoise |
| Tussock Albata | Sage | Emerald | Sage |

#### Scaulou AA-A g1656 A 2 d: neutron star → T dwarf

A planet of a T dwarf that orbits a black hole, with a neutron star as the black
hole's partner. The black hole gives no light in the model.

| Star | Class | L | SrvSurvey share | Avg distance | Orbit share |
|---|---|---:|---:|---:|---:|
| A 2 (own) | T | 0.0000135 | 47.9% | 0.0297 | **55.5%** |
| B | Neutron | 958 | **52.1%** | 280.6 | 44.5% |

| Species | Confirmed | SrvSurvey | New model |
|---|---|---|---|
| Aleoida Spica | Sage | Ocher | Sage |
| Bacterium Alcyoneum | Red | Indigo | Red |
| Stratum Laminamus | Grey | — | Grey |
| Tubus Rosarium | Mauve | Amethyst | Mauve |
| Tussock Cultro | Teal | Yellow | Teal |

#### Six more

In each of these, the new model gets every confirmed species and colour right,
and SrvSurvey gets none right.

| Body | SrvSurvey star → orbit star | Orbit share | Confirmed | SrvSurvey's prediction |
|---|---|---:|---|---|
| Wregoe RP-M d8-0 B 7 | G → M | 57.6% | Aurasus Teal, Metallicum Grey, Tectonicas Green, Sororibus Teal, Serrati Emerald | Emerald, Emerald, —, Red, Lime |
| Byoo Eurk JR-N d6-12 B 4 | A → M | 52.6% | Aurasus Teal, Cortexum Amethyst, Margaritus Turquoise, Metallicum Grey, Tectonicas Green | Yellow, Green, Orange, —, — |
| Oochonz GK-Q c21-0 C 5 | K → M | 54.3% | Coronamus Emerald, Aurasus Teal, Pullulanta Amethyst, Metallicum Grey, Tectonicas Green, Caputus Emerald | Turquoise, Green, —, —, Lime, Green |
| Drojuae LJ-O d7-0 B 1 f | N → K | 52.0% | Aurasus Green, Excutitus Lime, Compagibus Maroon, Ignis Green | Indigo, —, Amethyst, Yellow |
| Cyookuae JC-V d2-1 B 2 i | A → K | 58.6% | Aurasus Green, Paleas Lime, Cavas Maroon, Propagito Green | Yellow, —, Indigo, — |
| Nyeajaae VK-L b41-7 C 5 | L → T | 59.3% | Aurasus Red, Cortexum Orange, Fractus Emerald, Tectonicas Grey, Caputus Teal | Sage, Mauve, —, Turquoise, Sage |

### 6.5 Where the orbit model fails and SrvSurvey is right

The 588 records where SrvSurvey wins aren't noise. They come from a pattern.

| Situation where the models disagree | SrvSurvey right | Orbit right |
|---|---:|---:|
| Only the orbit model picks the star the body orbits | 28 | **5,317** |
| Only SrvSurvey picks the star the body orbits | **451** | 3 |
| Body orbits a barycentre (no own star) | 84 | **984** |
| Other | 25 | 16 |

Here, the star a body directly orbits was right 99.5% of the time when just one
model picked it. Most of the orbit model's failures are cases where a brighter
partner takes a majority of the light, yet the colours follow the body's own
star:

| Body | Own star | Orbit picks | Orbit share | Confirmed | Orbit colours |
|---|---|---|---:|---|---|
| Dryooe Prou NK-Y c1-178 B 9 | B, M | A, K | 60.8% | Aurasus Teal, Metallicum Grey, Tectonicas Green, Sororibus Teal, Propagito Emerald | Green, —, Lime, Maroon, Green |
| Lysooch MM-Z c27-7 B 7 | B, M | A, K | 61.1% | Alcyoneum Teal, Tectonicas Green | Green, Lime |
| Pha Airgh KO-S b17-1 B 5 | B, L | A, M | 59.9% | Aurasus Sage, Metallicum Teal, Tectonicas Turquoise, Sororibus Turquoise, Pennatis Sage | Teal, Grey, Green, Teal, Emerald |
| Dryooe Prou UF-V c16-40 B 5 | B, M | A, G | 56.3% | Cerbrus Teal, Tectonicas Green | Emerald, — |

One failure has no own star. Sifi IU-C b40-2 BC 4 orbits a barycentre, and the
models are nearly tied (M 50.3% against L 47.9%). The confirmed colours are the L
dwarf's.

This looked like the game favouring the body's own star, but section 6.6 shows that
only holds among these disagreements.

### 6.6 Is it just the parent hierarchy?

No. Brightness still decides, across both parents and companions. The one part of
the hierarchy idea that holds is that stars with no link to the body's chain can be
ignored.

Each star was classed by how it relates to the body, walking up its parents and
skipping planets. A barycentre shared by two planets holds no stars, so the walk
continues past it.

| Relation | Meaning |
|---|---|
| Near | The first star above the body, or the stars under a barycentre reached before any star |
| Ancestor | A star higher up the chain, which the body's own star orbits |
| Partner | Under a barycentre reached after a star, so a companion of a star above the body |
| Outside | None of these |

Rules were scored on all 165,458 confirmed records on the 63,866 bodies where a star
choice changes a prediction, counted against the orbit model:

| Stars allowed to compete | Records right | Gains | Losses |
|---|---:|---:|---:|
| All (orbit model) | 160,178 | | |
| All except outside | **160,195** | **17** | **0** |
| Near and ancestors | 123,047 | 2,027 | 39,158 |
| Near and partners | 84,645 | 102 | 75,635 |
| Near only | 47,341 | 2,086 | 114,923 |

The near star wins 99.3–99.5% of the time when the two light models disagree over it
(section 6.5). That was a selected group, though. When both models pick a star other
than the near one, that star wins 114,901 to 1,648, and most of those are bodies
orbiting Y dwarfs that themselves orbit F, G, A or K stars.

Requiring extra light before overruling the near star doesn't help. Any margin loses
more than it gains (1.1× gains 379 and loses 735; 2× gains 1,248 and loses 5,060), and
a margin fitted on either half of the sectors comes out at none.

By how much the brightest star outshines the near star, in records:

| Light over the near star | Ancestor right | Near right | Partner right | Near right |
|---:|---:|---:|---:|---:|
| 1–1.5× | 62 | 30 | 3,071 | 942 |
| 1.5–2× | 54 | 6 | 1,873 | 268 |
| 2–3× | 203 | 3 | 1,996 | 161 |
| 3–5× | 396 | 0 | 2,063 | 153 |
| 5–10× | 1,113 | 2 | 2,525 | 162 |
| 10–100× | 15,339 | 4 | 10,981 | 229 |
| 100× or more | 58,571 | 40 | 16,676 | 74 |

Where a partner only just outshines the near star the outcome is less certain, at 76.5%
in the closest band, but the brighter star still wins in every band. Those 942 records
are the orbit model's largest remaining star error, and no rule tested here separates
them. Among stars sharing a barycentre, the two brightness formulas differ on only 33
records, all for SrvSurvey's.

So the dominant star is the brightest under the orbit-averaged model, among stars in
the body's chain and the barycentres on it. The engine now excludes outside stars, which
moves the star class on 40 of 3,880,306 bodies. The comparison in sections 6.3–6.5 and
the database's stored predictions still let every star compete, and the final score
applies the difference. Results are in
`output/star_parent_rule_score_20261101` and `output/star_relation_score_20261101`.

## 7. Remaining limits and failures

### 7.1 Precision stops near 72%

Most wrong predictions are species that fit a body's rules but aren't there.
Their genus is often on the body as a different species. Rules alone can't
separate them, and machine-learned trees don't do it either.

| Method (complete bodies) | Species P | Species R | Species wrong | Species missed |
|---|---|---|---:|---:|
| SrvSurvey rules, orbit star | 68.83% | 99.31% | 497,560 | 7,615 |
| Rules + tree vetoes | 74.94% | 99.18% | 366,893 | 9,078 |
| Trees alone, at the same recall | 54.92% | 99.31% | 901,832 | 7,654 |
| Trees alone, p ≥ 0.5 | 84.90% | 76.50% | 150,521 | 259,971 |
| Trees + FSS signal count, p ≥ 0.5 | 87.14% | 80.21% | 130,987 | 218,993 |

These come from an earlier run that counts a slightly wider species list than
section 1, so compare them only with each other. Scores are out of fold, grouped
by sector.

The rules beat trees at matching recall. Trees reach higher precision only by
giving up a quarter of the real species, so they suit a shortlist ("most likely
to be right") rather than a complete prediction.

Final results by genus, on complete bodies:

| Genus | Right | Wrong | Missed | Precision | Recall |
|---|---:|---:|---:|---:|---:|
| Aleoida | 17,680 | 29,976 | 77 | 37.1% | 99.6% |
| Bacterium | 481,079 | 114,919 | 1,008 | 80.7% | 99.8% |
| Cactoida | 22,807 | 6,852 | 77 | 76.9% | 99.7% |
| Clypeus | 9,379 | 1,550 | 2 | 85.8% | 100.0% |
| Concha | 34,046 | 89,471 | 218 | 27.6% | 99.4% |
| Electricae | 7,029 | 2,712 | 162 | 72.2% | 97.7% |
| Fonticulua | 90,385 | 38,201 | 202 | 70.3% | 99.8% |
| Frutexa | 59,317 | 1,646 | 53 | 97.3% | 99.9% |
| Fumerola | 5,346 | 21 | 44 | 99.6% | 99.2% |
| Fungoida | 55,133 | 11,501 | 385 | 82.7% | 99.3% |
| Osseus | 42,957 | 5,050 | 154 | 89.5% | 99.6% |
| Recepta | 7,242 | 12,158 | 102 | 37.3% | 98.6% |
| Stratum | 130,182 | 80,207 | 1,077 | 61.9% | 99.2% |
| Tubus | 37,805 | 22,758 | 205 | 62.4% | 99.5% |
| Tussock | 106,961 | 3,619 | 126 | 96.7% | 99.9% |

The largest sources of wrong predictions are Concha Labiata (50,914), Bacterium Tela (38,874), Stratum Tectonicas (38,244), Bacterium Cerbrus (30,442) and Concha Aureolas (26,905).

The largest remaining misses are Stratum Tectonicas (476), Bacterium Tela (412), Stratum Excutitus (398), Fungoida Setisis (346) and Bacterium Verrata (171). Some of these are the known cost of the limits in section 5.6.
We haven't investigated the Excutitus, Verrata or Radialem misses.

### 7.2 Regional coin flips

Two pairs of species have nearly the same rules, and their region lists overlap
only in region 1 (Galactic Centre). There, each species appears about half the
time, and position doesn't separate them (49–52%).

| Pair | Bodies |
|---|---:|
| Tussock Propagito / Tussock Pennatis | 9,329 |
| Frutexa Acus / Frutexa Fera | 5,243 |

The rules can't choose between them, so about half of these predictions are wrong.

### 7.3 Material colours with two candidates

These 13 species get a colour from materials, but the materials leave two
candidates, and each is right about half the time (50.1% across 192,899 records):

Bacterium Omentum, Scopulum, Tela and Verrata; Concha Renibus; Fumerola Aquatis,
Carbosis, Extremus and Nitris; Fungoida Gelata and Stabitis; Osseus Discus;
Recepta Deltahedronix.

The pairs involve cadmium, mercury, molybdenum, niobium, tin and tungsten. Trying
to separate them:

| Test | Result |
|---|---|
| Material percentages | No help |
| Body ID | No help |
| Another species on the same body | 50.2% agreement across 69,798 pairs |
| Pairs in the same system | 36.7% across 965 pairs; unexplained, not explored further |

The engine predicts both candidates, which costs colour precision but keeps
recall. The cause remains unknown. The 36.7% result is the one lead, and it's
based on few pairs.

### 7.4 Weak age and other soft patterns

Some patterns are real but not clean enough for a rule. For example, Osseus
Cornibus above a 9,500 Myr star age is wrong 2,611 times and right 787 times.
Osseus Pellebantus there is wrong 1,407 times and right 562 times. Neither passes
the per-species age test in section 5.5, which also lists two that pass but aren't
applied. The gradual gravity and pressure pattern in Concha Labiata (section 5.6)
is another.

### 7.5 Coverage

We have less evidence for some species and colours. Of 846 species and colour
combinations:

| Confirmed records | Combinations |
|---:|---:|
| 1,000 or more | 343 |
| 100–999 | 202 |
| 30–99 | 65 |
| 1–29 | 176 |
| 0 | 60 |

So 236 combinations have too few records to test. Species with no records on
database bodies have tables but no figures in Appendix A.

### 7.6 Records with a missing colour

Every life form should have a recorded colour or form, so a blank is a recording
gap. **20,992 of 2,823,646 records (0.74%) in the 15 main genera have no colour in
Spansh.** They are spread evenly, at no more than 1.3% for any species. Another 75
records list several colours for one species on one body.

Both kinds count towards species scores but are left out of colour scores, so
colour recall is measured only on the records that have a colour. A prediction
matching a blank record counts as wrong in colour precision, which lowers it
slightly.

### 7.7 Airless life forms (scored separately)

Our Spansh walk never requested these life forms. The download built names as
genus + species, giving "Brain Trees Roseum Brain Tree", which matches nothing
(`scripts/download_spansh_colors.py`). Only 41 of the database's 586,237 airless
bodies were walked. The 432 records we have are incidental finds on bodies walked
for other species, and nearly all are on bodies with an atmosphere, which the rules
don't allow.

The engine also never predicted them until 2026-10-31. SrvSurvey files them with
an empty variant (`""`), and our port treated that as missing. This is fixed, but
the database's stored predictions and the section 1 scores still exclude them.

So species-level scores here mean little (3 right, 18 wrong and 273 missed on
complete bodies). FSS genus lists do cover airless bodies, and they give a fair
test of the rules at genus level:

| Life form | Bodies listed | Right | Wrong | Missed | Precision | Recall |
|---|---:|---:|---:|---:|---:|---:|
| Amphora Plant | 732 | 728 | 235 | 4 | 75.6% | 99.5% |
| Anemone | 17,426 | 16,712 | 11,057 | 714 | 60.2% | 95.9% |
| Bark Mounds | 8,165 | 6,934 | 1,447 | 1,231 | 82.7% | 84.9% |
| Brain Tree | 18,719 | 17,500 | 263 | 1,219 | 98.5% | 93.5% |
| Crystalline Shards | 6,036 | 4,873 | 4 | 1,163 | 99.9% | 80.7% |
| Sinuous Tubers | 7,110 | 5,970 | 15,781 | 1,140 | 27.4% | 84.0% |

Their recall is well below the main genera's, so these rules are the weakest part
of the engine. We can't yet say whether individual forms are predicted correctly,
which colour a body shows, or what the Appendix A figures would look like with
fair data. The codex lists Bark Mounds, Amphora Plant and Crystalline Shards as a
single form, so they have no colour to predict unless other evidence appears.

**The fix** is a new walk requesting the codex names (Roseum Brain Tree) and a
re-score including them. Output: `output/airless_species_score_final_20261031/`.

### 7.8 Risks in the evidence

- **No holdout for the corrections.** The 12 vetoes and the region changes were
  read from the same data that scores them. They are clean enough (166,720 wrong
  against 136 right) that overfitting isn't a major concern, but small thresholds
  could shift with new data.
- **Biased sample.** Complete bodies are those players fully scanned, and they
  may not represent the galaxy.
- **Unknown predictions can't be scored.** 123,880 veto removals and 46,422
  region 9 removals are on bodies with no truth available. We assume they behave
  like the scored ones.
- **The game's rule is unknown.** The orbit model is physical reasoning that
  fits the data. It may not be how the game decides, though section 6.6 finds no simpler rule that does better.
- **A deliberate deviation.** Herbig stars are reported as `AeBe`, but the
  criteria test `Ae`, so SrvSurvey never matches them. The engine maps one to the
  other, and this is counted in all our scores, including "SrvSurvey as is".
- **Monte Carlo noise.** Picks can vary very slightly near a 50% share. We saw 2
  changes out of 600 between 5,000 and 200,000 samples.

### 7.9 Lists of failures and improvements

Each row is one species on one body. All four files are in
`output/rule_corrections_score_final_v2_20261101/lists/` and cover every body
where the truth is known. Filter `complete` to `True` to match section 1.

| File | Rows | On complete bodies | What a row is |
|---|---:|---:|---|
| `failures_false_predictions.tsv` (270 MB) | 2,423,596 | 420,641 | Predicted, but definitely absent |
| `failures_missed.tsv` | 9,260 | 3,892 | Confirmed, but not predicted |
| `failures_wrong_colour.tsv` | 5,237 | 889 | Species predicted, confirmed colour not among those predicted |
| `improvements.tsv` | 389,581 | 80,394 | Right where SrvSurvey as is was wrong |

The improvements are 364,959 false predictions removed, 19,380 species now
predicted and 5,242 colours fixed.

Columns are `system`, `body`, `species`, `kind`, `confirmed_colour`,
`predicted_colours`, `srvsurvey_colours`, `srvsurvey_right`, `complete`,
`engine_star`, `srvsurvey_star` and `cause`. `cause` says only whether a star or
rule change made us differ from SrvSurvey, not why a prediction failed.

The failure lists include our regressions. Of the 9,260 missed, 3,926 differ from
SrvSurvey because of our changes, 3,815 of them through a rule; these are the cost of
the vetoes and limits. Of the 5,237 wrong colours, 545 come from our changes, 495 of
those involving the star.

## 8. How to fix SrvSurvey

| # | Problem | Fix | Effect (complete bodies unless noted) |
|---|---|---|---|
| 1 | Tussock regions missing | Add 8, 16, 17, 35 to the seven species | 18,291 right, 14 wrong (all known bodies) |
| 2 | Tussock region 9 wrong | Remove 9 from the seven species | 15,443 wrong removed, 0 right lost (all known bodies) |
| 3 | 14 species too loose | Add the thresholds in section 5.2 | 187,417 wrong removed, 312 right lost (all known bodies) |
| 3a | 31 species past sharp edges | Add the limits in section 5.6 | 158,241 wrong removed, 3,481 right lost (all known bodies) |
| 4 | Binary partners too close | Use `(a_A + a_B)²` for binary partners, or the full orbit average | Colour recall 99.34% to 99.57% with fixes 1–3a |
| 5 | Herbig stars never match | Map `AeBe` to `Ae` | Already in all scores here |

In our own tooling, the airless walk and re-score in section 7.7 remain to be done.

Fixes 1–3a are edits to the criteria data. Fix 4 changes `getRelativeBrightness`.
The one-line partner distance is a simpler option, but it's untested. Only the
full orbit average has been measured.

## 9. Reproducing this

```bash
# Final scores and per-species table
micromamba run -n mgst python -I experiments/bio_rule_optimization/score_rule_corrections.py \
  --output output/<new_dir> --workers 18

# Star comparison
micromamba run -n mgst python -I experiments/bio_rule_optimization/star_model_verdict.py --output output/<new_dir>

# Star comparison by species, with figure (needs matplotlib, in the mgst-plot environment)
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/star_model_species_figure.py --output output/<new_dir>

# Colours by species and the Clypeus Lacrimam and Speculumi split, with figures
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/star_model_discrimination_figures.py --output output/<new_dir>

# Airless life forms
micromamba run -n mgst python -I experiments/bio_rule_optimization/score_airless_species.py --output output/<new_dir> --workers 18

# Species tables and figures (needs matplotlib, in the mgst-plot environment); run after the heatmaps below, passing their folder as --heatmaps
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/species_reference.py \
  --per-species output/rule_corrections_score_final_v2_20261101/per_species.tsv --output output/<new_dir>

# Star age test for every species
micromamba run -n mgst python -I experiments/bio_rule_optimization/star_age_rules.py --output output/<new_dir>

# Star age by dominant star class (analysis needs scikit-learn, in mgst; figures need matplotlib, in mgst-plot)
micromamba run -n mgst python -I experiments/bio_rule_optimization/star_age_by_class.py --output output/<new_dir>
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/star_age_by_class_figures.py \
  --input output/<new_dir> --output output/<new_figures_dir>

# Temperature and gravity by body type, atmosphere, mix, pressure and volcanism, with figures; --after-limits starts from the current limits
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/temp_gravity_by_group.py --after-limits --output output/<new_dir>

# Temperature and gravity test for every species, with figures
micromamba run -n mgst-plot python -I experiments/bio_rule_optimization/temp_gravity_rules.py --output output/<new_dir>
```

Tests: `tests/test_bio/test_engine_corrections.py`.

## Appendix A: species reference

One entry per species in the corrected engine. Each ruleset row is one way to qualify; every filled column in the row must hold, and an empty cell means no limit. Gravity is in Spansh g and pressure in atm, converted from the journal units in SrvSurvey's files. Regions are numbers from SrvSurvey's region list, with corrections included.

The figures show P(value | group), so each group's bars add up to 1: how bodies in that group are spread, not the chance of finding the species at a value. The blue group is every body with the species confirmed. The orange group is bodies the corrected rules predict that certainly lack it, shown only with at least 30 bodies. Values past the plotted range are piled into the end bins. Both groups come from bodies Spansh has scanned, which may not represent the whole galaxy.

### Aleoida

#### Aleoida Arcus

On complete bodies with the final rules: precision 44.9%, recall 99.9% (4,763 right, 5,836 wrong, 5 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 175 – 180 | ≤ 0.2427 | ≥ 0.01589 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 175 – 180 | ≤ 0.2814 | ≥ 0.01589 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: Y | 5 |
| Emerald | star: M | 1,620 |
| Green | star: A; body: Rocky body | 1,571 |
| Grey | star: W | 1 |
| Indigo | star: D | 22 |
| Lime | star: L | 198 |
| Mauve | star: TTS | 42 |
| Ocher | star: N | 444 |
| Sage | star: T | 62 |
| Teal | star: F | 6,573 |
| Turquoise | star: K | 7,775 |
| Yellow | star: B | 11 |

![Temperature, gravity and atmosphere distributions for Aleoida Arcus](species_figures/Aleoida_Arcus.png)

![Right share across temperature and gravity for Aleoida Arcus](temp_gravity_figures/Aleoida_Arcus.png)

#### Aleoida Coronamus

On complete bodies with the final rules: precision 40.7%, recall 99.8% (4,637 right, 6,746 wrong, 8 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 190 | ≤ 0.2427 | ≥ 0.02408 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 190 | ≤ 0.2814 | ≥ 0.02408 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: Y | 11 |
| Emerald | star: M | 2,477 |
| Green | star: A; body: Rocky body | 2,062 |
| Grey | star: W | 0 |
| Indigo | star: D | 17 |
| Lime | star: L | 347 |
| Mauve | star: TTS | 61 |
| Ocher | star: N | 623 |
| Sage | star: T | 113 |
| Teal | star: F | 8,714 |
| Turquoise | star: K | 9,714 |
| Yellow | star: B | 19 |

![Temperature, gravity and atmosphere distributions for Aleoida Coronamus](species_figures/Aleoida_Coronamus.png)

![Right share across temperature and gravity for Aleoida Coronamus](temp_gravity_figures/Aleoida_Coronamus.png)

#### Aleoida Gravis

On complete bodies with the final rules: precision 53.6%, recall 98.7% (1,982 right, 1,717 wrong, 26 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 190 – 197 | ≤ 0.2427 | ≥ 0.05369 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 190 – 197 | ≤ 0.2814 | ≥ 0.05369 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: Y | 3 |
| Emerald | star: M | 1,849 |
| Green | star: A; body: Rocky body | 715 |
| Grey | star: W | 0 |
| Indigo | star: D | 4 |
| Lime | star: L | 627 |
| Mauve | star: TTS | 21 |
| Ocher | star: N | 278 |
| Sage | star: T | 152 |
| Teal | star: F | 2,820 |
| Turquoise | star: K | 3,771 |
| Yellow | star: B | 6 |

![Temperature, gravity and atmosphere distributions for Aleoida Gravis](species_figures/Aleoida_Gravis.png)

![Right share across temperature and gravity for Aleoida Gravis](temp_gravity_figures/Aleoida_Gravis.png)

#### Aleoida Laminiae

On complete bodies with the final rules: precision 24.7%, recall 99.7% (4,498 right, 13,732 wrong, 15 missed).

| Body | Atmosphere | Composition | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 |  | None, Minor Water Magma | 1, 4, 7, 8, 9, 16, 17, 18, 19, 20, 21, 22, 23, 35, 40 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | ≤ 0.007895 | None | 1, 4, 7, 8, 9, 16, 17, 18, 19, 20, 21, 22, 23, 35, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: Y | 3 |
| Emerald | star: M | 2,794 |
| Green | star: A; body: Rocky body | 2,376 |
| Grey | star: W | 1 |
| Indigo | star: D | 8 |
| Lime | star: L | 512 |
| Mauve | star: TTS | 46 |
| Ocher | star: N | 475 |
| Sage | star: T | 127 |
| Teal | star: F | 6,942 |
| Turquoise | star: K | 6,597 |
| Yellow | star: B | 22 |

![Temperature, gravity and atmosphere distributions for Aleoida Laminiae](species_figures/Aleoida_Laminiae.png)

![Right share across temperature and gravity for Aleoida Laminiae](temp_gravity_figures/Aleoida_Laminiae.png)

#### Aleoida Spica

**Temperature and gravity limits:** dropped on High metal content world: above 175.81 K or below 0.05175 g or above 0.27439 g.

On complete bodies with the final rules: precision 48.1%, recall 98.7% (1,800 right, 1,945 wrong, 23 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 170 | ≤ 0.2814 | None, Minor Metallic Magma, Minor Water Magma, Minor Silicate Vapour Geysers, Major Silicate Vapour Geysers | 7, 8, 9, 16, 17, 18, 19, 20, 21, 22, 23, 35, 40 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 170 | ≤ 0.2814 | None | 7, 8, 9, 16, 17, 18, 19, 20, 21, 22, 23, 35, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: Y | 2 |
| Emerald | star: M | 2,075 |
| Green | star: A; body: Rocky body | 1,239 |
| Grey | star: W | 0 |
| Indigo | star: D | 3 |
| Lime | star: L | 703 |
| Mauve | star: TTS | 24 |
| Ocher | star: N | 319 |
| Sage | star: T | 174 |
| Teal | star: F | 3,546 |
| Turquoise | star: K | 3,724 |
| Yellow | star: B | 20 |

![Temperature, gravity and atmosphere distributions for Aleoida Spica](species_figures/Aleoida_Spica.png)

![Right share across temperature and gravity for Aleoida Spica](temp_gravity_figures/Aleoida_Spica.png)

### Amphora

#### Amphora Plant

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Metal rich body | None | 1000 – 1800 | ≥ 0.04079 | ≤ 0 | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers | 10, 19, 20, 21, 22 |

The codex has one form of this life form. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

### Anemone

#### Blatteum Bioluminescent Anemone

| Body | Atmosphere | Temp (K) | Volcanism | Regions | Other |
|---|---|---|---|---|---|
| High metal content | None | 220 – 999 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: O, B, K, N, H; |
| Metal rich body | None | 800 – 6000 | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Rocky Magma, Silicate Vapour Geysers | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: O, B, K, N, H; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Blatteum Bioluminescent Anemone](species_figures/Blatteum_Bioluminescent_Anemone.png)

#### Croceum Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 440 | ≤ 0.4283 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: B, A; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Croceum Anemone](species_figures/Croceum_Anemone.png)

#### Luteolum Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 440 | ≤ 1.632 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Rocky Magma | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: B, A; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Luteolum Anemone](species_figures/Luteolum_Anemone.png)

#### Prasinum Bioluminescent Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Other |
|---|---|---|---|---|---|
| Rocky body | None | 110 – 999 | ≤ 1.937 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | primaryStar: O, B, A, Ae, W; |
| High metal content | None | 250 – 2400 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers | primaryStar: O, B, A, Ae, W; |
| Metal rich body | None | 1000 – 4000 |  | None, Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Silicate Vapour Geysers | primaryStar: O, B, A, Ae, W; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Prasinum Bioluminescent Anemone](species_figures/Prasinum_Bioluminescent_Anemone.png)

#### Puniceum Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Icy body | None | 75 – 800 | 0.102 – 2.651 | None | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: O, W; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Puniceum Anemone](species_figures/Puniceum_Anemone.png)

#### Roseum Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 440 | ≤ 0.3936 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | 7, 8, 9, 13, 14, 15, 16, 17, 18, 27, 31 | primaryStar: B; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Roseum Anemone](species_figures/Roseum_Anemone.png)

#### Roseum Bioluminescent Anemone

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Other |
|---|---|---|---|---|---|
| High metal content | None | 400 – 999 | ≥ 0.2447 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | primaryStar: B, A, H; |
| Metal rich body | None | 1000 – 5000 |  | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Rocky Magma, Silicate Vapour Geysers | primaryStar: B, A, H; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Roseum Bioluminescent Anemone](species_figures/Roseum_Bioluminescent_Anemone.png)

#### Rubeum Bioluminescent Anemone

| Body | Atmosphere | Temp (K) | Volcanism | Other |
|---|---|---|---|---|
| High metal content | None | 164 – 999 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | primaryStar: O, B, A, F, N, H; |
| Metal rich body | None | 1000 – 1800 | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Rocky Magma, Silicate Vapour Geysers | primaryStar: O, B, A, F, N, H; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Rubeum Bioluminescent Anemone](species_figures/Rubeum_Bioluminescent_Anemone.png)

### Bacterium

#### Bacterium Acies

On complete bodies with the final rules: precision 82.9%, recall 100.0% (54,419 right, 11,237 wrong, 26 missed).

| Body | Atmosphere | Composition | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Neon | Neon ≥ 100% or Nitrogen ≥ 46% or Helium ≥ 0.11% | ≤ 0.622 | ≤ 0.008389 | None, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Water Magma |
| Rocky ice | Neon | Neon ≥ 100% or Nitrogen ≥ 48% or Helium ≥ 1% | ≤ 0.622 | ≤ 0.004935 | None, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Yttrium | 16,634 |
| Cobalt | mats: Ruthenium | 13,432 |
| Cyan | mats: Antimony | 11,571 |
| Lime | mats: Technetium | 10,293 |
| Magenta | mats: Polonium | 7,801 |
| White | mats: Tellurium | 14,559 |

![Temperature, gravity and atmosphere distributions for Bacterium Acies](species_figures/Bacterium_Acies.png)

![Right share across temperature and gravity for Bacterium Acies](temp_gravity_figures/Bacterium_Acies.png)

#### Bacterium Alcyoneum

On complete bodies with the final rules: precision 96.9%, recall 100.0% (43,412 right, 1,404 wrong, 20 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% or SulphurDioxide ≥ 0.4% |  | ≤ 0.622 | None, Major Silicate Vapour Geysers, Minor Water Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Water Magma |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.622 | None |
| High metal content | Ammonia | Ammonia ≥ 100% or SulphurDioxide ≥ 0.6% |  | ≤ 0.622 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: W | 2 |
| Emerald | star: G | 21,262 |
| Green | star: K | 32,143 |
| Grey | star: B | 89 |
| Indigo | star: N | 2,110 |
| Lime | star: F | 29,055 |
| Maroon | star: TTS | 382 |
| Mauve | star: Y | 39 |
| Ocher | star: D | 42 |
| Orange | star: Ae | 1 |
| Red | star: T | 2,345 |
| Sage | star: L | 8,413 |
| Teal | star: M | 20,969 |
| Turquoise | star: O | 4 |
| Yellow | star: A | 8,672 |

![Temperature, gravity and atmosphere distributions for Bacterium Alcyoneum](species_figures/Bacterium_Alcyoneum.png)

![Right share across temperature and gravity for Bacterium Alcyoneum](temp_gravity_figures/Bacterium_Alcyoneum.png)

#### Bacterium Aurasus

On complete bodies with the final rules: precision 89.5%, recall 100.0% (106,534 right, 12,456 wrong, 45 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.7% | 145 – 400 | ≤ 0.5914 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.52% | 145 – 400 | ≤ 0.622 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: W | 10 |
| Emerald | star: G | 42,134 |
| Green | star: K | 63,129 |
| Grey | star: B | 180 |
| Indigo | star: N | 4,953 |
| Lime | star: F | 56,409 |
| Maroon | star: TTS | 632 |
| Mauve | star: Y | 98 |
| Ocher | star: D | 122 |
| Orange | star: Ae | 6 |
| Red | star: T | 3,209 |
| Sage | star: L | 9,181 |
| Teal | star: M | 28,143 |
| Turquoise | star: O | 4 |
| Yellow | star: A | 13,276 |

![Temperature, gravity and atmosphere distributions for Bacterium Aurasus](species_figures/Bacterium_Aurasus.png)

![Right share across temperature and gravity for Bacterium Aurasus](temp_gravity_figures/Bacterium_Aurasus.png)

#### Bacterium Bullaris

On complete bodies with the final rules: precision 95.6%, recall 99.9% (31,358 right, 1,452 wrong, 33 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.622 |  | None, Minor Methane Magma, Minor Nitrogen Magma |
| Rocky body | Methane | Methane ≥ 100% or Nitrogen ≥ 1% |  | ≤ 0.3365 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Methane | Methane ≥ 100% |  | ≤ 0.622 |  | None |
| High metal content | Methane | Methane ≥ 100% or Nitrogen ≥ 1% |  | ≤ 0.622 | ≤ 0.05922 | None |
| Rocky body | MethaneRich | Nitrogen ≥ 56% | ≤ 143 | ≤ 0.622 |  | None |
| High metal content | MethaneRich | Nitrogen ≥ 50% | ≤ 129 | ≤ 0.622 |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Ruthenium | 9,621 |
| Cobalt | mats: Antimony | 9,451 |
| Gold | mats: Technetium | 6,045 |
| Lime | mats: Tellurium | 11,344 |
| Red | mats: Yttrium | 9,581 |
| Yellow | mats: Polonium | 4,672 |

![Temperature, gravity and atmosphere distributions for Bacterium Bullaris](species_figures/Bacterium_Bullaris.png)

![Right share across temperature and gravity for Bacterium Bullaris](temp_gravity_figures/Bacterium_Bullaris.png)

#### Bacterium Cerbrus

On complete bodies with the final rules: precision 71.6%, recall 99.9% (76,842 right, 30,442 wrong, 115 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 0.8% | ≥ 132 | ≤ 0.622 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% or Ammonia ≥ 16% | ≤ 273 | ≤ 0.4283 |  | None |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 0.8% |  | ≥ 0.2723 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.622 |  | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.622 |  | None |
| Rocky ice | WaterRich | Argon ≥ 78% |  | ≤ 0.622 | ≥ 0.007895 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: W | 2 |
| Emerald | star: G | 11,710 |
| Green | star: K | 32,799 |
| Grey | star: B | 1,018 |
| Indigo | star: N | 4,392 |
| Lime | star: F | 24,045 |
| Maroon | star: TTS | 662 |
| Mauve | star: Y | 37 |
| Ocher | star: D | 45 |
| Orange | star: Ae | 24 |
| Red | star: T | 725 |
| Sage | star: L | 6,108 |
| Teal | star: M | 32,768 |
| Turquoise | star: O | 18 |
| Yellow | star: A | 11,081 |

![Temperature, gravity and atmosphere distributions for Bacterium Cerbrus](species_figures/Bacterium_Cerbrus.png)

![Right share across temperature and gravity for Bacterium Cerbrus](temp_gravity_figures/Bacterium_Cerbrus.png)

#### Bacterium Informem

On complete bodies with the final rules: precision 99.9%, recall 99.8% (43,269 right, 44 wrong, 81 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Nitrogen | Nitrogen ≥ 99.9% | ≤ 83 | ≤ 0.4487 | None, Major Water Geysers, Minor Nitrogen Magma, Minor Methane Magma, Carbon Dioxide Geysers, Water Geysers, Minor Carbon Dioxide Geysers |
| Rocky body | Nitrogen | Nitrogen ≥ 100% or SulphurDioxide ≥ 28% or Argon ≥ 0.06% | ≤ 150 | ≤ 0.571 | None |
| Rocky ice | Nitrogen | Nitrogen ≥ 100% |  | ≤ 0.622 | None |
| High metal content | Nitrogen | Nitrogen ≥ 100% or SulphurDioxide ≥ 22% or Argon ≥ 0.06% | ≤ 151 | ≥ 0.2447 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Technetium | 8,088 |
| Cobalt | mats: Yttrium | 12,584 |
| Gold | mats: Ruthenium | 13,109 |
| Lime | mats: Polonium | 6,483 |
| Red | mats: Antimony | 10,637 |
| Yellow | mats: Tellurium | 12,303 |

![Temperature, gravity and atmosphere distributions for Bacterium Informem](species_figures/Bacterium_Informem.png)

![Right share across temperature and gravity for Bacterium Informem](temp_gravity_figures/Bacterium_Informem.png)

#### Bacterium Nebulus

On complete bodies with the final rules: precision 84.1%, recall 99.8% (1,957 right, 369 wrong, 4 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism |
|---|---|---|---|---|
| Icy body | Helium | Helium ≥ 100% | ≤ 0.622 | None, Major Water Magma, Minor Ammonia Magma, Minor Methane Magma, Minor Water Magma, Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cobalt | mats: Yttrium | 513 |
| Cyan | mats: Technetium | 317 |
| Gold | mats: Polonium | 283 |
| Green | mats: Tellurium | 490 |
| Magenta | mats: Antimony | 436 |
| Orange | mats: Ruthenium | 539 |

![Temperature, gravity and atmosphere distributions for Bacterium Nebulus](species_figures/Bacterium_Nebulus.png)

![Right share across temperature and gravity for Bacterium Nebulus](temp_gravity_figures/Bacterium_Nebulus.png)

#### Bacterium Omentum

On complete bodies with the final rules: precision 35.9%, recall 99.3% (3,752 right, 6,711 wrong, 28 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 197 | 0.04487 – 0.4589 |  | Minor Nitrogen Magma, Minor Ammonia Magma |
| Icy body | Helium | Helium ≥ 100% |  | ≤ 0.622 | ≥ 0.05922 | Minor Nitrogen Magma, Minor Ammonia Magma |
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.04691 | ≥ 0.02961 | Minor Nitrogen Magma |
| Icy body | Neon | Neon ≥ 100% or Helium ≥ 0.44% or Nitrogen ≥ 48% | 20 – 61 | ≤ 0.622 |  | Minor Ammonia Magma, Minor Nitrogen Magma |
| Icy body | NeonRich | Helium ≥ 50% or Nitrogen ≥ 50% |  | ≤ 0.622 | ≥ 0.002566 | Minor Ammonia Magma, Minor Nitrogen Magma |
| Icy body | WaterRich | Argon ≥ 74% |  | ≤ 0.622 |  | Minor Nitrogen Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Molybdenum | 931 |
| Blue | mats: Tungsten | 820 |
| Lime | mats: Cadmium | 1,116 |
| Peach | mats: Niobium | 993 |
| Red | mats: Tin | 896 |
| White | mats: Mercury | 669 |

![Temperature, gravity and atmosphere distributions for Bacterium Omentum](species_figures/Bacterium_Omentum.png)

![Right share across temperature and gravity for Bacterium Omentum](temp_gravity_figures/Bacterium_Omentum.png)

#### Bacterium Scopulum

On complete bodies with the final rules: precision 35.0%, recall 99.2% (2,575 right, 4,783 wrong, 20 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body, Rocky ice | Argon | Argon ≥ 100% | 56 – 146 | ≤ 0.2549 |  | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | Helium | Helium ≥ 100% |  | ≤ 0.622 |  | Minor Methane Magma |
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.622 |  | Minor Methane Magma |
| Icy body | Neon | Neon ≥ 100% or Helium ≥ 0.13% |  | ≤ 0.622 |  | Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Rocky ice | Neon | Neon ≥ 100% or Helium ≥ 6% | 35 – 50 | ≥ 0.5404 | 0.001974 – 0.002961 | Minor Carbon Dioxide Geysers |
| Icy body, Rocky ice | NeonRich | Helium ≥ 50% | ≤ 71 | ≥ 0.4691 | ≥ 0.005231 | Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | 61 – 71 | ≤ 0.2651 | ≥ 0.001974 | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | Oxygen | Oxygen ≥ 100% |  | ≤ 0.3977 |  | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Tungsten | 574 |
| Lime | mats: Molybdenum | 645 |
| Mulberry | mats: Tin | 601 |
| Peach | mats: Mercury | 463 |
| Red | mats: Niobium | 654 |
| White | mats: Cadmium | 704 |

![Temperature, gravity and atmosphere distributions for Bacterium Scopulum](species_figures/Bacterium_Scopulum.png)

![Right share across temperature and gravity for Bacterium Scopulum](temp_gravity_figures/Bacterium_Scopulum.png)

#### Bacterium Tela

**Temperature and gravity limits:** dropped on CarbonDioxide atmosphere: below 274.64 K or below 0.4658 g or above 0.60975 g; on High metal content world with CarbonDioxide atmosphere: below 300.06 K or below 0.50091 g or above 0.60975 g.

On complete bodies with the final rules: precision 41.1%, recall 98.5% (27,152 right, 38,874 wrong, 412 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Ammonia | Ammonia ≥ 100% | ≥ 163 | ≤ 0.622 |  | Major Water Magma, Water Magma, Minor Carbon Dioxide Geysers |
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 163 | ≤ 0.0571 | ≥ 0.005922 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Water Magma, Minor Rocky Magma |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 163 | ≤ 0.09177 |  | Major Silicate Vapour Geysers |
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 1% | ≤ 193 | 0.04487 – 0.4181 |  | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Nitrogen Magma, Minor Methane Magma, Water Geysers, Minor Water Magma, Water Magma, Minor Ammonia Magma |
| Rocky ice | Argon | Argon ≥ 100% | ≤ 142 | 0.2345 – 0.3467 |  | Carbon Dioxide Geysers, Major Rocky Magma, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Nitrogen Magma, Minor Methane Magma, Water Geysers, Minor Water Magma, Water Magma, Minor Ammonia Magma |
| Icy body | ArgonRich | Nitrogen ≥ 78% or Neon ≥ 0.06% or Oxygen ≥ 80% | 70 – 186 | ≤ 0.3977 | ≥ 0.01974 | Minor Nitrogen Magma, Major Water Geysers, Minor Carbon Dioxide Geysers, Water Geysers |
| Rocky body | ArgonRich | Nitrogen ≥ 96% | ≤ 131 | 0.2957 – 0.4079 | ≤ 0.04935 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| High metal content | ArgonRich | Nitrogen ≥ 96% | 97 – 143 | 0.3875 – 0.4691 | ≤ 0.06908 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Icy body | CarbonDioxide | SulphurDioxide ≥ 0.9% | 100 – 200 | ≤ 0.2753 | ≥ 0.009869 | Major Water Geysers |
| Rocky body | CarbonDioxide | SulphurDioxide ≥ 0.9% | 166 – 483 | ≥ 0.465 |  |  |
| High metal content | CarbonDioxide | SulphurDioxide ≥ 0.9% | 193 – 452 | ≥ 0.4997 |  |  |
| Icy body | CarbonDioxideRich |  | ≤ 246 | ≤ 0.622 | ≥ 0.002961 | Minor Carbon Dioxide Geysers, Carbon Dioxide Geysers |
| Rocky body | CarbonDioxideRich |  | ≤ 246 | 0.4385 – 0.5201 | ≥ 0.002961 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Rocky magma, Major Rocky Magma |
| Rocky Ice | CarbonDioxideRich |  | ≤ 246 | ≤ 0.622 | ≥ 0.002961 | Water Geysers |
| High metal content | CarbonDioxideRich |  | ≤ 246 | ≥ 0.4997 | ≥ 0.002961 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Rocky magma, Major Rocky Magma |
| Icy body | Helium | Helium ≥ 100% |  | ≤ 0.622 | ≥ 0.06612 | Major Water Magma, Minor Ammonia Magma, Minor Methane Magma, Minor Water Magma, Water Magma |
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.04793 | ≥ 0.03158 | Minor Methane Magma, Minor Nitrogen Magma |
| Rocky body | Methane | Methane ≥ 100% | 78 – 109 | ≤ 0.1326 | ≥ 0.01184 | Any |
| High metal content | Methane | Methane ≥ 100% | 50 – 150 | ≤ 0.2039 | ≥ 0.04935 | Major Silicate Vapour Geysers, Minor Silicate Vapour Geysers |
| Icy body | Neon | Neon ≥ 100% or Helium ≥ 0.14% |  | ≤ 0.622 |  | Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Water Magma |
| Rocky ice | Neon | Neon ≥ 100% | 35 – 51 | ≤ 0.622 | ≤ 0.003948 | Major Water Magma, Minor Carbon Dioxide Geysers, Minor Water Magma, Water Magma |
| Icy body | NeonRich | Helium ≥ 50% or Nitrogen ≥ 50% |  | ≤ 0.622 | ≥ 0.001974 | Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Magma |
| Rocky ice | NeonRich | Helium ≥ 50% or Nitrogen ≥ 50% | ≤ 50 | ≤ 0.622 | ≥ 0.05922 | Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | 50 – 80 | ≤ 0.4079 |  | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Water Geysers, Water Magma |
| High metal content | Nitrogen | Nitrogen ≥ 100% | 50 – 80 | ≤ 0.4079 | ≥ 0.05922 | Carbon Dioxide Geysers |
| Icy body | Oxygen | Oxygen ≥ 100% or SulphurDioxide ≥ 0.06% |  | ≤ 0.622 | ≥ 0.01678 | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Water Geysers, Water Magma |
| Rocky body | Oxygen | Oxygen ≥ 100% | 160 – 239 | 0.4079 – 0.5303 | ≥ 0.01974 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| High metal content | Oxygen | Oxygen ≥ 100% | 160 – 239 | 0.4079 – 0.5303 | ≥ 0.01974 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 272 | ≤ 0.622 |  | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 30% | ≥ 147.7 | ≥ 0.2651 |  | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 30% | ≥ 300 | ≥ 0.3773 |  | None |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% |  | ≤ 0.4079 | ≥ 0.001974 |  |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | ≥ 150 | ≥ 0.2855 |  | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 3.7% | ≥ 300 | ≥ 0.3977 |  | None |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.622 | ≥ 0.05329 | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.622 | ≥ 0.05132 | None |
| Icy body | WaterRich | Argon ≥ 84% |  | ≤ 0.622 | ≥ 0.03948 | Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Nitrogen Magma, Water Geysers |
| Rocky ice | WaterRich | Argon ≥ 79% |  | ≤ 0.622 | ≥ 0.01974 | Major Water Geysers, Water Geysers |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cobalt | mats: Tin | 7,380 |
| Gold | mats: Cadmium | 8,724 |
| Green | mats: Tungsten | 6,730 |
| Magenta | mats: Niobium | 7,847 |
| Orange | mats: Mercury | 5,424 |
| Yellow | mats: Molybdenum | 7,616 |

![Temperature, gravity and atmosphere distributions for Bacterium Tela](species_figures/Bacterium_Tela.png)

![Right share across temperature and gravity for Bacterium Tela](temp_gravity_figures/Bacterium_Tela.png)

#### Bacterium Verrata

On complete bodies with the final rules: precision 40.4%, recall 95.3% (3,480 right, 5,131 wrong, 171 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Ammonia | Ammonia ≥ 100% | ≥ 161 | ≤ 0.622 | ≥ 0.001974 | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 171 | ≤ 0.06118 | ≥ 0.005922 | Minor Water Magma |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.622 | ≥ 0.001974 | Major Water Magma, Minor Water Magma, Water Magma |
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.5% | ≤ 229 | 0.1632 – 0.3365 |  | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Rocky ice | Argon | Argon ≥ 100% | 80 – 120 | 0.2345 – 0.3059 |  | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Icy body | ArgonRich | Nitrogen ≥ 70% | 80 – 90 | ≤ 0.3059 | ≥ 0.03948 | Major Water Geysers |
| Rocky body | ArgonRich | Nitrogen ≥ 70% | ≤ 150 | ≤ 0.4079 | ≥ 0.01974 | Minor Water Magma |
| Icy body | Helium | Helium ≥ 100% |  | ≥ 0.5048 | ≥ 0.06612 | Major Water Magma, Minor Water Magma, Water Magma |
| Icy body, Rocky ice | Neon | Neon ≥ 100% or Helium ≥ 0.14% |  | ≤ 0.622 |  | Major Water Magma, Minor Water Magma, Water Magma |
| Icy body, Rocky ice | NeonRich | Helium ≥ 50% | ≤ 65 | ≥ 0.4385 | ≥ 0.004935 | Major Water Magma, Minor Water Magma, Water Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | ≤ 80 | ≤ 0.2549 |  | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Icy body | Oxygen | Oxygen ≥ 99% or SulphurDioxide ≥ 0.06% |  | ≤ 0.3467 |  | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Rocky ice | Oxygen | Oxygen ≥ 100% | 168 – 220 | 0.3467 – 0.4079 |  | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Magma |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.622 |  | Minor Water Magma |
| Rocky ice | WaterRich | Argon ≥ 50% |  | ≤ 0.622 | ≥ 0.03948 | Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Blue | mats: Tin | 877 |
| Lime | mats: Tungsten | 759 |
| Mulberry | mats: Niobium | 968 |
| Peach | mats: Cadmium | 1,092 |
| Red | mats: Mercury | 675 |
| White | mats: Molybdenum | 932 |

![Temperature, gravity and atmosphere distributions for Bacterium Verrata](species_figures/Bacterium_Verrata.png)

![Right share across temperature and gravity for Bacterium Verrata](temp_gravity_figures/Bacterium_Verrata.png)

#### Bacterium Vesicula

On complete bodies with the final rules: precision 98.2%, recall 99.9% (80,772 right, 1,502 wrong, 46 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Oxygen ≥ 2.6% or Nitrogen ≥ 0.06% or CarbonDioxide ≥ 0.06% | ≤ 267 | ≤ 0.5099 | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor MethaneMagma, Minor Nitrogen Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | Argon | Argon ≥ 100% or Nitrogen ≥ 3% | ≤ 148 | 0.1326 – 0.4385 | None |
| Rocky ice | Argon | Argon ≥ 100% or Oxygen ≥ 30% or Nitrogen ≥ 0.06% or CarbonDioxide ≥ 0.06% | ≤ 245 | ≤ 0.5201 | None, Major Water Geysers, Major Water Magma, Minor Water Geysers, Water Geysers |
| High metal content | Argon | Argon ≥ 100% or Nitrogen ≥ 1.8% | ≤ 149 | ≤ 0.4691 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cyan | mats: Antimony | 23,757 |
| Gold | mats: Technetium | 17,228 |
| Lime | mats: Yttrium | 26,748 |
| Mulberry | mats: Ruthenium | 26,759 |
| Orange | mats: Polonium | 14,085 |
| Red | mats: Tellurium | 27,998 |

![Temperature, gravity and atmosphere distributions for Bacterium Vesicula](species_figures/Bacterium_Vesicula.png)

![Right share across temperature and gravity for Bacterium Vesicula](temp_gravity_figures/Bacterium_Vesicula.png)

#### Bacterium Volu

On complete bodies with the final rules: precision 91.5%, recall 99.9% (5,557 right, 514 wrong, 7 missed).

| Body | Atmosphere | Composition | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Oxygen | Oxygen ≥ 100% or Argon ≥ 0.5% or SulphurDioxide ≥ 0.06% or CarbonDioxide ≥ 0.06% | ≤ 0.622 |  | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Water Geysers |
| Rocky body | Oxygen | Oxygen ≥ 100% or Nitrogen ≥ 9% or SulphurDioxide ≥ 0.07% or CarbonDioxide ≥ 0.13% | ≤ 0.622 |  | None, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Oxygen | Oxygen ≥ 100% or Argon ≥ 0.43% or SulphurDioxide ≥ 0.06% or CarbonDioxide ≥ 0.06% | ≤ 0.4895 | ≥ 0.01579 | None, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Water Magma |
| High metal content | Oxygen | Oxygen ≥ 100% or SulphurDioxide ≥ 0.09% or CarbonDioxide ≥ 0.11% | ≤ 0.5404 | ≥ 0.01974 | None, Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Polonium | 735 |
| Cobalt | mats: Ruthenium | 1,487 |
| Cyan | mats: Tellurium | 1,626 |
| Gold | mats: Yttrium | 1,614 |
| Lime | mats: Technetium | 968 |
| Red | mats: Antimony | 1,266 |

![Temperature, gravity and atmosphere distributions for Bacterium Volu](species_figures/Bacterium_Volu.png)

![Right share across temperature and gravity for Bacterium Volu](temp_gravity_figures/Bacterium_Volu.png)

### Bark Mounds

#### Bark Mounds

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Icy body | None | 200 – 440 | ≤ 2.549 | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Water Geysers | 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34 | nebulae: ≤ 160; |
| Rocky body | None | 200 – 440 | ≤ 0.4997 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34 | nebulae: ≤ 160; |
| Rocky ice | None | 200 – 277 | ≤ 1.937 | None, Minor Carbon Dioxide Geysers, Water Geysers | 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34 | nebulae: ≤ 160; |
| High metal content | None | 200 – 440 | ≤ 3.569 | None, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Rocky Magma | 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34 | nebulae: ≤ 160; |
| MetalRich | None | 200 – 440 | ≤ 3.569 | Major Silicate Vapour Geysers | 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 25, 32, 33, 34 | nebulae: ≤ 160; |

The codex has one form of this life form. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Bark Mounds](species_figures/Bark_Mounds.png)

### Brain Tree

#### Aureum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 300 – 500 | ≤ 0.4487 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | 2, 9, 10, 17, 18, 35 | guardian: true; |
| High metal content | None | 300 – 500 | ≤ 2.957 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Rocky Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Aureum Brain Tree](species_figures/Aureum_Brain_Tree.png)

#### Gypseeum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 330 | 0.04283 – 0.3773 | Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Gypseeum Brain Tree](species_figures/Gypseeum_Brain_Tree.png)

#### Lindigoticum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 300 – 500 | ≤ 0.2549 | Major Silicate Vapour Geysers, Metallic Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |
| High metal content | None | 300 – 500 | ≤ 2.549 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

#### Lividum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky body | None | 300 – 500 | ≤ 0.4895 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Lividum Brain Tree](species_figures/Lividum_Brain_Tree.png)

#### Ostrinum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| High metal content | None | 20 – 1000 | ≤ 2.325 | Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma | 2, 17, 18, 35 | guardian: true; |
| Metal rich body | None | 1000 – 1800 | ≤ 2.388 | Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Rocky Magma, Silicate Vapour Geysers | 2, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Ostrinum Brain Tree](species_figures/Ostrinum_Brain_Tree.png)

#### Puniceum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| High metal content | None | 20 – 1000 | ≤ 4.895 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers | 9, 10, 35 | guardian: true; |
| Metal rich body | None | 1000 – 1800 | ≤ 2.957 | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Rocky Magma, Silicate Vapour Geysers | 9, 10, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Puniceum Brain Tree](species_figures/Puniceum_Brain_Tree.png)

#### Roseum Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Icy body | None | 200 – 500 | ≤ 0.4079 | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Nitrogen Magma, Minor Water Magma, Water Geysers, Water Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |
| Rocky body | None | 200 – 500 | ≤ 1.295 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 2, 9, 10, 17, 18, 35 | guardian: true; |
| Rocky ice | None | 200 – 255 | ≤ 0.3161 | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Metallic Magma, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Minor Water Magma, Rocky Magma, Water Geysers, Water Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |
| High metal content | None | 200 – 500 | ≤ 4.285 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Roseum Brain Tree](species_figures/Roseum_Brain_Tree.png)

#### Viride Brain Tree

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Rocky ice | None | 100 – 245 | ≤ 0.3977 | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Rocky Magma, Minor Metallic Magma, Water Geysers | 2, 9, 10, 17, 18, 35 | guardian: true; |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Viride Brain Tree](species_figures/Viride_Brain_Tree.png)

### Cactoida

#### Cactoida Cortexum

On complete bodies with the final rules: precision 85.4%, recall 99.9% (5,517 right, 943 wrong, 8 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 197 | ≤ 0.2427 | ≥ 0.02507 | None | 1, 4, 7, 8, 16, 17, 18, 35 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 197 | ≤ 0.2814 | ≥ 0.02566 | None | 1, 4, 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 3,429 |
| Green | star: A | 1,799 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Mauve | star: L | 1,318 |
| Ocher | star: Y | 14 |
| Orange | star: T | 409 |
| Red | star: TTS | 104 |
| Sage | star: N | 406 |
| Teal | star: G | 6,430 |
| Turquoise | star: D | 15 |
| Yellow | star: F | 8,490 |

![Temperature, gravity and atmosphere distributions for Cactoida Cortexum](species_figures/Cactoida_Cortexum.png)

![Right share across temperature and gravity for Cactoida Cortexum](temp_gravity_figures/Cactoida_Cortexum.png)

#### Cactoida Lapis

**Temperature and gravity limits:** dropped above 176.64 K or above 0.27372 g.

On complete bodies with the final rules: precision 74.1%, recall 99.6% (8,256 right, 2,881 wrong, 36 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Minor Metallic Magma, Rocky Magma | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 5,604 |
| Green | star: A | 3,479 |
| Grey | star: O | 1 |
| Indigo | star: W | 2 |
| Mauve | star: L | 2,394 |
| Ocher | star: Y | 17 |
| Orange | star: T | 746 |
| Red | star: TTS | 177 |
| Sage | star: N | 501 |
| Teal | star: G | 8,066 |
| Turquoise | star: D | 8 |
| Yellow | star: F | 11,198 |

![Temperature, gravity and atmosphere distributions for Cactoida Lapis](species_figures/Cactoida_Lapis.png)

![Right share across temperature and gravity for Cactoida Lapis](temp_gravity_figures/Cactoida_Lapis.png)

#### Cactoida Peperatis

**Temperature and gravity limits:** dropped above 0.27394 g.

On complete bodies with the final rules: precision 60.8%, recall 99.7% (2,913 right, 1,882 wrong, 10 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 | None | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 1,744 |
| Green | star: A | 1,632 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Mauve | star: L | 715 |
| Ocher | star: Y | 9 |
| Orange | star: T | 223 |
| Red | star: TTS | 25 |
| Sage | star: N | 470 |
| Teal | star: G | 3,446 |
| Turquoise | star: D | 7 |
| Yellow | star: F | 5,102 |

![Temperature, gravity and atmosphere distributions for Cactoida Peperatis](species_figures/Cactoida_Peperatis.png)

![Right share across temperature and gravity for Cactoida Peperatis](temp_gravity_figures/Cactoida_Peperatis.png)

#### Cactoida Pullulanta

On complete bodies with the final rules: precision 79.9%, recall 100.0% (2,334 right, 588 wrong, 1 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2427 | ≥ 0.02467 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2814 | ≥ 0.02467 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 1,510 |
| Green | star: A | 974 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Mauve | star: L | 523 |
| Ocher | star: Y | 4 |
| Orange | star: T | 167 |
| Red | star: TTS | 28 |
| Sage | star: N | 189 |
| Teal | star: G | 2,699 |
| Turquoise | star: D | 6 |
| Yellow | star: F | 3,910 |

![Temperature, gravity and atmosphere distributions for Cactoida Pullulanta](species_figures/Cactoida_Pullulanta.png)

![Right share across temperature and gravity for Cactoida Pullulanta](temp_gravity_figures/Cactoida_Pullulanta.png)

#### Cactoida Vermis

**Temperature and gravity limits:** dropped on Rocky body: above 450.03 K or above 0.27359 g.

On complete bodies with the final rules: precision 87.2%, recall 99.4% (3,787 right, 558 wrong, 22 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 160 – 207 | ≥ 0.2651 | ≤ 0.003454 | Major Silicate Vapour Geysers, Metallic Magma |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.0571 | ≥ 0.04935 | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.06526 | ≥ 0.04935 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 2,023 |
| Green | star: A | 3,088 |
| Grey | star: O | 1 |
| Indigo | star: W | 0 |
| Mauve | star: L | 1,277 |
| Ocher | star: Y | 3 |
| Orange | star: T | 334 |
| Red | star: TTS | 42 |
| Sage | star: N | 1,228 |
| Teal | star: G | 1,390 |
| Turquoise | star: D | 5 |
| Yellow | star: F | 3,111 |

![Temperature, gravity and atmosphere distributions for Cactoida Vermis](species_figures/Cactoida_Vermis.png)

![Right share across temperature and gravity for Cactoida Vermis](temp_gravity_figures/Cactoida_Vermis.png)

### Clypeus

#### Clypeus Lacrimam

On complete bodies with the final rules: precision 75.4%, recall 100.0% (4,724 right, 1,543 wrong, 2 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 190 – 199 | ≤ 0.2427 | ≥ 0.04737 | None |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: G | 3,732 |
| Green | star: Y | 5 |
| Grey | star: K | 5,033 |
| Lime | star: D | 15 |
| Maroon | star: B | 62 |
| Mauve | star: F | 4,921 |
| Orange | star: A | 2,976 |
| Teal | star: L | 86 |
| Turquoise | star: M | 1,206 |
| Yellow | star: N | 1,130 |

![Temperature, gravity and atmosphere distributions for Clypeus Lacrimam](species_figures/Clypeus_Lacrimam.png)

![Right share across temperature and gravity for Clypeus Lacrimam](temp_gravity_figures/Clypeus_Lacrimam.png)

#### Clypeus Margaritus

On complete bodies with the final rules: precision 100.0%, recall 100.0% (3,114 right, 1 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 190 – 195.4 | ≤ 0.2814 | ≥ 0.05329 | None |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: G | 842 |
| Green | star: Y | 2 |
| Grey | star: K | 2,558 |
| Lime | star: D | 6 |
| Maroon | star: B | 11 |
| Mauve | star: F | 968 |
| Orange | star: A | 306 |
| Teal | star: L | 2,655 |
| Turquoise | star: M | 4,071 |
| Yellow | star: N | 199 |

![Temperature, gravity and atmosphere distributions for Clypeus Margaritus](species_figures/Clypeus_Margaritus.png)

![Right share across temperature and gravity for Clypeus Margaritus](temp_gravity_figures/Clypeus_Margaritus.png)

#### Clypeus Speculumi

**Correction:** dropped when under 2,500 ls from the first star or binary up the parent chain (orbit sizes added up; arrival distance if an orbit on the way isn't on record).

On complete bodies with the final rules: precision 99.6%, recall 100.0% (1,541 right, 6 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 190 – 195.4 | ≤ 0.2427 | ≥ 0.05329 | None |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.0571 | ≥ 0.05132 | None, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: G | 890 |
| Green | star: Y | 0 |
| Grey | star: K | 411 |
| Lime | star: D | 0 |
| Maroon | star: B | 703 |
| Mauve | star: F | 2,715 |
| Orange | star: A | 1,072 |
| Teal | star: L | 0 |
| Turquoise | star: M | 90 |
| Yellow | star: N | 654 |

![Temperature, gravity and atmosphere distributions for Clypeus Speculumi](species_figures/Clypeus_Speculumi.png)

![Right share across temperature and gravity for Clypeus Speculumi](temp_gravity_figures/Clypeus_Speculumi.png)

### Concha

#### Concha Aureolas

On complete bodies with the final rules: precision 11.6%, recall 99.9% (3,531 right, 26,905 wrong, 3 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism |
|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| High metal content | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Rocky Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: N | 382 |
| Green | star: D | 3 |
| Grey | star: F | 6,584 |
| Indigo | star: B | 30 |
| Orange | star: L | 86 |
| Red | star: K | 4,223 |
| Teal | star: A | 3,303 |
| Turquoise | star: G | 4,158 |
| Yellow | star: Y | 17 |

![Temperature, gravity and atmosphere distributions for Concha Aureolas](species_figures/Concha_Aureolas.png)

![Right share across temperature and gravity for Concha Aureolas](temp_gravity_figures/Concha_Aureolas.png)

#### Concha Biconcavis

On complete bodies with the final rules: precision 96.7%, recall 99.4% (347 right, 12 wrong, 2 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Nitrogen | Nitrogen ≥ 100% | ≤ 52 | ≤ 0.2814 | ≤ 0.009869 | None |
| High metal content | Nitrogen | Nitrogen ≥ 100% | ≤ 50 | ≤ 0.2814 | ≤ 0.003948 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Gold | mats: Yttrium | 174 |
| Orange | mats: Ruthenium | 167 |
| Peach | mats: Antimony | 199 |
| Red | mats: Polonium | 77 |
| White | mats: Technetium | 111 |
| Yellow | mats: Tellurium | 227 |

![Temperature, gravity and atmosphere distributions for Concha Biconcavis](species_figures/Concha_Biconcavis.png)

![Right share across temperature and gravity for Concha Biconcavis](temp_gravity_figures/Concha_Biconcavis.png)

#### Concha Labiata

**Temperature and gravity limits:** dropped above 191.52 K or above 0.20919 g; on Rocky body: above 191.51 K or above 0.20673 g.

On complete bodies with the final rules: precision 26.8%, recall 99.1% (18,610 right, 50,914 wrong, 160 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.96% | 150 – 195.4 | ≤ 0.2427 | ≥ 0.002665 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 150 – 195.4 | ≤ 0.2814 | ≥ 0.001974 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: N | 1,474 |
| Green | star: D | 27 |
| Grey | star: F | 19,290 |
| Indigo | star: B | 46 |
| Lime | star: W | 9 |
| Orange | star: L | 780 |
| Red | star: K | 14,163 |
| Teal | star: A | 5,656 |
| Turquoise | star: G | 14,425 |
| Yellow | star: Y | 26 |

![Temperature, gravity and atmosphere distributions for Concha Labiata](species_figures/Concha_Labiata.png)

![Right share across temperature and gravity for Concha Labiata](temp_gravity_figures/Concha_Labiata.png)

#### Concha Renibus

**Temperature and gravity limits:** dropped below 79.64 K or above 0.25364 g; on Methane atmosphere: below 79.64 K or above 103.05 K or above 0.11161 g.

On complete bodies with the final rules: precision 49.8%, recall 99.5% (11,558 right, 11,640 wrong, 53 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 174 | ≤ 0.05099 | ≥ 0.008882 | Major Rocky Magma, Major Silicate Vapour Geysers |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 174 | ≤ 0.05099 | ≥ 0.008882 | Major Rocky Magma, Major Silicate Vapour Geysers |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2427 | ≥ 0.02467 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2814 | ≥ 0.02467 | None |
| Rocky body | Methane | Methane ≥ 100% | 79 – 104 | ≤ 0.1224 | ≥ 0.01086 | Major Metallic Magma, Major Silicate Vapour Geysers |
| High metal content | Methane | Methane ≥ 100% | 79 – 104 | ≤ 0.1224 | ≥ 0.01086 | Major Metallic Magma, Major Silicate Vapour Geysers |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.0571 | ≥ 0.05132 | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.06526 | ≥ 0.05132 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Tin | 6,495 |
| Blue | mats: Niobium | 8,142 |
| Mulberry | mats: Mercury | 5,829 |
| Peach | mats: Molybdenum | 6,464 |
| Red | mats: Cadmium | 8,073 |
| White | mats: Tungsten | 5,660 |

![Temperature, gravity and atmosphere distributions for Concha Renibus](species_figures/Concha_Renibus.png)

![Right share across temperature and gravity for Concha Renibus](temp_gravity_figures/Concha_Renibus.png)

### Crystalline Shards

#### Crystalline Shards

| Body | Atmosphere | Temp (K) | Gravity (g) | Volcanism | Regions | Other |
|---|---|---|---|---|---|---|
| Icy body | None | 20 – 207 | ≤ 0.2141 | Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Geysers, Minor Water Magma | 14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 34, 36, 37, 38, 39, 40, 41, 42 | dist: ≥ 1e+04; |
| Rocky body | None | 43 – 250 | ≤ 0.2447 | Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 34, 36, 37, 38, 39, 40, 41, 42 | dist: ≥ 1e+04; |
| Rocky ice | None | 86 – 240 | ≤ 0.1937 | Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Minor Water Magma | 14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 34, 36, 37, 38, 39, 40, 41, 42 | dist: ≥ 1e+04; |
| High metal content | None | 57 – 249 | ≤ 1.479 | Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 14, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 34, 36, 37, 38, 39, 40, 41, 42 | dist: ≥ 1e+04; |

The codex has one form of this life form. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Crystalline Shards](species_figures/Crystalline_Shards.png)

### Electricae

#### Electricae Pluma

On complete bodies with the final rules: precision 84.6%, recall 99.8% (2,224 right, 406 wrong, 5 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Other |
|---|---|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 150 | ≤ 0.2814 |  | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma | star: O, B, A, H, Ae, D, N, W; |
| Icy body | ArgonRich | Nitrogen ≥ 50% | ≤ 121 | ≤ 0.2814 | ≥ 0.01974 | None | star: O, B, A, H, Ae, D, N, W; |
| Icy body | Neon | Nitrogen ≥ 50% | ≤ 45 | ≤ 0.2814 | ≤ 0.001086 | None, Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma, Water Geysers | star: O, B, A, H, Ae, D, N, W; |
| Icy body | NeonRich | Nitrogen ≥ 87% | 60 – 69 | ≤ 0.2814 | ≤ 0.005922 | None | star: O, B, A, H, Ae, D, N, W; |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Blue | mats: Ruthenium | 1,149 |
| Cobalt | mats: Antimony | 1,068 |
| Cyan | mats: Polonium | 540 |
| Magenta | mats: Technetium | 702 |
| Mulberry | mats: Yttrium | 1,111 |
| Red | mats: Tellurium | 1,258 |

![Temperature, gravity and atmosphere distributions for Electricae Pluma](species_figures/Electricae_Pluma.png)

![Right share across temperature and gravity for Electricae Pluma](temp_gravity_figures/Electricae_Pluma.png)

#### Electricae Radialem

**Correction:** dropped when Methane atmosphere.

On complete bodies with the final rules: precision 67.6%, recall 96.8% (4,805 right, 2,306 wrong, 157 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Other |
|---|---|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 150 | ≤ 0.2814 |  | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma | nebulae: ≤ 150; |
| Icy body | ArgonRich | Nitrogen ≥ 50% | ≤ 121 | ≤ 0.2814 | ≥ 0.01974 | None | nebulae: ≤ 150; |
| Icy body | Methane |  |  | ≤ 0.2814 | ≥ 0.06908 |  | nebulae: ≤ 150; |
| Icy body | Neon |  |  | ≤ 0.2814 | ≥ 0.06908 | None, Minor Water Geysers, Minor Water Magma | nebulae: ≤ 150; |
| Icy body | NeonRich | Nitrogen ≥ 80% | 56 – 69 | ≤ 0.2814 | ≤ 0.03948 | None | nebulae: ≤ 150; |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Technetium | 1,656 |
| Blue | mats: Ruthenium | 2,605 |
| Cobalt | mats: Polonium | 1,248 |
| Cyan | mats: Antimony | 2,357 |
| Green | mats: Yttrium | 2,538 |
| Magenta | mats: Tellurium | 2,744 |

![Temperature, gravity and atmosphere distributions for Electricae Radialem](species_figures/Electricae_Radialem.png)

![Right share across temperature and gravity for Electricae Radialem](temp_gravity_figures/Electricae_Radialem.png)

### Fonticulua

#### Fonticulua Campestris

**Correction:** dropped when on a Rocky Ice body with an Argon atmosphere and a main star older than 10,000 My.

**Temperature and gravity limits:** dropped above 0.27424 g.

On complete bodies with the final rules: precision 74.6%, recall 99.7% (53,257 right, 18,133 wrong, 180 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 151 | ≤ 0.2814 | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky ice | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 132 | ≤ 0.2814 | None, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Silicate Vapour Geysers, Water Geysers, Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 64,735 |
| Emerald | star: K | 26,042 |
| Green | star: A | 842 |
| Grey | star: O | 6 |
| Indigo | star: W | 0 |
| Lime | star: B | 77 |
| Maroon | star: Ae | 174 |
| Mauve | star: L | 7,873 |
| Ocher | star: Y | 83 |
| Orange | star: T | 2,902 |
| Red | star: TTS | 1,764 |
| Sage | star: N | 1,984 |
| Teal | star: G | 6,581 |
| Turquoise | star: D | 1,301 |
| Yellow | star: F | 4,535 |

![Temperature, gravity and atmosphere distributions for Fonticulua Campestris](species_figures/Fonticulua_Campestris.png)

![Right share across temperature and gravity for Fonticulua Campestris](temp_gravity_figures/Fonticulua_Campestris.png)

#### Fonticulua Digitos

On complete bodies with the final rules: precision 38.5%, recall 100.0% (12,062 right, 19,273 wrong, 4 missed).

| Body | Atmosphere | Composition | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Methane | Methane ≥ 100% | ≤ 0.2814 |  | None, Minor Methane Magma, Minor Nitrogen Magma |
| Rocky ice | Methane | Methane ≥ 100% | ≤ 0.2814 | ≥ 0.0381 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 11,065 |
| Emerald | star: K | 12,891 |
| Green | star: A | 18 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Lime | star: B | 1 |
| Maroon | star: Ae | 0 |
| Mauve | star: L | 621 |
| Ocher | star: Y | 26 |
| Orange | star: T | 310 |
| Red | star: TTS | 104 |
| Sage | star: N | 264 |
| Teal | star: G | 1,376 |
| Turquoise | star: D | 95 |
| Yellow | star: F | 339 |

![Temperature, gravity and atmosphere distributions for Fonticulua Digitos](species_figures/Fonticulua_Digitos.png)

![Right share across temperature and gravity for Fonticulua Digitos](temp_gravity_figures/Fonticulua_Digitos.png)

#### Fonticulua Fluctus

On complete bodies with the final rules: precision 84.2%, recall 98.0% (351 right, 66 wrong, 7 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Oxygen | Oxygen ≥ 100% or SulphurDioxide ≥ 0.06% | 143 – 200 | ≤ 0.2814 | ≥ 0.009869 | None, Carbon Dioxide Geysers, Major Water Geysers |
| Icy body | CarbonDioxideRich |  | 170 – 180 | ≤ 0.2814 | ≥ 0.009869 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 111 |
| Emerald | star: K | 119 |
| Green | star: A | 59 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Lime | star: B | 3 |
| Maroon | star: Ae | 0 |
| Mauve | star: L | 48 |
| Ocher | star: Y | 0 |
| Orange | star: T | 9 |
| Red | star: TTS | 18 |
| Sage | star: N | 8 |
| Teal | star: G | 81 |
| Turquoise | star: D | 0 |
| Yellow | star: F | 199 |

![Temperature, gravity and atmosphere distributions for Fonticulua Fluctus](species_figures/Fonticulua_Fluctus.png)

![Right share across temperature and gravity for Fonticulua Fluctus](temp_gravity_figures/Fonticulua_Fluctus.png)

#### Fonticulua Lapida

**Correction:** dropped when gravity > 0.2753 g.

On complete bodies with the final rules: precision 95.4%, recall 100.0% (14,949 right, 718 wrong, 6 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Icy body | Nitrogen | Nitrogen ≥ 100% | ≤ 81 | ≤ 0.2814 | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Geysers |
| Rocky ice | Nitrogen | Nitrogen ≥ 100% | ≤ 70 | ≤ 0.2814 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 13,789 |
| Emerald | star: K | 6,812 |
| Green | star: A | 36 |
| Grey | star: O | 1 |
| Indigo | star: W | 0 |
| Lime | star: B | 17 |
| Maroon | star: Ae | 77 |
| Mauve | star: L | 2,298 |
| Ocher | star: Y | 31 |
| Orange | star: T | 708 |
| Red | star: TTS | 336 |
| Sage | star: N | 279 |
| Teal | star: G | 1,303 |
| Turquoise | star: D | 152 |
| Yellow | star: F | 327 |

![Temperature, gravity and atmosphere distributions for Fonticulua Lapida](species_figures/Fonticulua_Lapida.png)

![Right share across temperature and gravity for Fonticulua Lapida](temp_gravity_figures/Fonticulua_Lapida.png)

#### Fonticulua Segmentatus

On complete bodies with the final rules: precision 99.9%, recall 99.7% (1,581 right, 1 wrong, 5 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Neon | Neon ≥ 100% | 50 – 56.1 | ≤ 0.2814 | ≤ 0.001184 | None |
| Icy body | NeonRich | Nitrogen ≥ 74% | 56.2 – 75 | ≤ 0.2814 | 0.001283 – 0.06119 | None, Minor Nitrogen Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 962 |
| Emerald | star: K | 667 |
| Green | star: A | 5 |
| Grey | star: O | 0 |
| Indigo | star: W | 0 |
| Lime | star: B | 3 |
| Maroon | star: Ae | 37 |
| Mauve | star: L | 103 |
| Ocher | star: Y | 2 |
| Orange | star: T | 9 |
| Red | star: TTS | 46 |
| Sage | star: N | 35 |
| Teal | star: G | 96 |
| Turquoise | star: D | 22 |
| Yellow | star: F | 27 |

![Temperature, gravity and atmosphere distributions for Fonticulua Segmentatus](species_figures/Fonticulua_Segmentatus.png)

![Right share across temperature and gravity for Fonticulua Segmentatus](temp_gravity_figures/Fonticulua_Segmentatus.png)

#### Fonticulua Upupam

On complete bodies with the final rules: precision 99.9%, recall 100.0% (8,185 right, 10 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | ArgonRich | Nitrogen ≥ 50% | ≤ 124 | ≤ 0.2814 | ≥ 0.01678 | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Nitrogen Magma, Water Geysers |
| Rocky ice | ArgonRich | Nitrogen ≥ 50% | ≤ 80 | ≤ 0.2814 | ≥ 0.02961 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: M | 4,991 |
| Emerald | star: K | 3,250 |
| Green | star: A | 54 |
| Grey | star: O | 0 |
| Indigo | star: W | 1 |
| Lime | star: B | 12 |
| Maroon | star: Ae | 30 |
| Mauve | star: L | 841 |
| Ocher | star: Y | 7 |
| Orange | star: T | 246 |
| Red | star: TTS | 154 |
| Sage | star: N | 214 |
| Teal | star: G | 917 |
| Turquoise | star: D | 56 |
| Yellow | star: F | 371 |

![Temperature, gravity and atmosphere distributions for Fonticulua Upupam](species_figures/Fonticulua_Upupam.png)

![Right share across temperature and gravity for Fonticulua Upupam](temp_gravity_figures/Fonticulua_Upupam.png)

### Frutexa

#### Frutexa Acus

On complete bodies with the final rules: precision 96.3%, recall 99.9% (21,076 right, 802 wrong, 18 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.98% | 146 – 195.4 | ≤ 0.2427 | ≥ 0.002862 | None | 1, 4, 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 22,098 |
| Green | star: F | 26,307 |
| Grey | star: M | 4,746 |
| Indigo | star: D | 46 |
| Lime | star: B | 53 |
| Mauve | star: TTS | 200 |
| Orange | star: W | 10 |
| Red | star: N | 1,552 |
| Teal | star: L | 387 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Frutexa Acus](species_figures/Frutexa_Acus.png)

![Right share across temperature and gravity for Frutexa Acus](temp_gravity_figures/Frutexa_Acus.png)

#### Frutexa Collum

**Temperature and gravity limits:** dropped above 167.03 K or above 0.2747 g.

On complete bodies with the final rules: precision 99.3%, recall 99.3% (821 right, 6 wrong, 6 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 211 | ≤ 0.2814 | ≤ 0.01678 | None, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 151 | ≤ 0.2814 | ≤ 0.001678 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 642 |
| Green | star: F | 797 |
| Grey | star: M | 227 |
| Indigo | star: D | 4 |
| Lime | star: B | 5 |
| Mauve | star: TTS | 5 |
| Orange | star: W | 0 |
| Red | star: N | 68 |
| Teal | star: L | 49 |
| Yellow | star: O | 1 |

![Temperature, gravity and atmosphere distributions for Frutexa Collum](species_figures/Frutexa_Collum.png)

![Right share across temperature and gravity for Frutexa Collum](temp_gravity_figures/Frutexa_Collum.png)

#### Frutexa Fera

On complete bodies with the final rules: precision 86.1%, recall 100.0% (4,867 right, 787 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 146 – 195.4 | ≤ 0.2427 | ≥ 0.001974 | None | 1, 2, 5, 6, 13, 14, 27, 29, 31, 37, 41 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 5,536 |
| Green | star: F | 7,182 |
| Grey | star: M | 1,058 |
| Indigo | star: D | 21 |
| Lime | star: B | 16 |
| Mauve | star: TTS | 25 |
| Orange | star: W | 0 |
| Red | star: N | 908 |
| Teal | star: L | 75 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Frutexa Fera](species_figures/Frutexa_Fera.png)

![Right share across temperature and gravity for Frutexa Fera](temp_gravity_figures/Frutexa_Fera.png)

#### Frutexa Flabellum

**Correction:** dropped when atmosphere other than Ammonia.

**Temperature and gravity limits:** dropped above 0.27451 g.

On complete bodies with the final rules: precision 99.9%, recall 99.9% (11,182 right, 13 wrong, 13 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% |  | ≤ 0.2814 |  | None, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Water Magma | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 152 – 195.4 | ≤ 0.2814 | ≥ 0.003948 |  | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 14,961 |
| Green | star: F | 19,041 |
| Grey | star: M | 4,161 |
| Indigo | star: D | 18 |
| Lime | star: B | 65 |
| Mauve | star: TTS | 113 |
| Orange | star: W | 3 |
| Red | star: N | 905 |
| Teal | star: L | 196 |
| Yellow | star: O | 2 |

![Temperature, gravity and atmosphere distributions for Frutexa Flabellum](species_figures/Frutexa_Flabellum.png)

![Right share across temperature and gravity for Frutexa Flabellum](temp_gravity_figures/Frutexa_Flabellum.png)

#### Frutexa Flammasis

**Temperature and gravity limits:** dropped above 0.27438 g.

On complete bodies with the final rules: precision 99.9%, recall 99.9% (3,877 right, 3 wrong, 4 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 5,844 |
| Green | star: F | 7,700 |
| Grey | star: M | 1,600 |
| Indigo | star: D | 17 |
| Lime | star: B | 25 |
| Mauve | star: TTS | 19 |
| Orange | star: W | 0 |
| Red | star: N | 914 |
| Teal | star: L | 109 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Frutexa Flammasis](species_figures/Frutexa_Flammasis.png)

![Right share across temperature and gravity for Frutexa Flammasis](temp_gravity_figures/Frutexa_Flammasis.png)

#### Frutexa Metallicum

**Correction:** dropped when gravity > 0.2753 g.

**Temperature and gravity limits:** dropped above 399.97 K or above 0.27488 g.

On complete bodies with the final rules: precision 99.8%, recall 99.9% (15,395 right, 35 wrong, 12 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| High metal content | Ammonia | Ammonia ≥ 100% |  | ≤ 0.2814 |  | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 146 – 195.4 | ≤ 0.2814 | ≥ 0.001974 | None |
| High metal content | Methane |  | 100 – 102 | ≤ 0.2814 | ≥ 0.05231 | None |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.05303 | 0.05132 – 0.06711 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 5,075 |
| Green | star: F | 6,105 |
| Grey | star: M | 19,008 |
| Indigo | star: D | 14 |
| Lime | star: B | 14 |
| Mauve | star: TTS | 303 |
| Orange | star: W | 0 |
| Red | star: N | 1,033 |
| Teal | star: L | 11,083 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Frutexa Metallicum](species_figures/Frutexa_Metallicum.png)

![Right share across temperature and gravity for Frutexa Metallicum](temp_gravity_figures/Frutexa_Metallicum.png)

#### Frutexa Sponsae

On complete bodies with the final rules: precision 100.0%, recall 100.0% (2,099 right, 0 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.05132 | None, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: G | 1,113 |
| Green | star: F | 2,476 |
| Grey | star: M | 404 |
| Indigo | star: D | 3 |
| Lime | star: B | 645 |
| Mauve | star: TTS | 11 |
| Orange | star: W | 0 |
| Red | star: N | 1,284 |
| Teal | star: L | 12 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Frutexa Sponsae](species_figures/Frutexa_Sponsae.png)

![Right share across temperature and gravity for Frutexa Sponsae](temp_gravity_figures/Frutexa_Sponsae.png)

### Fumerola

#### Fumerola Aquatis

On complete bodies with the final rules: precision 99.3%, recall 99.1% (1,500 right, 11 wrong, 13 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body, Rocky Ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.2549 |  | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | Ammonia | Ammonia ≥ 100% |  | ≤ 0.06322 | ≥ 0.005922 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Icy body | Argon |  | ≤ 149 | ≥ 0.1632 |  | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky ice | Argon | Argon ≥ 100% | 81 – 125 | ≥ 0.2345 | ≥ 0.0009869 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Icy body | ArgonRich |  | 79 – 92 | ≤ 0.2814 | ≥ 0.0375 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Icy body | CarbonDioxide, CarbonDioxideRich |  | 160 – 180 | 0.2447 – 0.2804 | ≥ 0.009869 | Major Water Geysers |
| Rocky body | Methane | Methane ≥ 100% | 83 – 101 | ≤ 0.1122 | ≥ 0.009869 | Minor Water Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | ≤ 80 | ≤ 0.2814 | ≥ 0.0009869 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Icy body | Oxygen |  | ≤ 193 | ≤ 0.2814 |  | Major Water Geysers, Major Water Magma, Water Geysers, Water Magma |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 272 | ≤ 0.2814 | ≥ 0.0009869 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 149 – 272 | ≤ 0.2814 | ≥ 0.001974 | Minor Water Magma |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 234 | ≤ 0.2814 | ≥ 0.003257 | Major Water Geysers, Major Water Magma, Minor Water Geysers, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.06119 | Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cobalt | mats: Tungsten | 740 |
| Cyan | mats: Molybdenum | 874 |
| Gold | mats: Niobium | 936 |
| Green | mats: Cadmium | 1,025 |
| Orange | mats: Tin | 826 |
| Yellow | mats: Mercury | 638 |

![Temperature, gravity and atmosphere distributions for Fumerola Aquatis](species_figures/Fumerola_Aquatis.png)

![Right share across temperature and gravity for Fumerola Aquatis](temp_gravity_figures/Fumerola_Aquatis.png)

#### Fumerola Carbosis

On complete bodies with the final rules: precision 99.4%, recall 99.1% (1,569 right, 9 wrong, 15 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Ammonia |  | ≥ 160.7 | ≤ 0.03161 |  | Minor Carbon Dioxide Geysers |
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 35% | ≤ 150 |  |  | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Rocky ice | Argon | Argon ≥ 100% or Nitrogen ≥ 35% | 78 – 122 |  | ≥ 0.001974 | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | ArgonRich |  | ≤ 90 |  | ≥ 0.02961 | Minor Carbon Dioxide Geysers |
| Icy body | CarbonDioxideRich |  | 170 – 180 |  | ≥ 0.05922 | Carbon Dioxide Geysers |
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.04793 | ≥ 0.03059 | Minor Methane Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | ≤ 76 |  | ≥ 0.0009869 | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | Oxygen | Oxygen ≥ 100% | ≤ 177 | ≥ 0.2651 | ≤ 0.05922 | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 272 |  |  | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 212 |  | ≥ 0.001974 | Carbon Dioxide Geysers, Minor Carbon Dioxide Geysers, Minor Methane Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cobalt | mats: Niobium | 738 |
| Cyan | mats: Tin | 684 |
| Gold | mats: Molybdenum | 739 |
| Magenta | mats: Mercury | 515 |
| Orange | mats: Cadmium | 846 |
| Yellow | mats: Tungsten | 614 |

![Temperature, gravity and atmosphere distributions for Fumerola Carbosis](species_figures/Fumerola_Carbosis.png)

![Right share across temperature and gravity for Fumerola Carbosis](temp_gravity_figures/Fumerola_Carbosis.png)

#### Fumerola Extremus

**Temperature and gravity limits:** dropped above 0.27453 g.

On complete bodies with the final rules: precision 99.8%, recall 98.0% (440 right, 1 wrong, 9 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 173 | ≤ 0.04895 |  | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma, Silicate Vapour Geysers |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.09177 |  | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 174 | 0.04691 – 0.05506 | ≥ 0.009869 | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| Rocky body | Argon | Argon ≥ 100% | ≤ 57 | ≤ 0.08158 |  | Major Metallic Magma, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma, Silicate Vapour Geysers |
| Rocky ice | Argon | Argon ≥ 100% | 64 – 121 | ≥ 0.2141 |  | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| Rocky body | Methane | Methane ≥ 100% | 77 – 109 |  | ≥ 0.009869 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Rocky Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| High metal content | Methane | Methane ≥ 100% | 97 – 101 | ≤ 0.1224 |  | Major Silicate Vapour Geysers, Minor Silicate Vapour Geysers |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 147.7 – 168 | ≥ 0.2651 | ≤ 0.005922 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% | 153 – 204 |  | ≥ 0.001974 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Rocky Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Cadmium | 534 |
| Blue | mats: Molybdenum | 427 |
| Lime | mats: Mercury | 335 |
| Mulberry | mats: Tungsten | 383 |
| Peach | mats: Tin | 374 |
| White | mats: Niobium | 429 |

![Temperature, gravity and atmosphere distributions for Fumerola Extremus](species_figures/Fumerola_Extremus.png)

![Right share across temperature and gravity for Fumerola Extremus](temp_gravity_figures/Fumerola_Extremus.png)

#### Fumerola Nitris

**Correction:** dropped when gravity > 0.2743 g.

On complete bodies with the final rules: precision 100.0%, recall 99.6% (1,837 right, 0 wrong, 7 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 141 |  |  | Minor Ammonia Magma, Minor Nitrogen Magma |
| Icy body | ArgonRich |  | ≤ 110 |  |  | Minor Nitrogen Magma |
| Icy body | Methane | Methane ≥ 100% |  | ≤ 0.05099 | ≥ 0.02961 | Minor Nitrogen Magma |
| Icy body | Neon |  | 39 – 51 | ≥ 0.2651 |  | Minor Nitrogen Magma |
| Icy body | NeonRich |  | 60 – 68 | ≥ 0.2549 |  | Minor Nitrogen Magma |
| Icy body | Nitrogen | Nitrogen ≥ 100% | 60 – 81 | ≥ 0.1937 | ≤ 0.04342 | Minor Ammonia Magma, Minor Nitrogen Magma |
| Icy body | Oxygen |  | ≤ 154 |  |  | Minor Nitrogen Magma |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 249 |  |  | Minor Ammonia Magma, Minor Nitrogen Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Tungsten | 814 |
| Lime | mats: Molybdenum | 1,036 |
| Mulberry | mats: Tin | 905 |
| Peach | mats: Mercury | 687 |
| Red | mats: Niobium | 1,002 |
| White | mats: Cadmium | 1,128 |

![Temperature, gravity and atmosphere distributions for Fumerola Nitris](species_figures/Fumerola_Nitris.png)

![Right share across temperature and gravity for Fumerola Nitris](temp_gravity_figures/Fumerola_Nitris.png)

### Fungoida

#### Fungoida Bullarum

**Temperature and gravity limits:** dropped above 132.91 K or above 0.27462 g.

On complete bodies with the final rules: precision 94.9%, recall 99.2% (4,314 right, 231 wrong, 35 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Rocky body | Argon | Argon ≥ 100% | ≤ 93 | ≥ 0.102 | None |
| Rocky ice | Argon | Argon ≥ 50% or Nitrogen ≥ 50% | ≤ 133 | ≤ 0.2814 | None |
| High metal content | Argon | Argon ≥ 100% | ≤ 90 | ≤ 0.2814 | None |
| Rocky body | Nitrogen | Nitrogen ≥ 100% | 50 – 52 | ≥ 0.08158 | None |
| Rocky ice | Nitrogen | Nitrogen ≥ 100% | ≤ 77 | ≤ 0.2814 | None |
| High metal content | Nitrogen | Nitrogen ≥ 100% | 50 – 52 | ≤ 0.2814 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Gold | mats: Tellurium | 3,022 |
| Magenta | mats: Ruthenium | 3,513 |
| Mulberry | mats: Polonium | 1,591 |
| Orange | mats: Yttrium | 3,165 |
| Peach | mats: Technetium | 2,019 |
| Red | mats: Antimony | 2,506 |

![Temperature, gravity and atmosphere distributions for Fungoida Bullarum](species_figures/Fungoida_Bullarum.png)

![Right share across temperature and gravity for Fungoida Bullarum](temp_gravity_figures/Fungoida_Bullarum.png)

#### Fungoida Gelata

On complete bodies with the final rules: precision 75.9%, recall 100.0% (10,660 right, 3,377 wrong, 1 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 174 | ≤ 0.04895 | ≥ 0.001678 | Major Metallic Magma, Major Silicate Vapour Geysers | 7, 8, 16, 17, 18, 35 |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.0724 |  | Major Silicate Vapour Geysers | 7, 8, 16, 17, 18, 35 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 174 | ≤ 0.2814 |  | Major Rocky Magma | 7, 8, 16, 17, 18, 35 |
| Rocky ice | Argon | Argon ≥ 100% | 100 – 101 | ≤ 0.2814 |  | Major Silicate Vapour Geysers | 7, 8, 16, 17, 18, 35 |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2427 | ≥ 0.02527 | None | 7, 8, 16, 17, 18, 35 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2814 | ≥ 0.02566 | None | 7, 8, 16, 17, 18, 35 |
| Rocky body | Methane | Methane ≥ 100% | 77 – 107 | ≤ 0.1285 | ≥ 0.01273 | Major Metallic Magma, Major Silicate Vapour Geysers | 7, 8, 16, 17, 18, 35 |
| High metal content | Methane | Methane ≥ 100% | 77 – 107 | ≤ 0.1285 | ≥ 0.01273 | Major Metallic Magma, Major Silicate Vapour Geysers | 7, 8, 16, 17, 18, 35 |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None | 7, 8, 16, 17, 18, 35 |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None | 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cyan | mats: Cadmium | 7,268 |
| Green | mats: Niobium | 6,852 |
| Lime | mats: Mercury | 4,794 |
| Mulberry | mats: Molybdenum | 6,254 |
| Orange | mats: Tungsten | 5,547 |
| Red | mats: Tin | 5,897 |

![Temperature, gravity and atmosphere distributions for Fungoida Gelata](species_figures/Fungoida_Gelata.png)

![Right share across temperature and gravity for Fungoida Gelata](temp_gravity_figures/Fungoida_Gelata.png)

#### Fungoida Setisis

**Temperature and gravity limits:** dropped above 174.76 K or above 0.27357 g; on High metal content world: above 174.67 K or below 0.14743 g or above 0.27431 g.

On complete bodies with the final rules: precision 86.6%, recall 98.9% (31,542 right, 4,889 wrong, 346 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% |  | ≤ 0.2814 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Ammonia | Ammonia ≥ 100% |  | ≤ 0.2814 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| High metal content | Ammonia | Ammonia ≥ 100% |  | ≤ 0.2814 |  | None, Major Rocky Magma, Minor Carbon Dioxide Geysers, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% | 180 – 195.4 | ≤ 0.1632 | ≤ 0.04935 | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% | 160 – 195.4 | ≤ 0.1835 | ≤ 0.04935 |  |
| Rocky body | Methane | Methane ≥ 100% | 67 – 108 | ≤ 0.2814 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Methane | Methane ≥ 100% |  | ≤ 0.2814 |  | None |
| High metal content | Methane | Methane ≥ 100% | 68 – 102 | ≤ 0.2814 |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Gold | mats: Ruthenium | 17,012 |
| Lime | mats: Technetium | 11,121 |
| Orange | mats: Yttrium | 16,976 |
| Peach | mats: Antimony | 15,533 |
| White | mats: Polonium | 8,473 |
| Yellow | mats: Tellurium | 18,157 |

![Temperature, gravity and atmosphere distributions for Fungoida Setisis](species_figures/Fungoida_Setisis.png)

![Right share across temperature and gravity for Fungoida Setisis](temp_gravity_figures/Fungoida_Setisis.png)

#### Fungoida Stabitis

On complete bodies with the final rules: precision 74.2%, recall 100.0% (8,617 right, 3,004 wrong, 3 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 172 | ≤ 0.05099 | ≥ 0.008882 | Major Silicate Vapour Geysers | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 172 | ≤ 0.2814 | ≥ 0.003948 | Major Silicate Vapour Geysers | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky ice | Argon | Argon ≥ 100% | ≤ 111 | ≤ 0.2814 |  | Major Rocky Magma, Major Silicate Vapour Geysers | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2427 | ≥ 0.02517 | None | 1, 4, 7, 8, 16, 17, 18, 35 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2814 | ≥ 0.02517 | None | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky body | Methane | Methane ≥ 100% | 78 – 109 | ≤ 0.1295 | ≥ 0.01184 | Major Silicate Vapour Geysers | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None | 1, 4, 7, 8, 16, 17, 18, 35 |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 |  | None | 1, 4, 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Blue | mats: Cadmium | 5,619 |
| Green | mats: Mercury | 3,826 |
| Magenta | mats: Molybdenum | 4,663 |
| Orange | mats: Tin | 4,447 |
| Peach | mats: Tungsten | 4,124 |
| White | mats: Niobium | 5,191 |

![Temperature, gravity and atmosphere distributions for Fungoida Stabitis](species_figures/Fungoida_Stabitis.png)

![Right share across temperature and gravity for Fungoida Stabitis](temp_gravity_figures/Fungoida_Stabitis.png)

### Osseus

#### Osseus Cornibus

On complete bodies with the final rules: precision 89.6%, recall 100.0% (2,928 right, 339 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2427 | ≥ 0.02467 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 195.4 | ≤ 0.2814 | ≥ 0.02566 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: T | 224 |
| Green | star: TTS | 30 |
| Grey | star: G | 2,062 |
| Indigo | star: K | 3,422 |
| Lime | star: A | 746 |
| Maroon | star: Y | 5 |
| Turquoise | star: F | 3,273 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Osseus Cornibus](species_figures/Osseus_Cornibus.png)

![Right share across temperature and gravity for Osseus Cornibus](temp_gravity_figures/Osseus_Cornibus.png)

#### Osseus Discus

**Correction:** dropped when on a High metal content body with a Water atmosphere and a main star younger than 8,500 My.

On complete bodies with the final rules: precision 75.8%, recall 99.5% (3,514 right, 1,124 wrong, 17 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 169 | ≤ 0.05914 | ≥ 0.005922 | Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.09177 |  | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma, Rocky Magma, Water Magma |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 174 | 0.04691 – 0.05506 | ≥ 0.008882 | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Argon | Argon ≥ 100% | ≤ 119 | ≥ 0.2243 |  | Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Silicate Vapour Geysers, Water Geysers |
| Rocky body | Methane | Methane ≥ 100% | 78 – 109 | ≤ 0.1295 | ≥ 0.009869 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers |
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.04935 | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.04935 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Niobium | 2,081 |
| Blue | mats: Tin | 1,847 |
| Lime | mats: Mercury | 1,502 |
| Peach | mats: Molybdenum | 1,956 |
| Red | mats: Tungsten | 1,714 |
| White | mats: Cadmium | 2,275 |

![Temperature, gravity and atmosphere distributions for Osseus Discus](species_figures/Osseus_Discus.png)

![Right share across temperature and gravity for Osseus Discus](temp_gravity_figures/Osseus_Discus.png)

#### Osseus Fractus

On complete bodies with the final rules: precision 89.6%, recall 99.9% (8,038 right, 931 wrong, 8 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 190 | ≤ 0.2814 | ≥ 0.02467 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 180 – 190 | ≤ 0.2814 | ≥ 0.02467 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: T | 673 |
| Green | star: TTS | 113 |
| Grey | star: G | 6,263 |
| Indigo | star: K | 9,134 |
| Lime | star: A | 1,959 |
| Maroon | star: Y | 12 |
| Turquoise | star: F | 9,256 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Osseus Fractus](species_figures/Osseus_Fractus.png)

![Right share across temperature and gravity for Osseus Fractus](temp_gravity_figures/Osseus_Fractus.png)

#### Osseus Pellebantus

On complete bodies with the final rules: precision 90.8%, recall 99.9% (1,943 right, 196 wrong, 1 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 191 – 195.4 | ≤ 0.2814 | ≥ 0.05724 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 191 – 195.4 | ≤ 0.2814 | ≥ 0.05724 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: T | 166 |
| Green | star: TTS | 24 |
| Grey | star: G | 1,778 |
| Indigo | star: K | 2,803 |
| Lime | star: A | 669 |
| Maroon | star: Y | 4 |
| Turquoise | star: F | 2,818 |
| Yellow | star: O | 0 |

![Temperature, gravity and atmosphere distributions for Osseus Pellebantus](species_figures/Osseus_Pellebantus.png)

![Right share across temperature and gravity for Osseus Pellebantus](temp_gravity_figures/Osseus_Pellebantus.png)

#### Osseus Pumice

**Temperature and gravity limits:** dropped above 133.35 K or above 0.27459 g.

On complete bodies with the final rules: precision 95.9%, recall 99.1% (6,166 right, 262 wrong, 53 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Argon | Argon ≥ 100% | ≤ 93 | ≥ 0.102 |  | None |
| Rocky ice | Argon | Argon ≥ 100% or Nitrogen ≥ 0.06% | ≤ 134 | ≤ 0.2814 |  | None, Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Water Geysers |
| High metal content | Argon | Argon ≥ 100% | ≤ 82 | ≥ 0.102 |  | None |
| Rocky ice | ArgonRich |  | ≤ 79 | ≤ 0.2814 | ≥ 0.02961 | None |
| Rocky body | Methane | Methane ≥ 100% | 67 – 109 | ≤ 0.2814 |  | None, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| Rocky ice | Methane | Methane ≥ 100% |  | ≤ 0.2814 |  | None |
| High metal content | Methane | Methane ≥ 100% | 68 – 102 | ≤ 0.2814 |  | None, Major Silicate Vapour Geysers, Minor Silicate Vapour Geysers |
| Rocky body | Nitrogen | Nitrogen ≥ 100% | ≤ 52 | ≤ 0.2814 | ≤ 0.009869 | None |
| Rocky ice | Nitrogen | Nitrogen ≥ 100% | ≤ 81 | ≤ 0.2814 |  | None |
| High metal content | Nitrogen | Nitrogen ≥ 100% | ≤ 51 | ≤ 0.2814 | ≤ 0.003948 |  |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Gold | mats: Ruthenium | 5,952 |
| Green | mats: Tellurium | 5,732 |
| Lime | mats: Technetium | 3,737 |
| Peach | mats: Polonium | 2,889 |
| White | mats: Antimony | 4,829 |
| Yellow | mats: Yttrium | 5,858 |

![Temperature, gravity and atmosphere distributions for Osseus Pumice](species_figures/Osseus_Pumice.png)

![Right share across temperature and gravity for Osseus Pumice](temp_gravity_figures/Osseus_Pumice.png)

#### Osseus Spiralis

**Temperature and gravity limits:** dropped above 176.63 K or above 0.27408 g.

On complete bodies with the final rules: precision 90.3%, recall 99.6% (20,368 right, 2,198 wrong, 75 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 |  | None, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky ice | Ammonia | Ammonia ≥ 100% | ≥ 160.7 | ≤ 0.2814 | 0.001974 – 0.0148 | None |
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.2814 |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: T | 2,008 |
| Green | star: TTS | 313 |
| Grey | star: G | 18,505 |
| Indigo | star: K | 23,794 |
| Lime | star: A | 8,034 |
| Maroon | star: Y | 32 |
| Turquoise | star: F | 26,506 |
| Yellow | star: O | 2 |

![Temperature, gravity and atmosphere distributions for Osseus Spiralis](species_figures/Osseus_Spiralis.png)

![Right share across temperature and gravity for Osseus Spiralis](temp_gravity_figures/Osseus_Spiralis.png)

### Recepta

#### Recepta Conditivus

**Temperature and gravity limits:** dropped below 132.12 K or below 0.05098 g or above 0.2746 g.

On complete bodies with the final rules: precision 33.4%, recall 98.5% (2,650 right, 5,277 wrong, 39 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | CarbonDioxide | SulphurDioxide ≥ 1% | 158 – 190 | ≤ 0.2814 |  | None, Major Water Geysers |
| Rocky body | CarbonDioxide | SulphurDioxide ≥ 1% | 150 – 195.4 | ≤ 0.1835 | ≥ 0.005132 | None |
| High metal content | CarbonDioxide | SulphurDioxide ≥ 1% | 160 – 170 | ≤ 0.1326 | ≤ 0.03948 |  |
| Icy body | Oxygen | Oxygen ≥ 65% or SulphurDioxide ≥ 10% | ≤ 170 | ≥ 0.2345 | ≤ 0.04441 | None, Major Water Geysers |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 1% | ≤ 273 | ≤ 0.2814 |  |  |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 166 | ≤ 0.2814 | ≤ 0.00454 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% or Ammonia ≥ 25% | 147.7 – 234 | ≤ 0.2814 |  | None, Major Water Geysers, Minor Carbon Dioxide Geysers, Minor Water Geysers, Rocky Magma, Water Geysers |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 135 | ≥ 0.2743 | ≤ 0.001184 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Aquamarine | mats: Technetium | 536 |
| Cyan | mats: Tellurium | 952 |
| Green | mats: Yttrium | 838 |
| Lime | mats: Antimony | 779 |
| White | mats: Polonium | 422 |
| Yellow | mats: Ruthenium | 895 |

![Temperature, gravity and atmosphere distributions for Recepta Conditivus](species_figures/Recepta_Conditivus.png)

![Right share across temperature and gravity for Recepta Conditivus](temp_gravity_figures/Recepta_Conditivus.png)

#### Recepta Deltahedronix

**Temperature and gravity limits:** dropped below 0.04488 g or above 0.27483 g.

On complete bodies with the final rules: precision 27.6%, recall 96.5% (848 right, 2,222 wrong, 31 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Icy body | CarbonDioxide | SulphurDioxide ≥ 1% | ≥ 167 | ≤ 0.2814 | ≥ 0.009869 |  |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 98.9% or SulphurDioxide ≥ 1% | ≤ 195.4 | ≤ 0.2427 | ≥ 0.003948 | None |
| High metal content | CarbonDioxide | SulphurDioxide ≥ 1% | 180 – 195.4 | ≥ 0.1428 | ≥ 0.03948 |  |
| Icy body | CarbonDioxideRich | SulphurDioxide ≥ 1% | 170 – 180 | ≤ 0.2814 | ≥ 0.03948 |  |
| Icy body | Oxygen | SulphurDioxide ≥ 1% | ≤ 155 | ≥ 0.02447 | 0.02369 – 0.02763 |  |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 272 | ≥ 0.1917 |  | Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 207 | ≤ 0.2814 | ≤ 0.05922 | None, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 231 | ≥ 0.2243 |  | Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Metallic Magma, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers, Minor Water Geysers, Water Geysers |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 135 | ≤ 0.2814 | ≤ 0.001382 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Cyan | mats: Mercury | 339 |
| Gold | mats: Molybdenum | 493 |
| Lime | mats: Cadmium | 560 |
| Mulberry | mats: Niobium | 467 |
| Orange | mats: Tin | 440 |
| Red | mats: Tungsten | 393 |

![Temperature, gravity and atmosphere distributions for Recepta Deltahedronix](species_figures/Recepta_Deltahedronix.png)

![Right share across temperature and gravity for Recepta Deltahedronix](temp_gravity_figures/Recepta_Deltahedronix.png)

#### Recepta Umbrux

**Temperature and gravity limits:** dropped on SulphurDioxide atmosphere: above 272.84 K or below 0.04071 g or above 0.27464 g.

On complete bodies with the final rules: precision 44.6%, recall 99.2% (3,744 right, 4,659 wrong, 32 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | SulphurDioxide ≥ 1% | 166 – 167 | ≥ 0.1326 | ≤ 0.004935 | None |
| Icy body | CarbonDioxide | SulphurDioxide ≥ 1% | 158 – 201 | ≥ 0.2549 |  | None, Major Water Geysers, Minor Methane Magma |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 98.94% or SulphurDioxide ≥ 1.05% | 149 – 195.4 | ≤ 0.2141 | ≥ 0.004737 | None |
| High metal content | CarbonDioxide | SulphurDioxide ≥ 1% | 152 – 180 | ≤ 0.2814 | ≥ 0.006908 | None |
| Icy body | CarbonDioxideRich | SulphurDioxide ≥ 1% | 170 – 180 | ≤ 0.2814 | ≥ 0.05922 | Carbon Dioxide Geysers |
| Icy body | SulphurDioxide | SulphurDioxide ≥ 65% or Ammonia ≥ 25% | ≤ 273 | ≤ 0.2814 |  | None, Carbon Dioxide Geysers, Major Water Geysers, Major Water Magma, Minor Ammonia Magma, Minor Carbon Dioxide Geysers, Minor Methane Magma, Minor Nitrogen Magma, Minor Water Magma, Water Geysers, Water Magma |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 211 | ≤ 0.2814 | ≤ 0.01875 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky ice | SulphurDioxide | SulphurDioxide ≥ 70% or Ammonia ≥ 15% | ≤ 230 | 0.2141 – 0.2814 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Major Water Geysers, Major Water Magma, Minor Carbon Dioxide Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 152 | ≤ 0.2814 | ≤ 0.002073 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: A | 830 |
| Emerald | star: N | 115 |
| Grey | star: Ae | 3 |
| Lime | star: Y | 11 |
| Maroon | star: M | 1,845 |
| Mauve | star: F | 1,593 |
| Ocher | star: L | 290 |
| Orange | star: G | 944 |
| Red | star: K | 1,703 |
| Sage | star: TTS | 76 |
| Teal | star: T | 76 |
| Turquoise | star: B | 45 |
| Yellow | star: D | 9 |

![Temperature, gravity and atmosphere distributions for Recepta Umbrux](species_figures/Recepta_Umbrux.png)

![Right share across temperature and gravity for Recepta Umbrux](temp_gravity_figures/Recepta_Umbrux.png)

### Stratum

#### Stratum Araneamus

**Correction:** dropped when chosen star at or below 7,500 K.

On complete bodies with the final rules: precision 76.0%, recall 99.7% (4,761 right, 1,500 wrong, 16 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Volcanism |
|---|---|---|---|---|---|
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 1.2% | 165 – 373 | 0.2651 – 0.5812 | None, Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | always | 8,323 |

![Temperature, gravity and atmosphere distributions for Stratum Araneamus](species_figures/Stratum_Araneamus.png)

![Right share across temperature and gravity for Stratum Araneamus](temp_gravity_figures/Stratum_Araneamus.png)

#### Stratum Cucumisis

On complete bodies with the final rules: precision 69.8%, recall 98.3% (5,513 right, 2,386 wrong, 95 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.5% | 191 – 373 | ≤ 0.6016 |  | None, Major Silicate Vapour Geysers, Minor Metallic Magma | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |
| Rocky body | CarbonDioxideRich | oxygen ≥ 70% | 195 – 246 | ≤ 0.5914 | ≥ 0.01283 | None, Metallic Magma | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |
| Rocky body | Oxygen | SulphurDioxide ≥ 0.1% | ≥ 194 | 0.3977 – 0.6016 | ≥ 0.01974 | None, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 1.8% | 191 – 373 | 0.2651 – 0.5914 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 71 |
| Emerald | star: F | 6,634 |
| Green | star: M | 714 |
| Grey | star: T | 107 |
| Indigo | star: Y | 11 |
| Lime | star: K | 3,229 |
| Mauve | star: D | 5 |
| Red | star: W | 1 |
| Teal | star: Ae | 6 |
| Turquoise | star: L | 152 |

![Temperature, gravity and atmosphere distributions for Stratum Cucumisis](species_figures/Stratum_Cucumisis.png)

![Right share across temperature and gravity for Stratum Cucumisis](temp_gravity_figures/Stratum_Cucumisis.png)

#### Stratum Excutitus

On complete bodies with the final rules: precision 46.9%, recall 95.1% (7,760 right, 8,783 wrong, 398 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 165 – 190 | ≤ 0.4997 | ≥ 0.001974 | None | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky body | Oxygen |  | 165 – 190 | ≤ 0.4793 | ≥ 0.001974 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 1, 4, 7, 8, 16, 17, 18, 35 |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 11% | 165 – 190 | 0.2753 – 0.4181 | ≥ 0.001974 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma | 1, 4, 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 85 |
| Emerald | star: F | 10,561 |
| Green | star: M | 2,107 |
| Grey | star: T | 61 |
| Indigo | star: Y | 14 |
| Lime | star: K | 11,599 |
| Mauve | star: D | 25 |
| Red | star: W | 1 |
| Teal | star: Ae | 6 |
| Turquoise | star: L | 181 |

![Temperature, gravity and atmosphere distributions for Stratum Excutitus](species_figures/Stratum_Excutitus.png)

![Right share across temperature and gravity for Stratum Excutitus](temp_gravity_figures/Stratum_Excutitus.png)

#### Stratum Frigus

On complete bodies with the final rules: precision 66.6%, recall 99.1% (1,268 right, 635 wrong, 12 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.9% | 191 – 368 | ≤ 0.571 |  | None, Metallic Magma, Minor Metallic Magma | 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| Rocky body | CarbonDioxideRich | SulphurDioxide ≥ 0.08% | 205 – 246 | ≤ 0.571 | ≥ 0.02961 | None | 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| Rocky body | Oxygen |  | ≥ 194 | ≤ 0.4589 | ≥ 0.01974 | Metallic Magma, Minor Metallic Magma | 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 2% | 191 – 372 | ≥ 0.2957 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 25 |
| Emerald | star: F | 1,744 |
| Green | star: M | 138 |
| Grey | star: T | 27 |
| Indigo | star: Y | 6 |
| Lime | star: K | 803 |
| Mauve | star: D | 2 |
| Red | star: W | 0 |
| Teal | star: Ae | 6 |
| Turquoise | star: L | 37 |

![Temperature, gravity and atmosphere distributions for Stratum Frigus](species_figures/Stratum_Frigus.png)

![Right share across temperature and gravity for Stratum Frigus](temp_gravity_figures/Stratum_Frigus.png)

#### Stratum Laminamus

**Correction:** dropped when surface temperature at or below 165 K.

On complete bodies with the final rules: precision 24.0%, recall 99.7% (2,211 right, 7,001 wrong, 6 missed).

| Body | Atmosphere | Composition | Temp (K) | Volcanism |
|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% or SulphurDioxide ≥ 0.49% | ≥ 161 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 31 |
| Emerald | star: F | 5,037 |
| Green | star: M | 1,000 |
| Grey | star: T | 23 |
| Indigo | star: Y | 3 |
| Lime | star: K | 4,307 |
| Mauve | star: D | 5 |
| Red | star: W | 1 |
| Teal | star: Ae | 0 |
| Turquoise | star: L | 42 |

![Temperature, gravity and atmosphere distributions for Stratum Laminamus](species_figures/Stratum_Laminamus.png)

![Right share across temperature and gravity for Stratum Laminamus](temp_gravity_figures/Stratum_Laminamus.png)

#### Stratum Limaxus

On complete bodies with the final rules: precision 48.8%, recall 99.8% (2,889 right, 3,029 wrong, 5 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 165 – 190 | ≤ 0.3977 | ≥ 0.004935 | None | 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| Rocky body | Oxygen |  | 165 – 190 | ≤ 0.4895 | ≥ 0.01974 | Metallic Magma | 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 1.3% | 165 – 190 | 0.2753 – 0.4181 |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers | 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 18 |
| Emerald | star: F | 4,062 |
| Green | star: M | 633 |
| Grey | star: T | 18 |
| Indigo | star: Y | 7 |
| Lime | star: K | 3,935 |
| Mauve | star: D | 12 |
| Red | star: W | 0 |
| Teal | star: Ae | 2 |
| Turquoise | star: L | 56 |

![Temperature, gravity and atmosphere distributions for Stratum Limaxus](species_figures/Stratum_Limaxus.png)

![Right share across temperature and gravity for Stratum Limaxus](temp_gravity_figures/Stratum_Limaxus.png)

#### Stratum Paleas

On complete bodies with the final rules: precision 60.2%, recall 99.8% (28,190 right, 18,629 wrong, 69 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% or SulphurDioxide ≥ 0.5% | ≥ 165 |  |  | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Minor Water Magma |
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.5% | 165 – 447 | ≤ 0.6016 |  | None, Metallic Magma, Minor Metallic Magma |
| Rocky body | CarbonDioxideRich | Oxygen ≥ 63% or Nitrogen ≥ 50% | 170 – 257 | ≤ 0.6118 | ≥ 0.009869 | None, Metallic Magma, Minor Metallic Magma |
| Rocky body | Oxygen | SulphurDioxide ≥ 0.8% | ≥ 167 | ≤ 0.6016 | ≥ 0.01974 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| Rocky body | Water | Water ≥ 100% | ≥ 390 |  | ≥ 0.04935 | None, Minor Water Magma |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 248 |
| Emerald | star: F | 43,119 |
| Green | star: M | 8,263 |
| Grey | star: T | 198 |
| Indigo | star: Y | 31 |
| Lime | star: K | 39,473 |
| Mauve | star: D | 76 |
| Red | star: W | 1 |
| Teal | star: Ae | 10 |
| Turquoise | star: L | 598 |

![Temperature, gravity and atmosphere distributions for Stratum Paleas](species_figures/Stratum_Paleas.png)

![Right share across temperature and gravity for Stratum Paleas](temp_gravity_figures/Stratum_Paleas.png)

#### Stratum Tectonicas

On complete bodies with the final rules: precision 67.0%, recall 99.4% (77,590 right, 38,244 wrong, 476 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| High metal content | Ammonia | Ammonia ≥ 100% or SulphurDioxide ≥ 0.6% | ≥ 165 |  |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Minor Silicate Vapour Geysers |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.6% | 165 – 430 | ≤ 0.622 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| High metal content | CarbonDioxideRich | Oxygen ≥ 57% or Argon ≥ 50% or SulphurDioxide ≥ 0.9% | 165 – 246 | ≤ 0.622 | ≥ 0.006908 | None |
| High metal content | Oxygen | SulphurDioxide ≥ 0.8% | ≥ 165 | ≤ 0.5303 | ≥ 0.01974 | None, Major Rocky Magma, Major Silicate Vapour Geysers, Minor Metallic Magma, Minor Silicate Vapour Geysers |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% or CarbonDioxide ≥ 0.7% | 165 – 450 | 0.2855 – 0.6322 |  | None, Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Silicate Vapour Geysers, Rocky Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 |  |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: TTS | 1,006 |
| Emerald | star: F | 27,326 |
| Green | star: M | 59,953 |
| Grey | star: T | 6,515 |
| Indigo | star: Y | 88 |
| Lime | star: K | 55,491 |
| Mauve | star: D | 47 |
| Red | star: W | 2 |
| Teal | star: Ae | 1 |
| Turquoise | star: L | 22,237 |

![Temperature, gravity and atmosphere distributions for Stratum Tectonicas](species_figures/Stratum_Tectonicas.png)

![Right share across temperature and gravity for Stratum Tectonicas](temp_gravity_figures/Stratum_Tectonicas.png)

### Tubers

#### Albidum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.3059 | ≤ 0 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Rocky Magma | 9, 10, 11 |
| High metal content | None | 200 – 500 | ≤ 3.059 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Rocky Magma, Rocky Magma | 9, 10, 11 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

#### Blatteum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.5099 | ≤ 0 | Major Metallic Magma, Major Silicate Vapour Geysers, Minor Metallic Magma | 6, 7, 8, 9, 10, 18, 19 |
| High metal content | None | 200 – 500 | ≤ 3.059 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Rocky Magma | 6, 7, 8, 9, 10, 18, 19 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Blatteum Sinuous Tubers](species_figures/Blatteum_Sinuous_Tubers.png)

#### Caeruleum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.7138 | ≤ 0 | Major Silicate Vapour Geysers | 1, 2, 3, 4, 5, 8, 9, 10, 11 |
| High metal content | None | 200 – 500 | ≤ 3.059 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers | 1, 2, 3, 4, 5, 8, 9, 10, 11 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Caeruleum Sinuous Tubers](species_figures/Caeruleum_Sinuous_Tubers.png)

#### Lindigoticum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.5099 | ≤ 0 | Major Metallic Magma, Major Silicate Vapour Geysers | 4, 5, 6, 7, 8, 9, 10, 18, 19 |
| High metal content | None | 200 – 500 | ≤ 2.039 | ≤ 0 | Major Rocky Magma, Metallic Magma, Rocky Magma | 4, 5, 6, 7, 8, 9, 10, 18, 19 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

#### Prasinum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.3059 | ≤ 0 | Major Metallic Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma | 1, 2, 3, 4, 5, 8, 9, 10, 11 |
| High metal content | None | 200 – 500 | ≤ 3.059 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers, Minor Rocky Magma, Rocky Magma | 1, 2, 3, 4, 5, 8, 9, 10, 11 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Prasinum Sinuous Tubers](species_figures/Prasinum_Sinuous_Tubers.png)

#### Roseum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.3059 | ≤ 0 | Major Silicate Vapour Geysers, Minor Rocky Magma | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 18, 19 |
| High metal content | None | 200 – 500 | ≤ 4.079 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Minor Rocky Magma, Rocky Magma | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 18, 19 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

![Temperature, gravity and atmosphere distributions for Roseum Sinuous Tubers](species_figures/Roseum_Sinuous_Tubers.png)

#### Violaceum Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.5099 | ≤ 0 | Major Metallic Magma, Major Silicate Vapour Geysers | 2, 3, 4, 5, 6, 7, 8, 9, 10, 18, 19 |
| High metal content | None | 200 – 500 | ≤ 3.059 | ≤ 0 | Major Rocky Magma, Major Silicate Vapour Geysers, Metallic Magma, Rocky Magma | 2, 3, 4, 5, 6, 7, 8, 9, 10, 18, 19 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

#### Viride Sinuous Tubers

| Body | Atmosphere | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|
| Rocky body | None | 200 – 500 | ≤ 0.3059 | ≤ 0 | Major Silicate Vapour Geysers | 3, 4, 5, 6, 7, 8, 9, 10, 18, 19 |
| High metal content | None | 200 – 500 | ≤ 2.039 | ≤ 0 | Major Rocky Magma, Metallic Magma, Rocky Magma | 3, 4, 5, 6, 7, 8, 9, 10, 18, 19 |

The colour is part of this name, so each colour is listed as its own entry. Spansh has few records of it, so section 7.7 scores it by genus list and any figure below isn't representative.

No confirmed records on database bodies, so no figure.

### Tubus

#### Tubus Cavas

**Correction:** dropped when gravity > 0.1515 g.

**Temperature and gravity limits:** dropped above 0.14719 g.

On complete bodies with the final rules: precision 55.6%, recall 99.6% (6,957 right, 5,548 wrong, 30 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 160 – 197 | ≤ 0.156 | ≥ 0.002961 | None | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: N | 795 |
| Emerald | star: B | 11 |
| Green | star: O | 0 |
| Grey | star: F | 7,375 |
| Indigo | star: A | 2,207 |
| Lime | star: W | 0 |
| Maroon | star: K | 6,713 |
| Mauve | star: T | 7 |
| Ocher | star: TTS | 27 |
| Red | star: G | 5,732 |
| Teal | star: M | 932 |
| Turquoise | star: L | 75 |
| Yellow | star: D | 13 |

![Temperature, gravity and atmosphere distributions for Tubus Cavas](species_figures/Tubus_Cavas.png)

![Right share across temperature and gravity for Tubus Cavas](temp_gravity_figures/Tubus_Cavas.png)

#### Tubus Compagibus

**Correction:** dropped when gravity > 0.1515 g.

**Temperature and gravity limits:** dropped above 0.14704 g.

On complete bodies with the final rules: precision 67.1%, recall 99.5% (17,804 right, 8,711 wrong, 93 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 160 – 197 | ≤ 0.1632 | ≥ 0.002961 | None | 1, 4, 9, 18, 19, 20, 21, 22, 23, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: N | 888 |
| Emerald | star: B | 22 |
| Green | star: O | 0 |
| Grey | star: F | 15,663 |
| Indigo | star: A | 3,741 |
| Lime | star: W | 3 |
| Maroon | star: K | 15,993 |
| Mauve | star: T | 15 |
| Ocher | star: TTS | 118 |
| Red | star: G | 13,613 |
| Teal | star: M | 2,714 |
| Turquoise | star: L | 248 |
| Yellow | star: D | 15 |

![Temperature, gravity and atmosphere distributions for Tubus Compagibus](species_figures/Tubus_Compagibus.png)

![Right share across temperature and gravity for Tubus Compagibus](temp_gravity_figures/Tubus_Compagibus.png)

#### Tubus Conifer

**Temperature and gravity limits:** dropped above 195.3 K or below 0.04103 g or above 0.14707 g.

On complete bodies with the final rules: precision 69.6%, recall 99.4% (6,507 right, 2,836 wrong, 39 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 160 – 195.4 | ≤ 0.155 | ≥ 0.002961 | None | 1, 3, 7, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: N | 323 |
| Emerald | star: B | 12 |
| Green | star: O | 0 |
| Grey | star: F | 5,497 |
| Indigo | star: A | 1,429 |
| Lime | star: W | 0 |
| Maroon | star: K | 5,817 |
| Mauve | star: T | 11 |
| Ocher | star: TTS | 30 |
| Red | star: G | 4,430 |
| Teal | star: M | 791 |
| Turquoise | star: L | 83 |
| Yellow | star: D | 15 |

![Temperature, gravity and atmosphere distributions for Tubus Conifer](species_figures/Tubus_Conifer.png)

![Right share across temperature and gravity for Tubus Conifer](temp_gravity_figures/Tubus_Conifer.png)

#### Tubus Rosarium

**Correction:** dropped when gravity > 0.1515 g.

**Temperature and gravity limits:** dropped above 0.14996 g.

On complete bodies with the final rules: precision 43.3%, recall 99.4% (3,209 right, 4,197 wrong, 18 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) |
|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.1672 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: N | 287 |
| Emerald | star: B | 16 |
| Green | star: O | 1 |
| Grey | star: F | 6,120 |
| Indigo | star: A | 2,224 |
| Lime | star: W | 0 |
| Maroon | star: K | 4,595 |
| Mauve | star: T | 17 |
| Ocher | star: TTS | 25 |
| Red | star: G | 4,639 |
| Teal | star: M | 651 |
| Turquoise | star: L | 97 |
| Yellow | star: D | 8 |

![Temperature, gravity and atmosphere distributions for Tubus Rosarium](species_figures/Tubus_Rosarium.png)

![Right share across temperature and gravity for Tubus Rosarium](temp_gravity_figures/Tubus_Rosarium.png)

#### Tubus Sororibus

**Correction:** dropped when gravity > 0.1515 g.

**Temperature and gravity limits:** dropped above 0.14971 g; on CarbonDioxide atmosphere: above 0.14811 g.

On complete bodies with the final rules: precision 69.4%, recall 99.3% (3,328 right, 1,466 wrong, 25 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| High metal content | Ammonia | Ammonia ≥ 100% | ≥ 160 | ≤ 0.156 |  | None |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 160 – 195.4 | ≤ 0.156 | ≥ 0.001974 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Amethyst | star: N | 153 |
| Emerald | star: B | 2 |
| Green | star: O | 0 |
| Grey | star: F | 858 |
| Indigo | star: A | 206 |
| Lime | star: W | 0 |
| Maroon | star: K | 2,909 |
| Mauve | star: T | 1,430 |
| Ocher | star: TTS | 42 |
| Red | star: G | 686 |
| Teal | star: M | 3,990 |
| Turquoise | star: L | 2,765 |
| Yellow | star: D | 3 |

![Temperature, gravity and atmosphere distributions for Tubus Sororibus](species_figures/Tubus_Sororibus.png)

![Right share across temperature and gravity for Tubus Sororibus](temp_gravity_figures/Tubus_Sororibus.png)

### Tussock

#### Tussock Albata

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 100.0% (8,226 right, 1 wrong, 2 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 175 – 180 | ≤ 0.2427 | ≥ 0.01579 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 175 – 180 | ≤ 0.2814 | ≥ 0.01579 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 3,728 |
| Green | star: K | 8,993 |
| Lime | star: G | 5,001 |
| Maroon | star: D | 14 |
| Orange | star: W | 1 |
| Red | star: Y | 8 |
| Sage | star: L | 1,376 |
| Teal | star: T | 510 |
| Yellow | star: F, N | 6,852 |

![Temperature, gravity and atmosphere distributions for Tussock Albata](species_figures/Tussock_Albata.png)

![Right share across temperature and gravity for Tussock Albata](temp_gravity_figures/Tussock_Albata.png)

#### Tussock Capillum

**Temperature and gravity limits:** dropped above 0.27474 g.

On complete bodies with the final rules: precision 81.6%, recall 99.2% (2,639 right, 596 wrong, 21 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Argon |  | 80 – 129 | ≥ 0.2743 | ≥ 0.001974 |  |
| Rocky ice | Argon |  | 80 – 129 | ≥ 0.2243 |  |  |
| Rocky body | Methane | Methane ≥ 100% | 80 – 129 | ≤ 0.2814 |  |  |
| Rocky ice | Methane | Methane ≥ 100% | 80 – 129 | ≤ 0.2814 |  | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 8,137 |
| Green | star: K | 1,706 |
| Lime | star: G | 841 |
| Maroon | star: D | 10 |
| Orange | star: W | 0 |
| Red | star: Y | 120 |
| Sage | star: L | 931 |
| Teal | star: T | 457 |
| Yellow | star: F, N | 686 |

![Temperature, gravity and atmosphere distributions for Tussock Capillum](species_figures/Tussock_Capillum.png)

![Right share across temperature and gravity for Tussock Capillum](temp_gravity_figures/Tussock_Capillum.png)

#### Tussock Caputus

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 99.9% (7,745 right, 0 wrong, 7 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 181 – 190 | ≤ 0.2427 | ≥ 0.02724 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 181 – 190 | ≤ 0.2814 | ≥ 0.02724 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 4,226 |
| Green | star: K | 9,095 |
| Lime | star: G | 6,202 |
| Maroon | star: D | 10 |
| Orange | star: W | 0 |
| Red | star: Y | 7 |
| Sage | star: L | 1,602 |
| Teal | star: T | 457 |
| Yellow | star: F, N | 7,645 |

![Temperature, gravity and atmosphere distributions for Tussock Caputus](species_figures/Tussock_Caputus.png)

![Right share across temperature and gravity for Tussock Caputus](temp_gravity_figures/Tussock_Caputus.png)

#### Tussock Catena

**Temperature and gravity limits:** dropped above 0.27438 g.

On complete bodies with the final rules: precision 100.0%, recall 99.9% (4,727 right, 1 wrong, 6 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Metallic Magma, Minor Metallic Magma, Minor Rocky Magma, Minor Water Magma | 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Rocky Magma, Major Silicate Vapour Geysers | 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 2,567 |
| Green | star: K | 3,739 |
| Lime | star: G | 4,410 |
| Maroon | star: D | 9 |
| Orange | star: W | 0 |
| Red | star: Y | 12 |
| Sage | star: L | 1,078 |
| Teal | star: T | 368 |
| Yellow | star: F, N | 6,741 |

![Temperature, gravity and atmosphere distributions for Tussock Catena](species_figures/Tussock_Catena.png)

![Right share across temperature and gravity for Tussock Catena](temp_gravity_figures/Tussock_Catena.png)

#### Tussock Cultro

**Temperature and gravity limits:** dropped above 0.27428 g.

On complete bodies with the final rules: precision 98.7%, recall 99.8% (15,399 right, 197 wrong, 37 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 |  | 1, 4, 7, 8, 16, 17, 18, 35 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Silicate Vapour Geysers, Minor Metallic Magma | 1, 4, 7, 8, 16, 17, 18, 35 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 9,646 |
| Green | star: K | 15,696 |
| Lime | star: G | 11,107 |
| Maroon | star: D | 20 |
| Orange | star: W | 3 |
| Red | star: Y | 18 |
| Sage | star: L | 4,192 |
| Teal | star: T | 1,400 |
| Yellow | star: F, N | 14,734 |

![Temperature, gravity and atmosphere distributions for Tussock Cultro](species_figures/Tussock_Cultro.png)

![Right share across temperature and gravity for Tussock Cultro](temp_gravity_figures/Tussock_Cultro.png)

#### Tussock Divisa

**Temperature and gravity limits:** dropped above 0.27443 g.

On complete bodies with the final rules: precision 100.0%, recall 99.8% (5,971 right, 0 wrong, 10 missed).

| Body | Atmosphere | Composition | Gravity (g) | Volcanism | Regions |
|---|---|---|---|---|---|
| Rocky body | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 |  | 3, 15, 30, 32, 33, 34, 36, 38, 39 |
| High metal content | Ammonia | Ammonia ≥ 100% | ≤ 0.2814 | None, Major Rocky Magma, Minor Carbo Dioxide Geysers, Minor Silicate Vapour Geysers | 3, 15, 30, 32, 33, 34, 36, 38, 39 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 4,093 |
| Green | star: K | 6,835 |
| Lime | star: G | 4,262 |
| Maroon | star: D | 6 |
| Orange | star: W | 0 |
| Red | star: Y | 7 |
| Sage | star: L | 1,562 |
| Teal | star: T | 457 |
| Yellow | star: F, N | 5,971 |

![Temperature, gravity and atmosphere distributions for Tussock Divisa](species_figures/Tussock_Divisa.png)

![Right share across temperature and gravity for Tussock Divisa](temp_gravity_figures/Tussock_Divisa.png)

#### Tussock Ignis

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 99.9% (15,959 right, 1 wrong, 10 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 161 – 170 | ≤ 0.1744 | 0.002763 – 0.05231 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 161 – 170 | ≤ 0.1999 | 0.002763 – 0.05231 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 4,859 |
| Green | star: K | 14,211 |
| Lime | star: G | 11,243 |
| Maroon | star: D | 14 |
| Orange | star: W | 4 |
| Red | star: Y | 19 |
| Sage | star: L | 1,893 |
| Teal | star: T | 808 |
| Yellow | star: F, N | 13,605 |

![Temperature, gravity and atmosphere distributions for Tussock Ignis](species_figures/Tussock_Ignis.png)

![Right share across temperature and gravity for Tussock Ignis](temp_gravity_figures/Tussock_Ignis.png)

#### Tussock Pennata

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 100.0% (2,269 right, 0 wrong, 1 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 145 – 154 | ≤ 0.8056 | 0.002763 – 0.01115 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 145 – 154 | ≤ 0.09177 | 0.002763 – 0.01115 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 592 |
| Green | star: K | 2,216 |
| Lime | star: G | 2,387 |
| Maroon | star: D | 3 |
| Orange | star: W | 1 |
| Red | star: Y | 3 |
| Sage | star: L | 201 |
| Teal | star: T | 80 |
| Yellow | star: F, N | 3,154 |

![Temperature, gravity and atmosphere distributions for Tussock Pennata](species_figures/Tussock_Pennata.png)

![Right share across temperature and gravity for Tussock Pennata](temp_gravity_figures/Tussock_Pennata.png)

#### Tussock Pennatis

On complete bodies with the final rules: precision 86.0%, recall 100.0% (8,884 right, 1,445 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 146 – 195.4 | ≤ 0.2427 | ≥ 0.001974 | None | 1, 2, 5, 6, 13, 14, 27, 29, 31, 37, 41 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 146 – 195.4 | ≤ 0.2814 | ≥ 0.001974 | None | 1, 2, 5, 6, 13, 14, 27, 29, 31, 37, 41 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 2,932 |
| Green | star: K | 8,730 |
| Lime | star: G | 5,742 |
| Maroon | star: D | 23 |
| Orange | star: W | 0 |
| Red | star: Y | 20 |
| Sage | star: L | 1,147 |
| Teal | star: T | 434 |
| Yellow | star: F, N | 8,766 |

![Temperature, gravity and atmosphere distributions for Tussock Pennatis](species_figures/Tussock_Pennatis.png)

![Right share across temperature and gravity for Tussock Pennatis](temp_gravity_figures/Tussock_Pennatis.png)

#### Tussock Propagito

On complete bodies with the final rules: precision 92.7%, recall 100.0% (17,315 right, 1,373 wrong, 7 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 145 – 195.4 | ≤ 0.2427 | ≥ 0.002862 | None | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 145 – 195.4 | ≤ 0.2814 | ≥ 0.002862 | None | 1, 4, 9, 10, 11, 12, 24, 25, 26, 28, 42 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 4,152 |
| Green | star: K | 13,930 |
| Lime | star: G | 10,481 |
| Maroon | star: D | 57 |
| Orange | star: W | 0 |
| Red | star: Y | 24 |
| Sage | star: L | 1,754 |
| Teal | star: T | 799 |
| Yellow | star: F, N | 16,311 |

![Temperature, gravity and atmosphere distributions for Tussock Propagito](species_figures/Tussock_Propagito.png)

![Right share across temperature and gravity for Tussock Propagito](temp_gravity_figures/Tussock_Propagito.png)

#### Tussock Serrati

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 99.9%, recall 99.9% (5,631 right, 3 wrong, 6 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 171 – 174 | ≤ 0.2019 | ≥ 0.009869 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 171 – 174 | ≤ 0.2315 | ≥ 0.009869 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 1,991 |
| Green | star: K | 6,322 |
| Lime | star: G | 4,484 |
| Maroon | star: D | 6 |
| Orange | star: W | 0 |
| Red | star: Y | 6 |
| Sage | star: L | 700 |
| Teal | star: T | 290 |
| Yellow | star: F, N | 5,360 |

![Temperature, gravity and atmosphere distributions for Tussock Serrati](species_figures/Tussock_Serrati.png)

![Right share across temperature and gravity for Tussock Serrati](temp_gravity_figures/Tussock_Serrati.png)

#### Tussock Stigmasis

**Temperature and gravity limits:** dropped above 0.27481 g.

On complete bodies with the final rules: precision 100.0%, recall 99.0% (1,293 right, 0 wrong, 13 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | SulphurDioxide | SulphurDioxide ≥ 100% | 132 – 168 | ≤ 0.2814 | ≤ 0.02171 |  |
| High metal content | SulphurDioxide | SulphurDioxide ≥ 100% | ≤ 152 | ≤ 0.2814 | ≤ 0.02171 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 287 |
| Green | star: K | 994 |
| Lime | star: G | 893 |
| Maroon | star: D | 3 |
| Orange | star: W | 0 |
| Red | star: Y | 20 |
| Sage | star: L | 58 |
| Teal | star: T | 51 |
| Yellow | star: F, N | 1,215 |

![Temperature, gravity and atmosphere distributions for Tussock Stigmasis](species_figures/Tussock_Stigmasis.png)

![Right share across temperature and gravity for Tussock Stigmasis](temp_gravity_figures/Tussock_Stigmasis.png)

#### Tussock Triticum

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 99.9% (2,159 right, 0 wrong, 2 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 191 – 195.4 | ≤ 0.2427 | ≥ 0.05724 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 191 – 195.4 | ≤ 0.2814 | ≥ 0.05724 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 1,568 |
| Green | star: K | 3,126 |
| Lime | star: G | 1,737 |
| Maroon | star: D | 3 |
| Orange | star: W | 0 |
| Red | star: Y | 2 |
| Sage | star: L | 642 |
| Teal | star: T | 147 |
| Yellow | star: F, N | 2,581 |

![Temperature, gravity and atmosphere distributions for Tussock Triticum](species_figures/Tussock_Triticum.png)

![Right share across temperature and gravity for Tussock Triticum](temp_gravity_figures/Tussock_Triticum.png)

#### Tussock Ventusa

**Correction:** regions 8, 16, 17 and 35 added, region 9 removed.

On complete bodies with the final rules: precision 100.0%, recall 99.9% (4,819 right, 1 wrong, 4 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism | Regions |
|---|---|---|---|---|---|---|---|
| Rocky body | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 155 – 160 | ≤ 0.1632 | 0.002862 – 0.02171 | None, Minor Carbon Dioxide Geysers | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |
| High metal content | CarbonDioxide | CarbonDioxide ≥ 100% or SulphurDioxide ≥ 0.99% | 155 – 160 | ≤ 0.1326 | 0.002862 – 0.02171 | None | 3, 7, 8, 15, 16, 17, 18, 19, 20, 21, 22, 23, 30, 32, 33, 34, 35, 36, 38, 39, 40 |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 1,212 |
| Green | star: K | 4,362 |
| Lime | star: G | 4,285 |
| Maroon | star: D | 9 |
| Orange | star: W | 4 |
| Red | star: Y | 7 |
| Sage | star: L | 483 |
| Teal | star: T | 218 |
| Yellow | star: F, N | 5,304 |

![Temperature, gravity and atmosphere distributions for Tussock Ventusa](species_figures/Tussock_Ventusa.png)

![Right share across temperature and gravity for Tussock Ventusa](temp_gravity_figures/Tussock_Ventusa.png)

#### Tussock Virgam

**Temperature and gravity limits:** dropped above 449.99 K.

On complete bodies with the final rules: precision 100.0%, recall 100.0% (3,925 right, 1 wrong, 0 missed).

| Body | Atmosphere | Composition | Temp (K) | Gravity (g) | Pressure (atm) | Volcanism |
|---|---|---|---|---|---|---|
| Rocky body | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.04935 | None, Minor Water Magma |
| High metal content | Water | Water ≥ 100% | ≥ 390 | ≤ 0.2814 | ≥ 0.04935 | None |

| Colour | Decided by | Confirmed records |
|---|---|---|
| Emerald | star: M | 2,610 |
| Green | star: K | 1,818 |
| Lime | star: G | 1,443 |
| Maroon | star: D | 5 |
| Orange | star: W | 0 |
| Red | star: Y | 1 |
| Sage | star: L | 1,727 |
| Teal | star: T | 439 |
| Yellow | star: F, N | 4,238 |

![Temperature, gravity and atmosphere distributions for Tussock Virgam](species_figures/Tussock_Virgam.png)

![Right share across temperature and gravity for Tussock Virgam](temp_gravity_figures/Tussock_Virgam.png)

