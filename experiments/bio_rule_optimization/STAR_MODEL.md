# Orbit-Averaged Star Model

> Superseded by `PREDICTION_ALGORITHM.md` section 6.1, which describes the model as it now runs
> (5,000 samples, candidates limited to the parent chain) and adds checks against exact results.
> This file is kept as the original development notes.

How we choose the star that sets a body's exobiology colour (and decides
star-dependent species), why SrvSurvey's choice goes wrong in some binaries,
and how the replacement works.

Code: `star_brightness.py` (model), `compare_star_models.py` (side-by-side
report for one body), `srvsurv_engine.py` (SrvSurvey port and its star logic).

The engine lets only stars in the body's parent chain and the barycentres on it compete,
the same candidates as SrvSurvey. Comparisons here let every star in the system compete,
which changes the star class on 40 of 3,880,306 bodies (`PREDICTION_ALGORITHM.md`, section 6.6).

## 1. What the star choice decides

Most genera have one colour per star class. For example, Stratum is Emerald
around F stars, Lime around K, Green around M and Mauve around white dwarfs.
Some species also need a particular class, such as Electricae Pluma near bright
stars. So the predictor has to decide which star counts for each body. In
multi-star systems this is the main source of wrong colours and missed species.

SrvSurvey and the new model both answer it the same way: **the star that
delivers the most light to the body**. They differ only in how they estimate
that light.

## 2. SrvSurvey's model (baseline)

From `BioPredictor.cs` and `SystemData.cs`, ported in
`srvsurv_engine.SystemContext`:

1. **Candidates**: the stars in the body's own parent chain. For each `Star`
   parent, that star. For each barycentre parent, every star orbiting it,
   directly or through further barycentres.
2. **Score** for each candidate:

   ```
   score = R² T⁴ / (d₁² + d₂²)
   ```

   - `R²T⁴` is proportional to luminosity.
   - `d₁²` is the sum of squared semi-major axes from the body up to the
     nearest parent it shares with the star.
   - `d₂²` is the same sum from the star up to that shared parent.
3. **Choice**: the highest score wins. Its type is flattened (all `D*` → `D`,
   `W*` → `W`, `C*` → `C`, giants → their class letter) and used as `star`.
   SrvSurvey has code to also consider a close second star, but it's switched
   off.

The score is a reasonable shortcut, but its distance has two flaws.

### Flaw 1: binary partners are placed too close

Two stars in a binary always sit on opposite sides of their barycentre, so
their separation is the **sum** of their distances from it. Take a planet at
`a_p` around star A, where A and B are `a_A` and `a_B` from their barycentre.
SrvSurvey's distance from the planet to B is

```
SrvSurvey:   d² = a_p² + a_A² + a_B²
actual:     ⟨d²⟩ ≈ a_p² + (a_A + a_B)²
```

The missing term `2·a_A·a_B` puts the partner too close and overstates its
light. When the planet's orbit is small compared with the binary, the factor
is `(a_A + a_B)² / (a_A² + a_B²)`. That is up to **2×** when `a_A = a_B`, and
it is still about 1.9× in Blau Eur below. Every confirmed SrvSurvey failure
so far is this case. The planet's own star loses to its partner, usually a
neutron star or a brighter main-sequence star.

### Flaw 2: averaging distance instead of light

Light falls off as `1/d²`, so the long-run light is the average of `1/d²`. The
sum of squares approximates the average of `d²`, and `1/⟨d²⟩` is always smaller
than `⟨1/d²⟩`. The gap grows when two orbits are of similar size. For two
coplanar circular orbits with unrelated phases, the exact result is

```
⟨1/d²⟩ = 1 / |a₁² − a₂²|        SrvSurvey uses   1 / (a₁² + a₂²)
```

A planet circling a binary at three times the stars' distance from the centre
gets about 20% too little light from each star. At twice the distance, it gets
40% too little. This flaw matters less in practice than Flaw 1, because it
usually affects both candidate stars in a similar way.

## 3. The new model

### 3.1 The quantity

Exobiology is fixed when the system is generated. It doesn't depend on where
the bodies happen to be today. The right quantity is therefore the **light a
body receives on average over all time** from each star:

```
flux(star) = L(star) × ⟨ 1 / |r_body(t) − r_star(t)|² ⟩_t
```

The star with the largest flux is chosen.

### 3.2 Luminosity

```
L = R² (T / 5772 K)⁴          (solar units; R = solarRadius, T = surfaceTemperature)
```

This follows from the Stefan–Boltzmann law and is the same luminosity term
SrvSurvey uses, so only the distance changes. A star with no radius or
temperature gets `L = 0` and can't win. `absoluteMagnitude` isn't used yet.

### 3.3 Rebuilding the orbit tree

Each body's chain comes from its `parent_N_type` / `parent_N_id` columns.
Barycentre rows in the Parquet data have no parent columns of their own. Their
parents are therefore **inferred from the bodies below them**. If a star's
chain is `Null 3 → Null 2 → Null 0`, then barycentre 3 orbits barycentre 2,
which orbits barycentre 0. Without this step, each barycentre's own orbit would
be dropped, and every star under it would be placed wrongly. The first version
had exactly this bug: it placed Slegi's neutron star at 10.8 AU instead of
23.9 AU.

The root (usually barycentre 0, which has no row) is the origin.

### 3.4 Positions from Kepler orbits

Each body has an orbit around its parent, defined by `semiMajorAxis` (a),
`orbitalEccentricity` (e), `orbitalInclination` (i), `ascendingNode` (Ω) and
`argOfPeriapsis` (ω). At mean anomaly `M`, which advances uniformly in time:

1. Solve Kepler's equation `M = E − e·sin E` for E (30 Newton steps).
2. In-plane position: `x = a(cos E − e)`, `y = a·√(1−e²)·sin E`.
3. Rotate into the system frame by ω, i and Ω (standard 3-1-3 Euler rotation).

A body's position relative to the root is the sum of these vectors up its
chain: moon around planet, planet around star, star around barycentre, and so
on.

### 3.5 Phases: what moves independently and what moves together

Averaging over time means averaging over every orbit's mean anomaly.

- **Unrelated orbits get independent phases.** A planet's place around its
  star has no fixed relation to where that star is around its partner, so over
  long times every combination occurs.
- **Binary partners share one phase.** Two children of the same parent with
  the same orbital period (within 1 part in 10⁶) are treated as the two halves
  of one binary orbit, and get the same mean anomaly. In the data, partners'
  arguments of periapsis differ by exactly 180°. Examples: Blau Eur A/B at
  149.9°/329.9°, Qiefoea C/D at 149.8°/329.8°, Graea Hypue B/C at
  193.1°/13.1°. Sharing the mean anomaly therefore keeps them on opposite
  sides automatically. This is the step that fixes Flaw 1.
- **Planets circling a binary** (for example Slegi BC 2 around barycentre 2)
  have a different period from the stars, so they move independently, as they
  should.

### 3.6 Averaging

Each independent phase is sampled at 200,000 uniform random values. The
average of `1/|r_body − r_star|²` over the samples gives `⟨1/d²⟩` directly, with
no formula needed for any particular arrangement. Orbits shared by the body and
the star, such as a barycentre both sit under, use the same samples on both
sides and cancel out exactly. So only the orbits that actually separate the
body from the star affect the result.

**Precision.** Across the four systems below, the relative standard error is
at most 0.11% per star. Over five random seeds, the winner's share varies by
under 0.05 percentage points. That is far below the margins that decide any
choice so far.

### 3.7 Candidates and choice

All stars in the system are candidates, not just those in the body's parent
chain. Distant stars contribute almost nothing, so this rarely matters. The
star with the largest flux wins, and its flattened type is passed to the
SrvSurvey criteria in place of SrvSurvey's choice.

## 4. Results so far

"Share" is each star's fraction of the total across all stars. Colours marked
confirmed come from the raw Spansh body records.

| Body | SrvSurvey pick | New model pick | Confirmed | Outcome |
|---|---|---|---|---|
| Blau Eur NI-T d3-40 B 6 | A Neutron (60%) | **B K-star (57%)** | Bacterium Aurasus – Green, Stratum Tectonicas – Lime, Tussock Ignis – Green | New model matches all three. SrvSurvey gives Indigo and Yellow and misses Stratum. |
| Qiefoea KZ-D d13-73 D 3 | C G-star (51%) | **D M-star (60%)** | Bacterium Alcyoneum – Teal, Stratum Tectonicas – Green, Tussock Divisa – Emerald | New model matches all three. SrvSurvey gets two colours wrong and misses Stratum. |
| Slegi AV-Y d3 BC 2 | B M-star | B M-star (85%) | Bacterium Aurasus – Teal, Tussock Serrati – Emerald | Both correct |
| Graea Hypue EO-G d11-101 D 4 a | A Neutron (81%) | A Neutron (70%) | No species data on Spansh | Unresolved. The new model moves toward D (L-dwarf, 17% → 29%) but keeps the neutron star. |

The distance the partner error produces, in the two fixed cases:

| Body | Wrongly chosen star | SrvSurvey distance | Orbit-averaged distance | Light overstated by |
|---|---|---|---|---|
| Blau Eur B 6 | A (partner of B) | 502 AU | 695 AU | ~1.9× |
| Qiefoea D 3 | C (partner of D) | 8.8 AU | 10.9 AU | ~1.5× |

## 5. When the two models agree

- **Single star in the chain**: there's only one candidate.
- **Body far outside a binary**: both stars are at about the body's own
  orbital distance. Both models give similar shares and the same choice
  (Slegi BC 2).
- **Clear winners**: most rankings aren't close, so the partner error doesn't
  change the result.

They disagree only when two candidates are close **and** one of them is the
partner of the body's own star. In that case SrvSurvey consistently favours
the partner.

## 6. Assumptions and limits

- **The game's own rule is unknown.** This model is physically motivated and
  fixes both confirmed failures, but two systems aren't proof. It has to be
  tested against colour ground truth (section 7).
- **Common reference plane.** Inclinations and nodes are assumed to be measured
  in one frame for the whole system. Elite doesn't document this. It matters
  only when the separating orbits are of similar size; with very different
  sizes, the result is insensitive to orientation.
- **Independent phases.** Orbital resonances are ignored. The time average
  assumes unrelated periods sweep through every relative position.
- **Missing data.** Missing e, i, Ω or ω are treated as 0 (circular and
  coplanar). If an intermediate barycentre has no row, its orbit is dropped and
  bodies and stars under it are placed as if it sat at its parent. (Until
  2026-10-31 the code instead stopped climbing there, placing such a body near
  the system root and often picking the wrong star with a share above 90%.) Eccentricity is capped
  at 0.999.
- **Partner detection by period.** Partners are identified by equal orbital
  periods under one parent. A barycentre with more than two children sharing a
  period would be ambiguous; none has been seen.
- **Luminosity from R and T only.** It's bolometric. If the game weighs
  visible light, or some other measure, a hot, small star such as a neutron
  star may count for less than this model gives it. `absoluteMagnitude` could
  test that.
- **Units.** Spansh distances are in AU and radii in solar radii. Only ratios
  between stars matter, so the units cancel.
- **Cost.** 200,000 samples per star and body is fine for single reports, but
  slow galaxy-wide. A full run needs fewer samples (the precision allows it)
  or an exact shortcut for the common nested case.

## 7. Validation needed

1. Collect colour ground truth: complete bodies' landmark `variant` values from
   the Spansh body endpoint, prioritising bodies where the two models choose
   different star classes.
2. For each colour-by-star genus, score how often each model's star class gives
   the confirmed colour, separately for bodies where the models disagree.
3. Try `absoluteMagnitude` as the luminosity alongside `R²T⁴`, to settle how
   neutron stars and white dwarfs should be weighted. Graea Hypue is the test
   case once its colours are known.

## 8. Running it

```bash
python -I experiments/bio_rule_optimization/compare_star_models.py \
  --sector Blau_Eur --body "Blau Eur NI-T d3-40 B 6" \
  --api-dir <folder of raw Spansh body records named <id64>.json>
```

For every star in the system, this prints its class, luminosity, SrvSurvey
share, orbit-averaged distance and flux share. It then prints the species and
colours each model's star choice predicts, and the confirmed ones if a raw
record is supplied. The model is the default star in `srvsurv_engine.py`; see
`PREDICTION_ALGORITHM.md` for the galaxy-wide results.

## Verdict on the full walk (2026-10-31)

Scored with `star_model_verdict.py` on the colour walk, after the barycentre fix
above. Where the two stars' predictions differ and exactly one names the
confirmed colour, the orbit star is right on 6,320 records and SrvSurvey's on 588
(2,896 systems to 277). On all 523,485 complete bodies, species + colour recall
is 99.23% against 99.00%. Most of SrvSurvey's remaining wins are bodies where
the orbit star is not the star the body directly orbits and SrvSurvey's is.
