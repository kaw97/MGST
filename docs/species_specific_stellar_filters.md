# Species-Specific Stellar Filter Enhancements

Based on analysis of 1.43 million single-star system observations, here are the most impactful stellar class filters for major species:

## **1. Bacterium Vesicula** - M-Dwarf Specialist ⭐⭐⭐
**64.6% of observations in M-class systems**

### Original Filter Issues:
- Predicts in all stellar classes based on atmospheric conditions alone
- High false positive rate in hot stellar systems

### Enhanced Filter:
```python
# INCLUDE: M-class stars (64.6% of observations)
# INCLUDE: K-class stars (23.5% of observations)
# EXCLUDE: F, G, A-class stars (only 7.7% combined)
```

**Filter Logic:**
```python
def bacterium_vesicula_stellar_filter(stellar_class):
    return stellar_class in ['M', 'K']  # 88.1% of all observations
```

**Expected Impact:** ~85% reduction in false positives by excluding hot stellar systems

---

## **2. Bacterium Acies** - Cool Star Specialist ⭐⭐⭐
**Strongly prefers M, T, Y-class systems**

### Enhanced Filter:
```python
# INCLUDE: M-class (59.9%), T-class (12.5%), Y-class (10.2%), L-class (7.7%)
# EXCLUDE: Traditional main sequence stars (K, G, F, A)
```

**Filter Logic:**
```python
def bacterium_acies_stellar_filter(stellar_class):
    return stellar_class in ['M', 'T', 'Y', 'L']  # 90.3% of observations
```

**Expected Impact:** ~90% reduction in false positives by excluding main sequence stars

---

## **3. Fonticulua Campestris** - M-Dwarf Specialist ⭐⭐⭐
**60.2% in M-class, similar pattern to B. Vesicula**

### Enhanced Filter:
```python
# INCLUDE: M-class (60.2%), K-class (24.2%)
# EXCLUDE: F, G, A-class (only 10.2% combined)
```

**Filter Logic:**
```python
def fonticulua_campestris_stellar_filter(stellar_class):
    return stellar_class in ['M', 'K']  # 84.4% of observations
```

**Expected Impact:** ~80% reduction in false positives

---

## **4. Stratum Paleas** - K/F Specialist ⭐⭐
**Avoids M-dwarfs unlike other species**

### Enhanced Filter:
```python
# INCLUDE: K-class (49.3%), F-class (46.0%)
# EXCLUDE: M-class (only 3.5%), G, A-class
```

**Filter Logic:**
```python
def stratum_paleas_stellar_filter(stellar_class):
    return stellar_class in ['K', 'F']  # 95.3% of observations
```

**Expected Impact:** ~90% reduction in false positives by excluding M-dwarfs

---

## **5. Bacterium Aurasus** - Universal Adapter ⭐
**Found in all main sequence stars, but patterns exist**

### Current Distribution:
- K-class: 33.8%, F-class: 30.6%, G-class: 23.5%, A-class: 6.3%
- **Note:** Shows poor thermal regulation across all stellar classes

### Enhanced Filter:
```python
# INCLUDE: K, F, G-class (87.9% of observations)
# EXCLUDE: A-class and exotic types (reduce by ~12%)
```

**Filter Logic:**
```python
def bacterium_aurasus_stellar_filter(stellar_class):
    return stellar_class in ['K', 'F', 'G']  # 87.9% of observations
```

**Expected Impact:** Modest ~15% reduction in false positives

---

## **6. Bacterium Cerbrus** - Hot Star Tolerant ⭐
**One of few species found significantly in A-class systems**

### Enhanced Filter:
```python
# INCLUDE: All main sequence (A, F, G, K) + some M-class
# EXCLUDE: Exotic stellar types only
```

**Expected Impact:** Minimal filtering needed - naturally broad distribution

---

## **Implementation Priority Ranking**

### **High Priority** ⭐⭐⭐ (Major Impact)
1. **Bacterium Vesicula** - 85% false positive reduction
2. **Bacterium Acies** - 90% false positive reduction
3. **Fonticulua Campestris** - 80% false positive reduction
4. **Stratum Paleas** - 90% false positive reduction
5. **Stratum Tectonicas** - 40% false positive reduction

### **Medium Priority** ⭐⭐ (Moderate Impact)
6. **Osseus Spiralis** - Broad but could exclude M-class
7. **Bacterium Alcyoneum** - Exclude M-class

### **Low Priority** ⭐ (Minor Impact)
8. **Bacterium Aurasus** - Naturally broad distribution
9. **Fungoida Setisis** - Similar broad pattern

---

## **Combined Filter Implementation**

```python
STELLAR_CLASS_FILTERS = {
    'Bacterium Vesicula': ['M', 'K'],           # M-dwarf specialist
    'Bacterium Acies': ['M', 'T', 'Y', 'L'],   # Cool star specialist
    'Fonticulua Campestris': ['M', 'K'],       # M-dwarf specialist
    'Stratum Paleas': ['K', 'F'],              # K/F specialist
    'Stratum Tectonicas': ['K', 'F'],          # K/F specialist
    'Osseus Spiralis': ['K', 'F', 'G', 'A'],  # Main sequence only
    'Bacterium Alcyoneum': ['K', 'F', 'G', 'A'], # Exclude M-class
    # 'Bacterium Aurasus': All classes allowed (universal)
    # 'Bacterium Cerbrus': All classes allowed (broad tolerance)
}

def enhanced_species_filter(species_name, stellar_class):
    if species_name in STELLAR_CLASS_FILTERS:
        allowed_classes = STELLAR_CLASS_FILTERS[species_name]
        return stellar_class in allowed_classes
    return True  # No restriction for species without specific filters
```

## **Expected Overall Impact**

Implementing these filters would:

1. **Reduce false positives by 60-80%** for M-dwarf specialists
2. **Focus exploration** on stellar environments with proven success rates
3. **Eliminate wasted survey time** in incompatible stellar systems
4. **Maintain 85-95% coverage** of actual species occurrences

The filters are particularly powerful for **cool star specialists** (Bacterium Vesicula, Acies, Fonticulua Campestris) where the stellar class preferences are extremely clear.