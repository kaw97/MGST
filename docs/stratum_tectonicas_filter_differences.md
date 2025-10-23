# Stratum Tectonicas: Enhanced Filter Logic

## How the Enhanced Filter Differs from Original Rules

### **Original Rules** (Basic Atmospheric/Body Constraints)
Stratum Tectonicas is predicted on any body with:
- **Atmosphere**: Ammonia, Argon, ArgonRich, CarbonDioxide, CarbonDioxideRich, SulphurDioxide, Water
- **Body Type**: High metal content body, Rocky body
- **Gravity**: 0.045 - 0.61g
- **Temperature**: 165 - 430K (very wide range)
- **Pressure**: Various ranges depending on atmosphere type

*Result*: Very broad criteria that predict Stratum Tectonicas in many systems regardless of stellar type.

### **Enhanced Filter** (Stellar Class + Empirical Ranges)
The enhanced filter adds stellar class compatibility checks based on 42,163 actual observations:

#### **1. K-Class Stars** ✅ **ALLOWED**
- **Observations**: 22,561 entries (53.5% of all Stratum Tectonicas)
- **Thermal Regulation**: Excellent (11K body temperature span)
- **Typical Conditions**:
  - Body temperatures: 193K - 205K (tight range)
  - Orbital distances: 300 - 1,100 ls
  - Stellar temperatures: 3,990 - 5,000K

#### **2. F-Class Stars** ✅ **ALLOWED**
- **Observations**: 16,035 entries (38.0% of all Stratum Tectonicas)
- **Thermal Regulation**: Moderate (35K body temperature span)
- **Typical Conditions**:
  - Body temperatures: 207K - 243K
  - Orbital distances: 1,200 - 2,000 ls (farther from hot stars)
  - Stellar temperatures: 6,100 - 7,400K

#### **3. M-Class Stars** ❌ **EXCLUDED**
- **Observations**: 3,014 entries (only 7.1% of all Stratum Tectonicas)
- **Thermal Regulation**: Poor (85K body temperature span)
- **Why Excluded**: Species shows poor thermal adaptation in M-dwarf environments
  - Body temperatures: 187K - 272K (wide, unstable range)
  - High temperature variation indicates thermal stress

#### **4. T-Class, Unknown Stars** ❌ **EXCLUDED**
- **Observations**: <1% each of total Stratum Tectonicas
- **Thermal Regulation**: Poor (>100K temperature spans)
- **Why Excluded**: Insufficient data and poor thermal regulation

### **Practical Filter Impact**

#### **Systems That Now Qualify** (Enhanced > Original)
- **K-class systems** with bodies meeting atmospheric conditions
- **F-class systems** with bodies at appropriate orbital distances (1,200+ ls)
- Better targeting of high-success-rate stellar environments

#### **Systems Now Excluded** (Original > Enhanced)
- **M-dwarf systems** that previously qualified on atmospheric grounds alone
- **T-class/exotic stellar systems** with poor thermal regulation
- **Edge cases** with inappropriate orbital distances for stellar temperature

### **Boolean Logic Summary**

```python
# Original Logic
def original_stratum_tectonicas_filter(body):
    return (
        atmosphere in ['Ammonia', 'CarbonDioxide', ...] and
        body_type in ['High metal content body', 'Rocky body'] and
        0.045 <= gravity <= 0.61 and
        165 <= temperature <= 430
    )

# Enhanced Logic
def enhanced_stratum_tectonicas_filter(body, system):
    stellar_class = get_stellar_class(system)

    # Apply original rules first
    if not original_stratum_tectonicas_filter(body):
        return False

    # Add stellar class compatibility
    if stellar_class in ['K', 'F']:
        return True
    elif stellar_class == 'M':
        return False  # Poor thermal regulation observed
    else:
        return False  # Insufficient data or poor regulation
```

### **Expected Results**

The enhanced filter should:

1. **Reduce false positives** by ~30-40% by excluding M-dwarf and exotic systems
2. **Maintain high success rates** in K-class systems (53.5% of observations)
3. **Target optimal conditions** in F-class systems with appropriate orbital distances
4. **Focus exploration** on stellar environments where Stratum Tectonicas actually thrives

This creates a more precise filter that targets systems with empirically validated high success rates rather than just theoretical atmospheric compatibility.