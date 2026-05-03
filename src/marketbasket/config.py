"""Static config — company maps, pay-plan labels, bin boundaries, coverage column names.

Curated CompanyId->name mappings for IL and AZ are ported verbatim from the WSL
pipeline (dev/MarketBasket/data.py). All other states fall back to top-15 by
distinct PolicyLinkID count; unmapped companies are named by their CompanyId string.
"""
from __future__ import annotations

# ── Company maps (curated states) ──────────────────────────────────────────────

COMPANY_MAP_IL: dict[int, str] = {
    7870394:   "National",
    8915607:   "Progressive",
    9246038:   "Safeway",
    19663428:  "AmericanHeartland",
    48959310:  "UIC",
    48959311:  "UIC",
    95816297:  "AmFreedom",
    95817115:  "AmFreedom",
    107678282: "UnitedEquitable",
    110761262: "Kemper",
    128913468: "FirstChicago",
    128914375: "FirstChicago",
    128914886: "USIC",
    128914919: "USIC",
    128915050: "FirstChicago",
    128915051: "USIC",
    133697356: "AmericanAlliance",
    136187915: "SIC",
    156245551: "GEICO",
}

COMPANY_MAP_AZ: dict[int, str] = {
    7869459:   "NatGen",
    10031862:  "Hallmark",
    8917016:   "Progressive",
    2629060:   "Kemper",
    3870881:   "General",
    145628779: "Alpine",
    126227624: "GAINSCO",
    8130598:   "Mendota",
    30741178:  "Bristol West",
    139073527: "Falcon",
    9244878:   "Safeway",
    105189666: "Dairyland",
    131141924: "SunCoast",
    136190126: "SIC",
    130096799: "AssuranceAmerica",
    130093319: "AssuranceAmerica",
}

COMPANY_MAP_BY_STATE: dict[str, dict[int, str]] = {
    "IL": COMPANY_MAP_IL,
    "AZ": COMPANY_MAP_AZ,
}

# States whose curated map is considered exhaustive: rows for unmapped companies
# are dropped entirely (rather than kept by raw CompanyId).
EXHAUSTIVE_MAP_STATES: set[str] = {"IL"}

# Comparison column per state. Left (secondary) column vs the named company,
# right (always-present) column vs SIC. States absent from this dict → no
# comparison columns.
COMPARISON_COMPANY_BY_STATE: dict[str, str] = {
    "IL": "UIC",
    "AZ": "SunCoast",
}

# Companies "we represent" — the yellow-highlighted reference rows in the
# frontend. Discount Simulator inputs only adjust premiums for these companies.
OUR_COMPANIES_BY_STATE: dict[str, list[str]] = {
    "IL": ["UIC", "SIC"],
    "AZ": ["SunCoast", "SIC"],
}

# Active states — only these get refreshed and shown in the frontend.
ACTIVE_STATES: set[str] = {"IL", "AZ"}

# For non-curated states: how many top companies to display by distinct policy count.
TOP_N_NON_CURATED = 15

# Top-N counties (per state) kept individually; the rest collapse to "Other".
TOP_N_COUNTIES = 10


# ── PercentDown normalization ──────────────────────────────────────────────────

# Values within ~0.1 of each other that should be merged onto a canonical value.
PERCENT_DOWN_REMAP: dict[float, float] = {
    16.7: 16.66, 16.1: 16.66, 16.0: 16.66, 15.0: 16.66, 17.0: 16.66, 18.0: 16.66,
    9.0:  8.33,  8.0:  8.33,  8.3:  8.33,
    20.3: 20.0,
}

# Top-14 pay plans: (PercentDownR, NumOfPayments+1) → label. Rows not matching any
# key here are dropped from the aggregate (keeps the plan dropdown finite).
TOP_14_PLANS: dict[tuple[float, int], str] = {
    (16.66,  6): "16.66/6",
    (100.0,  1): "100/1",
    ( 8.33, 12): "8.33/12",
    (20.0,   6): "20/6",
    (25.0,   5): "25/5",
    (25.0,   4): "25/4",
    (50.0,   2): "50/2",
    (25.0,   6): "25/6",
    (20.0,   5): "20/5",
    (10.0,  12): "10/12",
    (22.0,   6): "22/6",
    (41.7,   5): "41.7/5",
    (30.0,   5): "30/5",
    (40.0,   3): "40/3",
}

# Display labels for pay plans (shown in the UI dropdown).
PAYPLAN_LABELS: dict[str, str] = {
    "8.33/12": "8% down, 12 payments",
    "10/12":   "10% down, 12 payments",
    "16.66/6": "17% down, 6 payments",
    "20/5":    "20% down, 5 payments",
    "20/6":    "20% down, 6 payments",
    "22/6":    "22% down, 6 payments",
    "25/4":    "25% down, 4 payments",
    "25/5":    "25% down, 5 payments",
    "25/6":    "25% down, 6 payments",
    "30/5":    "30% down, 5 payments",
    "40/3":    "40% down, 3 payments",
    "41.7/5":  "42% down, 5 payments",
    "50/2":    "50% down, 2 payments",
    "100/1":   "Full pay",
}
PAYPLAN_ORDER: list[str] = list(PAYPLAN_LABELS.values())


# ── Liability limits, coverage, bins ───────────────────────────────────────────

VALID_LIAB: dict[tuple[int, int], str] = {
    (25, 50):   "25/50",
    (50, 100):  "50/100",
    (100, 300): "100/300",
}

PREM_COLS: list[str] = [
    "LiabBIPremium", "LiabPDPremium", "CompPremium", "CollPremium",
    "MedPayPremium", "UIMBIPremium", "UIMPDPremium", "UninsBIPremium", "UninsPDPremium",
]

PREM_BIN_SIZE = 500
PREM_BIN_CAP = 5000
YEAR_BINS = [0, 2009, 2014, 2019, 9999]
YEAR_LABELS = ["pre-2010", "2010-2014", "2015-2019", "2020+"]


# ── Predicted Credit equation (Illinois) ───────────────────────────────────────
#
# Mirrors the Excel formula. Used as a placeholder for AZ until we get
# AZ-specific coefficients. States not in CREDIT_FORMULA_STATES → CreditBin=NaN.

CREDIT_FORMULA_STATES: set[str] = {"IL", "AZ"}

CREDIT_BASE_SCORE = 865

# Prior coverage duration (months) → pts. NO PC (no prior insurance) is treated
# as 0 duration, which falls into the 0-5 bucket.
def credit_prior_duration_pts(months: float) -> int:
    if months <= 5: return -50
    if months <= 12: return 0
    return 70

# Vehicle minimum age (years from RatedDate) → pts.
def credit_vehicle_min_age_pts(min_age: float) -> int:
    if min_age < 3: return 110
    if min_age <= 9: return 35
    return -40

# Prior carrier / lapse status — three mutually exclusive states.
CREDIT_CARRIER_STATE_NO_PC = "NO_PC"        # PriorInsurance=0
CREDIT_CARRIER_STATE_NO_LAPSE = "NO_LAPSE"  # PriorInsurance=1, PriorDaysLapse=0
CREDIT_CARRIER_STATE_LAPSE = "LAPSE"        # PriorInsurance=1, PriorDaysLapse>0

CREDIT_CARRIER_PTS: dict[str, int] = {
    CREDIT_CARRIER_STATE_NO_PC:    0,
    CREDIT_CARRIER_STATE_NO_LAPSE: 121,
    CREDIT_CARRIER_STATE_LAPSE:    -81,
}

# Named-insured age × carrier-state matrix (pts).
# Bands: (min_age, max_age) inclusive. Ages outside any band → 0 pts.
CREDIT_AGE_BANDS: list[tuple[int, int, dict[str, int]]] = [
    (18, 35,  {"NO_PC": -15, "NO_LAPSE": -15, "LAPSE": -40}),
    (36, 45,  {"NO_PC":   0, "NO_LAPSE":  10, "LAPSE": -40}),
    (46, 55,  {"NO_PC":  25, "NO_LAPSE":  40, "LAPSE": -40}),
    (56, 65,  {"NO_PC":  65, "NO_LAPSE":  85, "LAPSE": -10}),
    (66, 70,  {"NO_PC": 100, "NO_LAPSE": 120, "LAPSE":  30}),
    (71, 100, {"NO_PC":   0, "NO_LAPSE":   0, "LAPSE":   0}),
]

CREDIT_BI_LIMITS_PTS: dict[str, int] = {
    "25/50":   0,
    "50/100":  100,
    "100/300": 200,
}

# Vehicle count → pts. Cap at 5 (NumVehicles="5+" uses the 5 row).
CREDIT_VEHCOUNT_PTS: dict[str, int] = {
    "1": 27, "2": 54, "3": 81, "4": 108, "5+": 135,
}

CREDIT_HOMEOWNER_PTS = 100   # ResidencyStatus = 'O' → +100; else 0

CREDIT_BIN_SIZE = 50         # 50-pt buckets, e.g. 700, 750, 800...


# ── Group-by dimensions (MUST match the frontend filter UI) ────────────────────

GROUP_COLS: list[str] = [
    "CompanyName", "PremBin", "LiabLimits", "PayPlan",
    "NonOwner", "NumDrivers", "NumVehicles", "County",
    "PriorInsurance", "YearBin", "Term", "CreditBin",
]
