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

COMPANY_MAP_TX: dict[int, str] = {
    154672476:  "Entegra Paragon",
    140254175:  "Amwins",
    140253551:  "Amwins",
    10094447:   "Amwins",
    3873616:    "The General",
    2624660:    "Kemper Auto",
    1073745427: "Allinsco",
    1073745284: "Alinsco",
    8914786:    "Progressive",
    139071886:  "Falcon",
    5834791:    "Louis A Williams",
    126227627:  "GAINSCO",
    126226343:  "GAINSCO",
    139532371:  "Commonwealth General",
    30738444:   "Bristol West",
    151788374:  "Excellence",
    151788763:  "Ignition",
    10032078:   "Hallmark",
    145626170:  "Alpine Rio",
    105189511:  "Dairyland auto",
    48961154:   "Lamar Platinum",
    48962470:   "Lamar Platinum",
    48960076:   "Lamar Platinum",
    149494039:  "Acacia",
    130093013:  "AssuranceAmerica",
    155000185:  "Aguila",
    9246039:    "Safeway",
    49024490:   "United Auto",
    156245614:  "GEICO",
    144970758:  "SNAP",
    137105533:  "Connect Banner",
    129833720:  "Anchor General",
}

COMPANY_MAP_TN: dict[int, str] = {
    133436078: "AAA",
    133895781: "Acceptance",
    147855146: "Acuity Insurance",
    8132911:   "Advantage Auto",
    95819139:  "American Freedom",
    122558521: "Auto-Owners",
    30739635:  "Bristol West",
    148707552: "Central Insurance",
    150542796: "Cincinnati Insurance",
    105189808: "Dairyland",
    105189809: "Dairyland",
    128260502: "Encompass Auto",
    126227179: "GAINSCO",
    126227631: "GAINSCO",
    999996:    "Generic Financing",
    15666899:  "Grange Insurance",
    10032140:  "Hallmark",
    38407717:  "Hartford",
    7736334:   "Haulers",
    15209735:  "MetLife",
    7869976:   "National General",
    7870648:   "National General",
    141890680: "Nationwide",
    8915981:   "Progressive",
    125897863: "Safeco",
    9246037:   "Safeway",
    12588845:  "State Auto Insurance Company",
    138220909: "Stillwater",
    6097924:   "Tennessee Auto Insurance Plan",
    3871410:   "The General",
    3870517:   "The General",
    3870841:   "The General",
    94637795:  "Travelers",
    116001821: "Trexis",
    95162058:  "Trexis",
    48960076:  "UIC",
    149166423: "Westfield Insurance",
}

COMPANY_MAP_BY_STATE: dict[str, dict[int, str]] = {
    "IL": COMPANY_MAP_IL,
    "AZ": COMPANY_MAP_AZ,
    "TX": COMPANY_MAP_TX,
    "TN": COMPANY_MAP_TN,
}

# States whose curated map is considered exhaustive: ALL unmapped CompanyIds
# collapse into the single "Other" bucket (no top-N kept by ID).
EXHAUSTIVE_MAP_STATES: set[str] = {"IL", "AZ", "TX", "TN"}

# Comparison column per state. Left (secondary) column vs the named company,
# right (always-present) column vs SIC. States absent from this dict → no
# comparison columns.
COMPARISON_COMPANY_BY_STATE: dict[str, str] = {
    "IL": "UIC",
    "AZ": "SunCoast",
    "TX": "Lamar Platinum",
    "TN": "UIC",
}

# Companies "we represent" — the yellow-highlighted reference rows in the
# frontend. Discount Simulator inputs only adjust premiums for these companies.
OUR_COMPANIES_BY_STATE: dict[str, list[str]] = {
    "IL": ["UIC", "SIC"],
    "AZ": ["SunCoast", "SIC"],
    "TX": ["Lamar Platinum"],
    "TN": ["UIC"],
}

# Active states — only these get refreshed and shown in the frontend.
ACTIVE_STATES: set[str] = {"IL", "AZ", "TX", "TN"}

# For non-curated states: how many top companies to display by distinct policy count.
TOP_N_NON_CURATED = 15

# Counties: keep the largest counties (by Quotes) until cumulative coverage
# hits this fraction of total Quotes. Everything below collapses to "Other".
# 0.90 means Other will be at most ~10% of the state's quotes.
COUNTY_COVERAGE_TARGET = 0.90


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

# Per-state accepted (LiabLimits1, LiabLimits2) pairs. Quotes outside the listed
# tiers are dropped during preprocess. AZ/IL/IN/NM/TN use 25/50 as the floor;
# TX uses 30/60 (state-mandated minimum is higher).
VALID_LIAB_BY_STATE: dict[str, dict[tuple[int, int], str]] = {
    "AZ": {(25, 50): "25/50", (50, 100): "50/100", (100, 300): "100/300"},
    "IL": {(25, 50): "25/50", (50, 100): "50/100", (100, 300): "100/300"},
    "IN": {(25, 50): "25/50", (50, 100): "50/100", (100, 300): "100/300"},
    "NM": {(25, 50): "25/50", (50, 100): "50/100", (100, 300): "100/300"},
    "TN": {(25, 50): "25/50", (50, 100): "50/100", (100, 300): "100/300"},
    "TX": {(30, 60): "30/60", (50, 100): "50/100", (100, 300): "100/300"},
}
# Fallback for states not listed (any state without a registered formula).
VALID_LIAB: dict[tuple[int, int], str] = VALID_LIAB_BY_STATE["IL"]

PREM_COLS: list[str] = [
    "LiabBIPremium", "LiabPDPremium", "CompPremium", "CollPremium",
    "MedPayPremium", "UIMBIPremium", "UIMPDPremium", "UninsBIPremium", "UninsPDPremium",
]

PREM_BIN_SIZE = 500
PREM_BIN_CAP = 5000
YEAR_BINS = [0, 2009, 2014, 2019, 9999]
YEAR_LABELS = ["pre-2010", "2010-2014", "2015-2019", "2020+"]


# ── Predicted Credit ───────────────────────────────────────────────────────────
#
# Each state has its own complete formula. No defaults, no inheritance — if a
# state isn't in CREDIT_FORMULA_BY_STATE, no credit is computed for that state
# (CreditCode = NA, frontend hides the credit filter).

CREDIT_CARRIER_STATE_NO_PC = "NO_PC"        # PriorInsurance=0
CREDIT_CARRIER_STATE_NO_LAPSE = "NO_LAPSE"  # PriorInsurance=1, PriorDaysLapse=0
CREDIT_CARRIER_STATE_LAPSE = "LAPSE"        # PriorInsurance=1, PriorDaysLapse>0

# Named-insured age × carrier-state matrix — same for all 6 states we have
# spec for. Inlined into each formula below for self-containment.
_AGE_BANDS_COMMON: list[tuple[int, int, dict[str, int]]] = [
    (18, 35,  {"NO_PC": -15, "NO_LAPSE": -15, "LAPSE": -40}),
    (36, 45,  {"NO_PC":   0, "NO_LAPSE":  10, "LAPSE": -40}),
    (46, 55,  {"NO_PC":  25, "NO_LAPSE":  40, "LAPSE": -40}),
    (56, 65,  {"NO_PC":  65, "NO_LAPSE":  85, "LAPSE": -10}),
    (66, 70,  {"NO_PC": 100, "NO_LAPSE": 120, "LAPSE":  30}),
    (71, 100, {"NO_PC":   0, "NO_LAPSE":   0, "LAPSE":   0}),
]

# Each band is (lo_inclusive, hi_inclusive, pts). Values outside ALL bands -> 0.
# IN/NM/TN intentionally leave a gap at age=9 (not (3..9), but (3..8) and (10+)).
CREDIT_FORMULA_BY_STATE: dict[str, dict] = {
    "AZ": {
        "base": 850,
        "prior_duration": [(0, 5, -50), (6, 12, 0), (13, 9999, 70)],
        "vehicle_min_age": [(0, 2, 110), (3, 9, 35), (10, 9999, -40)],
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -81},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"25/50": 0, "50/100": 100, "100/300": 200},
        "veh_count": {"1": 27, "2": 54, "3": 81, "4": 108, "5+": 135},
        "homeowner_true": 100,
    },
    "IL": {
        "base": 865,
        "prior_duration": [(0, 5, -50), (6, 12, 0), (13, 9999, 70)],
        "vehicle_min_age": [(0, 2, 110), (3, 9, 35), (10, 9999, -40)],
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -81},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"25/50": 0, "50/100": 100, "100/300": 200},
        "veh_count": {"1": 27, "2": 54, "3": 81, "4": 108, "5+": 135},
        "homeowner_true": 100,
    },
    "IN": {
        "base": 837,
        "prior_duration": [(0, 5, -50), (6, 12, 0), (13, 9999, 70)],
        "vehicle_min_age": [(0, 2, 110), (3, 8, 35), (10, 9999, -40)],  # gap at 9
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -81},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"25/50": 0, "50/100": 100, "100/300": 200},
        "veh_count": {"1": 27, "2": 54, "3": 81, "4": 108, "5+": 135},
        "homeowner_true": 100,
    },
    "NM": {
        "base": 852,
        "prior_duration": [(0, 5, -50), (6, 12, 0), (13, 9999, 70)],
        "vehicle_min_age": [(0, 2, 110), (3, 8, 35), (10, 9999, -40)],  # gap at 9
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -81},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"25/50": 0, "50/100": 100, "100/300": 200},
        "veh_count": {"1": 27, "2": 54, "3": 81, "4": 108, "5+": 135},
        "homeowner_true": 100,
    },
    "TN": {
        "base": 1000,
        "prior_duration": [(0, 5, -50), (6, 12, 0), (13, 9999, 70)],
        "vehicle_min_age": [(0, 2, 110), (3, 8, 35), (10, 9999, -40)],  # gap at 9
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -81},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"25/50": 0, "50/100": 100, "100/300": 200},
        "veh_count": {"1": 27, "2": 54, "3": 81, "4": 108, "5+": 135},
        "homeowner_true": 100,
    },
    "TX": {
        "base": 840,
        "prior_duration": [(0, 5, -50), (6, 12, 50), (13, 9999, 190)],
        "vehicle_min_age": [(0, 2, 260), (3, 9, 85), (10, 9999, -40)],
        "carrier": {"NO_PC": 0, "NO_LAPSE": 121, "LAPSE": -61},
        "age_bands": _AGE_BANDS_COMMON,
        "bi_limits": {"30/60": 0, "50/100": 156, "100/300": 316},
        "veh_count": {"1": 26, "2": 51, "3": 77, "4": 102, "5+": 128},
        "homeowner_true": 200,
    },
}
CREDIT_FORMULA_STATES: set[str] = set(CREDIT_FORMULA_BY_STATE.keys())


# ── Predicted Credit → Letter Code ─────────────────────────────────────────────
#
# Each (lo, hi, code) entry: scores in [lo, hi] map to `code`. Skips letters
# N/O (after M -> P), R (after Q -> S), T (after S -> U), X/Y (after W -> Z).

CREDIT_CODE_ORDER: list[str] = [
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
    "K", "L", "M", "P", "Q", "S", "U", "V", "W", "Z",
]

# V1: used by AZ, IL (pre-2026-04-02), IN, NM, TN, TX.
CREDIT_CODE_MAP_V1: list[tuple[int, int, str]] = [
    (   0,  389, "A"), ( 390,  616, "B"), ( 617,  715, "C"), ( 716,  802, "D"),
    ( 803,  881, "E"), ( 882,  949, "F"), ( 950, 1019, "G"), (1020, 1084, "H"),
    (1085, 1137, "I"), (1138, 1202, "J"), (1203, 1268, "K"), (1269, 1334, "L"),
    (1335, 1399, "M"), (1400, 1462, "P"), (1463, 1511, "Q"), (1512, 1559, "S"),
    (1560, 1610, "U"), (1611, 1656, "V"), (1657, 1722, "W"), (1723, 2600, "Z"),
]

# V2: used ONLY by IL on/after 2026-04-02. Wider lower buckets.
CREDIT_CODE_MAP_IL_V2: list[tuple[int, int, str]] = [
    (   0,  495, "A"), ( 496,  647, "B"), ( 648,  785, "C"), ( 786,  878, "D"),
    ( 879,  929, "E"), ( 930,  986, "F"), ( 987, 1050, "G"), (1051, 1106, "H"),
    (1107, 1161, "I"), (1162, 1217, "J"), (1218, 1273, "K"), (1274, 1321, "L"),
    (1322, 1382, "M"), (1383, 1442, "P"), (1443, 1489, "Q"), (1490, 1535, "S"),
    (1536, 1569, "U"), (1570, 1618, "V"), (1619, 1662, "W"), (1663, 2500, "Z"),
]

# IL switched bucketing on this date. Pre-cutoff rows use V1, on/after use V2.
IL_CREDIT_CODE_CUTOFF = "2026-04-02"


# ── Group-by dimensions (MUST match the frontend filter UI) ────────────────────

GROUP_COLS: list[str] = [
    "CompanyName", "PremBin", "LiabLimits", "PayPlan",
    "NonOwner", "NumDrivers", "NumVehicles", "County",
    "PriorInsurance", "YearBin", "Term", "CreditCode",
]
