"""Row-level preprocessing.

`preprocess_rate` / `_car` / `_driver` each take a raw DataFrame (the shape
MarketUnified returns) and return a narrower one ready for merging. The
collapse-to-policy dedup happens in `aggregate.py` AFTER the merge, because
its key depends on car-side coverage flags.
"""
from __future__ import annotations

import pandas as pd

from .config import (
    COMPANY_MAP_BY_STATE,
    COUNTY_COVERAGE_TARGET,
    EXHAUSTIVE_MAP_STATES,
    PREM_COLS,
    TOP_N_NON_CURATED,
    VALID_LIAB,
    VALID_LIAB_BY_STATE,
)


def preprocess_driver(df_driver: pd.DataFrame, df_violation: pd.DataFrame) -> pd.DataFrame:
    """fact_Rate_Driver + fact_Rate_Violation → one row per RateLinkID.

    Aggregations (all drivers on the rate):
      PriorInsurance — 1 if any driver had prior insurance.
      AtFault        — 1 if any driver had any at-fault violation.
      NumDrivers     — count of drivers on the rate.

    Named-insured columns (Relation='I' row, fall back to first driver):
      NamedInsuredAge, ResidencyStatus, PriorMonthsCovg, PriorDaysLapse.
    Used by the Predicted Credit equation downstream.
    """
    viol = (
        df_violation.groupby("RateDriverLinkId")["AtFault"]
        .any().astype(int)
        .reset_index()
        .rename(columns={"RateDriverLinkId": "RateDriverId"})
    )

    df = df_driver.merge(viol, on="RateDriverId", how="left")
    df["AtFault"] = df["AtFault"].fillna(0).astype(int)

    agg = (
        df.groupby("RateLinkID")
          .agg(
              PriorInsurance=("PriorInsurance", "any"),
              AtFault=("AtFault", "max"),
              NumDrivers=("RateDriverId", "count"),
          )
          .reset_index()
          .assign(PriorInsurance=lambda x: x["PriorInsurance"].astype(int))
    )

    # Named-insured row: Relation='I' if present, otherwise the first driver
    # per RateLinkID. _is_ni=0 sorts before 1 so 'I' rows win.
    df = df.assign(_is_ni=(df["Relation"] != "I").astype(int))
    df = df.sort_values(["RateLinkID", "_is_ni", "RateDriverId"])
    ni = (
        df.drop_duplicates("RateLinkID", keep="first")
          [["RateLinkID", "Age", "ResidencyStatus",
            "PriorMonthsCovg", "PriorDaysLapse"]]
          .rename(columns={"Age": "NamedInsuredAge"})
    )

    return (
        agg.merge(ni, on="RateLinkID", how="left")
           .rename(columns={"RateLinkID": "RateId"})
    )


_COUNTY_SYNONYMS: dict[str, str] = {
    # Same county, different spellings observed in IL fact_Rate_Car.
    "SAINTCLAIR": "STCLAIR",
}


def _normalize_county(s: pd.Series) -> pd.Series:
    """Uppercase, strip non-letters, then apply synonym map.

    Collapses 'DU PAGE' / 'DuPage' / 'du-page' all to 'DUPAGE'; 'St. Clair'
    and 'SAINT CLAIR' both end up as 'STCLAIR' via the synonym table.
    Empty / null / non-string -> 'UNKNOWN'.
    """
    out = (
        s.astype("string").str.upper()
         .str.replace(r"[^A-Z]", "", regex=True)
         .replace({"": pd.NA})
         .fillna("UNKNOWN")
    )
    return out.replace(_COUNTY_SYNONYMS)


def preprocess_car(df: pd.DataFrame, state: str) -> pd.DataFrame:
    """fact_Rate_Car → one row per RateLinkID.

    1. Drop RateLinkIDs where ANY car has invalid LiabLimits (not in the state's
       accepted tier set — TX uses 30/60 as the floor; everywhere else 25/50).
    2. Normalize County: uppercase, strip non-letters, apply synonyms; empty -> "UNKNOWN".
    3. Aggregate: LiabLimits/County (first car), NumVehicles, Year (max),
       coverage premiums (sum).
    4. Derive coverage-signature flags (used as dedup key dimensions):
         HasPhysDmg  — Comp or Coll on
         HasUM_UIM   — Uninsured or Underinsured Motorist on
         HasMedPay   — Medical Payments on

    Year=max corresponds to the newest car on the policy (smallest age) — used
    both for YearBin and for the PredictedCredit "Vehicle Min Age Range".
    """
    df = df.copy()
    valid_liab = VALID_LIAB_BY_STATE.get(state, VALID_LIAB)
    df["LiabLimits"] = list(zip(df["LiabLimits1"], df["LiabLimits2"]))
    df["LiabLimits"] = df["LiabLimits"].map(valid_liab)
    invalid_ids = df[df["LiabLimits"].isna()]["RateLinkID"].unique()
    df = df[~df["RateLinkID"].isin(invalid_ids)]

    df["County"] = _normalize_county(df["County"])

    agg = (
        df.groupby("RateLinkID")
          .agg(
              LiabLimits=("LiabLimits", "first"),
              County=("County", "first"),
              NumVehicles=("Year", "count"),
              Year=("Year", "max"),         # newest car (smallest age)
              **{col: (col, "sum") for col in PREM_COLS},
          )
          .reset_index()
          .rename(columns={"RateLinkID": "RateId"})
    )

    # Coverage-signature flags. The customer was offered N rates that differ on
    # these; we use them as dedup-key dimensions so each combo gets its own row.
    agg["HasPhysDmg"] = ((agg["CompPremium"]    > 0) | (agg["CollPremium"]    > 0)).astype(int)
    agg["HasUM_UIM"]  = ((agg["UninsBIPremium"] > 0) | (agg["UninsPDPremium"] > 0)
                       | (agg["UIMBIPremium"]   > 0) | (agg["UIMPDPremium"]   > 0)).astype(int)
    agg["HasMedPay"]  = (agg["MedPayPremium"] > 0).astype(int)
    return agg


def preprocess_rate(df: pd.DataFrame, yyyymm: int, state: str) -> pd.DataFrame:
    """fact_Rate → row-level cleaned rate frame (NOT yet deduplicated).

    Dedup runs after the rate/car/driver merge in aggregate.py, because the
    collapse key (PolicyLinkID, CompanyId, HasPhysDmg, HasUM_UIM, HasMedPay,
    PayPlanType) depends on car-side coverage flags.

    1. Keep rows where RatedDate matches the yyyymm of the pull.
    2. Drop policies with inconsistent NonOwner/AssumedCredit across quotes.
    3. Repair dollar-code PercentDown (Kemper etc.) using DownPayment/TotalPremium.
    4. Derive PayPlanType: "Pay in Full" when PercentDown == 100, else "Various".
       All installment plans share the same total premium (only PIF differs),
       so the 14-plan breakdown was inflating quote counts.
    5. Map CompanyId → CompanyName (curated states) OR keep as string.
    """
    df = df.copy()

    # ── 1 ────────────────────────────────────────────────────────────────────
    rated_yyyymm = df["RatedDate"].dt.year * 100 + df["RatedDate"].dt.month
    df = df[rated_yyyymm == yyyymm].copy()
    if df.empty:
        return df

    # ── 2 ────────────────────────────────────────────────────────────────────
    non_owner_varies = df.groupby("PolicyLinkID")["NonOwner"].nunique()
    credit_varies = df.groupby("PolicyLinkID")["AssumedCredit"].nunique()
    bad_policies = set(non_owner_varies[non_owner_varies > 1].index) | set(
        credit_varies[credit_varies > 1].index
    )
    df = df[~df["PolicyLinkID"].isin(bad_policies)]

    # ── 3  (PercentDown > 100 means the field stores a dollar amount) ────────
    dollar_code = df["PercentDown"] > 100
    if dollar_code.any():
        derived = (
            df.loc[dollar_code, "DownPayment"]
            / df.loc[dollar_code, "TotalPremium"].replace(0, float("nan"))
            * 100
        ).round(0).fillna(0.0).clip(0, 100)
        df.loc[dollar_code, "PercentDown"] = derived

    # ── 4 ────────────────────────────────────────────────────────────────────
    df["PayPlanType"] = (df["PercentDown"] == 100.0).map({True: "Pay in Full", False: "Various"})

    # ── 5 ────────────────────────────────────────────────────────────────────
    # Unmapped companies are ALWAYS kept (as CompanyId-as-string). The
    # downstream apply_top_n_on_aggregated step then decides how to bucket
    # them per state: exhaustive states bucket all unmapped into a single
    # "Other (N=X)" row; other curated states keep top-5 + Other; non-curated
    # states keep top-15 + Other.
    company_map = COMPANY_MAP_BY_STATE.get(state, {})
    df["CompanyName"] = df["CompanyId"].map(
        lambda c: company_map.get(c, str(c))
    ).astype("object")

    return df


def apply_top_n_on_aggregated(
    df: pd.DataFrame, state: str, group_cols: list[str],
) -> pd.DataFrame:
    """Collapse unmapped companies (numeric CompanyName) into top-N + 'Other (N=X)'.

    MUST be called on the CONCATENATED multi-month aggregate, not per month.
    If you apply it per-month, each month's Other bucket will have a different
    N and they won't merge when concatenated.

    "Top" is sum(Quotes) in the aggregated data (close proxy for policy count,
    and the only company-ranking metric available after the row-level groupby).
    For curated states keep top-5 unmapped; for non-curated, top-N defined by
    TOP_N_NON_CURATED.
    """
    numeric_mask = df["CompanyName"].str.match(r"^\d+$")
    preother_mask = df["CompanyName"].str.match(r"^Other$")
    unmapped_mask = numeric_mask | preother_mask

    if not unmapped_mask.any():
        return df

    if state in EXHAUSTIVE_MAP_STATES:
        n_keep = 0
    elif state in COMPANY_MAP_BY_STATE:
        n_keep = 5
    else:
        n_keep = TOP_N_NON_CURATED

    # Only numeric IDs compete for top-N slots; pre-bucketed Other rows always
    # collapse back into the Other bucket.
    top = (
        df.loc[numeric_mask]
          .groupby("CompanyName")["Quotes"]
          .sum()
          .sort_values(ascending=False)
          .head(n_keep)
          .index.tolist()
    )

    is_other = unmapped_mask & ~df["CompanyName"].isin(top)
    # N = fresh numeric IDs being bucketed + N values extracted from any
    # pre-existing "Other (N=X)" labels (they represent companies we can no
    # longer enumerate individually after a prior bucketing pass).
    df = df.copy()
    df.loc[is_other, "CompanyName"] = "Other"

    # Re-group to merge the now-identically-named Other rows into one per
    # (YYYYMM, group_cols) combination.
    return _regroup_aggregated(df, group_cols)


def apply_county_top_n_on_aggregated(
    df: pd.DataFrame, group_cols: list[str],
) -> pd.DataFrame:
    """Collapse small counties into 'Other'.

    Keeps the largest counties (by Quotes) until cumulative coverage hits
    COUNTY_COVERAGE_TARGET (default 0.90). Everything else -> Other. Adapts
    to each state — AZ needs ~5 to hit 90%, IL needs ~20.

    MUST be called on the CONCATENATED multi-month aggregate (same reasoning
    as apply_top_n_on_aggregated).
    """
    if "County" not in df.columns or df.empty:
        return df

    by_county = (
        df.groupby("County")["Quotes"].sum()
          .sort_values(ascending=False)
    )
    total = by_county.sum()
    if total <= 0:
        return df

    # Rank ONLY real counties for the keep decision. A pre-existing "Other"
    # bucket must never re-enter the ranking: if it did, once it grew large it
    # would consume the coverage budget and push real counties out, collapsing
    # a few more counties on every re-bucket (a runaway that ends with
    # everything in Other). Its mass still counts toward `total`, so the target
    # denominator is stable across passes — that makes this idempotent: the
    # counties kept on the first pass stay kept on every subsequent pass.
    real = by_county[by_county.index != "Other"]
    cumshare = real.cumsum() / total
    # Keep counties up to and including the one that crosses the target.
    keep_mask = cumshare.shift(fill_value=0.0) < COUNTY_COVERAGE_TARGET
    keep = real.index[keep_mask].tolist()

    df = df.copy()
    df.loc[~df["County"].isin(keep), "County"] = "Other"
    return _regroup_aggregated(df, group_cols)


# Value (non-dimension) columns produced by aggregate._aggregate_one_state.
# Kept here so both top-N reducers regroup the same metrics.
AGG_VALUE_COLS: list[str] = [
    "Quotes", "SumPremium", "BridgingCount", "SumBridgingPremium",
    "SumLiabBIPremium", "SumLiabPDPremium", "SumCompPremium", "SumCollPremium",
    "SumMedPayPremium", "SumUIMBIPremium", "SumUIMPDPremium",
    "SumUninsBIPremium", "SumUninsPDPremium",
    "SumLiabBIBridging", "SumLiabPDBridging", "SumCompBridging", "SumCollBridging",
    "SumMedPayBridging", "SumUIMBIBridging", "SumUIMPDBridging",
    "SumUninsBIBridging", "SumUninsPDBridging",
]


def _regroup_aggregated(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Re-aggregate after collapsing values in some dimension column."""
    cols = [c for c in AGG_VALUE_COLS if c in df.columns]
    return (
        df.groupby(group_cols + ["YYYYMM"], dropna=False)[cols]
          .sum()
          .reset_index()
    )
