"""Row-level preprocessing.

Ported verbatim from /home/dchemaly/dev/MarketBasket/data.py. Do not change the
math without discussion — the Excel output and this pipeline must stay
bit-identical on the groupby result for IL/AZ, because those are the curated
states users already trust.

Each function takes a raw DataFrame (the shape MarketUnified returns) and
returns a narrower one ready for merging.
"""
from __future__ import annotations

import pandas as pd

from .config import (
    COMPANY_MAP_BY_STATE,
    EXHAUSTIVE_MAP_STATES,
    PERCENT_DOWN_REMAP,
    PREM_COLS,
    TOP_14_PLANS,
    TOP_N_NON_CURATED,
    VALID_LIAB,
)


def preprocess_driver(df_driver: pd.DataFrame, df_violation: pd.DataFrame) -> pd.DataFrame:
    """fact_Rate_Driver + fact_Rate_Violation → one row per RateLinkID.

    PriorInsurance = 1 if any driver had prior insurance.
    AtFault        = 1 if any driver had any at-fault violation.
    NumDrivers     = count of drivers on the rate.
    """
    viol = (
        df_violation.groupby("RateDriverLinkId")["AtFault"]
        .any().astype(int)
        .reset_index()
        .rename(columns={"RateDriverLinkId": "RateDriverId"})
    )

    df = df_driver.merge(viol, on="RateDriverId", how="left")
    df["AtFault"] = df["AtFault"].fillna(0).astype(int)

    return (
        df.groupby("RateLinkID")
          .agg(
              PriorInsurance=("PriorInsurance", "any"),
              AtFault=("AtFault", "max"),
              NumDrivers=("RateDriverId", "count"),
          )
          .reset_index()
          .rename(columns={"RateLinkID": "RateId"})
          .assign(PriorInsurance=lambda x: x["PriorInsurance"].astype(int))
    )


def preprocess_car(df: pd.DataFrame) -> pd.DataFrame:
    """fact_Rate_Car → one row per RateLinkID.

    1. Drop RateLinkIDs where ANY car has an invalid LiabLimits (not in {25/50, 50/100, 100/300}).
    2. Aggregate: LiabLimits (first), NumVehicles (count), Year (max), coverage premiums (sum).
    """
    df = df.copy()
    df["LiabLimits"] = list(zip(df["LiabLimits1"], df["LiabLimits2"]))
    df["LiabLimits"] = df["LiabLimits"].map(VALID_LIAB)
    invalid_ids = df[df["LiabLimits"].isna()]["RateLinkID"].unique()
    df = df[~df["RateLinkID"].isin(invalid_ids)]

    return (
        df.groupby("RateLinkID")
          .agg(
              LiabLimits=("LiabLimits", "first"),
              NumVehicles=("Year", "count"),
              Year=("Year", "max"),
              **{col: (col, "sum") for col in PREM_COLS},
          )
          .reset_index()
          .rename(columns={"RateLinkID": "RateId"})
    )


def preprocess_rate(df: pd.DataFrame, yyyymm: int, state: str) -> pd.DataFrame:
    """fact_Rate → one row per (PolicyLinkID, CompanyId, PayPlan).

    1. Keep rows where RatedDate matches the yyyymm of the pull.
    2. Drop policies with inconsistent NonOwner/AssumedCredit across quotes.
    3. Repair dollar-code PercentDown (Kemper etc.) using DownPayment/TotalPremium.
    4. Normalize PercentDown → closest canonical value; derive PayPlan label.
    5. PurchasedFinal = any row in the group was Purchased=1.
    6. Keep earliest RateIteration, lowest TotalPremium per key.
    7. Map CompanyId → CompanyName (curated states) OR keep as string.

    For non-curated states: names are left as CompanyId-string here; the
    aggregation step later applies the top-15-plus-Other rule.
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
    pct_r = df["PercentDown"].round(1).replace(PERCENT_DOWN_REMAP)
    pay = (df["NumOfPayments"] + 1).where(df["PercentDown"] != 100.0, 1)
    df["PayPlan"] = list(zip(pct_r, pay))
    df["PayPlan"] = df["PayPlan"].map(TOP_14_PLANS)
    df = df[df["PayPlan"].notna()].copy()

    # ── 5 ────────────────────────────────────────────────────────────────────
    key = ["PolicyLinkID", "CompanyId", "PayPlan"]
    purchased_flag = df.groupby(key)["Purchased"].max().rename("PurchasedFinal")
    df = df.merge(purchased_flag, on=key, how="left")

    # ── 6 ────────────────────────────────────────────────────────────────────
    df = df.sort_values("RateIteration").drop_duplicates(key, keep="first")
    df = (
        df.sort_values("TotalPremium")
          .drop_duplicates(key, keep="first")
          .reset_index(drop=True)
    )

    # ── 7 ────────────────────────────────────────────────────────────────────
    # Unmapped companies are ALWAYS kept (as CompanyId-as-string). The
    # downstream apply_top_n_on_aggregated step then decides how to bucket
    # them per state: exhaustive states bucket all unmapped into a single
    # "Other (N=X)" row; other curated states keep top-5 + Other; non-curated
    # states keep top-15 + Other. The result: every state gets an Other row
    # that represents the unmapped market, matching the original Excel.
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
    unmapped_mask = df["CompanyName"].str.match(r"^\d+$")
    if not unmapped_mask.any():
        return df

    if state in EXHAUSTIVE_MAP_STATES:
        # Bucket ALL unmapped into a single "Other" — only curated companies
        # are shown individually.
        n_keep = 0
    elif state in COMPANY_MAP_BY_STATE:
        # Non-exhaustive curated (e.g. AZ): keep top-5 unmapped visible.
        n_keep = 5
    else:
        # No curated map: keep top-N visible.
        n_keep = TOP_N_NON_CURATED

    top = (
        df.loc[unmapped_mask]
          .groupby("CompanyName")["Quotes"]
          .sum()
          .sort_values(ascending=False)
          .head(n_keep)
          .index.tolist()
    )

    is_other = unmapped_mask & ~df["CompanyName"].isin(top)
    n_other = df.loc[is_other, "CompanyName"].nunique()
    df = df.copy()
    df.loc[is_other, "CompanyName"] = f"Other (N={n_other})"

    # Re-group to merge the now-identically-named Other rows into one per
    # (YYYYMM, group_cols) combination.
    value_cols = ["Quotes", "SumPremium", "BridgingCount", "SumBridgingPremium"]
    return (
        df.groupby(group_cols + ["YYYYMM"])[value_cols]
          .sum()
          .reset_index()
    )
