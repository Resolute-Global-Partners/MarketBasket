"""Merge + group-by aggregation.

Per-(state, month) pipeline:
  1. Four SQL pulls (rate / car / driver / violation) for this one (state, yyyymm).
  2. preprocess_rate / _car / _driver (ported verbatim from the WSL pipeline).
  3. Merge on RateId, derive bin columns, groupby on 10 dimensions.
  4. Return one-month partial aggregate.

Top-N + Other bucketing for unmapped companies is applied LATER, on the
concatenated multi-month DataFrame — doing it per-month produces diverging
"Other (N=X)" labels that don't merge.
"""
from __future__ import annotations

import time

import pandas as pd

from . import sql
from .config import (
    CREDIT_CARRIER_STATE_LAPSE, CREDIT_CARRIER_STATE_NO_LAPSE,
    CREDIT_CARRIER_STATE_NO_PC, CREDIT_CODE_MAP_IL_V2, CREDIT_CODE_MAP_V1,
    CREDIT_FORMULA_BY_STATE, CREDIT_FORMULA_STATES, GROUP_COLS,
    IL_CREDIT_CODE_CUTOFF, PREM_BIN_CAP, PREM_BIN_SIZE,
    PREM_COLS, YEAR_BINS, YEAR_LABELS,
)
from .preprocess import (
    preprocess_car,
    preprocess_driver,
    preprocess_rate,
)


def _cap_drivers(n) -> str:
    if pd.isna(n):
        return "1"
    return "4+" if int(n) >= 4 else str(int(n))


def _cap_vehicles(n) -> str:
    if pd.isna(n):
        return "1"
    n = int(n)
    if n >= 5:
        return "5+"
    return str(n)


def _band_pts(values: pd.Series, bands: list[tuple[int, int, int]]) -> pd.Series:
    """For each value, find which (lo, hi, pts) band it falls in. 0 outside all
    bands — IN/NM/TN intentionally leave a gap at vehicle min age = 9.
    """
    pts = pd.Series(0, index=values.index, dtype="float64")
    for lo, hi, p in bands:
        in_band = (values >= lo) & (values <= hi)
        pts.loc[in_band] = p
    return pts


def _compute_predicted_credit(df: pd.DataFrame, state: str) -> pd.Series:
    """Row-level PredictedCredit using the per-state formula in
    CREDIT_FORMULA_BY_STATE. Vectorized.

    Inputs (must already be on df):
        PriorInsurance      int 0/1
        PriorMonthsCovg     int (months prior coverage)
        PriorDaysLapse      int
        Year                int (newest car year on policy)
        RatedDate           datetime
        NamedInsuredAge     int
        ResidencyStatus     str ('O' = own = homeowner)
        LiabLimits          str ('25/50' | '30/60' | '50/100' | '100/300')
        NumVehicles         str ('1'..'5+')

    Returns a Series of integer scores (NaN for rows missing inputs OR for
    states without a registered formula).
    """
    formula = CREDIT_FORMULA_BY_STATE.get(state)
    if formula is None:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")

    score = pd.Series(formula["base"], index=df.index, dtype="float64")

    # ── Prior Carrier / Lapse state ──────────────────────────────────────────
    pi = df["PriorInsurance"].fillna(0).astype(int)
    lapse = df["PriorDaysLapse"].fillna(0)
    carrier_state = pd.Series(CREDIT_CARRIER_STATE_NO_PC, index=df.index)
    carrier_state.loc[(pi == 1) & (lapse == 0)] = CREDIT_CARRIER_STATE_NO_LAPSE
    carrier_state.loc[(pi == 1) & (lapse > 0)]  = CREDIT_CARRIER_STATE_LAPSE

    score += carrier_state.map(formula["carrier"]).astype("float64")

    # ── Prior Duration pts ───────────────────────────────────────────────────
    # NO PC → treat as 0 months (falls in 0-5 band).
    months = df["PriorMonthsCovg"].where(pi == 1, 0).fillna(0).clip(lower=0)
    score += _band_pts(months, formula["prior_duration"])

    # ── Vehicle Min Age pts ──────────────────────────────────────────────────
    rated_year = df["RatedDate"].dt.year
    min_age = (rated_year - df["Year"]).clip(lower=0)
    score += _band_pts(min_age, formula["vehicle_min_age"])

    # ── Named Insured Age × carrier-state matrix ─────────────────────────────
    age = df["NamedInsuredAge"].fillna(-1)
    age_pts = pd.Series(0, index=df.index, dtype="float64")
    for lo, hi, mapping in formula["age_bands"]:
        in_band = (age >= lo) & (age <= hi)
        for cs_name, pts in mapping.items():
            age_pts.loc[in_band & (carrier_state == cs_name)] = pts
    score += age_pts

    # ── BI Limits / Vehicle Count / Homeowner ───────────────────────────────
    score += df["LiabLimits"].map(formula["bi_limits"]).fillna(0).astype("float64")
    score += df["NumVehicles"].map(formula["veh_count"]).fillna(0).astype("float64")
    score += (df["ResidencyStatus"] == "O").astype("float64") * formula["homeowner_true"]

    # Rows missing critical inputs → NaN.
    valid = (
        df["NamedInsuredAge"].notna()
        & df["LiabLimits"].notna()
        & df["NumVehicles"].notna()
        & df["Year"].notna()
        & df["RatedDate"].notna()
    )
    return score.where(valid).round().astype("Int64")


def _bucket_to_code(scores: pd.Series, code_map: list[tuple[int, int, str]]) -> pd.Series:
    """Map numeric scores to letter codes via a (lo, hi, code) lookup table.
    NaN preserved.
    """
    import numpy as np
    if scores.isna().all():
        return pd.Series(pd.NA, index=scores.index, dtype="object")
    boundaries = np.array([entry[1] for entry in code_map])
    labels = np.array([entry[2] for entry in code_map])
    # searchsorted with default side='left': first index where boundary >= value.
    filled = scores.fillna(-1).to_numpy()
    idx = np.searchsorted(boundaries, filled, side="left")
    idx = np.clip(idx, 0, len(labels) - 1)
    out = pd.Series(labels[idx], index=scores.index, dtype="object")
    out[scores.isna()] = pd.NA
    return out


def _assign_credit_code(
    scores: pd.Series, dates: pd.Series, state: str,
) -> pd.Series:
    """Compute CreditCode per row. IL has a date-dependent split (V1 before
    2026-04-02, V2 on/after). All other states use V1.
    """
    if state == "IL":
        cutoff = pd.Timestamp(IL_CREDIT_CODE_CUTOFF)
        is_v2 = dates >= cutoff
        result = pd.Series(pd.NA, index=scores.index, dtype="object")
        if (~is_v2).any():
            result.loc[~is_v2] = _bucket_to_code(scores[~is_v2], CREDIT_CODE_MAP_V1)
        if is_v2.any():
            result.loc[is_v2] = _bucket_to_code(scores[is_v2], CREDIT_CODE_MAP_IL_V2)
        return result
    return _bucket_to_code(scores, CREDIT_CODE_MAP_V1)


def _aggregate_one_state(
    state: str,
    yyyymm: str,
    rate: pd.DataFrame,
    car: pd.DataFrame,
    drv: pd.DataFrame,
    viol: pd.DataFrame,
) -> pd.DataFrame:
    """Preprocess + merge + groupby for one state's slice of one month."""
    if rate.empty:
        return pd.DataFrame()

    rate = preprocess_rate(rate, int(yyyymm), state)
    if rate.empty:
        return pd.DataFrame()

    if car.empty:
        return pd.DataFrame()
    car = preprocess_car(car, state)

    if not drv.empty:
        if viol.empty:
            viol = pd.DataFrame(columns=["RateDriverLinkId", "AtFault"])
        drv = preprocess_driver(drv, viol)
    else:
        drv = pd.DataFrame(columns=["RateId", "PriorInsurance", "AtFault", "NumDrivers"])

    df = (
        rate.merge(car, on="RateId", how="inner")
            .merge(drv, on="RateId", how="left")
    )
    if df.empty:
        return pd.DataFrame()

    # ── Collapse to one row per policy variant ──────────────────────────────
    # Grain: (PolicyLinkID, CompanyId, HasPhysDmg, HasUM_UIM, HasMedPay, PayPlanType).
    # PurchasedFinal = any row in the group had Purchased=1.
    # Pick MIN(TotalPremium) — drops the ~2% unexplained tier residual (NatGen
    # offers multiple base prices for the same coverage; we take the cheaper).
    collapse_key = [
        "PolicyLinkID", "CompanyId",
        "HasPhysDmg", "HasUM_UIM", "HasMedPay", "PayPlanType",
    ]
    df["PurchasedFinal"] = df.groupby(collapse_key)["Purchased"].transform("max")
    df = (
        df.sort_values("TotalPremium")
          .drop_duplicates(collapse_key, keep="first")
          .reset_index(drop=True)
    )

    df["YearBin"] = pd.cut(df["Year"], bins=YEAR_BINS, labels=YEAR_LABELS).astype(str)
    df["NumDrivers"] = df["NumDrivers"].apply(_cap_drivers)
    df["NumVehicles"] = df["NumVehicles"].apply(_cap_vehicles)
    df["PriorInsurance"] = df["PriorInsurance"].fillna(0).astype(int)
    df["NonOwner"] = df["NonOwner"].fillna(0).astype(int)

    df["PremBin"] = (
        df["TotalPremium"].clip(upper=PREM_BIN_CAP) // PREM_BIN_SIZE * PREM_BIN_SIZE
    ).astype(int)
    df["_bridge_prem"] = (
        df["TotalPremium"].where(df["PurchasedFinal"] == 1, 0).fillna(0)
    )
    # Per-coverage bridging premium — exact mirror of _bridge_prem, one per
    # coverage column. Lets the frontend report avg <C> per bridged policy
    # without scaling-by-ratio approximation.
    bridged = df["PurchasedFinal"] == 1
    for col in PREM_COLS:
        df[f"_bridge_{col}"] = df[col].where(bridged, 0).fillna(0)

    # PredictedCredit + CreditCode (only for states with a credit formula).
    # CreditCode is the letter grade (A..Z with skips). For IL, the bucketing
    # changes on 2026-04-02 so we pass RatedDate to the assigner.
    if state in CREDIT_FORMULA_STATES:
        df["PredictedCredit"] = _compute_predicted_credit(df, state)
        df["CreditCode"] = _assign_credit_code(df["PredictedCredit"], df["RatedDate"], state)
    else:
        df["CreditCode"] = pd.Series([pd.NA] * len(df), dtype="object")

    coverage_aggs = {f"Sum{col}": (col, "sum") for col in PREM_COLS}
    # SumLiabBIBridging, SumLiabPDBridging, ... — naming matches what the
    # frontend already expects (Sum<C>Bridging without the "Premium" suffix).
    coverage_bridge_aggs = {
        f"Sum{col[:-len('Premium')]}Bridging": (f"_bridge_{col}", "sum")
        for col in PREM_COLS
    }

    agg = (
        df.groupby(GROUP_COLS, dropna=False)
          .agg(
              Quotes=("TotalPremium", "count"),
              SumPremium=("TotalPremium", "sum"),
              BridgingCount=("PurchasedFinal", "sum"),
              SumBridgingPremium=("_bridge_prem", "sum"),
              **coverage_aggs,
              **coverage_bridge_aggs,
          )
          .reset_index()
    )
    agg["YYYYMM"] = int(yyyymm)
    return agg


def fetch_and_aggregate(
    state: str, yyyymm: str, *, verbose: bool = True,
) -> pd.DataFrame:
    """Pull 4 tables from SQL for one (state, month) and return the partial aggregate."""
    if verbose:
        print(f"  {state} {yyyymm}:", flush=True)

    t0 = time.perf_counter()
    rate = sql.fetch_rate(state, yyyymm)
    if verbose:
        print(f"    fact_Rate           {len(rate):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)
    if rate.empty:
        return pd.DataFrame()

    t0 = time.perf_counter()
    car = sql.fetch_car(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Car       {len(car):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    drv = sql.fetch_driver(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Driver    {len(drv):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    viol = sql.fetch_violation(state, yyyymm)
    if verbose:
        print(f"    fact_Rate_Violation {len(viol):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    agg = _aggregate_one_state(state, yyyymm, rate, car, drv, viol)
    if verbose:
        print(f"    -> aggregated to {len(agg):,} groupby rows   {time.perf_counter()-t0:6.1f}s", flush=True)
    return agg


def fetch_and_aggregate_state(
    state: str, months: list[str], *, verbose: bool = True,
) -> list[pd.DataFrame]:
    """Pull 4 tables ONCE for a state across many months, then aggregate each
    month locally. Each table is one full-table scan instead of N — ~10-15x
    faster than the per-(state, month) loop on scan-dominated workloads.

    Returns a list of per-month aggregates (one DataFrame per month, in input
    order). Empty months are skipped.
    """
    if not months:
        return []
    if verbose:
        print(f"  {state} (batched: {len(months)} months {months[0]}..{months[-1]}):", flush=True)

    t0 = time.perf_counter()
    rate_all = sql.fetch_rate_state(state, months)
    if verbose:
        print(f"    fact_Rate           {len(rate_all):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    car_all = sql.fetch_car_state(state, months)
    if verbose:
        print(f"    fact_Rate_Car       {len(car_all):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    drv_all = sql.fetch_driver_state(state, months)
    if verbose:
        print(f"    fact_Rate_Driver    {len(drv_all):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    t0 = time.perf_counter()
    viol_all = sql.fetch_violation_state(state, months)
    if verbose:
        print(f"    fact_Rate_Violation {len(viol_all):>10,} rows   {time.perf_counter()-t0:6.1f}s", flush=True)

    # Year_Month came back as a string column. Build per-month indices once.
    for df in (rate_all, car_all, drv_all):
        df["Year_Month"] = df["Year_Month"].astype(str).str.strip()

    # fact_Rate_Violation joins drivers via RateDriverLinkId; we only need
    # rows whose drivers fell into our months. Filter via Year_Month if
    # present (the join key is per-driver, so the column is duplicated).
    if "Year_Month" in viol_all.columns:
        viol_all["Year_Month"] = viol_all["Year_Month"].astype(str).str.strip()

    out: list[pd.DataFrame] = []
    for m in months:
        rate_m = rate_all[rate_all["Year_Month"] == m].drop(columns=["Year_Month"])
        if rate_m.empty:
            if verbose:
                print(f"    {m}: no fact_Rate rows", flush=True)
            continue
        car_m  = car_all[car_all["Year_Month"] == m].drop(columns=["Year_Month"])
        drv_m  = drv_all[drv_all["Year_Month"] == m].drop(columns=["Year_Month"])
        viol_m = (
            viol_all[viol_all["Year_Month"] == m].drop(columns=["Year_Month"])
            if "Year_Month" in viol_all.columns else viol_all
        )

        t0 = time.perf_counter()
        agg = _aggregate_one_state(state, m, rate_m, car_m, drv_m, viol_m)
        if verbose:
            print(f"    {m}: {len(rate_m):>7,} rate -> {len(agg):>7,} groupby rows   {time.perf_counter()-t0:6.1f}s",
                  flush=True)
        if not agg.empty:
            out.append(agg)
    return out
