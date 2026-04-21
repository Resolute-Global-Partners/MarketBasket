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
    GROUP_COLS, PAYPLAN_LABELS, PREM_BIN_CAP, PREM_BIN_SIZE,
    YEAR_BINS, YEAR_LABELS,
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
    return "3+" if int(n) >= 3 else str(int(n))


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
    car = preprocess_car(car)

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

    df["PayPlan"] = df["PayPlan"].map(PAYPLAN_LABELS).fillna(df["PayPlan"])
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

    agg = (
        df.groupby(GROUP_COLS)
          .agg(
              Quotes=("TotalPremium", "count"),
              SumPremium=("TotalPremium", "sum"),
              BridgingCount=("PurchasedFinal", "sum"),
              SumBridgingPremium=("_bridge_prem", "sum"),
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
