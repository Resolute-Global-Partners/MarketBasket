# Market Basket — Data Transformation Documentation

Producers National Corporation
Live dashboard: https://resolute-global-partners.github.io/MarketBasket/

> Supersedes `MarketBasket_Transformations.pdf` (April 2026), which documents the
> retired pipeline: a 14-label `PayPlan` dimension, a 10-column group-by, and no
> `County`, `CreditCode`, coverage-signature flags or `RateSource`. Treat that PDF
> as historical only.

For each (state, month): query 4 tables from `MarketUnified`, clean and join
them, collapse each customer's quotes to one row per rate variant, derive
bucketed columns, aggregate on 16 dimensions, then apply company and county
bucketing across the whole multi-month frame. Output is one Parquet per state
(sharded per month when it would exceed 95 MiB).

---

## Source tables

Every query filters on `State_Name` and `Year_Month`.

| Table | Columns selected |
|---|---|
| `fact_Rate` | PolicyLinkID, RateId, CompanyId, RateIteration, RatedDate, TotalPremium, DownPayment, PercentDown, NumOfPayments, Purchased, NonOwner, AssumedCredit, Term, ThirdPartyId, **Rate_Source** |
| `fact_Rate_Car` | RateLinkID, RateVehicleId, LiabLimits1, LiabLimits2, Year, **County**, LiabBIPremium, LiabPDPremium, CompPremium, CollPremium, MedPayPremium, UIMBIPremium, UIMPDPremium, UninsBIPremium, UninsPDPremium |
| `fact_Rate_Driver` | RateLinkID, RateDriverId, PriorInsurance, **Age, Relation, ResidencyStatus, PriorMonthsCovg, PriorDaysLapse** |
| `fact_Rate_Violation` | RateDriverLinkId, AtFault |

> `Year_Month` is the **extract/snapshot partition, not the rated month**. A
> single monthly pull carries up to ~18 months of back-history plus a few days
> of spill into the next month. Step 3 filters to the target month.

---

## Step 1 — Driver (`fact_Rate_Driver` + `fact_Rate_Violation` → one row per RateId)

- Per driver: `AtFault = 1` if any violation row for that driver has AtFault.
- Collapse to one row per `RateLinkID` (= RateId):
  - `PriorInsurance` = 1 if **any** driver had prior insurance
  - `AtFault` = MAX across drivers
  - `NumDrivers` = COUNT of drivers
- **Named-insured columns** (the `Relation = 'I'` row, else the first driver):
  `NamedInsuredAge`, `ResidencyStatus`, `PriorMonthsCovg`, `PriorDaysLapse`.
  These feed the Predicted Credit equation in Step 6.

## Step 2 — Vehicle (`fact_Rate_Car` → one row per RateId)

- Map `(LiabLimits1, LiabLimits2)` to a label via the state's accepted tiers
  (TX floor is **30/60**; every other state **25/50**). **Drop the whole quote**
  if any vehicle has an unaccepted pair.
- Normalise `County`: uppercase, strip non-letters, apply a synonym table
  (`DU PAGE`/`DuPage` → `DUPAGE`; `SAINT CLAIR` → `STCLAIR`); empty → `UNKNOWN`.
- Collapse to one row per `RateLinkID`:
  - `LiabLimits`, `County` = first vehicle
  - `NumVehicles` = COUNT, `Year` = **MAX** model year (newest car)
  - all premium columns = SUM across vehicles
- Derive the **coverage-signature flags**:
  - `HasPhysDmg` = Comp or Coll premium > 0
  - `HasUM_UIM` = any Uninsured/Underinsured premium > 0
  - `HasMedPay` = MedPay premium > 0

## Step 3 — Rate (`fact_Rate`, row-level clean; **no dedup yet**)

1. Keep only rows where `YEAR(RatedDate)*100 + MONTH(RatedDate)` equals the
   target YYYYMM. (Also removes the `9999-12-31` sentinel rows.)
2. Drop any policy where `NonOwner` or `AssumedCredit` varies across its rows.
3. Repair dollar-coded `PercentDown`: if > 100, replace with
   `ROUND(DownPayment / TotalPremium * 100, 0)` clipped to [0, 100].
4. `PayPlanType` = `"Pay in Full"` when `PercentDown = 100`, else `"Various"`.
   All installment plans share the same total premium, so the old 14-label
   breakdown only inflated quote counts.
5. `RateSource` = trimmed `Rate_Source`; blank/null → `"Unknown"`.
6. Map `CompanyId` → `CompanyName` via the state's curated map; unmapped IDs
   stay as their numeric string (bucketed in Step 7).

## Step 4 — Join

`Rate` **INNER JOIN** `Car` on RateId (quotes with no valid vehicle are excluded)
→ **LEFT JOIN** `Driver` on RateId.

## Step 5 — Collapse rate → policy

The grain that defines a dashboard row:

```
(PolicyLinkID, CompanyId, RateSource, HasPhysDmg, HasUM_UIM, HasMedPay, PayPlanType)
```

- `PurchasedFinal` = MAX(Purchased) within the group.
- Keep **MIN(TotalPremium)** within the group. This drops the ~2% unexplained
  sub-tier residual (NatGen, Progressive and Acceptance publish several base
  prices for an identical coverage signature); we take the cheaper and accept a
  slight under-statement.
- `RateSource` is in the key so the 0.67% of policies quoted through two
  platforms keep both quotes rather than having one silently dropped.

## Step 6 — Derived columns

- `PremBin` = `FLOOR(MIN(TotalPremium, 5000) / 500) * 500`
- `YearBin` from newest vehicle year: ≤2009 `pre-2010`, 2010-2014, 2015-2019, ≥2020 `2020+`
- `NumDrivers`: ≥4 → `"4+"`; `NumVehicles`: ≥5 → `"5+"`
- `NonOwner`, `PriorInsurance`: null → 0
- `_bridge_prem` = TotalPremium where `PurchasedFinal = 1`, else 0, plus one
  `_bridge_<coverage>` mirror per coverage column
- **PredictedCredit** → **CreditCode**: a per-state additive points formula
  (base + prior-carrier/lapse state + prior duration + vehicle min age +
  named-insured age × carrier-state matrix + BI limits + vehicle count +
  homeowner), bucketed into letter grades A…Z skipping N/O/R/T/X/Y.
  IL switches to a second bucketing table on **2026-04-02**.

## Step 7 — Group-by aggregation (16 dimensions)

**Policy filters** (`"Any"` allowed in the UI — they describe the customer):
`CompanyName, PremBin, LiabLimits, NonOwner, NumDrivers, NumVehicles, County,
PriorInsurance, YearBin, Term, CreditCode, RateSource`

**Rate filters** (REQUIRED in the UI, no `"Any"` — they identify *which* of the
several rates a customer was offered; without forcing a choice the grid would
double-count): `PayPlanType, HasPhysDmg, HasUM_UIM, HasMedPay`

Measures: `Quotes` (COUNT), `SumPremium`, `BridgingCount` (SUM PurchasedFinal),
`SumBridgingPremium`, `Sum<Coverage>Premium` ×9, `Sum<Coverage>Bridging` ×9,
plus `YYYYMM`.

## Step 8 — Bucketing (runs on the full multi-month frame, never per month)

- **Companies** — all five active states have an exhaustive curated map, so
  every unmapped `CompanyId` collapses into one `Other` row.
- **Counties** — keep the largest counties by Quotes until cumulative coverage
  reaches `COUNTY_COVERAGE_TARGET` (0.90); the rest collapse to `Other`.
  Only **real** counties are ranked for the keep decision — an existing `Other`
  bucket must not re-enter the ranking, or each re-bucket eats more of the
  budget and coverage erodes on every refresh (the bug fixed in `5b0b0ad`).

  At 0.90 this keeps roughly **16 of 124** counties in IL and **40 of 268** in
  TX, leaving `Other` at ~8–11%. That is the intended trade: named counties
  cover 90% of *quotes*, not 90% of *counties*.

  > **Do not read the pre-2026-09 parquets as evidence of better coverage.**
  > Months 202606–202607 appeared to carry 124+ counties only because the
  > surrounding eroded months held ~49% of quotes in `Other`; that mass counts
  > toward the denominator, so the cumulative share of real counties never
  > reached 0.90 and the cutoff never fired. Once every month is clean the
  > cutoff engages normally and the county list gets shorter. This is the rule
  > working, not a regression.
  >
  > Raising the target is **one-way-destructive**: once a county is written as
  > `Other`, re-bucketing cannot recover it — it needs a fresh SQL pull.
  > Coverage curve if you ever revisit it (measured on 202606):
  > IL 0.95→27, 0.99→68, 0.999→102 counties; TX 0.95→63, 0.99→137, 0.999→213.
- **Withheld months** — `EXCLUDED_MONTHS_BY_STATE` drops months whose *source*
  load is incomplete, before bucketing, so they cannot skew the kept sets.

## Step 9 — Write

One `docs/data/<STATE>.parquet`, or `docs/data/<STATE>/<YYYYMM>.parquet` shards
when a single file would exceed 95 MiB (GitHub rejects pushes over 100 MiB).
`index.json` records months, companies, counties, credit codes, liab limits and
the shard list.

---

## Invariants worth knowing

- **Quotes ≤ policy variants.** With all four rate filters set, each combination
  selects exactly one rate per `(PolicyLinkID, CompanyId, RateSource)`.
  Selecting `RateSource = "Any"` can exceed this by ≤0.7%, the share of policies
  quoted on more than one platform.
- **A per-month county set is fixed at write time.** Bucketing cannot restore
  detail for months already collapsed to `Other`; that needs a re-pull.
- **National (IL CompanyId 7870394)** reports $0 UM ~84% of the time and so
  lands in the liability-only bucket where other IL carriers do not. Its
  liability-only figures are not comparable.
