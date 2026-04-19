"""core/calculator.py

Scope3Calculator: Scope 3 emissions calculator (logistics transport, V1).

Front-end contract (DO NOT change):
- calculate_emissions(df=cleaned_df, weight_val_col='Weight', distance_val_col='Distance', mode_col='Transport_Mode')

Waterfall Fallback Strategy (Chain of Responsibility, row-by-row):
1) Primary (Activity-based)
   - Condition: weight, distance, transport mode, and transport EF are all valid.
   - Action: Carbon_Emission (tCO2e) = (weight * distance * transport_ef) / 1000
   - Meta: Data_Tier = "Primary (Activity-based)"
   - Transparency: Applied_Emission_Factor = transport_ef

2) Fallback (Spend-based)
   - Condition: Tier 1 fails, but Spend_Amount + Sector + spend EF are valid.
   - Action: Carbon_Emission (tCO2e) = (Spend_Amount * kg_co2e_per_usd) / 1000
   - Meta: Data_Tier = "Estimated (Spend-based)"
   - Transparency: Applied_Emission_Factor = spend_ef
   - Self-healing (CRITICAL): strip missing-data warnings (weight/distance/mode) from
     existing flags for this row. If nothing else remains, force "Clean".

3) True failure
   - Condition: All tiers fail.
   - Action: Carbon_Emission (tCO2e) = NaN
   - Meta: Keep existing warnings untouched.
   - Transparency: Applied_Emission_Factor = NaN

Graceful degradation (unchanged behavior):
- If activity-based path has valid weight+distance but Transport_Mode is missing/invalid,
  emissions must be NaN and a "Missing Transport Mode" flag must be present.

Outputs (standardized):
- Final carbon result column (ONLY ONE): `Carbon_Emission (tCO2e)`
- Factor transparency: `Applied_Emission_Factor`
- Meta: `Data_Tier`
- Flags: `ETL_Review_Flag` (canonical) and `Review_Flag` (UI-friendly alias)

Redundant columns like `Emissions_tCO2e` or `Carbon_Emission` are dropped before return.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd


DEFAULT_ACTIVITY_EF_MAPPING: Dict[str, float] = {
    "road": 0.15,
    "rail": 0.02,
    "ocean": 0.01,
    "air": 1.25,
}

_MISSING_DATA_FLAGS_TO_STRIP: set[str] = {
    "Needs Manual Review",
    "Needs Manual Review: Weight",
    "Needs Manual Review: Distance",
    "Missing Transport Mode",
    "Missing Weight/Distance",
}


class Scope3Calculator:
    def __init__(
        self,
        *,
        tenant_id: Optional[str] = None,
        ef_mapping: Optional[Dict[str, float]] = None,
        spend_ef_mapping: Optional[Dict[str, float]] = None,
    ):
        self.tenant_id = tenant_id
        self.ef_mapping = self._normalize_mapping(ef_mapping, default=DEFAULT_ACTIVITY_EF_MAPPING)
        self.spend_ef_mapping = self._normalize_mapping(spend_ef_mapping, default={})

    def _normalize_mapping(self, mapping: Optional[Dict[str, float]], *, default: Dict[str, float]) -> Dict[str, float]:
        if not mapping:
            return default.copy()

        normalized: Dict[str, float] = {}
        for k, v in dict(mapping).items():
            key = str(k).strip().lower()
            if not key:
                continue
            try:
                normalized[key] = float(v)
            except Exception:
                continue

        return normalized or default.copy()

    def _append_flag(self, existing: str, flag: str) -> str:
        existing = (existing or "").strip()
        flag = (flag or "").strip()
        if not flag:
            return existing or "Clean"

        if existing in ("", "Clean"):
            return flag

        tokens = [t.strip() for t in existing.split("|")]
        tokens = [t for t in tokens if t]
        if flag in tokens:
            return " | ".join(tokens)
        return " | ".join(tokens + [flag])

    def _strip_missing_data_flags(self, existing: str) -> str:
        """Remove known missing-data flags. If nothing remains, return "Clean"."""

        existing = (existing or "").strip()
        if existing in ("", "Clean"):
            return "Clean"

        tokens = [t.strip() for t in existing.split("|")]
        kept: list[str] = []
        for token in tokens:
            if not token:
                continue

            if token in _MISSING_DATA_FLAGS_TO_STRIP:
                continue

            lower = token.lower()
            if lower.startswith("needs manual review") and ("weight" in lower or "distance" in lower):
                continue

            kept.append(token)

        return "Clean" if len(kept) == 0 else " | ".join(kept)

    def _coerce_float(self, value) -> float:
        try:
            return float(value)
        except Exception:
            return float("nan")

    def _row_waterfall(
        self,
        row: pd.Series,
        *,
        weight_val_col: str,
        distance_val_col: str,
        mode_col: str,
        existing_tier: str,
        existing_flag: str,
    ) -> tuple[float, float, str, str]:
        """Return (emissions_tco2e, applied_factor, new_data_tier_or_empty, new_etl_flag)."""

        flag = (existing_flag or "Clean").strip() or "Clean"

        weight = self._coerce_float(row.get(weight_val_col))
        distance = self._coerce_float(row.get(distance_val_col))

        mode = row.get(mode_col)
        mode_norm = ("" if pd.isna(mode) else str(mode)).strip().lower()
        transport_factor = self.ef_mapping.get(mode_norm) if mode_norm else None

        has_physical = (not np.isnan(weight)) and (not np.isnan(distance))

        # Tier 1: Primary (Activity-based)
        if has_physical and mode_norm and transport_factor is not None:
            emissions = (weight * distance * float(transport_factor)) / 1000.0
            return float(emissions), float(transport_factor), "Primary (Activity-based)", flag

        # Physical exists, but transport mode invalid -> audit flag
        if has_physical and (not mode_norm or transport_factor is None):
            flag = self._append_flag(flag, "Missing Transport Mode")

        # Tier 2: Fallback (Spend-based)
        spend_amt = self._coerce_float(row.get("Spend_Amount"))
        sector = row.get("Sector")
        sector_norm = ("" if pd.isna(sector) else str(sector)).strip().lower()
        spend_factor = self.spend_ef_mapping.get(sector_norm) if sector_norm else None

        if (not np.isnan(spend_amt)) and sector_norm and (spend_factor is not None):
            emissions = (spend_amt * float(spend_factor)) / 1000.0
            healed_flag = self._strip_missing_data_flags(flag)
            return float(emissions), float(spend_factor), "Estimated (Spend-based)", healed_flag

        # Tier N: True failure
        _ = existing_tier
        return float("nan"), float("nan"), "", flag

    def calculate_emissions(
        self,
        df: pd.DataFrame,
        weight_val_col: str,
        distance_val_col: str,
        mode_col: str,
    ) -> pd.DataFrame:
        """Calculate emissions and return a new DataFrame.

        Signature is locked to match frontend calls. Do not rename parameters.
        """

        logger = logging.getLogger(__name__)

        for col in (weight_val_col, distance_val_col, mode_col):
            if col not in df.columns:
                raise ValueError(f"Missing required column `{col}`. Available columns: {list(df.columns)}")

        result_df = df.copy()

        # Ensure metadata columns exist.
        if "Data_Tier" not in result_df.columns:
            result_df["Data_Tier"] = ""
        if "ETL_Review_Flag" not in result_df.columns:
            if "Review_Flag" in result_df.columns:
                src = result_df["Review_Flag"].astype("string").fillna("").str.strip()
                result_df["ETL_Review_Flag"] = src.mask(src == "", "Clean")
            else:
                result_df["ETL_Review_Flag"] = "Clean"

        existing_tiers = result_df["Data_Tier"].astype("string").fillna("").str.strip()
        existing_flags = result_df["ETL_Review_Flag"].astype("string").fillna("").str.strip()
        existing_flags = existing_flags.mask(existing_flags == "", "Clean")

        def _apply(row: pd.Series) -> pd.Series:
            idx = row.name
            emissions, applied_factor, tier, flag = self._row_waterfall(
                row,
                weight_val_col=weight_val_col,
                distance_val_col=distance_val_col,
                mode_col=mode_col,
                existing_tier=str(existing_tiers.loc[idx]),
                existing_flag=str(existing_flags.loc[idx]),
            )
            return pd.Series({"_em": emissions, "_factor": applied_factor, "_tier": tier, "_flag": flag})

        out = result_df.apply(_apply, axis=1)

        emissions_series = pd.to_numeric(out["_em"], errors="coerce").astype(float)
        factor_series = pd.to_numeric(out["_factor"], errors="coerce").astype(float)

        result_df["Applied_Emission_Factor"] = factor_series
        result_df["Carbon_Emission (tCO2e)"] = emissions_series

        tier_series = out["_tier"].astype("string").fillna("").str.strip()
        tier_mask = tier_series != ""
        result_df.loc[tier_mask, "Data_Tier"] = tier_series.loc[tier_mask]

        flag_series = out["_flag"].astype("string").fillna("").str.strip()
        flag_series = flag_series.mask(flag_series == "", "Clean")
        result_df["ETL_Review_Flag"] = flag_series
        result_df["Review_Flag"] = flag_series.replace({"Clean": ""})

        # Drop redundant historical columns to keep output clean.
        for redundant in ("Emissions_tCO2e", "Carbon_Emission"):
            if redundant in result_df.columns:
                result_df = result_df.drop(columns=[redundant])

        total_rows = int(len(result_df))
        success_rows = int(result_df["Carbon_Emission (tCO2e)"].notna().sum())
        failed_rows = int(total_rows - success_rows)
        activity_rows = int((result_df["Data_Tier"] == "Primary (Activity-based)").sum())
        spend_rows = int((result_df["Data_Tier"] == "Estimated (Spend-based)").sum())

        logger.info(
            "Scope3Calculator complete. total=%s success=%s failed=%s activity=%s spend=%s",
            total_rows,
            success_rows,
            failed_rows,
            activity_rows,
            spend_rows,
        )

        return result_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    demo_df = pd.DataFrame(
        [
            {
                "Shipment_ID": "T1_OK",
                "Std_Weight (t)": 10.0,
                "Std_Distance (km)": 100.0,
                "Transport_Mode": "road",
                "ETL_Review_Flag": "Clean",
            },
            {
                "Shipment_ID": "T1_MISS_MODE",
                "Std_Weight (t)": 10.0,
                "Std_Distance (km)": 100.0,
                "Transport_Mode": np.nan,
                "ETL_Review_Flag": "Needs Manual Review",
            },
            {
                "Shipment_ID": "T2_SPEND_HEAL",
                "Std_Weight (t)": np.nan,
                "Std_Distance (km)": np.nan,
                "Spend_Amount": 2500,
                "Sector": "Logistics",
                "Transport_Mode": "",
                "ETL_Review_Flag": "Needs Manual Review: Weight | Missing Transport Mode",
            },
            {
                "Shipment_ID": "FAIL",
                "Std_Weight (t)": np.nan,
                "Std_Distance (km)": np.nan,
                "Spend_Amount": np.nan,
                "Sector": "",
                "Transport_Mode": "",
                "ETL_Review_Flag": "Needs Manual Review: Weight",
            },
        ]
    )

    ef = {"road": 0.145, "rail": 0.019, "ocean": 0.011, "air": 1.23}
    spend = {"logistics": 0.5}

    calc = Scope3Calculator(tenant_id="demo", ef_mapping=ef, spend_ef_mapping=spend)
    out = calc.calculate_emissions(
        df=demo_df,
        weight_val_col="Std_Weight (t)",
        distance_val_col="Std_Distance (km)",
        mode_col="Transport_Mode",
    )
    print(out[["Shipment_ID", "Applied_Emission_Factor", "Carbon_Emission (tCO2e)", "Data_Tier", "ETL_Review_Flag", "Review_Flag"]])
