"""core/calculator.py

Scope3Calculator: Scope 3 emissions calculator (logistics transport, V1).

Front-end contract (DO NOT change):
- calculate_emissions(df=cleaned_df, weight_val_col='Weight', distance_val_col='Distance', mode_col='Transport_Mode')

Business rules:
- Emissions_tCO2e = (weight * distance * factor) / 1000
- Factors (case-insensitive): road=0.15, rail=0.02, ocean=0.01, air=1.25
- Strong downgrade control:
  * If mode is NA/empty or not in factor library: Emissions_tCO2e must be NaN (never raise, never stop).
  * Audit trail via ETL_Review_Flag:
    - If current flag == "Clean" (or empty/NA): overwrite with "Missing Transport Mode"
    - Else: append " | Missing Transport Mode"
- Dirty numeric data: if weight/distance cannot be converted to float, that row's emissions is NaN.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd


class Scope3Calculator:
    _MODE_TO_FACTOR_KGCO2E_PER_TKM: Dict[str, float] = {
        "road": 0.15,
        "rail": 0.02,
        "ocean": 0.01,
        "air": 1.25,
    }

    def __init__(self, *, tenant_id: Optional[str] = None):
        self.tenant_id = tenant_id

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

        weight_val = pd.to_numeric(result_df[weight_val_col], errors="coerce")
        distance_val = pd.to_numeric(result_df[distance_val_col], errors="coerce")

        mode_norm = result_df[mode_col].astype("string").str.strip().str.lower()
        factor = mode_norm.map(self._MODE_TO_FACTOR_KGCO2E_PER_TKM)

        invalid_mode_mask = mode_norm.isna() | (mode_norm == "") | factor.isna()

        emissions_tco2e = (weight_val * distance_val * factor.astype(float)) / 1000.0
        emissions_tco2e = emissions_tco2e.where(~invalid_mode_mask, np.nan)
        result_df["Emissions_tCO2e"] = emissions_tco2e.astype(float)

        if bool(invalid_mode_mask.any()):
            if "ETL_Review_Flag" not in result_df.columns:
                result_df["ETL_Review_Flag"] = "Clean"

            missing_mode_flag = "Missing Transport Mode"
            existing_flag = result_df["ETL_Review_Flag"].astype("string").fillna("").str.strip()

            clean_or_empty = (existing_flag == "") | (existing_flag == "Clean")
            already_appended = existing_flag.str.contains(missing_mode_flag, regex=False)

            updated_flag = pd.Series(existing_flag, index=result_df.index, dtype="string")
            updated_flag = updated_flag.mask(clean_or_empty, missing_mode_flag)
            updated_flag = updated_flag.mask(
                (~clean_or_empty) & (~already_appended),
                existing_flag + " | " + missing_mode_flag,
            )

            result_df.loc[invalid_mode_mask, "ETL_Review_Flag"] = updated_flag.loc[invalid_mode_mask]

        total_rows = int(len(result_df))
        success_rows = int(result_df["Emissions_tCO2e"].notna().sum())
        failed_rows = int(total_rows - success_rows)
        invalid_mode_rows = int(invalid_mode_mask.sum())

        logger.info(
            "Scope3Calculator complete. total=%s success=%s failed=%s invalid_mode=%s",
            total_rows,
            success_rows,
            failed_rows,
            invalid_mode_rows,
        )

        return result_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    demo_df = pd.DataFrame(
        [
            {
                "Shipment_ID": "OK_001",
                "Weight": 10.0,
                "Distance": 100.0,
                "Transport_Mode": "road",
                "ETL_Review_Flag": "Clean",
            },
            {
                "Shipment_ID": "DIRTY_NUM_001",
                "Weight": "oops",
                "Distance": 100.0,
                "Transport_Mode": "rail",
                "ETL_Review_Flag": "Clean",
            },
            {
                "Shipment_ID": "MISS_MODE_001",
                "Weight": 10.0,
                "Distance": 100.0,
                "Transport_Mode": np.nan,
                "ETL_Review_Flag": "Needs Manual Review",
            },
            {
                "Shipment_ID": "BAD_MODE_001",
                "Weight": 10.0,
                "Distance": 100.0,
                "Transport_Mode": "unknown_mode",
                "ETL_Review_Flag": "Clean",
            },
        ]
    )

    calc = Scope3Calculator(tenant_id="demo")
    out_df = calc.calculate_emissions(
        df=demo_df,
        weight_val_col="Weight",
        distance_val_col="Distance",
        mode_col="Transport_Mode",
    )
    print(out_df)
