from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from core.auditor import AuditReportGenerator
from core.calculator import Scope3Calculator
from core.cleaner import Scope3Cleaner


def _append_flag(existing: pd.Series, mask: pd.Series, flag: str) -> pd.Series:
    s = existing.astype("string").fillna("").str.strip()
    contains_flag = s.str.contains(flag, regex=False)
    empty_or_clean = (s == "") | (s == "Clean")

    updated = pd.Series(s, index=s.index, dtype="string")
    updated = updated.mask(empty_or_clean, flag)
    updated = updated.mask(~empty_or_clean & ~contains_flag, s + " | " + flag)

    out = existing.copy()
    out.loc[mask] = updated.loc[mask]
    return out


def _standardize_units(cleaned_df: pd.DataFrame) -> pd.DataFrame:
    df = cleaned_df.copy()

    if "ETL_Review_Flag" not in df.columns:
        df["ETL_Review_Flag"] = "Clean"

    weight_raw = pd.to_numeric(df.get("Weight"), errors="coerce")
    distance_raw = pd.to_numeric(df.get("Distance"), errors="coerce")

    w_unit = (
        df.get("Std_Weight_Unit", df.get("Weight_Unit"))
        .astype("string")
        .fillna("")
        .str.strip()
        .str.lower()
    )
    d_unit = (
        df.get("Std_Distance_Unit", df.get("Distance_Unit"))
        .astype("string")
        .fillna("")
        .str.strip()
        .str.lower()
    )

    # Std_Weight (t)
    weight_t = pd.Series(np.nan, index=df.index, dtype="float")
    weight_t = weight_t.mask(w_unit == "t", weight_raw)
    weight_t = weight_t.mask(w_unit == "kg", weight_raw / 1000.0)
    weight_t = weight_t.mask(w_unit == "lbs", weight_raw * 0.00045359237)
    df["Std_Weight (t)"] = weight_t

    # Std_Distance (km)
    distance_km = pd.Series(np.nan, index=df.index, dtype="float")
    distance_km = distance_km.mask(d_unit == "km", distance_raw)
    distance_km = distance_km.mask(d_unit == "mile", distance_raw * 1.609344)
    df["Std_Distance (km)"] = distance_km

    missing_weight = df["Std_Weight (t)"].isna()
    missing_distance = df["Std_Distance (km)"].isna()

    df["ETL_Review_Flag"] = _append_flag(df["ETL_Review_Flag"], missing_weight, "Needs Manual Review: Weight")
    df["ETL_Review_Flag"] = _append_flag(df["ETL_Review_Flag"], missing_distance, "Needs Manual Review: Distance")

    # Data_Tier: treat any missing/dirty weight/distance as Estimated
    df["Data_Tier"] = np.where((~missing_weight) & (~missing_distance), "Primary", "Estimated")

    return df


def main() -> None:
    input_path = Path("data") / "sample_input.csv"
    print(f"[1/5] Load Data: reading {input_path} ...")

    if not input_path.exists():
        raise FileNotFoundError(
            f"Missing required input file: {input_path}. "
            "Create it or re-run the setup that generates sample data."
        )

    raw_df = pd.read_csv(input_path)
    print(f"Loaded rows: {len(raw_df)}")

    print("[2/5] Clean: running Scope3Cleaner.clean_logistics_data() ...")
    cleaner = Scope3Cleaner()
    cleaned_df = cleaner.clean_logistics_data(raw_df, weight_col="Weight_Unit", distance_col="Distance_Unit")
    print("Clean complete.")

    print("[3/5] Calculate: standardize units then run Scope3Calculator.calculate_emissions() ...")
    standardized_df = _standardize_units(cleaned_df)

    calculator = Scope3Calculator(tenant_id="demo")
    calculated_df = calculator.calculate_emissions(
        df=standardized_df,
        weight_val_col="Std_Weight (t)",
        distance_val_col="Std_Distance (km)",
        mode_col="Transport_Mode",
    )

    calculated_df["Carbon_Emission (tCO2e)"] = calculated_df["Emissions_tCO2e"]
    calculated_df["Review_Flag"] = (
        calculated_df.get("ETL_Review_Flag", "")
        .astype("string")
        .fillna("")
        .str.strip()
        .replace({"Clean": ""})
    )

    print("Calculation complete.")

    print("[4/5] Audit: generating PDF audit log ...")
    auditor = AuditReportGenerator()
    auditor.generate_pdf_log(calculated_df, output_path="Scope3_Audit_Log_V1.pdf")
    print("PDF exported: Scope3_Audit_Log_V1.pdf")

    print("[5/5] Output: essential columns preview")
    essential_cols = [
        col
        for col in [
            "Shipment_ID",
            "Std_Weight (t)",
            "Std_Distance (km)",
            "Data_Tier",
            "Review_Flag",
            "Carbon_Emission (tCO2e)",
        ]
        if col in calculated_df.columns
    ]
    print(calculated_df[essential_cols].head(50).to_string(index=False))

    print("SUCCESS: Closed-loop pipeline completed (Clean -> Calculate -> Audit).")


if __name__ == "__main__":
    main()
