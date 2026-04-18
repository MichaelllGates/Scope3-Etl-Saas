from __future__ import annotations

import io
import tempfile
from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
import streamlit as st

from core.auditor import AuditReportGenerator
from core.calculator import Scope3Calculator
from core.cleaner import Scope3Cleaner


st.set_page_config(page_title="Scope 3 ETL SaaS", layout="centered")

st.markdown(
    """
<style>
  /* Hide Streamlit clutter */
  #MainMenu { visibility: hidden; }
  header { visibility: hidden; }
  footer { visibility: hidden; }

  /* Premium typography + antigravity background */
  .stApp {
    font-family: ui-sans-serif, system-ui, -apple-system, "Inter", "Segoe UI", Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
    color: #0f172a;
    background:
      radial-gradient(1200px 600px at 20% 0%, rgba(56,189,248,0.16), transparent 60%),
      radial-gradient(900px 520px at 90% 10%, rgba(34,197,94,0.14), transparent 55%),
      linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
  }

  .block-container {
    padding-top: 2.4rem;
    padding-bottom: 3rem;
    max-width: 980px;
  }

  h1 {
    letter-spacing: -0.03em;
    line-height: 1.12;
    margin-bottom: 0.35rem;
  }

  /* Vercel-like buttons */
  div[data-testid="stButton"] button,
  div[data-testid="stDownloadButton"] button {
    background: #111827 !important;
    color: #ffffff !important;
    border: 0 !important;
    border-radius: 12px !important;
    padding: 0.72rem 1.05rem !important;
    font-weight: 650 !important;
    letter-spacing: 0.01em;
    box-shadow: 0 10px 24px rgba(2, 6, 23, 0.12) !important;
    transition: transform 120ms ease, box-shadow 120ms ease, filter 120ms ease;
  }

  div[data-testid="stButton"] button:hover,
  div[data-testid="stDownloadButton"] button:hover {
    transform: translateY(-1px);
    box-shadow: 0 14px 32px rgba(2, 6, 23, 0.16) !important;
    filter: brightness(1.02);
  }

  div[data-testid="stButton"] button:active,
  div[data-testid="stDownloadButton"] button:active {
    transform: translateY(0px) scale(0.99);
    box-shadow: 0 10px 24px rgba(2, 6, 23, 0.12) !important;
  }

  /* Make audit download visually distinct */
  div[data-testid="stDownloadButton"] button[aria-label*="Download PDF Audit Log"] {
    background: linear-gradient(90deg, #0f172a 0%, #111827 45%, #1f2937 100%) !important;
    box-shadow: 0 14px 36px rgba(17, 24, 39, 0.22) !important;
  }

  /* Card-like containers */
  div[data-testid="stFileUploader"] > section {
    background: rgba(255,255,255,0.75);
    border: 1px solid rgba(148,163,184,0.22);
    border-radius: 16px;
    padding: 0.85rem 0.9rem;
    box-shadow: 0 10px 30px rgba(2,6,23,0.06);
  }

  div[data-testid="stDataFrame"] > div {
    border-radius: 16px;
    overflow: hidden;
    border: 1px solid rgba(148,163,184,0.22);
    background: rgba(255,255,255,0.78);
    box-shadow: 0 10px 30px rgba(2,6,23,0.06);
  }

  /* Subtle tabs */
  button[role="tab"] {
    border-radius: 12px !important;
  }

  /* Reduce label noise */
  .stCaption { color: rgba(15, 23, 42, 0.72); }
</style>
""",
    unsafe_allow_html=True,
)


def _excel_engine() -> str:
    try:
        import xlsxwriter  # noqa: F401

        return "xlsxwriter"
    except Exception:
        return "openpyxl"


def _df_to_excel_bytes(df: pd.DataFrame, *, sheet_name: str) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine=_excel_engine()) as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


def _read_uploaded_file(uploaded_file: st.runtime.uploaded_file_manager.UploadedFile) -> pd.DataFrame:
    filename = (uploaded_file.name or "").lower()
    if filename.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Unsupported file type. Please upload a .csv or .xlsx file.")


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _append_flag(existing_flag: pd.Series, mask: pd.Series, flag: str) -> pd.Series:
    s = existing_flag.astype("string").fillna("").str.strip()
    contains_flag = s.str.contains(flag, regex=False)
    empty_or_clean = (s == "") | (s == "Clean")

    updated = pd.Series(s, index=s.index, dtype="string")
    updated = updated.mask(empty_or_clean, flag)
    updated = updated.mask(~empty_or_clean & ~contains_flag, s + " | " + flag)

    out = existing_flag.copy()
    out.loc[mask] = updated.loc[mask]
    return out


def _build_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Shipment_ID": "SAMPLE_001",
                "Weight": 1000,
                "Weight_Unit": "KGS",
                "Distance": 1200,
                "Distance_Unit": "KMS",
                "Transport_Mode": "road",
            },
            {
                "Shipment_ID": "SAMPLE_002",
                "Weight": None,
                "Weight_Unit": "kg",
                "Distance": 800,
                "Distance_Unit": "km",
                "Transport_Mode": "rail",
            },
            {
                "Shipment_ID": "SAMPLE_003",
                "Weight": 2200,
                "Weight_Unit": "lbs",
                "Distance": 500,
                "Distance_Unit": "mile",
                "Transport_Mode": "ocean",
            },
            {
                "Shipment_ID": "SAMPLE_004",
                "Weight": 2.5,
                "Weight_Unit": "WT_UNKNOWN",
                "Distance": 300,
                "Distance_Unit": "km",
                "Transport_Mode": "air",
            },
            {
                "Shipment_ID": "SAMPLE_005",
                "Weight": 10,
                "Weight_Unit": "t",
                "Distance": "oops",
                "Distance_Unit": "km",
                "Transport_Mode": None,
            },
        ]
    )


def _standardize_for_engine(cleaned_df: pd.DataFrame) -> pd.DataFrame:
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

    weight_t = pd.Series(np.nan, index=df.index, dtype="float")
    weight_t = weight_t.mask(w_unit == "t", weight_raw)
    weight_t = weight_t.mask(w_unit == "kg", weight_raw / 1000.0)
    weight_t = weight_t.mask(w_unit == "lbs", weight_raw * 0.00045359237)
    df["Std_Weight (t)"] = weight_t

    distance_km = pd.Series(np.nan, index=df.index, dtype="float")
    distance_km = distance_km.mask(d_unit == "km", distance_raw)
    distance_km = distance_km.mask(d_unit == "mile", distance_raw * 1.609344)
    df["Std_Distance (km)"] = distance_km

    missing_weight = df["Std_Weight (t)"].isna()
    missing_distance = df["Std_Distance (km)"].isna()

    df["ETL_Review_Flag"] = _append_flag(df["ETL_Review_Flag"], missing_weight, "Needs Manual Review: Weight")
    df["ETL_Review_Flag"] = _append_flag(df["ETL_Review_Flag"], missing_distance, "Needs Manual Review: Distance")

    df["Data_Tier"] = np.where((~missing_weight) & (~missing_distance), "Primary", "Estimated")

    return df


def _generate_audit_pdf_bytes(df_for_audit: pd.DataFrame) -> bytes:
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = f"{tmpdir}/Scope3_Audit_Log_V1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        AuditReportGenerator().generate_pdf_log(df_for_audit, output_path=out_path)
        with open(out_path, "rb") as f:
            return f.read()


with st.sidebar:
    st.markdown("### Scope 3 ETL SaaS")
    st.caption("Minimal UI · Zero-Retention")

    sample_df = _build_sample_df()
    st.download_button(
        label="📄 Download Sample Data (Excel)",
        data=_df_to_excel_bytes(sample_df, sheet_name="Sample Data"),
        file_name="scope3_sample_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width='stretch',
    )


st.title("🌱 Scope 3 ETL Engine")
st.caption("Premium, in-memory ETL. Zero-retention policy: uploaded files are processed securely in memory and never stored.")

uploaded_file = st.file_uploader("Data Upload (.csv / .xlsx)", type=["csv", "xlsx"])

raw_df: pd.DataFrame | None = None
if uploaded_file is not None:
    try:
        raw_df = _read_uploaded_file(uploaded_file)
    except Exception as exc:
        st.error(f"Failed to read file: {exc}")

if raw_df is not None:
    _require_columns(raw_df, ["Weight", "Weight_Unit", "Distance", "Distance_Unit", "Transport_Mode"])

    st.markdown("#### Raw Data Preview")
    st.dataframe(raw_df.head(5), width='stretch')

    run_clicked = st.button("Run ETL & Calculate", type="primary", width='stretch')
    if run_clicked:
        try:
            with st.spinner("Processing data securely in memory (Zero-Retention)..."):
                cleaner = Scope3Cleaner()
                cleaned_df = cleaner.clean_logistics_data(raw_df, weight_col="Weight_Unit", distance_col="Distance_Unit")

                standardized_df = _standardize_for_engine(cleaned_df)

                calculator = Scope3Calculator(tenant_id="demo")
                result_df = calculator.calculate_emissions(
                    df=standardized_df,
                    weight_val_col="Std_Weight (t)",
                    distance_val_col="Std_Distance (km)",
                    mode_col="Transport_Mode",
                )

                result_df["Carbon_Emission (tCO2e)"] = result_df["Emissions_tCO2e"]
                result_df["Review_Flag"] = (
                    result_df.get("ETL_Review_Flag", "")
                    .astype("string")
                    .fillna("")
                    .str.strip()
                    .replace({"Clean": ""})
                )

                st.session_state["result_df"] = result_df

                st.session_state["cleaned_excel_bytes"] = _df_to_excel_bytes(result_df, sheet_name="ETL Result")
                st.session_state["audit_pdf_bytes"] = _generate_audit_pdf_bytes(
                    result_df[
                        [
                            col
                            for col in [
                                "Shipment_ID",
                                "Std_Weight (t)",
                                "Std_Distance (km)",
                                "Data_Tier",
                                "Review_Flag",
                                "Carbon_Emission (tCO2e)",
                            ]
                            if col in result_df.columns
                        ]
                    ].copy()
                )

            st.success("ETL complete. Results are ready.")
        except Exception as exc:
            st.session_state.pop("result_df", None)
            st.session_state.pop("cleaned_excel_bytes", None)
            st.session_state.pop("audit_pdf_bytes", None)
            st.error(f"Processing failed: {exc}")


result_df = st.session_state.get("result_df")
if isinstance(result_df, pd.DataFrame):
    st.markdown("#### Results")

    if "ETL_Review_Flag" in result_df.columns:
        ok_df = result_df[result_df["ETL_Review_Flag"] == "Clean"]
        review_df = result_df[result_df["ETL_Review_Flag"] != "Clean"]

        tab_ok, tab_review = st.tabs(["✅ Clean", "⚠️ Needs Review"])
        with tab_ok:
            st.dataframe(ok_df, width='stretch')
        with tab_review:
            st.dataframe(review_df, uwidth='stretch')
    else:
        st.dataframe(result_df, uwidth='stretch')

    st.markdown("#### Exports")
    c1, c2 = st.columns(2)

    cleaned_excel_bytes = st.session_state.get("cleaned_excel_bytes")
    audit_pdf_bytes = st.session_state.get("audit_pdf_bytes")

    with c1:
        st.download_button(
            label="📥 Download Cleaned Excel",
            data=cleaned_excel_bytes if isinstance(cleaned_excel_bytes, (bytes, bytearray)) else b"",
            file_name="Scope3_Cleaned_Result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width='stretch',
        )

    with c2:
        st.download_button(
            label="🔒 Download PDF Audit Log (SHA-256)",
            data=audit_pdf_bytes if isinstance(audit_pdf_bytes, (bytes, bytearray)) else b"",
            file_name="Scope3_Audit_Log_V1.pdf",
            mime="application/pdf",
            width='stretch',
        )
