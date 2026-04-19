from __future__ import annotations

import io
import tempfile
from datetime import datetime
from typing import Iterable

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from api.supabase_db import SupabaseManager
from core.auditor import AuditReportGenerator
from core.calculator import Scope3Calculator
from core.cleaner import Scope3Cleaner


st.set_page_config(page_title="Scope 3 ETL SaaS", layout="wide")

# Phase 2: Rule Memory (Supabase)
db = SupabaseManager()

# CSS injection must happen at the very beginning of the app to avoid flicker.
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

  /* Make audit download visually distinct (best-effort selector) */
  div[data-testid="stDownloadButton"] button[aria-label*="Download PDF Audit Log"],
  div[data-testid="stDownloadButton"] button[aria-label*="PDF Audit Log"] {
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


# ---------------------------- Auth Wall ----------------------------
st.session_state.setdefault("authenticated", False)
st.session_state.setdefault("username", None)

_VALID_USERS: dict[str, str] = {
    "admin": "scopify2026",
    "demo": "123456",
}


def _logout() -> None:
    st.session_state["authenticated"] = False
    st.session_state["username"] = None
    st.session_state.pop("result_df", None)
    st.session_state.pop("cleaned_excel_bytes", None)
    st.session_state.pop("audit_pdf_bytes", None)
    st.rerun()


def _render_login_screen() -> None:
    left, center, right = st.columns([1.15, 1.0, 1.15])
    with center:
        st.title("🔐 Welcome to Scopify")
        st.caption("Secure demo login. Zero-retention ETL runs in memory.")

        username = st.text_input("Username", value="", placeholder="admin or demo")
        password = st.text_input("Password", value="", type="password")

        if st.button("Sign in", type="primary", use_container_width=True):
            expected = _VALID_USERS.get((username or "").strip())
            if expected is not None and password == expected:
                st.session_state["authenticated"] = True
                st.session_state["username"] = (username or "").strip()
                st.rerun()
            else:
                st.error("Invalid credentials")

        st.caption("Demo accounts: admin / scopify2026 · demo / 123456")


if not st.session_state["authenticated"]:
    _render_login_screen()
    st.stop()


tenant_id = str(st.session_state.get("username") or "").strip() or "demo"


# ---------------------------- Helpers ----------------------------
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


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [col for col in required if col not in df.columns]


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

    df["Std_Weight (t)"] = np.nan
    df.loc[w_unit == "t", "Std_Weight (t)"] = weight_raw
    df.loc[w_unit == "kg", "Std_Weight (t)"] = weight_raw / 1000.0
    df.loc[w_unit == "lbs", "Std_Weight (t)"] = weight_raw * 0.00045359237

    df["Std_Distance (km)"] = np.nan
    df.loc[d_unit == "km", "Std_Distance (km)"] = distance_raw
    df.loc[d_unit == "mile", "Std_Distance (km)"] = distance_raw * 1.609344

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
    st.markdown("### Scopify")
    st.caption(f"Signed in as: `{tenant_id}`")

    if st.button("Logout", use_container_width=True):
        _logout()

    sample_df = _build_sample_df()
    st.download_button(
        label="📄 Download Sample Data (Excel)",
        data=_df_to_excel_bytes(sample_df, sheet_name="Sample Data"),
        file_name="scope3_sample_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )


top_left, top_right = st.columns([0.78, 0.22])
with top_left:
    st.title("🌱 Scope 3 ETL Engine")
    st.caption(
        "Premium, in-memory ETL. Zero-retention policy: uploaded files are processed securely in memory and never stored."
    )
with top_right:
    if st.button("Logout", use_container_width=True):
        _logout()


uploaded_file = st.file_uploader("Data Upload (.csv / .xlsx)", type=["csv", "xlsx"])

raw_df: pd.DataFrame | None = None
if uploaded_file is not None:
    try:
        raw_df = _read_uploaded_file(uploaded_file)
    except Exception as exc:
        st.error(f"Failed to read file: {exc}")

required_cols = ["Weight", "Weight_Unit", "Distance", "Distance_Unit", "Transport_Mode"]

if raw_df is not None:
    missing = _missing_columns(raw_df, required_cols)
    if missing:
        st.error(f"Missing required columns: {missing}")
    else:
        st.markdown("#### Raw Data Preview")
        st.dataframe(raw_df.head(5), use_container_width=True)

        run_clicked = st.button("Run ETL & Calculate", type="primary", use_container_width=True)
        if run_clicked:
            try:
                with st.spinner("Processing data securely in memory (Zero-Retention)..."):
                    custom_rules = db.get_tenant_mappings(tenant_id=tenant_id)
                    ef_dict = db.get_emission_factors()

                    cleaner = Scope3Cleaner(custom_mapping=custom_rules)
                    cleaned_df = cleaner.clean_logistics_data(
                        raw_df,
                        weight_col="Weight_Unit",
                        distance_col="Distance_Unit",
                    )

                    standardized_df = _standardize_for_engine(cleaned_df)

                    calculator = Scope3Calculator(tenant_id=tenant_id, ef_mapping=ef_dict)
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

                    audit_cols = [
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
                    st.session_state["audit_pdf_bytes"] = _generate_audit_pdf_bytes(result_df[audit_cols].copy())

                st.success("ETL complete. Results are ready.")
            except Exception as exc:
                st.session_state.pop("result_df", None)
                st.session_state.pop("cleaned_excel_bytes", None)
                st.session_state.pop("audit_pdf_bytes", None)
                st.error(f"Processing failed: {exc}")


result_df = st.session_state.get("result_df")
if isinstance(result_df, pd.DataFrame):
    st.markdown("### 📊 Executive Overview")

    emissions_col = None
    for candidate in ("Carbon_Emission", "Carbon_Emission (tCO2e)", "Emissions_tCO2e"):
        if candidate in result_df.columns:
            emissions_col = candidate
            break

    if emissions_col is None:
        emissions = pd.Series(np.nan, index=result_df.index, dtype="float")
    else:
        emissions = pd.to_numeric(result_df[emissions_col], errors="coerce")

    total_rows = int(len(result_df))
    total_emissions = float(np.nansum(emissions.to_numpy(dtype=float))) if total_rows else 0.0

    clean_rate = 0.0
    if total_rows:
        if "ETL_Review_Flag" in result_df.columns:
            flags = result_df["ETL_Review_Flag"].astype("string").fillna("").str.strip()
            clean_rate = float((flags == "Clean").mean() * 100.0)
        elif "Review_Flag" in result_df.columns:
            flags = result_df["Review_Flag"].astype("string").fillna("").str.strip()
            clean_rate = float(((flags == "Clean") | (flags == "")).mean() * 100.0)

    k1, k2, k3 = st.columns(3)
    with k1:
        st.metric("Total Rows Processed", f"{total_rows}")
    with k2:
        st.metric("Total Carbon Emissions", f"{total_emissions:.2f}")
    with k3:
        st.metric("Clean Data Rate", f"{clean_rate:.1f}%")

    if emissions.notna().any():
        c_left, c_right = st.columns(2)

        with c_left:
            if "Transport_Mode" in result_df.columns:
                by_mode = pd.DataFrame(
                    {"Transport_Mode": result_df["Transport_Mode"], "Carbon_Emission": emissions}
                ).dropna(subset=["Carbon_Emission"])
                if by_mode.empty:
                    st.warning("No valid emissions to chart by mode.")
                else:
                    mode_sum = (
                        by_mode.groupby("Transport_Mode", dropna=False)["Carbon_Emission"]
                        .sum(min_count=1)
                        .reset_index()
                    )
                    mode_sum["Transport_Mode"] = mode_sum["Transport_Mode"].astype("string").fillna("(Missing)")
                    fig1 = px.pie(
                        mode_sum,
                        names="Transport_Mode",
                        values="Carbon_Emission",
                        hole=0.4,
                        title="Emissions by Mode",
                    )
                    st.plotly_chart(fig1, use_container_width=True)
            else:
                st.warning("Missing column: Transport_Mode")

        with c_right:
            if "Shipment_ID" in result_df.columns:
                hotspots = pd.DataFrame(
                    {"Shipment_ID": result_df["Shipment_ID"], "Carbon_Emission": emissions}
                )
                hotspots = (
                    hotspots.dropna(subset=["Carbon_Emission"])
                    .sort_values("Carbon_Emission", ascending=False)
                    .head(5)
                )
                if hotspots.empty:
                    st.warning("No valid emissions to chart hotspots.")
                else:
                    fig2 = px.bar(
                        hotspots,
                        x="Shipment_ID",
                        y="Carbon_Emission",
                        title="Top 5 Carbon Hotspots",
                    )
                    st.plotly_chart(fig2, use_container_width=True)
            else:
                st.warning("Missing column: Shipment_ID")
    else:
        st.warning("No emissions available for charts (all values are NaN).")

    st.markdown("#### Results")

    if "ETL_Review_Flag" in result_df.columns:
        ok_df = result_df[result_df["ETL_Review_Flag"] == "Clean"]
        review_df = result_df[result_df["ETL_Review_Flag"] != "Clean"]

        tab_ok, tab_review = st.tabs(["✅ Clean", "⚠️ Needs Review"])
        with tab_ok:
            st.dataframe(ok_df, use_container_width=True)
        with tab_review:
            st.dataframe(review_df, use_container_width=True)
    else:
        st.dataframe(result_df, use_container_width=True)

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
            use_container_width=True,
        )

    with c2:
        st.download_button(
            label="🔒 Download PDF Audit Log (SHA-256)",
            data=audit_pdf_bytes if isinstance(audit_pdf_bytes, (bytes, bytearray)) else b"",
            file_name="Scope3_Audit_Log_V1.pdf",
            mime="application/pdf",
            use_container_width=True,
        )


with st.expander("🧠 Teach the Engine (Add Custom Unit Mapping)", expanded=False):
    st.caption("Save a custom unit rule to Supabase. The engine will remember it next time.")

    with st.form("rule_memory_form"):
        raw_input = st.text_input("Raw/Dirty Unit (e.g., 三大箱)")
        std_input = st.selectbox("Map to Standard Unit", ["t", "kg", "lbs", "km", "mile"])
        submitted = st.form_submit_button("Save Rule to Cloud")

    if submitted:
        raw_unit = (raw_input or "").strip()
        std_unit = (std_input or "").strip()

        if not raw_unit:
            st.error("Please enter a raw/dirty unit.")
        else:
            ok = db.add_mapping(tenant_id=tenant_id, raw_unit=raw_unit, std_unit=std_unit)
            if ok:
                st.success("Rule saved! The engine will remember this next time.")
            else:
                st.error("Failed to save rule. Check Supabase secrets/network and try again.")


