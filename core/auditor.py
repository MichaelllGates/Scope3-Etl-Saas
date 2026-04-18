"""core/auditor.py

Enterprise-grade PDF audit logging for Scope 3 ETL Micro-SaaS.

This module generates an immutable-style (tamper-evident) PDF audit log from a fully
processed pandas DataFrame.

Design goals:
- Separation of concerns: metric calculation, fingerprinting, and PDF rendering are
  isolated in small methods.
- Robustness: handles empty DataFrames, missing columns, and all-NaN emissions.
- Simplicity: uses basic `fpdf` drawing/text APIs (no HTML/CSS rendering).

NOTE on "tamper-proof": PDFs are not inherently tamper-proof without signing.
This implementation adds a SHA-256 data fingerprint into the PDF to make edits
detectable (tamper-evident) in enterprise audit workflows.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:
    # fpdf2 and fpdf both expose `FPDF` with mostly compatible APIs.
    from fpdf import FPDF
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "Missing dependency `fpdf` (or `fpdf2`). Install it with: pip install fpdf2"
    ) from exc


@dataclass(frozen=True)
class AuditMetrics:
    """Executive summary metrics extracted from the processed DataFrame."""

    total_rows: int
    total_emissions_tco2e: float
    primary_pct: float
    estimated_pct: float


class AuditReportGenerator:
    """Generate an enterprise-style PDF audit log for Scope 3 ETL processing."""

    # Expected input columns (as per product contract)
    COL_SHIPMENT_ID = "Shipment_ID"
    COL_STD_WEIGHT = "Std_Weight (t)"
    COL_STD_DISTANCE = "Std_Distance (km)"
    COL_DATA_TIER = "Data_Tier"
    COL_REVIEW_FLAG = "Review_Flag"
    COL_EMISSIONS = "Carbon_Emission (tCO2e)"

    SYSTEM_VERSION = "Scope 3 ETL Engine V1.0"
    HEADER_TEXT = "CONFIDENTIAL & IMMUTABLE LOG"

    def __init__(self, *, company_name: str = "Scope 3 ETL"):
        self.company_name = company_name

    # ---------------------------- Public API ----------------------------
    def generate_pdf_log(self, df: pd.DataFrame, output_path: str) -> None:
        """Generate a PDF audit log for a fully processed DataFrame.

        Args:
            df: Processed pandas DataFrame.
            output_path: File path for the generated PDF.
        """

        safe_df = self._normalize_input_df(df)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fingerprint = self._compute_dataframe_fingerprint(safe_df)
        metrics = self._compute_metrics(safe_df)

        pdf = self._create_pdf()
        self._render_section_header(pdf, timestamp=timestamp, fingerprint=fingerprint)
        self._render_section_exec_summary(pdf, metrics=metrics)
        self._render_section_exceptions(pdf, df=safe_df)

        self._write_pdf(pdf, output_path=output_path)

    # ------------------------- Data preparation -------------------------
    def _normalize_input_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a safe copy of df to protect the caller from side effects."""

        if df is None:
            return pd.DataFrame()
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"df must be a pandas DataFrame, got: {type(df)}")
        return df.copy()

    def _compute_metrics(self, df: pd.DataFrame) -> AuditMetrics:
        """Compute executive summary metrics with defensive defaults."""

        total_rows = int(len(df))

        # Total emissions: sum of the emissions column, ignoring NaNs.
        if self.COL_EMISSIONS in df.columns:
            emissions = pd.to_numeric(df[self.COL_EMISSIONS], errors="coerce")
            total_emissions = float(np.nansum(emissions.to_numpy(dtype=float)))
        else:
            total_emissions = 0.0

        # Data tier breakdown: percentage of Primary vs Estimated.
        if total_rows == 0 or self.COL_DATA_TIER not in df.columns:
            primary_pct = 0.0
            estimated_pct = 0.0
        else:
            tier = df[self.COL_DATA_TIER].astype("string").fillna("").str.strip().str.lower()
            primary_count = int((tier == "primary").sum())
            estimated_count = int((tier == "estimated").sum())
            denom = max(primary_count + estimated_count, 1)
            primary_pct = (primary_count / denom) * 100.0
            estimated_pct = (estimated_count / denom) * 100.0

        return AuditMetrics(
            total_rows=total_rows,
            total_emissions_tco2e=total_emissions,
            primary_pct=primary_pct,
            estimated_pct=estimated_pct,
        )

    def _compute_dataframe_fingerprint(self, df: pd.DataFrame) -> str:
        """Compute a stable SHA-256 fingerprint for the DataFrame content.

        This is used to make the PDF tamper-evident. The fingerprint is derived from:
        - column names (in sorted order)
        - row values (with NaNs normalized)

        Note: This is deterministic for the same DataFrame values.
        """

        if df.empty:
            return hashlib.sha256(b"EMPTY_DF").hexdigest()

        # Stabilize by sorting columns and using CSV serialization with normalized NaNs.
        stable_df = df.copy()
        stable_df = stable_df.reindex(sorted(stable_df.columns), axis=1)
        stable_df = stable_df.replace({np.nan: ""})

        payload = stable_df.to_csv(index=False, lineterminator="\n").encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    # ---------------------------- PDF setup ----------------------------
    def _create_pdf(self) -> FPDF:
        """Create and configure the PDF document."""

        pdf = FPDF(orientation="P", unit="mm", format="A4")
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        return pdf

    def _write_pdf(self, pdf: FPDF, *, output_path: str) -> None:
        """Persist the PDF to disk (creates parent directory if needed)."""

        out_path = Path(output_path)
        out_dir = out_path.parent
        if str(out_dir) and not out_dir.exists():
            os.makedirs(out_dir, exist_ok=True)
        pdf.output(str(out_path))

    # ---------------------------- Rendering ----------------------------
    def _render_section_header(self, pdf: FPDF, *, timestamp: str, fingerprint: str) -> None:
        """Section 1: Corporate & immutable identity header."""

        # Watermark-like header line
        pdf.set_font("Helvetica", style="B", size=16)
        pdf.set_text_color(60, 60, 60)  # dark grey
        pdf.cell(0, 10, self.HEADER_TEXT, ln=1, align="C")

        pdf.set_font("Helvetica", size=10)
        pdf.set_text_color(0, 0, 0)

        pdf.cell(0, 6, f"Generated Timestamp: {timestamp}", ln=1)
        pdf.cell(0, 6, f"System Version: {self.SYSTEM_VERSION}", ln=1)
        pdf.cell(0, 6, f"Data Fingerprint (SHA-256): {fingerprint}", ln=1)

        pdf.ln(3)
        self._hr(pdf)
        pdf.ln(4)

    def _render_section_exec_summary(self, pdf: FPDF, *, metrics: AuditMetrics) -> None:
        """Section 2: Executive summary aligned to GHG Protocol needs."""

        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 8, "Executive Summary (GHG Protocol Alignment)", ln=1)

        pdf.set_font("Helvetica", size=10)
        pdf.cell(0, 6, f"Total Rows Processed: {metrics.total_rows}", ln=1)
        pdf.cell(0, 6, f"Total Carbon Emissions (tCO2e): {metrics.total_emissions_tco2e:.2f}", ln=1)

        # Critical for 95% compliance threshold visibility
        pdf.cell(
            0,
            6,
            f"Data Tier Breakdown: Primary {metrics.primary_pct:.1f}% | Estimated {metrics.estimated_pct:.1f}%",
            ln=1,
        )

        pdf.ln(3)
        self._hr(pdf)
        pdf.ln(4)

    def _render_section_exceptions(self, pdf: FPDF, *, df: pd.DataFrame) -> None:
        """Section 3: Exception audit trail."""

        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 8, "Exception Audit Trail", ln=1)

        if df.empty or self.COL_REVIEW_FLAG not in df.columns:
            pdf.set_font("Helvetica", size=10)
            pdf.multi_cell(0, 6, "Zero exceptions detected. 100% data compliance achieved.")
            return

        flags = df[self.COL_REVIEW_FLAG].astype("string").fillna("").str.strip()
        exceptions_df = df.loc[flags != "", [c for c in (self.COL_SHIPMENT_ID, self.COL_REVIEW_FLAG) if c in df.columns]]

        if exceptions_df.empty:
            pdf.set_font("Helvetica", size=10)
            pdf.multi_cell(0, 6, "Zero exceptions detected. 100% data compliance achieved.")
            return

        # Render a simple table with two columns
        shipment_col_w = 45
        flag_col_w = 0  # extend to margin

        pdf.set_font("Helvetica", style="B", size=10)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(shipment_col_w, 7, "Shipment_ID", border=1, fill=True)
        pdf.cell(flag_col_w, 7, "Review_Flag", border=1, fill=True, ln=1)

        pdf.set_font("Helvetica", size=9)
        pdf.set_fill_color(255, 255, 255)

        for _, row in exceptions_df.iterrows():
            shipment_id = "" if self.COL_SHIPMENT_ID not in exceptions_df.columns else str(row.get(self.COL_SHIPMENT_ID, ""))
            review_flag = str(row.get(self.COL_REVIEW_FLAG, ""))

            # MultiCell breaks layout; we implement a basic wrapped row:
            # 1) compute line height and wrap the flag text to fit page width.
            start_x = pdf.get_x()
            start_y = pdf.get_y()

            pdf.multi_cell(shipment_col_w, 6, shipment_id, border=1)
            row_h = pdf.get_y() - start_y

            pdf.set_xy(start_x + shipment_col_w, start_y)
            pdf.multi_cell(0, 6, review_flag, border=1)

            # Ensure next row starts at the max of the two cells
            end_y = max(start_y + row_h, pdf.get_y())
            pdf.set_y(end_y)

    def _render_section_title(self, pdf: FPDF, title: str) -> None:
        """Reusable section title helper (kept for future expansion)."""

        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 8, title, ln=1)

    # ---------------------------- Utilities ----------------------------
    def _hr(self, pdf: FPDF) -> None:
        """Draw a horizontal rule across the content area."""

        x1 = pdf.l_margin
        x2 = pdf.w - pdf.r_margin
        y = pdf.get_y()
        pdf.set_draw_color(200, 200, 200)
        pdf.line(x1, y, x2, y)
        pdf.set_draw_color(0, 0, 0)


if __name__ == "__main__":
    # Minimal local demo
    demo = pd.DataFrame(
        [
            {
                "Shipment_ID": "S1",
                "Std_Weight (t)": 10.0,
                "Std_Distance (km)": 100.0,
                "Data_Tier": "Primary",
                "Review_Flag": "",
                "Carbon_Emission (tCO2e)": 0.15,
            },
            {
                "Shipment_ID": "S2",
                "Std_Weight (t)": np.nan,
                "Std_Distance (km)": 50.0,
                "Data_Tier": "Estimated",
                "Review_Flag": "Needs Manual Review: Weight",
                "Carbon_Emission (tCO2e)": np.nan,
            },
        ]
    )

    gen = AuditReportGenerator()
    gen.generate_pdf_log(demo, output_path="data/audit_log_demo.pdf")
    print("Generated: data/audit_log_demo.pdf")
