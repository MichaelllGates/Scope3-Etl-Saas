"""core/auditor.py

Enterprise-grade PDF audit logging for Scopify (Scope 3 ETL Micro-SaaS).

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

import numpy as np
import pandas as pd

try:
    from fpdf import FPDF
except Exception as exc:  # pragma: no cover
    raise ImportError(
        "Missing dependency `fpdf` (or `fpdf2`). Install it with: pip install fpdf2"
    ) from exc


@dataclass(frozen=True)
class AuditMetrics:
    total_rows: int
    total_emissions_tco2e: float
    primary_pct: float
    estimated_pct: float


class AuditReportGenerator:
    """Generate an enterprise-style PDF audit log for Scope 3 ETL processing."""

    # Required proof columns
    COL_SHIPMENT_ID = "Shipment_ID"
    COL_APPLIED_FACTOR = "Applied_Emission_Factor"
    COL_CARBON = "Carbon_Emission (tCO2e)"
    COL_DATA_TIER = "Data_Tier"
    COL_REVIEW_FLAG = "Review_Flag"

    # Fallback column sources (for robustness)
    _ALT_CARBON = ("Carbon_Emission (tCO2e)", "Emissions_tCO2e", "Carbon_Emission")
    _ALT_REVIEW = ("Review_Flag", "ETL_Review_Flag")

    SYSTEM_VERSION = "Scope 3 ETL Engine V1.0"
    HEADER_TEXT = "CONFIDENTIAL & IMMUTABLE LOG"

    def __init__(self, *, company_name: str = "Scopify"):
        self.company_name = company_name

    def generate_pdf_log(self, df: pd.DataFrame, output_path: str) -> None:
        safe_df = self._normalize_input_df(df)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fingerprint = self._compute_dataframe_fingerprint(safe_df)
        metrics = self._compute_metrics(safe_df)

        pdf = self._create_pdf()
        self._render_section_header(pdf, timestamp=timestamp, fingerprint=fingerprint)
        self._render_section_exec_summary(pdf, metrics=metrics)
        self._render_section_detailed_audit(pdf, df=safe_df)

        self._write_pdf(pdf, output_path=output_path)

    # ------------------------- Data preparation -------------------------
    def _normalize_input_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"df must be a pandas DataFrame, got: {type(df)}")
        return df.copy()

    def _resolve_carbon_series(self, df: pd.DataFrame) -> pd.Series:
        for col in self._ALT_CARBON:
            if col in df.columns:
                return pd.to_numeric(df[col], errors="coerce")
        return pd.Series(np.nan, index=df.index, dtype="float")

    def _resolve_factor_series(self, df: pd.DataFrame) -> pd.Series:
        if self.COL_APPLIED_FACTOR in df.columns:
            return pd.to_numeric(df[self.COL_APPLIED_FACTOR], errors="coerce")
        return pd.Series(np.nan, index=df.index, dtype="float")

    def _resolve_review_series(self, df: pd.DataFrame) -> pd.Series:
        for col in self._ALT_REVIEW:
            if col in df.columns:
                s = df[col].astype("string").fillna("").str.strip()
                return s.replace({"Clean": ""})
        return pd.Series("", index=df.index, dtype="string")

    def _compute_metrics(self, df: pd.DataFrame) -> AuditMetrics:
        total_rows = int(len(df))

        emissions = self._resolve_carbon_series(df)
        total_emissions = float(np.nansum(emissions.to_numpy(dtype=float))) if total_rows else 0.0

        if total_rows == 0 or self.COL_DATA_TIER not in df.columns:
            primary_pct = 0.0
            estimated_pct = 0.0
        else:
            tier = df[self.COL_DATA_TIER].astype("string").fillna("").str.strip().str.lower()
            primary_mask = tier.str.startswith("primary")
            estimated_mask = tier.str.startswith("estimated")
            denom = max(int(primary_mask.sum() + estimated_mask.sum()), 1)
            primary_pct = (int(primary_mask.sum()) / denom) * 100.0
            estimated_pct = (int(estimated_mask.sum()) / denom) * 100.0

        return AuditMetrics(
            total_rows=total_rows,
            total_emissions_tco2e=total_emissions,
            primary_pct=primary_pct,
            estimated_pct=estimated_pct,
        )

    def _compute_dataframe_fingerprint(self, df: pd.DataFrame) -> str:
        if df.empty:
            return hashlib.sha256(b"EMPTY_DF").hexdigest()

        stable_df = df.copy()
        stable_df = stable_df.reindex(sorted(stable_df.columns), axis=1)
        stable_df = stable_df.replace({np.nan: ""})

        payload = stable_df.to_csv(index=False, lineterminator="\n").encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    # ---------------------------- PDF setup ----------------------------
    def _create_pdf(self) -> FPDF:
        pdf = FPDF(orientation="P", unit="mm", format="A4")
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        return pdf

    def _write_pdf(self, pdf: FPDF, *, output_path: str) -> None:
        out_path = Path(output_path)
        out_dir = out_path.parent
        if str(out_dir) and not out_dir.exists():
            os.makedirs(out_dir, exist_ok=True)
        pdf.output(str(out_path))

    # ---------------------------- Rendering ----------------------------
    def _render_section_header(self, pdf: FPDF, *, timestamp: str, fingerprint: str) -> None:
        pdf.set_font("Helvetica", style="B", size=16)
        pdf.set_text_color(60, 60, 60)
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
        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 8, "Executive Summary (GHG Protocol Alignment)", ln=1)

        pdf.set_font("Helvetica", size=10)
        pdf.cell(0, 6, f"Total Rows Processed: {metrics.total_rows}", ln=1)
        pdf.cell(0, 6, f"Total Carbon Emissions (tCO2e): {metrics.total_emissions_tco2e:.2f}", ln=1)
        pdf.cell(
            0,
            6,
            f"Data Tier Breakdown: Primary {metrics.primary_pct:.1f}% | Estimated {metrics.estimated_pct:.1f}%",
            ln=1,
        )

        pdf.ln(3)
        self._hr(pdf)
        pdf.ln(4)

    def _render_section_detailed_audit(self, pdf: FPDF, *, df: pd.DataFrame) -> None:
        """Section 3: Explicit result proof table."""

        pdf.set_font("Helvetica", style="B", size=12)
        pdf.cell(0, 8, "Detailed Shipment Audit", ln=1)

        if df.empty:
            pdf.set_font("Helvetica", size=10)
            pdf.multi_cell(0, 6, "No shipment rows to audit.")
            return

        shipment = (
            df[self.COL_SHIPMENT_ID].astype("string").fillna("").str.strip()
            if self.COL_SHIPMENT_ID in df.columns
            else pd.Series("", index=df.index, dtype="string")
        )

        applied_factor = self._resolve_factor_series(df)
        carbon = self._resolve_carbon_series(df)
        tier = (
            df[self.COL_DATA_TIER].astype("string").fillna("").str.strip()
            if self.COL_DATA_TIER in df.columns
            else pd.Series("", index=df.index, dtype="string")
        )
        review = self._resolve_review_series(df)

        view = pd.DataFrame(
            {
                self.COL_SHIPMENT_ID: shipment,
                self.COL_APPLIED_FACTOR: applied_factor,
                self.COL_CARBON: carbon,
                self.COL_DATA_TIER: tier,
                self.COL_REVIEW_FLAG: review,
            }
        )

        # Table layout
        w_ship = 22
        w_factor = 24
        w_carbon = 28
        w_tier = 48
        w_flag = 0
        line_h = 5

        def _wrap(text: str, width: float) -> list[str]:
            text = "" if text is None else str(text)
            text = text.replace("\r", " ").replace("\n", " ").strip()
            if text == "":
                return [""]

            words = text.split(" ")
            lines: list[str] = []
            current = ""

            for word in words:
                trial = word if current == "" else current + " " + word
                if pdf.get_string_width(trial) <= width:
                    current = trial
                else:
                    if current:
                        lines.append(current)
                    if pdf.get_string_width(word) <= width:
                        current = word
                    else:
                        chunk = ""
                        for ch in word:
                            trial2 = chunk + ch
                            if pdf.get_string_width(trial2) <= width:
                                chunk = trial2
                            else:
                                if chunk:
                                    lines.append(chunk)
                                chunk = ch
                        current = chunk

            if current:
                lines.append(current)

            return lines or [""]

        def _ensure_space(row_h: float) -> None:
            if pdf.get_y() + row_h > (pdf.h - pdf.b_margin):
                pdf.add_page()

        def _render_header_row() -> None:
            # Requirement: the PDF must display these exact column names:
            # ['Shipment_ID', 'Applied_Emission_Factor', 'Carbon_Emission (tCO2e)', 'Data_Tier', 'Review_Flag']
            headers = [
                (self.COL_SHIPMENT_ID, w_ship),
                (self.COL_APPLIED_FACTOR, w_factor),
                (self.COL_CARBON, w_carbon),
                (self.COL_DATA_TIER, w_tier),
                (self.COL_REVIEW_FLAG, (pdf.w - pdf.l_margin - pdf.r_margin - w_ship - w_factor - w_carbon - w_tier)),
            ]

            pdf.set_font("Helvetica", style="B", size=7)
            pdf.set_fill_color(240, 240, 240)

            x0 = pdf.get_x()
            y0 = pdf.get_y()
            header_h = 8

            # Draw header cell borders + background and then write wrapped header text.
            x = x0
            for _, w in headers:
                pdf.rect(x, y0, w, header_h, style="DF")
                x += w

            x = x0
            for text, w in headers:
                lines = _wrap(text, w - 2)
                pdf.set_xy(x + 1, y0 + 1)
                pdf.multi_cell(w - 2, 3.5, "\n".join(lines), border=0)
                x += w

            pdf.set_xy(x0, y0 + header_h)
            pdf.set_font("Helvetica", size=8)
            pdf.set_fill_color(255, 255, 255)

        _render_header_row()

        for _, r in view.iterrows():
            shipment_id = str(r.get(self.COL_SHIPMENT_ID, "") or "").strip()

            f_val = r.get(self.COL_APPLIED_FACTOR, np.nan)
            if pd.isna(f_val):
                f_text = "-"
            else:
                try:
                    f_text = f"{float(f_val):.6g}"
                except Exception:
                    f_text = "-"

            c_val = r.get(self.COL_CARBON, np.nan)
            if pd.isna(c_val):
                c_text = "-"
            else:
                try:
                    c_text = f"{float(c_val):.4f}"
                except Exception:
                    c_text = "-"

            tier_text = str(r.get(self.COL_DATA_TIER, "") or "").strip()
            flag_text = str(r.get(self.COL_REVIEW_FLAG, "") or "").strip()

            ship_lines = _wrap(shipment_id, w_ship - 2)
            factor_lines = _wrap(f_text, w_factor - 2)
            carbon_lines = _wrap(c_text, w_carbon - 2)
            tier_lines = _wrap(tier_text, w_tier - 2)
            flag_lines = _wrap(
                flag_text,
                (pdf.w - pdf.l_margin - pdf.r_margin - w_ship - w_factor - w_carbon - w_tier) - 2,
            )

            row_lines = max(len(ship_lines), len(factor_lines), len(carbon_lines), len(tier_lines), len(flag_lines))
            row_h = max(7.0, row_lines * line_h)

            _ensure_space(row_h)

            x0 = pdf.get_x()
            y0 = pdf.get_y()

            # Draw bordered rectangles
            pdf.rect(x0, y0, w_ship, row_h)
            pdf.rect(x0 + w_ship, y0, w_factor, row_h)
            pdf.rect(x0 + w_ship + w_factor, y0, w_carbon, row_h)
            pdf.rect(x0 + w_ship + w_factor + w_carbon, y0, w_tier, row_h)
            pdf.rect(
                x0 + w_ship + w_factor + w_carbon + w_tier,
                y0,
                pdf.w - pdf.r_margin - (x0 + w_ship + w_factor + w_carbon + w_tier),
                row_h,
            )

            def _write_cell(x: float, y: float, w: float, lines: list[str]) -> None:
                pdf.set_xy(x + 1, y + 1)
                pdf.multi_cell(w - 2, line_h, "\n".join(lines), border=0)

            _write_cell(x0, y0, w_ship, ship_lines)
            _write_cell(x0 + w_ship, y0, w_factor, factor_lines)
            _write_cell(x0 + w_ship + w_factor, y0, w_carbon, carbon_lines)
            _write_cell(x0 + w_ship + w_factor + w_carbon, y0, w_tier, tier_lines)
            _write_cell(
                x0 + w_ship + w_factor + w_carbon + w_tier,
                y0,
                pdf.w - pdf.r_margin - (x0 + w_ship + w_factor + w_carbon + w_tier),
                flag_lines,
            )

            pdf.set_xy(x0, y0 + row_h)

        pdf.ln(1)

    def _hr(self, pdf: FPDF) -> None:
        x1 = pdf.l_margin
        x2 = pdf.w - pdf.r_margin
        y = pdf.get_y()
        pdf.set_draw_color(200, 200, 200)
        pdf.line(x1, y, x2, y)
        pdf.set_draw_color(0, 0, 0)


if __name__ == "__main__":
    demo = pd.DataFrame(
        [
            {
                "Shipment_ID": "S1",
                "Applied_Emission_Factor": 0.145,
                "Carbon_Emission (tCO2e)": 0.152345,
                "Data_Tier": "Primary (Activity-based)",
                "Review_Flag": "",
            },
            {
                "Shipment_ID": "S2",
                "Applied_Emission_Factor": np.nan,
                "Carbon_Emission (tCO2e)": np.nan,
                "Data_Tier": "Estimated (Spend-based)",
                "Review_Flag": "Needs Manual Review: Distance",
            },
        ]
    )

    gen = AuditReportGenerator()
    gen.generate_pdf_log(demo, output_path="data/audit_log_demo.pdf")
    print("Generated: data/audit_log_demo.pdf")
