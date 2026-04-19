"""api/supabase_db.py

Phase 2+: Supabase "Rule Memory" + dynamic Emission Factors + Spend Factors.

This module provides a small, robust wrapper around the `supabase` Python client.
It is intentionally conservative:
- Uses `streamlit` to read secrets from `st.secrets`.
- Catches network/cloud exceptions and degrades safely (empty dict / False / default
  EF mapping) so UI never hard-crashes due to transient Supabase issues.

Tables expected:
- unit_mappings
  - tenant_id (text)
  - raw_unit (text)
  - std_unit (text)
  - UNIQUE (tenant_id, raw_unit)

- emission_factors
  - transport_mode (text)
  - factor_value (numeric)

- spend_emission_factors
  - sector (text)
  - kg_co2e_per_usd (numeric)

Secrets contract (Streamlit):
- st.secrets["supabase"]["url"]
- st.secrets["supabase"]["key"]
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import streamlit as st

try:
    # `supabase-py` (supabase) client
    from supabase import Client, create_client
except Exception:  # pragma: no cover
    Client = Any  # type: ignore[misc,assignment]
    create_client = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


DEFAULT_EF_MAPPING: Dict[str, float] = {
    "road": 0.15,
    "rail": 0.02,
    "ocean": 0.01,
    "air": 1.25,
}


class SupabaseManager:
    """Supabase access layer for tenant-specific unit mappings and factor tables."""

    _TABLE_UNIT_MAPPINGS = "unit_mappings"
    _TABLE_EMISSION_FACTORS = "emission_factors"
    _TABLE_SPEND_FACTORS = "spend_emission_factors"

    def __init__(self) -> None:
        """Initialize the Supabase client from Streamlit secrets.

        If initialization fails, `self.client` becomes None and methods will
        gracefully degrade.
        """

        self.client: Optional[Client] = None

        try:
            if create_client is None:
                raise ImportError(
                    "Supabase client is not available. Install dependency: pip install supabase"
                )

            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]

            if not isinstance(url, str) or not url.strip():
                raise ValueError("Invalid st.secrets['supabase']['url']")
            if not isinstance(key, str) or not key.strip():
                raise ValueError("Invalid st.secrets['supabase']['key']")

            self.client = create_client(url.strip(), key.strip())
            logger.info("SupabaseManager initialized.")
        except Exception as exc:
            self.client = None
            logger.exception("SupabaseManager init failed (degrading to no-op): %s", exc)

    def get_tenant_mappings(self, tenant_id: str) -> Dict[str, str]:
        """Fetch unit mappings for a tenant.

        Returns:
            Dict mapping raw_unit -> std_unit. Returns {} on any failure.
        """

        if self.client is None:
            return {}

        try:
            tenant_id = str(tenant_id).strip()
            if not tenant_id:
                return {}

            resp = (
                self.client.table(self._TABLE_UNIT_MAPPINGS)
                .select("raw_unit,std_unit")
                .eq("tenant_id", tenant_id)
                .execute()
            )

            rows = getattr(resp, "data", None)
            if not rows:
                return {}

            mapping: Dict[str, str] = {}
            for row in rows:
                raw_unit = str(row.get("raw_unit", "")).strip()
                std_unit = str(row.get("std_unit", "")).strip()
                if raw_unit and std_unit:
                    mapping[raw_unit] = std_unit

            return mapping
        except Exception as exc:
            logger.exception("get_tenant_mappings failed (tenant_id=%s): %s", tenant_id, exc)
            return {}

    def add_mapping(self, tenant_id: str, raw_unit: str, std_unit: str) -> bool:
        """Insert or update a unit mapping for a tenant.

        Uses upsert to avoid UNIQUE(tenant_id, raw_unit) conflicts.

        Returns:
            True if successful, else False.
        """

        if self.client is None:
            return False

        try:
            tenant_id = str(tenant_id).strip()
            raw_unit = str(raw_unit).strip()
            std_unit = str(std_unit).strip()

            if not tenant_id or not raw_unit or not std_unit:
                return False

            payload = {
                "tenant_id": tenant_id,
                "raw_unit": raw_unit,
                "std_unit": std_unit,
            }

            table = self.client.table(self._TABLE_UNIT_MAPPINGS)

            try:
                table.upsert(payload, on_conflict="tenant_id,raw_unit").execute()
            except TypeError:
                table.upsert(payload).execute()

            return True
        except Exception as exc:
            logger.exception(
                "add_mapping failed (tenant_id=%s raw_unit=%s std_unit=%s): %s",
                tenant_id,
                raw_unit,
                std_unit,
                exc,
            )
            return False

    def get_emission_factors(self) -> Dict[str, float]:
        """Fetch dynamic emission factors from Supabase.

        Queries `emission_factors` and returns a mapping:
            {transport_mode(lowercased): factor_value(float)}

        Graceful fallback:
        - If Supabase is unavailable or query fails, returns DEFAULT_EF_MAPPING.
        """

        if self.client is None:
            return DEFAULT_EF_MAPPING.copy()

        try:
            resp = (
                self.client.table(self._TABLE_EMISSION_FACTORS)
                .select("transport_mode,factor_value")
                .execute()
            )

            rows = getattr(resp, "data", None)
            if not rows:
                return DEFAULT_EF_MAPPING.copy()

            ef: Dict[str, float] = {}
            for row in rows:
                mode = str(row.get("transport_mode", "")).strip().lower()
                value = row.get("factor_value", None)

                if not mode:
                    continue

                try:
                    ef[mode] = float(value)
                except Exception:
                    continue

            return ef or DEFAULT_EF_MAPPING.copy()
        except Exception as exc:
            logger.exception("get_emission_factors failed (fallback to defaults): %s", exc)
            return DEFAULT_EF_MAPPING.copy()

    def get_spend_factors(self) -> Dict[str, float]:
        """Fetch spend-based emission factors.

        Queries `spend_emission_factors` and returns a mapping:
            {sector(lowercased): kg_co2e_per_usd(float)}

        Graceful fallback:
        - If Supabase is unavailable or query fails, returns an empty dict.
        """

        if self.client is None:
            return {}

        try:
            resp = (
                self.client.table(self._TABLE_SPEND_FACTORS)
                .select("sector,kg_co2e_per_usd")
                .execute()
            )

            rows = getattr(resp, "data", None)
            if not rows:
                return {}

            out: Dict[str, float] = {}
            for row in rows:
                sector = str(row.get("sector", "")).strip().lower()
                value = row.get("kg_co2e_per_usd", None)

                if not sector:
                    continue
                try:
                    out[sector] = float(value)
                except Exception:
                    continue

            return out
        except Exception as exc:
            logger.exception("get_spend_factors failed (fallback to empty): %s", exc)
            return {}
