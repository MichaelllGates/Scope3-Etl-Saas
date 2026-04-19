"""api/supabase_db.py

Phase 2: Supabase "Rule Memory" for unit mappings.

This module provides a small, robust wrapper around the `supabase` Python client.
It is intentionally conservative:
- Uses `streamlit` to read secrets from `st.secrets`.
- Catches network/cloud exceptions and degrades safely (empty dict / False) so UI
  never hard-crashes due to transient Supabase issues.

Expected Supabase table:
- unit_mappings
  - tenant_id (text)
  - raw_unit (text)
  - std_unit (text)
  - UNIQUE (tenant_id, raw_unit)

Secrets contract (Streamlit):
- st.secrets["supabase"]["url"]
- st.secrets["supabase"]["key"]

NOTE: "tamper-proof" is handled at the audit/PDF layer; this module only stores
unit mapping rules.
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


class SupabaseManager:
    """Supabase access layer for tenant-specific unit mappings."""

    _TABLE_UNIT_MAPPINGS = "unit_mappings"

    def __init__(self) -> None:
        """Initialize the Supabase client from Streamlit secrets.

        This constructor must read:
        - st.secrets["supabase"]["url"]
        - st.secrets["supabase"]["key"]

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
            # Do not crash the app on cloud misconfig; fail closed.
            self.client = None
            logger.exception("SupabaseManager init failed (degrading to no-op): %s", exc)

    def get_tenant_mappings(self, tenant_id: str) -> Dict[str, str]:
        """Fetch unit mappings for a tenant.

        Args:
            tenant_id: Tenant identifier.

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

        Args:
            tenant_id: Tenant identifier.
            raw_unit: Raw unit string from user data (e.g., "三大箱").
            std_unit: Standard unit (e.g., "t").

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

            # Prefer explicit conflict target when supported.
            try:
                resp = table.upsert(payload, on_conflict="tenant_id,raw_unit").execute()
            except TypeError:
                # Older supabase-py versions may not support on_conflict kw.
                resp = table.upsert(payload).execute()

            # If Supabase returns a payload, treat as success; otherwise still
            # consider it successful if no exception was thrown.
            _ = getattr(resp, "data", None)
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
