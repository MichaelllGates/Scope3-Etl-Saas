"""
supabase_db.py（骨架）

数据库交互层（Supabase）

建议实践：
- 统一在此处创建/复用 Supabase Client
- 所有表操作都显式带 tenant_id（或使用 RLS 强制隔离）
- 对外仅暴露“业务语义”函数，避免在 UI 层散落 SQL/表名
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def create_supabase_client(*, url: str, key: str) -> Any:
    """
    创建 Supabase 客户端（骨架）。

    参数：
    - url：Supabase Project URL
    - key：Service role key 或 anon key（按部署场景选择）
    """

    # from supabase import create_client
    # return create_client(url, key)
    raise NotImplementedError("TODO: 创建并返回 supabase client（supabase-py）")


def get_tenant_settings(client: Any, *, tenant_id: str) -> Dict[str, Any]:
    """
    获取租户配置（骨架）。

    用途示例：
    - 自定义排放因子来源
    - 功能开关（商业版套餐差异）
    """

    raise NotImplementedError("TODO: 从 Supabase 表中读取 tenant settings")


def save_job_record(client: Any, *, tenant_id: str, job_payload: Dict[str, Any]) -> str:
    """
    保存一条 ETL 作业记录（骨架）。

    返回：
    - job_id：用于后续查询进度/审计
    """

    raise NotImplementedError("TODO: 写入 jobs 表并返回 job_id")


def save_audit_events(client: Any, *, tenant_id: str, job_id: str, events: list[Dict[str, Any]]) -> None:
    """
    批量写入审计事件（骨架）。
    """

    raise NotImplementedError("TODO: 写入 audit_events 表（建议批量 upsert）")


def get_user_subscription(client: Any, *, tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    获取用户订阅状态（骨架）。

    说明：
    - 可从你自建表或 Stripe 同步表读取
    """

    raise NotImplementedError("TODO: 读取订阅信息并返回")

