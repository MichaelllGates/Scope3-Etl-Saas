"""
stripe_service.py（骨架）

Stripe 支付/订阅校验层

建议实践：
- Webhook 验签：防止伪造回调
- 订阅状态查询：决定商业版功能是否可用
- 订阅变更落库：回写到 Supabase（便于 RLS/报表）

注意：
- 本项目 requirements.txt 暂未加入 stripe SDK；
  若你后续需要直接调用 Stripe API，再添加 `stripe` 依赖即可。
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def verify_webhook_signature(*, payload: bytes, sig_header: str, endpoint_secret: str) -> Dict[str, Any]:
    """
    Webhook 验签（骨架）。

    参数：
    - payload：HTTP 原始 body
    - sig_header：Stripe-Signature header
    - endpoint_secret：Webhook endpoint secret

    返回：
    - event dict：解析后的 Stripe event（占位）
    """

    raise NotImplementedError("TODO: 使用 Stripe SDK 验签并解析事件")


def get_subscription_status(*, stripe_customer_id: str) -> str:
    """
    查询订阅状态（骨架）。

    返回建议：
    - 'active' / 'trialing' / 'past_due' / 'canceled' 等
    """

    raise NotImplementedError("TODO: 调用 Stripe API 获取订阅状态")


def is_feature_allowed(*, plan: str, feature_key: str) -> bool:
    """
    功能开关判定（骨架）。

    用途：
    - 按套餐限制：行数、并发任务数、导出 PDF、水印、团队成员数等
    """

    raise NotImplementedError("TODO: 根据 plan 与 feature_key 返回是否允许")

