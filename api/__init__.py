"""
api：商业化接口层（骨架）

职责建议：
- supabase_db：数据访问层（租户隔离、RLS/表结构约定、作业队列）
- stripe_service：计费/订阅校验（Webhook 验签、订阅状态查询）
"""

