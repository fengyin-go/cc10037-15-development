"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import batch_replay, business, resource

# batch_replay 含有两段式固定路径，必须排在资源路由前，
# 否则会被 /api/{module}/{entry_id} 的动态路径抢先匹配。
ROUTERS = [batch_replay, resource, business]
