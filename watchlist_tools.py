"""
watchlist_tools.py — 自选组合管理模块

职责：
  - 管理自选组合配置（JSON 文件读写）
  - 计算组合等权指数、估值统计、个股行情
  - 不建新表，数据全走 daily_bar 实时聚合

依赖：
  pip install pandas
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

import duckdb_tools as dt

# ─────────────────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("watchlist")

# ─────────────────────────────────────────────────────────────────────────────
# 默认路径
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_JSON_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "watchlist.json"
)

_INDEX_BASE = 1000.0  # 等权指数基点


# ─────────────────────────────────────────────────────────────────────────────
# JSON 读写
# ─────────────────────────────────────────────────────────────────────────────

def _load_json(path: str = _DEFAULT_JSON_PATH) -> dict:
    """读取 watchlist.json，文件不存在则返回空 dict。"""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(data: dict, path: str = _DEFAULT_JSON_PATH) -> None:
    """写入 watchlist.json，自动创建目录。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# ── 组合管理 ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def list_groups(path: str = _DEFAULT_JSON_PATH) -> list[str]:
    """返回所有组合名称。"""
    return list(_load_json(path).keys())


def get_group_info(group_name: str, path: str = _DEFAULT_JSON_PATH) -> dict:
    """
    返回组合完整信息。

    返回:
        dict，结构::

            {
                "name": "spacex概念",
                "description": "太空探索相关产业链",
                "created_at": "2026-05-06",
                "stocks": [
                    {"code": "sh.600118", "note": "火箭发动机", "added_date": "2026-05-06"},
                    ...
                ]
            }

        组合不存在则返回空 dict。
    """
    data = _load_json(path)
    if group_name not in data:
        return {}
    info = data[group_name].copy()
    info["name"] = group_name
    return info


def create_group(
    group_name: str,
    description: str = "",
    path: str = _DEFAULT_JSON_PATH,
) -> None:
    """创建新组合。已存在则跳过。"""
    data = _load_json(path)
    if group_name in data:
        logger.warning(f"组合 '{group_name}' 已存在，跳过创建")
        return
    data[group_name] = {
        "description": description,
        "created_at": date.today().isoformat(),
        "stocks": [],
    }
    _save_json(data, path)
    logger.info(f"组合 '{group_name}' 已创建")


def delete_group(
    group_name: str,
    path: str = _DEFAULT_JSON_PATH,
) -> None:
    """删除组合。不存在则跳过。"""
    data = _load_json(path)
    if group_name not in data:
        logger.warning(f"组合 '{group_name}' 不存在，跳过删除")
        return
    del data[group_name]
    _save_json(data, path)
    logger.info(f"组合 '{group_name}' 已删除")


def rename_group(
    old_name: str,
    new_name: str,
    path: str = _DEFAULT_JSON_PATH,
) -> None:
    """重命名组合。"""
    data = _load_json(path)
    if old_name not in data:
        logger.warning(f"组合 '{old_name}' 不存在")
        return
    if new_name in data:
        logger.warning(f"组合 '{new_name}' 已存在，无法重命名")
        return
    data[new_name] = data.pop(old_name)
    _save_json(data, path)
    logger.info(f"组合 '{old_name}' 已重命名为 '{new_name}'")


# ─────────────────────────────────────────────────────────────────────────────
# ── 成员管理 ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def add_stock(
    group_name: str,
    code: str,
    note: str = "",
    path: str = _DEFAULT_JSON_PATH,
) -> None:
    """向组合添加股票。已存在则跳过。"""
    data = _load_json(path)
    if group_name not in data:
        logger.warning(f"组合 '{group_name}' 不存在")
        return
    stocks = data[group_name]["stocks"]
    if any(s["code"] == code for s in stocks):
        logger.warning(f"{code} 已在组合 '{group_name}' 中")
        return
    stocks.append({
        "code": code,
        "note": note,
        "added_date": date.today().isoformat(),
    })
    _save_json(data, path)
    logger.info(f"{code} 已加入组合 '{group_name}'")


def remove_stock(
    group_name: str,
    code: str,
    path: str = _DEFAULT_JSON_PATH,
) -> None:
    """从组合移除股票。"""
    data = _load_json(path)
    if group_name not in data:
        logger.warning(f"组合 '{group_name}' 不存在")
        return
    stocks = data[group_name]["stocks"]
    data[group_name]["stocks"] = [s for s in stocks if s["code"] != code]
    _save_json(data, path)
    logger.info(f"{code} 已从组合 '{group_name}' 移除")


def get_stock_codes(
    group_name: str,
    path: str = _DEFAULT_JSON_PATH,
) -> list[str]:
    """返回组合的所有股票代码。"""
    info = get_group_info(group_name, path)
    return [s["code"] for s in info.get("stocks", [])]


# ─────────────────────────────────────────────────────────────────────────────
# ── 数据计算 ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

def calc_group_index(
    group_name: str,
    start_date: str,
    end_date: str,
    path: str = _DEFAULT_JSON_PATH,
) -> pd.DataFrame:
    """
    计算自选组合等权指数时序。

    逻辑:
        1. 读 JSON 拿成分股 code 列表
        2. 批量获取日线（含透明补拉）
        3. 对齐日期，停牌股前值填充（收益率=0）
        4. 每日等权收益率 = mean(个股 pct_chg) / 100
        5. 累乘 × 基点 → 指数序列

    参数:
        group_name: 组合名称
        start_date: 起始日期 'YYYY-MM-DD'
        end_date:   截止日期 'YYYY-MM-DD'

    返回:
        pd.DataFrame(date, index_value, daily_return)
        组合为空或无数据则返回空 DataFrame
    """
    codes = get_stock_codes(group_name, path)
    if not codes:
        logger.warning(f"组合 '{group_name}' 无成分股")
        return pd.DataFrame(columns=["date", "index_value", "daily_return"])

    # 批量获取日线
    all_returns: dict[str, pd.Series] = {}
    for code in codes:
        df = dt.get_daily(code, start_date, end_date, adjustflag="3", auto_fetch=True)
        if df.empty:
            continue
        df = df.sort_values("date").copy()
        s = df.set_index("date")["pct_chg"].astype(float) / 100.0
        s.name = code
        all_returns[code] = s

    if not all_returns:
        logger.warning(f"组合 '{group_name}' 无可用行情数据")
        return pd.DataFrame(columns=["date", "index_value", "daily_return"])

    ret_df = pd.DataFrame(all_returns).fillna(0.0)  # 停牌=0收益
    equal_ret = ret_df.mean(axis=1)  # 等权平均

    index_series = (1 + equal_ret).cumprod() * _INDEX_BASE

    return pd.DataFrame({
        "date": index_series.index,
        "index_value": index_series.values,
        "daily_return": equal_ret.values * 100,
    })


def calc_group_valuation(
    group_name: str,
    query_date: str | None = None,
    path: str = _DEFAULT_JSON_PATH,
) -> dict:
    """
    计算组合估值统计。

    参数:
        group_name:  组合名称
        query_date:  查询日期 'YYYY-MM-DD'，None 则取最新交易日

    返回:
        dict::

            {
                "pe_mean": 32.5,
                "pe_median": 28.3,
                "pb_mean": 4.2,
                "pb_median": 3.8,
                "valid_count": 3,   # peTTM > 0 的股数
                "total_count": 5,   # 总成分股数
            }
    """
    codes = get_stock_codes(group_name, path)
    if not codes:
        return {
            "pe_mean": None, "pe_median": None,
            "pb_mean": None, "pb_median": None,
            "valid_count": 0, "total_count": 0,
        }

    # 确定查询日期：用交易日历取最近交易日，而非 daily_bar MAX(date)
    if query_date is None:
        nearest = dt.find_nearest_trade_date(date.today().isoformat(), direction="backward")
        query_date = nearest if nearest else date.today().isoformat()

    # 批量查询估值
    conn = dt.get_read_connection()
    placeholders = ", ".join(["?"] * len(codes))
    val_df = conn.execute(
        f"""
        SELECT code, pe_ttm, pb_mrq FROM daily_bar
        WHERE code IN ({placeholders})
          AND date = ?
          AND adjustflag = '3'
        """,
        codes + [query_date],
    ).df()
    conn.close()

    if val_df.empty:
        return {
            "pe_mean": None, "pe_median": None,
            "pb_mean": None, "pb_median": None,
            "valid_count": 0, "total_count": len(codes),
        }

    # PE 排除亏损股
    pe_valid = val_df[val_df["pe_ttm"] > 0]["pe_ttm"]
    pb_valid = val_df["pb_mrq"].dropna()

    return {
        "pe_mean": round(pe_valid.mean(), 1) if not pe_valid.empty else None,
        "pe_median": round(pe_valid.median(), 1) if not pe_valid.empty else None,
        "pb_mean": round(pb_valid.mean(), 2) if not pb_valid.empty else None,
        "pb_median": round(pb_valid.median(), 2) if not pb_valid.empty else None,
        "valid_count": len(pe_valid),
        "total_count": len(codes),
    }


def get_group_overview(
    group_name: str,
    query_date: str | None = None,
    path: str = _DEFAULT_JSON_PATH,
) -> pd.DataFrame:
    """
    组合内个股最新行情一览。

    参数:
        group_name:  组合名称
        query_date:  查询日期，None 取最新交易日

    返回:
        pd.DataFrame(code, code_name, open, close, high, low, pct_chg, volume, amount)
    """
    codes = get_stock_codes(group_name, path)
    if not codes:
        return pd.DataFrame()

    # 确定查询日期：用交易日历取最近交易日，而非 daily_bar MAX(date)
    if query_date is None:
        nearest = dt.find_nearest_trade_date(date.today().isoformat(), direction="backward")
        query_date = nearest if nearest else date.today().isoformat()

    conn = dt.get_read_connection()
    placeholders = ", ".join(["?"] * len(codes))
    df = conn.execute(
        f"""
        SELECT code, code_name, open, close, high, low,
               pct_chg, volume, amount
        FROM daily_bar
        WHERE code IN ({placeholders})
          AND date = ?
          AND adjustflag = '3'
        ORDER BY pct_chg DESC
        """,
        codes + [query_date],
    ).df()
    conn.close()

    return df


def get_group_summary(
    path: str = _DEFAULT_JSON_PATH,
) -> pd.DataFrame:
    """
    获取所有组合的摘要信息（供主页使用）。

    返回:
        pd.DataFrame(name, description, stock_count, created_at)
    """
    data = _load_json(path)
    rows = []
    for name, info in data.items():
        rows.append({
            "name": name,
            "description": info.get("description", ""),
            "stock_count": len(info.get("stocks", [])),
            "created_at": info.get("created_at", ""),
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# 模块自测
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    print("\n===== 测试 1: 创建组合 =====")
    create_group("spacex概念", "太空探索相关产业链")
    create_group("AI算力", "算力基础设施")

    print("\n===== 测试 2: 列出组合 =====")
    print(list_groups())

    print("\n===== 测试 3: 添加股票 =====")
    add_stock("spacex概念", "sh.600118", "火箭发动机")
    add_stock("spacex概念", "sz.002465", "卫星通信")
    add_stock("AI算力", "sh.603019", "GPU")

    print("\n===== 测试 4: 组合信息 =====")
    print(get_group_info("spacex概念"))

    print("\n===== 测试 5: 股票代码 =====")
    print(get_stock_codes("spacex概念"))

    print("\n===== 测试 6: 组合摘要 =====")
    print(get_group_summary().to_string())

    print("\n===== 测试 7: 删除股票 =====")
    remove_stock("spacex概念", "sz.002465")
    print(get_stock_codes("spacex概念"))

    print("\n===== 测试 8: 删除组合 =====")
    delete_group("AI算力")
    print(list_groups())

    print("\n===== 测试 9: 重命名 =====")
    rename_group("spacex概念", "太空概念")
    print(list_groups())

    # 清理
    delete_group("太空概念")
    print("\n所有测试完成")
