"""notify.py — 微信推送模块

通过企业微信推送 BS 区间策略信号，支持两种通道：
  - 智能机器人（aibot SDK，WebSocket 长连接，支持 markdown）
  - 群机器人 webhook（HTTP POST，支持 markdown）

暴露接口：
    push_signals(date=None)              — 默认用智能机器人推送
    push_signals_webhook(date=None)      — 用群机器人 webhook 推送

配置：
    智能机器人：WECOM_BOT_ID / WECOM_BOT_SECRET / WECOM_CHAT_ID
    群机器人：  WECOM_WEBHOOK_KEY
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime

import requests
from aibot import WSClient, WSClientOptions

from strategies import bs_zone

logger = logging.getLogger("notify")

# ── 企业微信智能机器人配置 ────────────────────────────────────────────────────
_BOT_ID = 'aibqek_ufJ1vARousN9FpU-2QS7dfKCD45p'      # 机器人 ID
_BOT_SECRET = 'SDNAX7U8jl36zJgDlflTD28ZJav9H251tOu2jx8hTQj'  # 机器人 Secret
_CHAT_ID = "wrwGcwOwAAaP43gPQwnPpV3XwtmKMJ2w"      # 推送目标会话 ID

# ── 企业微信群机器人 Webhook 配置 ─────────────────────────────────────────────
_WEBHOOK_KEY = '40166bf6-0423-40e2-b0de-322a7b4c2623'  # 群机器人 webhook key
_WEBHOOK_URL = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={_WEBHOOK_KEY}"


def _get_ws_client() -> WSClient:
    """创建 WebSocket 客户端实例。"""
    return WSClient(
        WSClientOptions(
            bot_id=_BOT_ID,
            secret=_BOT_SECRET,
        )
    )


_MAX_CONTENT_LEN = 18000  # 20480 上限，留余量（智能机器人）
_MAX_WEBHOOK_LEN = 3800   # 4096 上限，留余量（群机器人 webhook）


def _format_markdown(date: str, df) -> str:
    """将信号 DataFrame 格式化为 markdown 消息内容。"""
    golden = df[df["signal"] == 1]
    dead = df[df["signal"] == -1]

    lines = [f"## BS区间信号 {date}", ""]

    if not golden.empty:
        lines.append("### 金叉（买入）")
        for _, row in golden.iterrows():
            name = row.get("code_name", "")
            code = row.get("code", "")
            stype = row.get("signal_type", "")
            lines.append(f"- {code} {name}（{stype}）")
        lines.append("")

    if not dead.empty:
        lines.append("### 死叉（卖出）")
        for _, row in dead.iterrows():
            name = row.get("code_name", "")
            code = row.get("code", "")
            stype = row.get("signal_type", "")
            lines.append(f"- {code} {name}（{stype}）")
        lines.append("")

    if golden.empty and dead.empty:
        lines.append("今日无信号")

    return "\n".join(lines)


async def _send_wecom_markdown(content: str) -> bool:
    """通过企业微信智能机器人主动推送 markdown 消息。返回是否成功。"""
    if not _BOT_ID or not _BOT_SECRET:
        logger.warning("WECOM_BOT_ID / WECOM_BOT_SECRET 未配置，跳过推送")
        return False

    if not _CHAT_ID:
        logger.warning("WECOM_CHAT_ID 未配置，跳过推送")
        return False

    # 按长度拆分为多条消息
    messages = _split_content(content, _MAX_CONTENT_LEN)

    client = _get_ws_client()
    authenticated = asyncio.Event()

    @client.on('authenticated')
    def on_auth():
        authenticated.set()

    try:
        await client.connect()
        # 等待认证成功（最多 10 秒）
        try:
            await asyncio.wait_for(authenticated.wait(), timeout=10)
        except asyncio.TimeoutError:
            logger.error("企业微信认证超时")
            return False

        for i, msg in enumerate(messages):
            await client.send_message(_CHAT_ID, {
                "msgtype": "markdown",
                "markdown": {"content": msg},
            })
            # 多条消息间等待，确保 ack 回执到达后再发下一条
            if i < len(messages) - 1:
                await asyncio.sleep(1)
        logger.info(f"企业微信推送成功，共 {len(messages)} 条消息")
        return True
    except Exception as e:
        logger.error(f"企业微信推送异常: {e}")
        return False
    finally:
        client.disconnect()


def _split_content(content: str, max_bytes: int) -> list[str]:
    """按字节长度拆分内容为多条消息。"""
    raw = content.encode("utf-8")
    if len(raw) <= max_bytes:
        return [content]

    lines = content.split("\n")
    parts: list[str] = []
    current: list[str] = []

    for line in lines:
        test = "\n".join(current + [line]) if current else line
        if len(test.encode("utf-8")) > max_bytes and current:
            parts.append("\n".join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        parts.append("\n".join(current))

    return parts


def push_signals(date: str | None = None) -> dict:
    """
    推送指定日期的 BS 区间信号到企业微信（智能机器人通道）。

    参数:
        date: 日期字符串，如 "2025-05-16"；为 None 则取今天
    返回:
        {"status": "DONE"/"SKIPPED"/"FAILED", "golden": int, "dead": int, "date": str}
    """
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")

    logger.info(f"开始推送 BS 区间信号（智能机器人）: {date}")

    df = bs_zone.query_signals(date)

    if df is None or df.empty:
        logger.info(f"{date} 无信号，跳过推送")
        return {"status": "SKIPPED", "golden": 0, "dead": 0, "date": date}

    golden_count = int((df["signal"] == 1).sum())
    dead_count = int((df["signal"] == -1).sum())

    content = _format_markdown(date, df)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        ok = pool.submit(asyncio.run, _send_wecom_markdown(content)).result()

    result = {
        "status": "DONE" if ok else "FAILED",
        "golden": golden_count,
        "dead": dead_count,
        "date": date,
    }
    logger.info(f"推送完成: 金叉 {golden_count}, 死叉 {dead_count}")
    return result


# ── 群机器人 Webhook 通道 ─────────────────────────────────────────────────────

def _send_webhook_markdown(content: str) -> bool:
    """通过企业微信群机器人 webhook 发送 markdown 消息。返回是否成功。"""
    if not _WEBHOOK_KEY:
        logger.warning("WECOM_WEBHOOK_KEY 未配置，跳过推送")
        return False

    messages = _split_content(content, _MAX_WEBHOOK_LEN)

    all_ok = True
    for i, msg in enumerate(messages):
        payload = {
            "msgtype": "markdown",
            "markdown": {"content": msg},
        }
        try:
            resp = requests.post(_WEBHOOK_URL, json=payload, timeout=10)
            data = resp.json()
            if data.get("errcode") == 0:
                logger.info(f"Webhook 推送成功 ({i + 1}/{len(messages)})")
            else:
                logger.error(f"Webhook 推送失败: {data}")
                all_ok = False
        except Exception as e:
            logger.error(f"Webhook 推送异常: {e}")
            all_ok = False

        # 多条消息间等待，避免频率限制
        if i < len(messages) - 1:
            time.sleep(1)

    return all_ok


def push_signals_webhook(date: str | None = None) -> dict:
    """
    推送指定日期的 BS 区间信号到企业微信（群机器人 webhook 通道）。

    参数:
        date: 日期字符串，如 "2025-05-16"；为 None 则取今天
    返回:
        {"status": "DONE"/"SKIPPED"/"FAILED", "golden": int, "dead": int, "date": str}
    """
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")

    logger.info(f"开始推送 BS 区间信号（Webhook）: {date}")

    df = bs_zone.query_signals(date)

    if df is None or df.empty:
        logger.info(f"{date} 无信号，跳过推送")
        return {"status": "SKIPPED", "golden": 0, "dead": 0, "date": date}

    golden_count = int((df["signal"] == 1).sum())
    dead_count = int((df["signal"] == -1).sum())

    content = _format_markdown(date, df)
    ok = _send_webhook_markdown(content)

    result = {
        "status": "DONE" if ok else "FAILED",
        "golden": golden_count,
        "dead": dead_count,
        "date": date,
    }
    logger.info(f"Webhook 推送完成: 金叉 {golden_count}, 死叉 {dead_count}")
    return result
