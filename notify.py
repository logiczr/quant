"""notify.py — 钉钉推送模块（基于 dingtalk-stream SDK）

使用钉钉 Stream 模式企业内部应用机器人推送 BS 区间策略信号。

暴露接口：
    push_signals(date=None)     — 推送信号到钉钉群（发互动卡片）
    start_dingtalk_stream()    — 启动 Stream 连接（接收指令）

环境变量（如不设置则模块跳过推送）：
    DINGTALK_CLIENT_ID          — 应用 AppKey（即 robotCode）
    DINGTALK_CLIENT_SECRET      — 应用 AppSecret
    DINGTALK_OPEN_CONVERSATION_ID — 目标群的 openConversationId

交互指令（需先启动 Stream）：
    在群内 @机器人 发送"信号"或"今日信号" → 返回当日 BS 区间信号
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime

import dingtalk_stream
from strategies import bs_zone

logger = logging.getLogger("notify")

# ── 钉钉应用配置（从环境变量读取，不建议硬编码） ────────────────────────────
_CLIENT_ID = os.environ.get("DINGTALK_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("DINGTALK_CLIENT_SECRET", "")
_OPEN_CONVERSATION_ID = os.environ.get("DINGTALK_OPEN_CONVERSATION_ID", "")

# ── 全局客户端 ───────────────────────────────────────────────────────────────
_client: dingtalk_stream.DingTalkStreamClient | None = None


def _get_client() -> dingtalk_stream.DingTalkStreamClient:
    """获取或创建 DingTalkStreamClient 单例。"""
    global _client
    if _client is None:
        if not _CLIENT_ID or not _CLIENT_SECRET:
            raise RuntimeError("钉钉 DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET 未配置")
        credential = dingtalk_stream.Credential(_CLIENT_ID, _CLIENT_SECRET)
        _client = dingtalk_stream.DingTalkStreamClient(credential)
        _client.register_callback_handler(
            dingtalk_stream.chatbot.ChatbotMessage.TOPIC,
            BSZoneBotHandler(),
        )
    return _client


# ─────────────────────────────────────────────────────────────────────────────
# 消息格式化
# ─────────────────────────────────────────────────────────────────────────────

def _format_dead_markdown(date: str, df) -> str:
    """格式化死叉信号为 markdown。"""
    dead = df[df["signal"] == -1]
    lines = [f"## BS区间死叉信号 {date}", ""]
    if dead.empty:
        lines.append("今日无死叉信号")
    else:
        lines.append("### 死叉（卖出）")
        for _, row in dead.iterrows():
            code = row.get("code", "")
            name = row.get("code_name", "")
            lines.append(f"- {code} {name}")
    lines.append("")
    return "\n".join(lines)


def _format_golden_markdown(date: str, df) -> str:
    """格式化金叉信号为 markdown。"""
    golden = df[df["signal"] == 1]
    lines = [f"## BS区间金叉信号 {date}", ""]
    if golden.empty:
        lines.append("今日无金叉信号")
    else:
        lines.append("### 金叉（买入）")
        for _, row in golden.iterrows():
            code = row.get("code", "")
            name = row.get("code_name", "")
            lines.append(f"- {code} {name}")
    lines.append("")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 推送
# ─────────────────────────────────────────────────────────────────────────────

def _send_markdown_card(content: str, title: str = "BS区间信号", at_all: bool = False) -> bool:
    """通过互动卡片向目标群发送 markdown 内容。返回是否成功。"""
    if not _OPEN_CONVERSATION_ID:
        logger.warning("钉钉 DINGTALK_OPEN_CONVERSATION_ID 未配置，跳过推送")
        return False

    client = _get_client()
    incoming = dingtalk_stream.reply_specified_group_chat(_OPEN_CONVERSATION_ID)
    card = dingtalk_stream.MarkdownCardInstance(client, incoming)
    card.set_title_and_logo(title, "")
    card.reply(content, at_all=at_all)
    logger.info(f"钉钉互动卡片已发送: {title}")
    return True


def push_signals(date: str | None = None) -> dict:
    """
    推送指定日期的 BS 区间信号到钉钉群。

    参数:
        date: 日期字符串，如 "2025-05-16"；为 None 则取今天
    返回:
        {"status": "DONE"/"SKIPPED"/"FAILED", "golden": int, "dead": int, "date": str}
    """
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")

    logger.info(f"开始推送 BS 区间信号（钉钉）: {date}")

    df = bs_zone.query_signals(date)

    if df is None or df.empty:
        logger.info(f"{date} 无信号，跳过推送")
        return {"status": "SKIPPED", "golden": 0, "dead": 0, "date": date}

    golden_count = int((df["signal"] == 1).sum())
    dead_count = int((df["signal"] == -1).sum())

    # 先推死叉，后推金叉
    ok = True
    if dead_count > 0:
        dead_content = _format_dead_markdown(date, df)
        if not _send_markdown_card(dead_content, title="BS区间死叉信号"):
            ok = False
    if golden_count > 0:
        golden_content = _format_golden_markdown(date, df)
        if not _send_markdown_card(golden_content, title="BS区间金叉信号"):
            ok = False
    if dead_count == 0 and golden_count == 0:
        ok = False

    result = {
        "status": "DONE" if ok else "FAILED",
        "golden": golden_count,
        "dead": dead_count,
        "date": date,
    }
    logger.info(f"钉钉推送完成: 金叉 {golden_count}, 死叉 {dead_count}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ChatbotHandler — 接收群内指令
# ─────────────────────────────────────────────────────────────────────────────

class BSZoneBotHandler(dingtalk_stream.ChatbotHandler):
    """处理群内 @机器人 的指令消息。"""

    async def process(self, callback: dingtalk_stream.CallbackMessage):
        incoming = dingtalk_stream.ChatbotMessage.from_dict(callback.data)
        text = incoming.text.content.strip()

        if re.search(r"群id|群ID|群号|会话id", text, re.IGNORECASE):
            self.reply_text(
                f"openConversationId: {incoming.conversation_id}", incoming
            )
        elif re.search(r"信号|金叉|死叉", text):
            date = datetime.today().strftime("%Y-%m-%d")
            df = bs_zone.query_signals(date)
            if df is None or df.empty:
                self.reply_text(f"{date} 暂无 BS 区间信号", incoming)
            else:
                dead = df[df["signal"] == -1]
                golden = df[df["signal"] == 1]
                if not dead.empty:
                    self.reply_markdown_card(
                        _format_dead_markdown(date, df), incoming, title="BS区间死叉信号"
                    )
                if not golden.empty:
                    self.reply_markdown_card(
                        _format_golden_markdown(date, df), incoming, title="BS区间金叉信号"
                    )

        return dingtalk_stream.AckMessage.STATUS_OK, "OK"


# ─────────────────────────────────────────────────────────────────────────────
# Stream 连接
# ─────────────────────────────────────────────────────────────────────────────

def start_dingtalk_stream():
    """启动钉钉 Stream 连接（阻塞，通常放在后台线程）。"""
    client = _get_client()
    logger.info("启动钉钉 Stream 连接...")
    client.start_forever()


async def start_dingtalk_stream_async():
    """异步启动钉钉 Stream 连接（用于 asyncio 事件循环）。"""
    client = _get_client()
    logger.info("启动钉钉 Stream 连接（async）...")
    await client.start()
