"""
agent-hub 操作ツール — Pipecat / Gemini Live 向け。

Pipecat の GeminiLiveLLMService は google.genai の FunctionDeclaration 形式で
ツールを受け取る。各関数はモジュールレベル変数 _hub で接続中の HubSession を共有する。

set_hub_session() / clear_hub_session() で hub_listener から制御する。

expose するのは read/write 系の 5 ツールのみ。
破壊的・管理系ツール (register / create_team / delete_* 等) は expose しない
（音声誤操作防止）。
"""
import logging

from google.genai import types as genai_types

logger = logging.getLogger(__name__)

# モジュールレベル hub セッション (single-session service なので安全)
_hub = None


def set_hub_session(hub) -> None:
    """セッション開始時に hub セッションを登録する。"""
    global _hub
    _hub = hub
    logger.debug("Hub session set")


def clear_hub_session() -> None:
    """セッション終了時に hub セッションを解除する。"""
    global _hub
    _hub = None
    logger.debug("Hub session cleared")


# ---------------------------------------------------------------------------
# ツール実装 (Pipecat FunctionCallParams を受け取る async 関数)
# ---------------------------------------------------------------------------

async def handle_send_message(params) -> dict:
    """agent-hub でメッセージを送信する。

    宛先は @handle 形式で指定。送信前に必ずユーザーに宛先と内容を確認すること。
    """
    hub = _hub
    if hub is None:
        return {"error": "hub not connected", "success": False}
    args = params.arguments if hasattr(params, "arguments") else params
    to = args.get("to", "")
    message = args.get("message", "")
    try:
        await hub.send(to, message)
        return {"result": "sent", "success": True}
    except Exception as e:
        logger.error("send_message error: %s", e)
        return {"error": str(e), "success": False}


async def handle_get_messages(params) -> dict:
    """自分の未読メッセージを取得する。"""
    hub = _hub
    if hub is None:
        return {"error": "hub not connected", "success": False}
    args = params.arguments if hasattr(params, "arguments") else params
    limit = int(args.get("limit", 20))
    try:
        messages = await hub.get_unread()
        result = [
            {
                "id": m.id,
                "from": m.sender,
                "body": m.body,
                "timestamp": m.timestamp,
            }
            for m in messages[:limit]
        ]
        return {"result": result, "success": True}
    except Exception as e:
        logger.error("get_messages error: %s", e)
        return {"error": str(e), "success": False}


async def handle_get_history(params) -> dict:
    """メッセージ履歴を取得する。"""
    hub = _hub
    if hub is None:
        return {"error": "hub not connected", "success": False}
    args = params.arguments if hasattr(params, "arguments") else params
    with_participant = args.get("with_participant")
    keyword = args.get("keyword")
    limit = int(args.get("limit", 20))
    try:
        call_args: dict = {"limit": limit}
        if with_participant:
            call_args["with_participant"] = with_participant
        if keyword:
            call_args["keyword"] = keyword
        text = await hub._call_tool_raw("get_history", call_args)
        return {"result": text, "success": True}
    except Exception as e:
        logger.error("get_history error: %s", e)
        return {"error": str(e), "success": False}


async def handle_get_participants(params) -> dict:
    """agent-hub に登録されている参加者一覧を取得する。"""
    hub = _hub
    if hub is None:
        return {"error": "hub not connected", "success": False}
    try:
        participants = await hub.get_participants()
        result = [
            {
                "name": p.name,
                "display_name": p.display_name,
                "mode": p.mode,
                "is_online": p.is_online,
            }
            for p in participants
        ]
        return {"result": result, "success": True}
    except Exception as e:
        logger.error("get_participants error: %s", e)
        return {"error": str(e), "success": False}


async def handle_mark_as_read(params) -> dict:
    """指定メッセージを既読にする。"""
    hub = _hub
    if hub is None:
        return {"error": "hub not connected", "success": False}
    args = params.arguments if hasattr(params, "arguments") else params
    message_id = args.get("message_id", "")
    try:
        await hub.ack(message_id)
        return {"result": "marked", "success": True}
    except Exception as e:
        logger.error("mark_as_read error: %s", e)
        return {"error": str(e), "success": False}


# ---------------------------------------------------------------------------
# Gemini ツール定義 (google.genai.types.Tool 形式)
# ---------------------------------------------------------------------------

HUB_TOOL_DEFINITIONS = [
    genai_types.Tool(
        function_declarations=[
            genai_types.FunctionDeclaration(
                name="send_message",
                description="agent-hub 経由で指定した participant にメッセージを送る。送信前に必ずユーザーに宛先と内容を確認すること。",
                parameters=genai_types.Schema(
                    type="OBJECT",
                    properties={
                        "to": genai_types.Schema(
                            type="STRING",
                            description="宛先 handle (@alice, @team-review 等)",
                        ),
                        "message": genai_types.Schema(
                            type="STRING",
                            description="送信するメッセージ本文",
                        ),
                    },
                    required=["to", "message"],
                ),
            ),
            genai_types.FunctionDeclaration(
                name="get_messages",
                description="自分の未読メッセージを取得する",
                parameters=genai_types.Schema(
                    type="OBJECT",
                    properties={
                        "limit": genai_types.Schema(
                            type="INTEGER",
                            description="取得件数上限 (default: 20)",
                        ),
                    },
                ),
            ),
            genai_types.FunctionDeclaration(
                name="get_history",
                description="メッセージ履歴を取得する。特定の相手との履歴やキーワード検索が可能",
                parameters=genai_types.Schema(
                    type="OBJECT",
                    properties={
                        "with_participant": genai_types.Schema(
                            type="STRING",
                            description="相手の @handle (optional)",
                        ),
                        "keyword": genai_types.Schema(
                            type="STRING",
                            description="検索キーワード (optional)",
                        ),
                        "limit": genai_types.Schema(
                            type="INTEGER",
                            description="取得件数上限 (default: 20)",
                        ),
                    },
                ),
            ),
            genai_types.FunctionDeclaration(
                name="get_participants",
                description="agent-hub に登録されている参加者一覧を取得する。is_online で在席確認も可",
                parameters=genai_types.Schema(
                    type="OBJECT",
                    properties={},
                ),
            ),
            genai_types.FunctionDeclaration(
                name="mark_as_read",
                description="指定メッセージを既読にする",
                parameters=genai_types.Schema(
                    type="OBJECT",
                    properties={
                        "message_id": genai_types.Schema(
                            type="STRING",
                            description="既読にするメッセージの ID",
                        ),
                    },
                    required=["message_id"],
                ),
            ),
        ]
    )
]

# ツール名 → ハンドラ関数のマッピング (pipeline.py で llm.register_function() に使用)
HUB_TOOL_HANDLERS: dict = {
    "send_message": handle_send_message,
    "get_messages": handle_get_messages,
    "get_history": handle_get_history,
    "get_participants": handle_get_participants,
    "mark_as_read": handle_mark_as_read,
}
