"""
voice-gateway v2 — メインサーバー

エンドポイント:
  GET  /           → client/index.html (LiveKit JS SDK UI)
  POST /auth       → OTP 検証 + LiveKit token 発行 + Pipecat パイプライン起動

起動:
  python main.py

環境変数:
  LIVEKIT_URL          LiveKit server WebSocket URL (e.g. ws://localhost:7880)
  LIVEKIT_API_KEY      LiveKit API key
  LIVEKIT_API_SECRET   LiveKit API secret
  GEMINI_API_KEY       Gemini API key
  GEMINI_MODEL         Gemini model name (default: gemini-2.0-flash-live-001)
  AGENT_HUB_URL        agent-hub server URL
  AGENT_HUB_USER       @handle (e.g. voice)
  AGENT_HUB_GITHUB_PAT Personal Access Token
  AGENT_HUB_TENANT     tenant (optional)
  SYSTEM_PROMPT        Gemini system instruction (optional)
  PORT                 HTTP listen port (default: 8765)

## hub 接続設計 (bridge-claude worker.py と同じパターン)

AgentHub.connect() は main() で 1 回だけ呼ぶ。単一の MCP セッションを
_run_hub_with_reconnect() が管理し、hub.inbox(commands=router) で受信する。

CommandRouter に /generate-code を登録する:
  - router が /generate-code を処理 → OTP 生成 + 返信 + auto-ack
  - router が /ping /status /help を処理 (built-in)
  - 通常メッセージ (非コマンド) だけが inbox iterator から yield される
  - yield されたメッセージ → HubListener.handle_message() → Pipecat pipeline に inject
"""
import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

from aiohttp import web

from agent_hub_sdk import AgentHub, CommandRouter, HubSession, IncomingMessage

from auth import OTPStore
from hub_listener import HubListener
from hub_tools import HUB_TOOL_DEFINITIONS, HUB_TOOL_HANDLERS, set_hub_session, clear_hub_session
from pipeline import create_pipeline_and_run
from session_manager import SessionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
LIVEKIT_URL = os.environ["LIVEKIT_URL"]
LIVEKIT_API_KEY = os.environ["LIVEKIT_API_KEY"]
LIVEKIT_API_SECRET = os.environ["LIVEKIT_API_SECRET"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-live-001")
HUB_URL = os.environ["AGENT_HUB_URL"]
HUB_USER = os.environ["AGENT_HUB_USER"]
HUB_PAT = os.environ["AGENT_HUB_GITHUB_PAT"]
HUB_TENANT = os.environ.get("AGENT_HUB_TENANT")
PORT = int(os.environ.get("PORT", 8765))

DISPLAY_NAME = "voice-gateway v2 — Gemini Live + LiveKit voice interface"

DEFAULT_SYSTEM_PROMPT = (
    "あなたは agent-hub の音声インターフェース「voice」です。"
    "ユーザーの音声指示に従い、agent-hub 上のメッセージ送受信・参加者確認などを行います。"
    "簡潔で自然な日本語で応答してください。"
)
SYSTEM_PROMPT = os.environ.get("SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)

# ---------------------------------------------------------------------------
# シングルトン
# ---------------------------------------------------------------------------
otp_store = OTPStore()
session_manager = SessionManager()

# 共有 AgentHub セッション (_run_hub_with_reconnect が管理)
_active_hub = None

# アクティブな HubListener (パイプラインセッション中のみ非 None)
# asyncio は single-thread のため、単純な代入でスレッドセーフ。
_active_hub_listener: HubListener | None = None

# ---------------------------------------------------------------------------
# Hub 管理 (bridge-claude worker.py と同じ CommandRouter パターン)
# ---------------------------------------------------------------------------

async def _run_hub_with_reconnect() -> None:
    """単一の AgentHub セッションを管理する。切断時は自動再接続。

    CommandRouter に /generate-code を登録し、
    hub.inbox(commands=router) でメッセージを受信する。

    - /generate-code → router が OTP 生成 + 返信 + auto-ack (inbox に届かない)
    - /ping /status /help → router の built-in が処理 (inbox に届かない)
    - 通常メッセージ → inbox iterator から yield → HubListener → pipeline inject
    """
    global _active_hub
    backoff = 5.0

    # CommandRouter: /generate-code を登録。未知スラッシュコマンドは SDK が自動返信。
    # unknown="reject": 未知の /cmd を受け取ると SDK が reject_format を返信して auto-ack。
    # bare text (/ なし) は常に "yield" で consumer に届く (セッション有: HubListener、無: ack のみ)。
    _UNKNOWN_CMD_REPLY = (
        "コマンドが認識できません。\n"
        "使用可能なコマンド: /generate-code"
    )
    router = CommandRouter(unknown="reject", reject_format=_UNKNOWN_CMD_REPLY)

    @router.command("/generate-code", description="OTP コードを生成してセッションを開始する")
    async def _handle_generate_code(
        msg: IncomingMessage, hub: HubSession, args: str
    ) -> None:
        """OTP を生成して送信者に返信する。router が自動で ack する。"""
        code, ttl = otp_store.generate()
        ttl_min = ttl // 60
        reply = (
            f"🔑 **{code}** ({ttl_min}分有効)\n\n"
            "スマホブラウザでこのコードを入力してセッションを開始してください。"
        )
        await hub.send(msg.sender, reply)
        # セキュリティ: コードの先頭 2 桁のみログに残す
        logger.info("/generate-code: sent to %s (code=%s****)", msg.sender, code[:2])
        # return None → SDK が ack する (明示的な ack 不要)

    while True:
        try:
            async with AgentHub.connect(
                user=HUB_USER,
                url=HUB_URL,
                pat=HUB_PAT,
                tenant=HUB_TENANT,
                display_name=DISPLAY_NAME,
            ) as hub:
                _active_hub = hub
                set_hub_session(hub)
                backoff = 5.0  # 正常接続できたらリセット
                logger.info("Hub: @%s に接続しました", HUB_USER)

                # commands=router: /generate-code などは router が処理し inbox に届かない
                # 通常メッセージだけが async for msg in messages に届く
                async with hub.inbox(commands=router) as messages:
                    async for msg in messages:
                        listener = _active_hub_listener
                        if listener is not None:
                            # パイプラインがアクティブ: Gemini に inject
                            await listener.handle_message(hub, msg)
                        else:
                            # パイプラインなし: ack して無視
                            try:
                                await hub.ack(msg.id)
                            except Exception:
                                pass

        except asyncio.CancelledError:
            logger.info("Hub: シャットダウン")
            _active_hub = None
            clear_hub_session()
            raise
        except Exception as e:
            logger.error("Hub: 切断 (%s) — %.1fs 後に再接続", e, backoff)
            _active_hub = None
            clear_hub_session()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


# ---------------------------------------------------------------------------
# LiveKit token 生成
# ---------------------------------------------------------------------------

def _generate_livekit_token(room_name: str, identity: str) -> str:
    """LiveKit access token を生成して返す。"""
    from livekit.api import AccessToken, VideoGrants  # type: ignore
    token = AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
    token.with_identity(identity)
    token.with_name(identity)
    token.with_grants(VideoGrants(room_join=True, room=room_name))
    return token.to_jwt()


# ---------------------------------------------------------------------------
# HTTP ハンドラ
# ---------------------------------------------------------------------------

CLIENT_DIR = Path(__file__).parent.parent / "client"


async def handle_index(request: web.Request) -> web.Response:
    """GET / → index.html"""
    index_path = CLIENT_DIR / "index.html"
    if not index_path.exists():
        return web.Response(status=404, text="index.html not found")
    return web.FileResponse(index_path)


async def handle_auth(request: web.Request) -> web.Response:
    """POST /auth — OTP 検証 → LiveKit token 発行 → パイプライン起動

    Request body: {"code": "123456"}
    Response (200): {"token": "<livekit-token>", "url": "<livekit-url>"}
    Response (401): {"error": "invalid_otp"}
    Response (409): {"error": "session_in_use"}
    """
    global _active_hub_listener

    try:
        body = await request.json()
    except Exception:
        return web.Response(
            status=400,
            content_type="application/json",
            text=json.dumps({"error": "invalid_json"}),
        )

    code = str(body.get("code", "")).strip()
    if not otp_store.validate(code):
        logger.warning("Auth failed: invalid OTP")
        return web.Response(
            status=401,
            content_type="application/json",
            text=json.dumps({"error": "invalid_otp"}),
        )

    if session_manager.is_active:
        logger.info("Auth rejected: session already active")
        return web.Response(
            status=409,
            content_type="application/json",
            text=json.dumps({"error": "session_in_use"}),
        )

    # セッション開始
    room_name = f"voice-{uuid.uuid4().hex[:8]}"
    try:
        user_token = _generate_livekit_token(room_name, identity="user")
        bot_token = _generate_livekit_token(room_name, identity="voice-bot")
    except Exception as e:
        logger.error("LiveKit token generation failed: %s", e)
        return web.Response(
            status=500,
            content_type="application/json",
            text=json.dumps({"error": "token_generation_failed"}),
        )

    # HubListener (hub 接続を持たない — main の hub セッションを使う)
    hub_listener = HubListener()

    def _on_session_done():
        global _active_hub_listener
        _active_hub_listener = None
        session_manager.on_session_ended()

    runner_task = asyncio.create_task(
        create_pipeline_and_run(
            livekit_url=LIVEKIT_URL,
            bot_token=bot_token,
            room_name=room_name,
            gemini_api_key=GEMINI_API_KEY,
            gemini_model=GEMINI_MODEL,
            system_prompt=SYSTEM_PROMPT,
            tool_definitions=HUB_TOOL_DEFINITIONS,
            tool_handlers=HUB_TOOL_HANDLERS,
            hub_listener=hub_listener,
            on_session_ended=_on_session_done,
        ),
        name=f"pipeline-{room_name}",
    )

    session_manager.register(
        room_name=room_name,
        pipeline_task=None,   # PipelineTask の参照は pipeline.py 内で管理
        runner_task=runner_task,
    )

    # アクティブ HubListener を登録 (hub inbox から dispatch される)
    _active_hub_listener = hub_listener

    logger.info("Session started: room=%s", room_name)
    return web.Response(
        status=200,
        content_type="application/json",
        text=json.dumps({"token": user_token, "url": LIVEKIT_URL}),
    )


# ---------------------------------------------------------------------------
# 起動
# ---------------------------------------------------------------------------

async def main() -> None:
    # 単一 Hub セッション管理タスク (CommandRouter + inbox dispatch 担当)
    hub_task = asyncio.create_task(
        _run_hub_with_reconnect(), name="hub_manager"
    )
    logger.info("Hub manager started")

    # HTTP サーバー
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_post("/auth", handle_auth)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("voice-gateway v2 started on port %d", PORT)

    # 永久待機
    try:
        await asyncio.Event().wait()
    finally:
        hub_task.cancel()
        try:
            await hub_task
        except asyncio.CancelledError:
            pass
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
