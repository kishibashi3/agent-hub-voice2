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

## hub 接続設計

AgentHub.connect() は main() で 1 回だけ呼ぶ。単一の MCP セッションを
_run_hub_with_reconnect() が管理し、hub.inbox() も 1 本だけ開く。

メッセージは以下のルールで dispatch する:
  - /generate-code → CommandListener.handle() (OTP 発行)
  - それ以外 → HubListener.handle_message() → パイプラインに TextFrame inject

これにより同一 @voice ハンドルで 2 本の MCP セッションが並走する問題と、
inbox の race condition (メッセージ消失) を解消する。
"""
import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

from aiohttp import web

from agent_hub_sdk import AgentHub

from auth import OTPStore
from command_listener import CommandListener, CMD_GENERATE_CODE, DISPLAY_NAME
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
# Hub 管理
# ---------------------------------------------------------------------------

async def _run_hub_with_reconnect(cmd_listener: CommandListener) -> None:
    """単一の AgentHub セッションを管理する。切断時は自動再接続。

    hub.inbox() を 1 本だけ開き、メッセージを以下のルールで dispatch する:
      - /generate-code → CommandListener.handle() (OTP 発行)
      - それ以外、パイプラインあり → HubListener.handle_message() → TextFrame inject
      - それ以外、パイプラインなし → ack して無視

    これにより同一 @voice ハンドルで 2 本の MCP セッションが並走する問題と、
    inbox の race condition を解消する。
    """
    global _active_hub
    backoff = 5.0
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

                async with hub.inbox() as messages:
                    async for msg in messages:
                        body = (msg.body or "").strip()
                        if body.lower() == CMD_GENERATE_CODE:
                            # /generate-code: セッション有無に関わらず CommandListener が処理
                            await cmd_listener.handle(hub, msg)
                        else:
                            listener = _active_hub_listener
                            if listener is not None:
                                # パイプラインがアクティブ: pipeline に inject
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
    # CommandListener (OTP 発行ハンドラ) を生成
    # hub 接続は _run_hub_with_reconnect が管理するため、ここでは接続しない
    cmd_listener = CommandListener(otp_store=otp_store)

    # 単一 Hub セッション管理タスク (inbox dispatch も担当)
    hub_task = asyncio.create_task(
        _run_hub_with_reconnect(cmd_listener), name="hub_manager"
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
