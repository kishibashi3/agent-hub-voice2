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
"""
import asyncio
import json
import logging
import os
import uuid
from pathlib import Path

from aiohttp import web

from auth import OTPStore
from command_listener import CommandListener
from hub_listener import HubListener
from hub_tools import HUB_TOOL_DEFINITIONS, HUB_TOOL_HANDLERS
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

    # Pipecat パイプラインを非同期で起動
    hub_listener = HubListener(
        hub_url=HUB_URL,
        hub_user=HUB_USER,
        hub_pat=HUB_PAT,
        hub_tenant=HUB_TENANT,
    )

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
            on_session_ended=session_manager.on_session_ended,
        ),
        name=f"pipeline-{room_name}",
    )

    session_manager.register(
        room_name=room_name,
        pipeline_task=None,   # PipelineTask の参照は pipeline.py 内で管理
        runner_task=runner_task,
    )

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
    # CommandListener (agent-hub /generate-code)
    cmd_listener = CommandListener(
        hub_url=HUB_URL,
        hub_user=HUB_USER,
        hub_pat=HUB_PAT,
        hub_tenant=HUB_TENANT,
        otp_store=otp_store,
    )
    asyncio.create_task(cmd_listener.run(), name="command_listener")
    logger.info("CommandListener started")

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
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
