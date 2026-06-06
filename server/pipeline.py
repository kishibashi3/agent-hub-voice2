"""
Pipecat パイプライン factory。

LiveKitTransport + SileroVADAnalyzer + GeminiLiveLLMService を
Pipecat Pipeline DSL で組み立てる。

pipecat-ai v1.3.0 で検証済みの import パス (2026-06-06):
  - GeminiLiveLLMService: pipecat.services.google.gemini_live.llm
  - LiveKitTransport/LiveKitParams: pipecat.transports.livekit.transport
  - SileroVADAnalyzer: pipecat.audio.vad.silero
  - VADParams: pipecat.audio.vad.vad_analyzer
  - Pipeline: pipecat.pipeline.pipeline
  - PipelineRunner: pipecat.pipeline.runner (v1.3.0 では deprecated shim、実体は pipecat.workers.runner.WorkerRunner)
  - PipelineTask/PipelineParams: pipecat.pipeline.task (v1.3.0 では deprecated shim、実体は pipecat.pipeline.worker)
  - TextFrame/EndFrame: pipecat.frames.frames

参考: https://docs.pipecat.ai/server/services/s2s/gemini-live
"""
import asyncio
import logging

from agent_hub_sdk import IncomingMessage

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pipecat imports
# NOTE: バージョン差異がある場合は以下を調整する
# ---------------------------------------------------------------------------
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.frames.frames import TextFrame, EndFrame

# pipecat v1.3.0: pipecat.services.google.gemini_live.llm に移動
# - pipecat.services.google.GeminiLiveLLMService は存在しない (google/__init__.py は空)
# - pipecat.services.gemini_multimodal_live は廃止済み
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
_GEMINI_CLASS = GeminiLiveLLMService

# pipecat v1.3.0: pipecat.transports.livekit.transport に移動
# - pipecat.transports.services.livekit は存在しない (services/ ディレクトリなし)
from pipecat.transports.livekit.transport import LiveKitTransport, LiveKitParams
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams


def _format_messages(messages: list[IncomingMessage]) -> str:
    """pikon メッセージを Gemini context 注入用テキストに変換する。"""
    lines = ["【agent-hub 未読メッセージ】"]
    for m in messages:
        lines.append(f"  {m.sender}: {m.body}")
    return "\n".join(lines)


async def create_pipeline_and_run(
    livekit_url: str,
    bot_token: str,
    room_name: str,
    gemini_api_key: str,
    gemini_model: str,
    system_prompt: str,
    tool_definitions: list,
    tool_handlers: dict,
    hub_listener,
    on_session_ended,
) -> None:
    """
    Pipecat パイプラインを構築・起動し、セッション終了まで待機する。

    この関数は asyncio.create_task() で実行する (非ブロッキング)。

    Args:
        livekit_url:       LiveKit サーバーの WebSocket URL
        bot_token:         Pipecat bot participant 用 LiveKit access token
        room_name:         LiveKit room 名
        gemini_api_key:    Gemini API キー
        gemini_model:      モデル名 (例: gemini-2.0-flash-live-001)
        system_prompt:     Gemini system instruction
        tool_definitions:  HUB_TOOL_DEFINITIONS (google.genai.types.Tool list)
        tool_handlers:     HUB_TOOL_HANDLERS ({name: async_fn})
        hub_listener:      HubListener instance (pikon inject 用)
        on_session_ended:  セッション終了コールバック (SessionManager.on_session_ended)
    """
    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------
    transport = LiveKitTransport(
        url=livekit_url,
        token=bot_token,
        room_name=room_name,
        params=LiveKitParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_enabled=True,
            vad_analyzer=SileroVADAnalyzer(
                params=VADParams(
                    start_secs=0.2,   # 発話開始判定: 0.2 秒以上の音声
                    stop_secs=0.8,    # 発話終了判定: 0.8 秒の無音
                )
            ),
        ),
    )

    # ------------------------------------------------------------------
    # LLM Service
    # ------------------------------------------------------------------
    llm = _GEMINI_CLASS(
        api_key=gemini_api_key,
        model=gemini_model,
        system_instruction=system_prompt,
        tools=tool_definitions,
    )

    # ツールハンドラを登録
    for name, handler in tool_handlers.items():
        llm.register_function(name, handler)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------
    pipeline = Pipeline([
        transport.input(),
        llm,
        transport.output(),
    ])

    task = PipelineTask(
        pipeline,
        params=PipelineParams(allow_interruptions=True),
    )

    # ------------------------------------------------------------------
    # pikon (agent-hub メッセージ) → TextFrame inject
    # ------------------------------------------------------------------
    async def on_hub_messages(messages: list[IncomingMessage]) -> None:
        text = _format_messages(messages)
        await task.queue_frame(TextFrame(text))
        logger.info("Injected %d hub messages to pipeline", len(messages))

    hub_listener.set_callback(on_hub_messages)
    hub_task = asyncio.create_task(hub_listener.run(), name="hub_listener")

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------
    runner = PipelineRunner()
    try:
        logger.info("Pipeline starting: room=%s model=%s", room_name, gemini_model)
        await runner.run(task)
    except asyncio.CancelledError:
        logger.info("Pipeline cancelled: room=%s", room_name)
        await task.queue_frame(EndFrame())
    except Exception as e:
        logger.error("Pipeline error: room=%s error=%s", room_name, e)
    finally:
        hub_task.cancel()
        try:
            await hub_task
        except asyncio.CancelledError:
            pass
        on_session_ended()
        logger.info("Pipeline ended: room=%s", room_name)
