"""
セッション排他制御。

voice-gateway v2 はシングルセッション制約: 同時に 1 ブラウザ接続のみ許可。

SessionManager は:
  - アクティブセッションの有無を管理
  - Pipecat PipelineTask の参照を保持
  - 新しい接続要求 (POST /auth) が来たとき:
      - アクティブセッションがあれば 409 を返す
      - なければ新セッションを登録してパイプラインを起動
  - セッション終了時にクリーンアップ
"""
import asyncio
import logging

logger = logging.getLogger(__name__)


class SessionManager:
    """シングルトン。アクティブな Pipecat セッションを 1 つだけ管理する。"""

    def __init__(self) -> None:
        self._active: bool = False
        self._room_name: str | None = None
        self._pipeline_task = None   # pipecat PipelineTask
        self._runner_task: asyncio.Task | None = None  # asyncio task

    @property
    def is_active(self) -> bool:
        """アクティブセッションが存在するか。"""
        return self._active

    @property
    def room_name(self) -> str | None:
        return self._room_name

    def register(
        self,
        room_name: str,
        pipeline_task,
        runner_task: asyncio.Task,
    ) -> None:
        """パイプライン起動後にセッションを登録する。"""
        self._active = True
        self._room_name = room_name
        self._pipeline_task = pipeline_task
        self._runner_task = runner_task
        logger.info("Session registered: room=%s", room_name)

    async def terminate(self) -> None:
        """現在のセッションを graceful に停止してクリーンアップする。"""
        if not self._active:
            return

        logger.info("Terminating session: room=%s", self._room_name)

        # Pipecat PipelineTask をキャンセル
        if self._pipeline_task is not None:
            try:
                await self._pipeline_task.cancel()
            except Exception as e:
                logger.warning("pipeline_task.cancel() error: %s", e)

        # asyncio task をキャンセル
        if self._runner_task is not None and not self._runner_task.done():
            self._runner_task.cancel()
            try:
                await asyncio.wait_for(self._runner_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        self._active = False
        self._room_name = None
        self._pipeline_task = None
        self._runner_task = None
        logger.info("Session terminated")

    def on_session_ended(self) -> None:
        """パイプラインが自然終了した場合のコールバック (runner 側から呼ぶ)。"""
        logger.info("Session ended naturally: room=%s", self._room_name)
        self._active = False
        self._room_name = None
        self._pipeline_task = None
        self._runner_task = None
