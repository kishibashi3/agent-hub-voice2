"""
agent-hub inbox を listen し、受信メッセージを Pipecat パイプラインに inject する。

役割:
  - agent-hub に接続して inbox を監視
  - 受信メッセージを TextFrame としてパイプラインに送信 (pikon 通知)
  - set_hub_session / clear_hub_session で hub_tools と hub を共有
  - MCP セッション切断時に自動再接続 (v1 の hub_manager_loop と同等)

v1 の _hub_manager_loop + PikonListener を Pipecat 用に移植。
"""
import asyncio
import logging

from agent_hub_sdk import AgentHub, IncomingMessage

from hub_tools import set_hub_session, clear_hub_session

logger = logging.getLogger(__name__)

_HUB_RECONNECT_BACKOFF_MIN_S = 1.0
_HUB_RECONNECT_BACKOFF_MAX_S = 30.0


class HubListener:
    """
    agent-hub に接続して inbox を listen する。
    受信メッセージは `on_messages` コールバックに渡す。

    on_messages(messages: list[IncomingMessage]) はパイプラインに TextFrame を inject する関数。
    """

    def __init__(
        self,
        hub_url: str,
        hub_user: str,
        hub_pat: str,
        hub_tenant: str | None,
    ) -> None:
        self._hub_url = hub_url
        self._hub_user = hub_user
        self._hub_pat = hub_pat
        self._hub_tenant = hub_tenant
        self._on_messages = None  # コールバック: set_callback() で設定

    def set_callback(self, on_messages) -> None:
        """メッセージ受信時のコールバックを設定する。"""
        self._on_messages = on_messages

    async def run(self) -> None:
        """接続 → inbox listen ループ（再接続バックオフ付き）。

        CancelledError で停止する。セッション終了時は cancel() で停止すること。
        """
        backoff = _HUB_RECONNECT_BACKOFF_MIN_S
        while True:
            try:
                async with AgentHub.connect(
                    user=self._hub_user,
                    url=self._hub_url,
                    pat=self._hub_pat,
                    tenant=self._hub_tenant,
                ) as hub:
                    set_hub_session(hub)
                    backoff = _HUB_RECONNECT_BACKOFF_MIN_S
                    logger.info("HubListener: connected as @%s", self._hub_user)
                    try:
                        await self._inbox_loop(hub)
                    finally:
                        clear_hub_session()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "HubListener: disconnected (retry in %.1fs): %s",
                    backoff,
                    e,
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _HUB_RECONNECT_BACKOFF_MAX_S)

    async def _inbox_loop(self, hub) -> None:
        """inbox を listen してコールバックに渡す。"""
        pending: list[IncomingMessage] = []
        async with hub.inbox() as messages:
            async for msg in messages:
                pending.append(msg)
                # ack
                try:
                    await hub.ack(msg.id)
                except Exception as ack_err:
                    logger.warning("ack failed for %s: %s", msg.id, ack_err)

                if self._on_messages and pending:
                    try:
                        await self._on_messages(pending[:])
                    except Exception as cb_err:
                        logger.error("on_messages callback error: %s", cb_err)
                    pending.clear()
