"""
agent-hub inbox メッセージを Pipecat パイプラインに inject するルーター。

役割:
  - main.py の単一 AgentHub セッションからメッセージを受け取る (handle_message)
  - 受信メッセージを on_messages コールバック経由でパイプラインに注入
  - hub 接続は持たない (main.py が管理する)

hub 接続を持たない設計にすることで、CommandListener との inbox 競合 (race condition) を解消する。
hub.inbox() を 1 箇所だけで開き、/generate-code は CommandListener に、
それ以外はパイプラインにルーティングする。

v1 の _hub_manager_loop + PikonListener を Pipecat 用に移植した hub_listener.py から
hub 接続部分を分離したもの。
"""
import logging

from agent_hub_sdk import HubSession, IncomingMessage

logger = logging.getLogger(__name__)


class HubListener:
    """
    受信メッセージを Pipecat パイプラインに inject するルーター。

    hub 接続は持たない。main.py が単一の AgentHub セッションを管理し、
    handle_message() 経由でメッセージを受け取る設計。

    set_callback() / clear_callback() でパイプライン起動・終了時に
    TextFrame inject 関数を登録・解除する。
    """

    def __init__(self) -> None:
        self._on_messages = None  # コールバック: set_callback() で設定

    def set_callback(self, on_messages) -> None:
        """メッセージ受信時のコールバックを設定する (パイプライン起動時に呼ぶ)。"""
        self._on_messages = on_messages

    def clear_callback(self) -> None:
        """コールバックを解除する (パイプライン終了時に呼ぶ)。"""
        self._on_messages = None

    async def handle_message(self, hub: HubSession, msg: IncomingMessage) -> None:
        """
        inbox から受け取った 1 件のメッセージを処理する。

        ack を送信した後、on_messages コールバックに渡す。
        コールバックが未設定の場合は ack のみ行い無視する。
        """
        try:
            await hub.ack(msg.id)
        except Exception as ack_err:
            logger.warning("ack failed for %s: %s", msg.id, ack_err)

        if self._on_messages:
            try:
                await self._on_messages([msg])
            except Exception as cb_err:
                logger.error("on_messages callback error: %s", cb_err)
