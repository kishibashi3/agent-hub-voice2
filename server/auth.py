"""
OTP (ワンタイムコード) 認証ストア。
voice-gateway v2 への接続 (HTTP /auth) に使用する 6 桁コードを管理する。

v2 変更点 (issue #10):
- time.monotonic() → time.time() (wall clock): コンテナ再起動後も有効期限が正確
- ファイル永続化: コンテナ再起動後も OTP が失われない
  - 保存先: OTP_STORE_PATH 環境変数 (デフォルト: /tmp/voice2-otp.json)
  - docker-compose で named volume をマウントして恒久化推奨
- validate() のログ詳細化: 失敗理由を INFO レベルで出力 (no_entry / expired / mismatch)
"""
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

OTP_TTL_SECONDS = 300  # 5 分

# OTP 永続化ファイルパス (環境変数で上書き可)
# docker-compose で named volume をマウントして永続化する場合は OTP_STORE_PATH=/otp/otp.json を設定
OTP_STORE_PATH = Path(os.environ.get("OTP_STORE_PATH", "/tmp/voice2-otp.json"))


@dataclass
class _OTPEntry:
    code: str
    expires_at: float  # time.time() ベース (wall clock)


class OTPStore:
    """
    シングルトン OTP ストア。
    同時に有効なコードは 1 つのみ（新規生成で旧コードを上書き）。

    ファイルに永続化することで、コンテナ再起動後も OTP が失われないようにする。
    expires_at は time.time() (wall clock) ベースで管理するため、
    再起動後も有効期限の検証が正確に行われる。
    """

    def __init__(self) -> None:
        self._entry: _OTPEntry | None = None
        self._load()

    # ------------------------------------------------------------------
    # 永続化
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """起動時にファイルから OTP を復元する。期限切れなら捨てる。"""
        try:
            if OTP_STORE_PATH.exists():
                data = json.loads(OTP_STORE_PATH.read_text())
                entry = _OTPEntry(code=data["code"], expires_at=float(data["expires_at"]))
                if time.time() <= entry.expires_at:
                    self._entry = entry
                    remaining = int(entry.expires_at - time.time())
                    logger.info("OTP restored from %s (remaining=%ds)", OTP_STORE_PATH, remaining)
                else:
                    logger.info("OTP in %s is expired — discarding", OTP_STORE_PATH)
                    OTP_STORE_PATH.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("OTP file load failed (%s) — starting fresh", e)
            self._entry = None

    def _save(self) -> None:
        """現在の OTP をファイルに書き込む。エラーはログのみ（非致命的）。"""
        try:
            if self._entry:
                OTP_STORE_PATH.write_text(json.dumps({
                    "code": self._entry.code,
                    "expires_at": self._entry.expires_at,
                }))
            else:
                OTP_STORE_PATH.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("OTP file save failed (%s)", e)

    # ------------------------------------------------------------------
    # パブリック API
    # ------------------------------------------------------------------

    def generate(self) -> tuple[str, int]:
        """
        新しい 6 桁 OTP を生成して返す。
        既存コードがあれば無効化する。

        Returns:
            (code, ttl_seconds)
        """
        code = f"{secrets.randbelow(1_000_000):06d}"
        self._entry = _OTPEntry(
            code=code,
            expires_at=time.time() + OTP_TTL_SECONDS,
        )
        self._save()
        logger.info("OTP generated (ttl=%ds)", OTP_TTL_SECONDS)
        return code, OTP_TTL_SECONDS

    def validate(self, code: str) -> bool:
        """
        コードを検証して消費する。有効なら True を返す（使い捨て）。
        失敗理由は INFO ログで記録する (診断用)。
        """
        if self._entry is None:
            logger.info(
                "OTP validate failed: no_entry"
                " (OTP not generated, or lost due to container restart)"
            )
            return False
        if time.time() > self._entry.expires_at:
            self._entry = None
            self._save()
            logger.info("OTP validate failed: expired")
            return False
        if self._entry.code != code:
            # セキュリティ: 不一致の先頭 2 桁のみログに残す
            submitted_hint = (code[:2] + "****") if len(code) >= 2 else "(empty)"
            logger.info("OTP validate failed: mismatch (submitted=%s)", submitted_hint)
            return False
        self._entry = None  # 使い捨て
        self._save()
        logger.info("OTP validated and consumed")
        return True

    def ttl_remaining(self) -> int:
        """残り有効秒数（なければ 0）。"""
        if self._entry is None:
            return 0
        remaining = self._entry.expires_at - time.time()
        return max(0, int(remaining))
