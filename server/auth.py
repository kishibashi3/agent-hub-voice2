"""
OTP (ワンタイムコード) 認証ストア。
voice-gateway v2 への接続 (HTTP /auth) に使用する 6 桁コードを管理する。

## v2 変更点

- **ファイル永続化**: Docker コンテナ再起動後も OTP を復元できるよう
  `OTP_PERSIST_PATH`（デフォルト: `/tmp/otp_store.json`）に保存する。
  expires_at は wall clock (Unix timestamp) で保存し、ロード時に
  time.monotonic() ベースに変換することでクロックリセットに対応する。

- **generate() の冪等化**: 有効な OTP が既に存在する場合は新規生成せず
  既存コードを返す。`/generate-code` の二重送信でコードが上書きされる
  問題を防ぐ。
"""
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

OTP_TTL_SECONDS = 300  # 5 分
OTP_PERSIST_PATH = os.environ.get("OTP_PERSIST_PATH", "/tmp/otp_store.json")


@dataclass
class _OTPEntry:
    code: str
    expires_at: float  # time.monotonic() ベース


class OTPStore:
    """
    シングルトン OTP ストア。
    同時に有効なコードは 1 つのみ。

    Docker 再起動耐性のため OTP をファイルに永続化する。
    `restart: unless-stopped` によるコンテナ再起動後も OTP が復元される。
    """

    def __init__(self, persist_path: str = OTP_PERSIST_PATH) -> None:
        self._persist_path = persist_path
        self._entry: _OTPEntry | None = None
        self._load()  # 起動時にファイルから OTP を復元

    # ------------------------------------------------------------------
    # 永続化 (内部)
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """起動時に永続化ファイルから OTP を復元する。

        expires_at はファイル上では wall clock (Unix timestamp) で保存されている。
        ロード時に time.monotonic() ベースに変換することで、コンテナ再起動後も
        正確な残り TTL を維持する。
        """
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
        except FileNotFoundError:
            return
        except Exception as e:
            logger.warning("OTP persist file read error: %s", e)
            return

        wall_expires_at: float = data.get("expires_at_wall", 0.0)
        remaining = wall_expires_at - time.time()

        if remaining <= 0:
            logger.info("OTP persist file found but expired — removing")
            self._remove_persist_file()
            return

        # wall clock 残り TTL を monotonic ベースに変換
        self._entry = _OTPEntry(
            code=data["code"],
            expires_at=time.monotonic() + remaining,
        )
        logger.info(
            "OTP restored from persist file (ttl_remaining=%ds)", int(remaining)
        )

    def _save(self) -> None:
        """現在の OTP をファイルに書き込む。_entry が None の場合はファイルを削除する。"""
        if self._entry is None:
            self._remove_persist_file()
            return

        # expires_at は monotonic ベースなので wall clock に変換して保存
        remaining = self._entry.expires_at - time.monotonic()
        wall_expires_at = time.time() + remaining

        try:
            with open(self._persist_path, "w") as f:
                json.dump(
                    {"code": self._entry.code, "expires_at_wall": wall_expires_at}, f
                )
        except Exception as e:
            logger.warning("OTP persist file write error: %s", e)

    def _remove_persist_file(self) -> None:
        """永続化ファイルを削除する（エラーは無視）。"""
        try:
            os.unlink(self._persist_path)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("OTP persist file remove error: %s", e)

    # ------------------------------------------------------------------
    # 公開 API
    # ------------------------------------------------------------------

    def generate(self) -> tuple[str, int]:
        """OTP を生成して返す。

        有効な OTP が既に存在する場合は新規生成せず既存コードを返す（冪等）。
        `/generate-code` の二重送信で旧コードが上書きされる問題を防ぐ。

        Returns:
            (code, ttl_seconds_remaining)
        """
        if self._entry is not None:
            remaining = self._entry.expires_at - time.monotonic()
            if remaining > 0:
                logger.info("OTP reused (ttl_remaining=%ds)", int(remaining))
                return self._entry.code, int(remaining)

        # 新規生成（既存なし or 期限切れ）
        code = f"{secrets.randbelow(1_000_000):06d}"
        self._entry = _OTPEntry(
            code=code,
            expires_at=time.monotonic() + OTP_TTL_SECONDS,
        )
        self._save()
        logger.info("OTP generated (ttl=%ds)", OTP_TTL_SECONDS)
        return code, OTP_TTL_SECONDS

    def validate(self, code: str) -> bool:
        """コードを検証して消費する。有効なら True を返す（使い捨て）。"""
        if self._entry is None:
            return False
        if time.monotonic() > self._entry.expires_at:
            self._entry = None
            self._save()
            logger.debug("OTP expired")
            return False
        if self._entry.code != code:
            logger.debug("OTP mismatch")
            return False
        self._entry = None
        self._save()
        logger.info("OTP validated and consumed")
        return True

    def ttl_remaining(self) -> int:
        """残り有効秒数（なければ 0）。"""
        if self._entry is None:
            return 0
        remaining = self._entry.expires_at - time.monotonic()
        return max(0, int(remaining))
