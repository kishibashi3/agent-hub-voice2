"""
OTP (ワンタイムコード) 認証ストア。
voice-gateway v2 への接続 (HTTP /auth) に使用する 6 桁コードを管理する。

## v2 変更点 (issue #10)

- **`time.time()` (wall clock) に統一**: コンテナ再起動後も有効期限の比較が正確。
  元の `time.monotonic()` はプロセス起動からの相対時刻のため、再起動後に
  有効期限の基準がリセットされる問題があった。

- **ファイル永続化**: `OTP_STORE_PATH`（デフォルト: `/tmp/voice2-otp.json`）に保存。
  `restart: unless-stopped` によるコンテナ再起動後も OTP が復元される。
  docker-compose の named volume と組み合わせることでコンテナ再作成にも対応。

- **`generate()` の冪等化**: 有効な OTP が既に存在する場合は新規生成せず
  既存コードを返す。`/generate-code` の二重送信でコードが上書きされる問題を防ぐ。

- **`validate()` の診断ログ強化**: 失敗理由を INFO レベルで記録
  (`no_entry` / `expired` / `mismatch`)。実機デバッグに活用できる。
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
# docker-compose で named volume をマウントする場合は OTP_STORE_PATH=/otp/otp.json を設定
OTP_STORE_PATH = Path(os.environ.get("OTP_STORE_PATH", "/tmp/voice2-otp.json"))


@dataclass
class _OTPEntry:
    code: str
    expires_at: float  # time.time() ベース (wall clock)


class OTPStore:
    """
    シングルトン OTP ストア。
    同時に有効なコードは 1 つのみ。

    Docker 再起動耐性のため OTP をファイルに永続化する。
    expires_at は wall clock (time.time()) で管理するため、
    コンテナ再起動後も有効期限の検証が正確に行われる。
    """

    def __init__(self, store_path: Path = OTP_STORE_PATH) -> None:
        self._store_path = store_path
        self._entry: _OTPEntry | None = None
        self._load()  # 起動時にファイルから OTP を復元

    # ------------------------------------------------------------------
    # 永続化 (内部)
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """起動時にファイルから OTP を復元する。期限切れなら捨てる。"""
        try:
            if not self._store_path.exists():
                return
            data = json.loads(self._store_path.read_text())
            entry = _OTPEntry(
                code=data["code"],
                expires_at=float(data["expires_at"]),
            )
            if time.time() <= entry.expires_at:
                self._entry = entry
                remaining = int(entry.expires_at - time.time())
                logger.info(
                    "OTP restored from %s (remaining=%ds)", self._store_path, remaining
                )
            else:
                logger.info("OTP in %s is expired — discarding", self._store_path)
                self._store_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("OTP file load failed (%s) — starting fresh", e)
            self._entry = None

    def _save(self) -> None:
        """現在の OTP をファイルに書き込む。_entry が None の場合はファイルを削除する。"""
        try:
            if self._entry:
                self._store_path.write_text(
                    json.dumps(
                        {"code": self._entry.code, "expires_at": self._entry.expires_at}
                    )
                )
            else:
                self._store_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("OTP file save failed (%s)", e)

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
            remaining = self._entry.expires_at - time.time()
            if remaining > 0:
                logger.info("OTP reused (ttl_remaining=%ds)", int(remaining))
                return self._entry.code, int(remaining)

        # 新規生成（既存なし or 期限切れ）
        code = f"{secrets.randbelow(1_000_000):06d}"
        self._entry = _OTPEntry(
            code=code,
            expires_at=time.time() + OTP_TTL_SECONDS,
        )
        self._save()
        logger.info("OTP generated (ttl=%ds)", OTP_TTL_SECONDS)
        return code, OTP_TTL_SECONDS

    def validate(self, code: str) -> bool:
        """コードを検証して消費する。有効なら True を返す（使い捨て）。

        失敗理由を INFO ログで記録する（診断用）。
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
        self._entry = None
        self._save()
        logger.info("OTP validated and consumed")
        return True

    def ttl_remaining(self) -> int:
        """残り有効秒数（なければ 0）。"""
        if self._entry is None:
            return 0
        remaining = self._entry.expires_at - time.time()
        return max(0, int(remaining))
