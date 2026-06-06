# Voice Gateway v2 — 設計ドキュメント

**ステータス**: Draft  
**依頼元**: @ope-ultp1635 (DM: cbc36312-7b24-4b73-a2ad-bab3579ec238)  
**作成**: 2026-06-06

---

## 1. 動機 — なぜ v2 が必要か

### v1 の根本問題

| 問題 | 原因 | v1 での対処 | 限界 |
|---|---|---|---|
| **AEC 非機能** | WebSocket 経由の AI 音声にはブラウザ WebRTC AEC3 の参照信号がない | worklet でゼロ PCM mute + RMS 検知 | 自然な barge-in が困難 |
| **ADK の重さ** | google-adk の多エージェント機能・SessionState が不要なのに pip に載る | 使用していない機能を無視 | pip 依存競合リスク、メンテ負債 |
| **独自 VAD** | Gemini server-side VAD + worklet RMS 閾値の組み合わせ | RMS 閾値でカバー | 精度・遅延が非最適 |

### v2 での解決

```
[v1] Browser (AudioWorklet+WebSocket) → Server (ADK+独自セッション) → Gemini
         ↑ AEC reference signal なし

[v2] Browser (LiveKit JS SDK / WebRTC) → LiveKit Server → Pipecat → Gemini Live
         ↑ WebRTC remote audio track → ブラウザ AEC3 が reference signal を自動取得
```

研究レポート結論 (`2026-06-06-voice-gateway-pipecat-adk-design.md`):
> **Pipecat + WebRTC transport（LiveKit）移行が AEC の根本解決策**。  
> WebRTC 経由で AI 音声がブラウザに届くと AEC3 が reference signal を自動取得できる。

---

## 2. アーキテクチャ全体図

```
┌──────────────────────── Pi5 (self-hosted) ─────────────────────────┐
│                                                                      │
│  ┌─────────────────┐    ┌──────────────────────────────────────┐   │
│  │  LiveKit Server  │    │  Pipecat Server (Python / aiohttp)   │   │
│  │  (port 7880)     │◄──►│                                      │   │
│  │  WebRTC SFU      │    │  ┌──────────────────────────────┐   │   │
│  └─────────────────┘    │  │  Pipeline                     │   │   │
│                           │  │  LiveKitTransport.input()    │   │   │
│                           │  │    ↓ AudioRawFrame           │   │   │
│                           │  │  SileroVADAnalyzer           │   │   │
│                           │  │    ↓ VAD events              │   │   │
│                           │  │  GeminiLiveLLMService        │───┼───┼──► Gemini Live API
│                           │  │    ↓ AudioRawFrame (AI音声) │   │   │
│                           │  │  LiveKitTransport.output()   │   │   │
│                           │  └──────────────────────────────┘   │   │
│                           │                                      │   │
│                           │  CommandListener (agent-hub SDK)     │◄──┼──► agent-hub
│                           │  HTTP /auth /  GET /                 │   │
│                           └──────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                    ▲ WebRTC (audio + data channel)
                    │
            ┌───────────────┐
            │  スマホブラウザ │
            │  LiveKit JS SDK│
            │  AEC3 (自動)   │
            └───────────────┘
```

---

## 3. コンポーネント詳細

### 3.1 LiveKit Server (self-hosted)

| 項目 | 値 |
|---|---|
| 役割 | WebRTC SFU / メディアルーター |
| ポート | 7880 (HTTP/WS), 7881 (RTC/UDP) |
| デプロイ | Docker (`livekit/livekit-server`) |
| ARM64 | 公式サポートあり |
| コスト | 無料 (OSS / self-hosted) |

- 各セッションに 1 room を割り当て (`room = UUID`)
- ブラウザ participant と Pipecat bot participant の 2 者が 1 room に入る
- セッション終了時に room を削除

### 3.2 Pipecat Server (Python / aiohttp)

| ファイル | 役割 |
|---|---|
| `main.py` | aiohttp サーバー: `GET /`, `POST /auth`, シングルトン起動 |
| `auth.py` | OTPStore (v1 から流用) |
| `command_listener.py` | agent-hub inbox listen + /generate-code + 未知コマンドエラー |
| `pipeline.py` | Pipecat パイプライン factory |
| `hub_tools.py` | Gemini function tools (send_message 等) |
| `session_manager.py` | セッション排他制御 (1 セッション制約) |

**HTTP エンドポイント:**

```
GET  /           → index.html (ブラウザ UI)
POST /auth       {code: "123456"}
                 → {token: "<livekit-token>", url: "ws://pi.local:7880"}
                    or 4xx {error: "invalid_otp"}
```

### 3.3 Pipecat パイプライン

```python
transport = LiveKitTransport(
    url=livekit_url,
    token=bot_token,
    room_name=room_name,
    params=LiveKitParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        vad_enabled=True,
        vad_analyzer=SileroVADAnalyzer(params=VADParams(
            start_secs=0.2,
            stop_secs=0.8,
        )),
    ),
)

llm = GeminiMultimodalLiveLLMService(
    api_key=GEMINI_API_KEY,
    model="gemini-2.0-flash-live-001",
    system_instruction=SYSTEM_PROMPT,
    tools=HUB_TOOL_DEFINITIONS,
    tool_choice="auto",
)

pipeline = Pipeline([
    transport.input(),
    llm,
    transport.output(),
])
```

### 3.4 ブラウザ UI

- **v1 との差分**: AudioWorklet / worklet.js / カスタム PCM 処理は全て廃止
- **v2**: LiveKit JS SDK (`@livekit/client`) のみ使用
- OTP 入力 → `POST /auth` → token 取得 → `room.connect(url, token)`
- 音声 I/O は LiveKit SDK が担当 (AEC3, VAD は WebRTC スタックに委任)

---

## 4. OTP 認証フロー

```
[agent-hub]
  @user → /generate-code → @voice
  CommandListener → OTPStore.generate() → @user に 6 桁コードを返信

[ブラウザ]
  1. ブラウザ: POST /auth {code: "123456"}
  2. Pipecat server: OTPStore.validate(code)
     - 失敗: 401 {error: "invalid_otp"}
     - 成功:
       a. room_name = UUID
       b. bot_token  = LiveKit token (bot participant 用)
       c. user_token = LiveKit token (ブラウザ参加者用)
       d. asyncio.create_task(start_pipeline(room_name, bot_token))
       e. 200 {token: user_token, url: livekit_url}
  3. ブラウザ: room.connect(url, user_token) → WebRTC 接続確立
  4. LiveKit SFU がブラウザ ↔ Pipecat bot の音声を中継
```

**セッション排他制御:**
- アクティブセッションが存在する状態で `/auth` が叩かれた場合は `409 {error: "session_in_use"}` を返す
- ブラウザ UI に `セッション使用中` エラーを表示

---

## 5. agent-hub 統合

### 5.1 CommandListener (継続)

v1 の `command_listener.py` をそのまま流用:
- `@voice` の inbox を listen
- `/generate-code` → OTP 生成・返信
- 未知コマンド → エラーレスポンス (issue #11 実装済み)

### 5.2 pikon 通知

Pipecat にはフレームをパイプラインに inject する機構がある。  
`PipelineTask.queue_frame(TextFrame(...))` で Gemini context にテキストを挿入:

```python
# hub_listener.py: agent-hub inbox をポーリング
async def on_message(messages):
    text = format_messages(messages)
    await pipeline_task.queue_frame(TextFrame(text))
```

### 5.3 Gemini 関数ツール

`hub_tools.py`: Pipecat の `FunctionCallResultFrame` 機構でツールを登録:

```python
HUB_TOOL_DEFINITIONS = [
    {
        "function_declarations": [{
            "name": "send_message",
            "description": "agent-hub 経由で指定した participant にメッセージを送る",
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {"type": "string"},
                    "message": {"type": "string"},
                },
                "required": ["to", "message"],
            },
        }]
    }
]
```

---

## 6. ファイル構成

```
agent-hub-voice2/
├── DESIGN.md                 # 本ドキュメント
├── server/
│   ├── main.py               # aiohttp サーバー (HTTP auth + static)
│   ├── auth.py               # OTPStore (v1 流用)
│   ├── command_listener.py   # agent-hub CommandListener (v1 流用 + #11 修正)
│   ├── pipeline.py           # Pipecat パイプライン factory
│   ├── hub_tools.py          # Gemini function tools
│   ├── hub_listener.py       # agent-hub inbox → pipeline frame inject
│   ├── session_manager.py    # セッション排他制御
│   └── requirements.txt
├── client/
│   └── index.html            # LiveKit JS SDK のみ使用
├── livekit/
│   └── livekit.yaml          # LiveKit server 設定
├── Dockerfile                # Pipecat server
├── docker-compose.yml        # LiveKit + Pipecat server
└── deploy/
    └── README.md
```

---

## 7. デプロイ (Pi5 self-hosted)

### 7.1 LiveKit Server

```yaml
# docker-compose.yml (抜粋)
services:
  livekit:
    image: livekit/livekit-server:latest
    platform: linux/arm64
    ports:
      - "7880:7880"
      - "7881:7881/udp"
    volumes:
      - ./livekit/livekit.yaml:/livekit.yaml
    command: --config /livekit.yaml

  pipecat:
    build: .
    environment:
      - LIVEKIT_URL=ws://localhost:7880
      - LIVEKIT_API_KEY=...
      - LIVEKIT_API_SECRET=...
      - GEMINI_API_KEY=...
      - AGENT_HUB_URL=...
      - AGENT_HUB_USER=voice
      - AGENT_HUB_GITHUB_PAT=...
    ports:
      - "8765:8765"
```

### 7.2 LiveKit 設定 (livekit.yaml)

```yaml
port: 7880
rtc:
  udp_port: 7881
  tcp_port: 7882
  use_external_ip: false
keys:
  <api_key>: <api_secret>
```

---

## 8. v1 との差分サマリー

| 機能 | v1 | v2 |
|---|---|---|
| Transport | WebSocket + AudioWorklet | LiveKit WebRTC |
| AEC | worklet RMS ゲーティング (workaround) | WebRTC AEC3 (根本解決) |
| VAD | Gemini server-side VAD | Pipecat SileroVAD + Gemini |
| LLM service | ADK Runner + LiveRequestQueue | GeminiMultimodalLiveLLMService |
| barge-in | RMS 閾値 + interrupt 送信 | Pipecat built-in (SmartTurn) |
| 再接続 | hub_manager_loop 自前実装 | Pipecat 内蔵 |
| ブラウザ | AudioWorklet + カスタム PCM | LiveKit JS SDK のみ |
| OTP 認証 | WS 最初のメッセージで検証 | HTTP POST /auth → LiveKit token 発行 |
| インフラ | Pipecat server のみ | LiveKit SFU + Pipecat server |

---

## 9. 既知の懸念点 / 未確認事項

| 懸念 | 詳細 | 対処方針 |
|---|---|---|
| **LiveKit ARM64 動作** | Pi5 での動作は公式 Docker イメージが ARM64 対応だが未実証 | 初期 spike で `docker pull livekit/livekit-server` して動作確認 |
| **Pipecat `GeminiMultimodalLiveLLMService` の名称** | バージョンにより `GeminiLiveLLMService` の場合あり | `pip show pipecat-ai` で確認して import を調整 |
| **Pipecat Gemini issue #2791** | Gemini 2.5 での割り込み中断バグ | `gemini-2.0-flash-live-001` を使用してバグを回避 |
| **pikon inject API** | `pipeline_task.queue_frame(TextFrame)` の exact API | Pipecat docs/examples で確認して実装 |
| **1 セッション制約と LiveKit room** | OTP 再発行 → 新 room 生成 → 旧セッション残留の可能性 | session_manager で旧 pipeline を graceful stop してから新 room を起動 |

---

## 10. 実装順序

1. **[S1]** `server/auth.py` — v1 から流用 (変更なし)
2. **[S2]** `server/command_listener.py` — v1 から流用 + #11 修正取込
3. **[S3]** `server/session_manager.py` — セッション排他制御
4. **[S4]** `server/hub_tools.py` — Pipecat 用ツール定義
5. **[S5]** `server/hub_listener.py` — agent-hub inbox → pipeline frame inject
6. **[S6]** `server/pipeline.py` — Pipecat パイプライン factory
7. **[S7]** `server/main.py` — aiohttp エンドポイント + 起動
8. **[S8]** `client/index.html` — LiveKit JS SDK UI
9. **[S9]** `livekit/livekit.yaml` + `docker-compose.yml`
10. **[S10]** `Dockerfile` + `requirements.txt`
