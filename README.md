# qq-dsh-bot

A small bridge from NapCat QQ groups to [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). An explicit @mention in a trusted group starts a headless DSH session, with recent conversation context, Python and web tools, searchable chat history, image input, and group-file uploads. The final answer is posted as a short plain-text reply.

The default model is `deepseek-flash` (DeepSeek V4.1 Flash). DSH runs inside the bot container; no separate agent service or Docker socket is needed. The Dockerfile builds from a public Node base and installs the pinned Harness version directly.

## Requirements

- A Linux server with Docker Engine and Docker Compose v2. The container has been tested on an x86-64 Rocky Linux host; other architectures have not been verified.
- A running NapCat container, logged into the QQ account that will act as the bot.
- An official DeepSeek API key and access to its API from the server.
- A Docker network shared by NapCat and this bot. The default is `napcat_default`.

The Compose file allows up to 2 CPUs and 2 GiB RAM for the bot. Building the image requires access to Debian package servers, npm and PyPI.

## Deploy

### 1. Clone and configure

```sh
git clone https://github.com/GuruGuru-SEU/qq-dsh-bot.git
cd qq-dsh-bot
cp .env.example .env
chmod 600 .env
mkdir -p config state
cp config.example.json config/config.json
```

Edit `.env`:

- `DEEPSEEK_API_KEY`: your official DeepSeek key.
- `ONEBOT_TOKEN`: a random token of at least 32 characters. Generate one with `python3 -c 'import secrets; print(secrets.token_hex(32))'`. Use the same token in NapCat.
- `NAPCAT_NETWORK`: the Docker network your NapCat container already uses.

Replace the example IDs in `config/config.json` with the bot's QQ account and the groups allowed to use it:

```json
{
  "bot_qq": 1234567890,
  "trusted_groups": [123456789, 987654321],
  "history_sync": true
}
```

IDs are integers, not strings. Adding/removing trusted groups takes effect on the next message without a restart. Changing `bot_qq` requires a restart and matching NapCat login/configuration. Invalid configuration stops processing group messages.

### 2. Prepare storage and the shared network

The bot runs as UID/GID `1000:1000`. Its state directory must be writable by that user; configuration must be readable:

```sh
sudo chown -R 1000:1000 state
sudo chmod 700 state
chmod 755 config dsh-config
chmod 644 config/config.json dsh-config/agent.patch.yml
docker network ls
```

If NapCat does not already have a suitable shared network, create one and connect its container, then set `NAPCAT_NETWORK=qq-bot` in `.env`:

```sh
docker network create qq-bot
docker network connect qq-bot YOUR_NAPCAT_CONTAINER
```

If you use a separate Compose project for NapCat, also declare that network in its Compose configuration so the attachment survives container recreation. SELinux bind-mount labels are included for hosts such as Rocky Linux.

### 3. Configure NapCat

In NapCat's OneBot configuration, add an enabled **reverse WebSocket client**:

| Setting | Value |
| --- | --- |
| URL | `ws://qq-dsh-bot:8080/onebot` |
| Token | The exact `ONEBOT_TOKEN` from `.env` |
| Message format | Array |

NapCat must send the `Authorization: Bearer <token>` and `X-Self-ID` headers. The bridge additionally verifies the logged-in account with `get_login_info`. Enable reconnects in NapCat if configurable. This URL is for the Docker network, not a public endpoint; the bot publishes no host ports. Manage NapCat's login/WebUI separately.

### 4. Start and verify

```sh
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail=50
docker compose exec bot curl -fsS http://127.0.0.1:8080/health
```

Health becomes ready after the authenticated NapCat connection succeeds. In a trusted group, @mention the bot and ask a question. Send an image with the mention, or reply to an image while @mentioning the bot, to test vision.

The agent profile is in `dsh-config/agent.patch.yml`. Restart the bot after changing `.env`, Compose, or code. The profile is loaded by each fresh DSH invocation. To change the Harness version, edit the Dockerfile's `DSH_VERSION` default or pass `--build-arg DSH_VERSION=...` to `docker compose build` and verify compatibility before deploying.

## Behavior

Only explicit OneBot `at` segments addressed to the bot trigger work. Private messages, untrusted groups, @all, text resembling an @mention, and the bot's own messages do not trigger tasks. Live messages more than five minutes old are archived without triggering. Trigger deduplication persists for seven days.

Each request starts `dsh --profile headless --json` with a fresh session and the previous 30 saved messages. Each group has its own persistent workspace and DSH home. The agent can use Python, web search/fetch, the read-only history helper, and native `read_image`. The bridge posts the final answer as a quoted reply; DSH is instructed not to send QQ messages itself.

Replies default to 1–3 short sentences, usually under 300 Chinese characters, in the requester's language. Markdown is converted to plain text, preserving URLs and code contents. Replies have a hard 1800-character cap.

One request runs at a time, with up to five queued requests. Further mentions are ignored while the queue is full. Image preparation is limited to 60 seconds, followed by a five-minute DSH deadline. Timeouts and failures receive a short error reply. Restarting loses in-memory queued work; ambiguous send failures are not retried automatically to avoid duplicates.

## Persistent history and images

```text
state/
  seen.sqlite3
  groups/<group-id>/
    history.sqlite3
    media/<sha256>.<ext>
    workspace/
    home/
```

Every received trusted-group message and successful bot reply is saved with its message ID, sender, timestamp, full text and original segments. Recent prompt excerpts are limited to 3000 characters, while the archive retains full received text. Removed groups stop triggering and recording, but their existing archives remain on disk.

DSH must query the archive for questions about past conversation. From its current group workspace it can run:

```sh
/opt/bot-venv/bin/python /opt/bot/history.py --date 2026-10-01
/opt/bot-venv/bin/python /opt/bot/history.py --start 2026-10-01 --end 2026-10-07 --contains 国庆
/opt/bot-venv/bin/python /opt/bot/history.py --user 123456 --limit 100 --offset 0
/opt/bot-venv/bin/python /opt/bot/history.py --message-id 123
```

Dates use Asia/Shanghai. `10.1` uses the current year; ISO dates are unambiguous. Date ranges include both endpoint days. Results include archive coverage, total matches, truncation flags, image paths and `next_offset`. The helper binds to the current group's workspace and opens SQLite read-only. DELETE journaling lets read-only queries work without creating WAL sidecar files; existing WAL archives migrate when opened by the bridge.

On connection, the bridge attempts to import up to 30 days of available NapCat history, capped at 100 pages of 100 messages. Subsequent passes run every five minutes to repair recent gaps. This does not guarantee a complete archive. `incremental_caught_up` describes the sync cutoff; `pagination_stalled` means NapCat repeated a cursor. Neither proves that older messages do not exist. Set `history_sync` to `false` to disable imports while retaining live recording.

A background worker downloads trusted-group images even without @mentions and resumes pending work after restart. Images are saved by content hash within each group. Missing/expired URLs are refreshed with NapCat `get_msg`, with group and message identity checks. Triggering messages, recent context, quoted messages outside recent context, and history results expose saved paths to DSH's native `read_image` tool. Unavailable images are marked explicitly, never described from a placeholder. Failed downloads retry hourly and can retry sooner for an active request.

Downloads accept QQ media endpoints only, validate public DNS addresses and redirect targets, time out after 20 seconds, and cap each image at 16 MiB. PNG, JPEG, GIF and WebP are validated, with large pixel counts rejected. Animated-image interpretation depends on Harness/model support. Old or deleted QQ media may be unrecoverable. Other non-text message types remain placeholders.

Messages and cached images have no automatic expiry. Monitor disk usage and back up the complete state directory.

## Upload files to the group

Ask the bot to deliver a file, for example: “下载这篇论文 PDF，上传到群文件” with its URL. DSH downloads or creates the file inside the current group workspace, then uses its bash tool to run:

```sh
/opt/bot-venv/bin/python /opt/bot/upload.py ./paper.pdf --name paper.pdf
```

The helper uploads to the triggering group only, with a limit of 20 MiB per file and three attempts per request. It is available only while DSH is processing that request. The bridge writes a private, temporary `.upload-request.json` beside the group's workspace and removes it when the run ends; the helper reads this grant because DSH scrubs credential-shaped environment variables from shell tools. Local files outside the group's workspace, symlink escapes, empty files and invalid filenames are rejected. No destination group or OneBot credentials are exposed to DSH.

The bridge snapshots the bytes and serves them temporarily through an unguessable URL on the existing Docker network. NapCat fetches those bytes and calls its normal `upload_group_file` action. No new ports, NapCat filesystem mounts or NapCat configuration changes are required. The URL uses the Compose network alias `qq-dsh-bot:8080`, so NapCat must be on the shared network described above.

The helper returns JSON with `status=uploaded` and a QQ file ID only after NapCat confirms it. A rejected upload returns `failed`; missing acknowledgement or file ID returns `unknown`, because the file may already have arrived. DSH must not claim success or blindly retry in those cases. Identical content/name within a triggering request reuses the persisted result, including failures and uncertain outcomes, to avoid duplicate sends. Audit results are stored in `state/seen.sqlite3`; a restart marks in-flight uploads as uncertain. QQ's actual account/group restrictions and group storage limits still apply.

The default tests simulate NapCat and never upload to QQ. An explicit live smoke test is also provided; it uses DSH to create and upload one `hello.txt` and checks the matching file in the group root listing, then downloads it to verify its contents. Run it only when you want a real upload to the chosen trusted group. It temporarily runs the bridge in place of the normal container; wait for the normal bot to be idle first:

```sh
docker compose stop bot
docker compose run --rm --use-aliases --entrypoint /opt/bot-venv/bin/python \
  bot /opt/bot/smoke_upload.py --group YOUR_TRUSTED_GROUP_ID
docker compose up -d
```

Restart the normal bot even if the test fails. If its result is uncertain, inspect QQ group files before repeating the test. No extra chat reply is sent for the smoke test. To verify an already acknowledged upload without sending another file, add `--verify-file-id ID` using the ID recorded by the helper.

## Operations and migration

```sh
docker compose logs --tail=50
docker compose restart bot
docker compose stop bot
docker compose build
docker compose up -d
```

For a consistent backup, stop the bot, copy `state/`, `config/config.json`, `.env`, and any customized agent profile, then restart it. Keep backups private. To move servers, clone this repository, restore those files, restore UID/GID `1000:1000` ownership on `state/`, configure the new NapCat login/network, and start the bot. Avoid running two instances for the same bot account during the move.

Do not copy a live SQLite file directly while writers are active. Use SQLite's backup API if downtime is not possible, and include cached media separately. Never use `immutable=1` when querying the live archive.

The `.gitignore` excludes credentials, configuration containing real account/group IDs, state, logs and backups. The Docker build context includes only explicitly allowed source files. Review new files before publishing: chat content and images belong in private runtime storage, not Git.

## Execution boundary

The container has a read-only root filesystem, no capabilities, no host ports, no Docker socket, and CPU/memory/process limits. DSH uses workspace-write permissions, no interactive approvals, and disabled telemetry/session uploads. The OneBot token is removed from the DSH subprocess environment; the DeepSeek key remains available for API access. Group directories separate ordinary state but are not a hostile multi-tenant security boundary. Trust only groups that are allowed to use this single-owner tool runtime. Group context and relevant images are sent to DeepSeek, and DSH session data remains in the private state directory.

## Tests

The tests use synthetic messages and a simulated OneBot server. They do not call DeepSeek or send anything to QQ.

```sh
docker build -t qq-dsh-bot:test .
docker run --rm --entrypoint /opt/bot-venv/bin/python \
  qq-dsh-bot:test -m unittest -v test_bot test_history test_media test_upload
```

GitHub Actions builds the standalone image, runs the tests as the unprivileged runtime user, and validates Compose configuration.
