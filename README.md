# qq-dsh-bot

A small bridge from SnowLuma or NapCat QQ groups to [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness). An explicit @mention in a trusted group starts a headless DSH session, with recent conversation context, Python and web tools, searchable chat history, image input and inline image replies, group-file uploads, progress updates, persistent message schedules, and approved background DSH jobs. The final answer is posted as a short plain-text reply.

The default model is `deepseek-flash` (DeepSeek V4.1 Flash). DSH runs inside the bot container; no separate agent service or Docker socket is needed. The Dockerfile builds from a public Node base and installs the pinned Harness version directly.

## Requirements

- A Linux server with Docker Engine and Docker Compose v2. The container has been tested on an x86-64 Rocky Linux host; other architectures have not been verified.
- A running SnowLuma or NapCat container, logged into the QQ account that will act as the bot. SnowLuma is the example configuration default.
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
- `ONEBOT_NETWORK`: the Docker network shared with the gateway (`napcat_default` remains the compatibility default; `NAPCAT_NETWORK` is accepted as a fallback).

Replace the example IDs in `config/config.json` with the bot's QQ account and the groups allowed to use it:

```json
{
  "gateway": "snowluma",
  "bot_qq": 1234567890,
  "trusted_groups": [123456789, 987654321],
  "history_sync": true,
  "history_lookback_days": 365
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

### 3. Configure the QQ gateway

In the gateway's OneBot configuration, add an enabled **reverse WebSocket client**:

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

## SnowLuma deployment and migration

The bridge supports SnowLuma v1.14.22. Set `"gateway": "snowluma"` in
`config/config.json`; omitting the field preserves legacy NapCat behavior.
Changing the gateway requires restarting the bot. Both use the same reverse
WebSocket URL, token and array message format. SnowLuma's role must be `Universal`.

`deploy/snowluma.compose.yaml` is a separate gateway Compose project. Copy it to
its own deployment directory as `compose.yaml`; attach it to the same
`ONEBOT_NETWORK` as the bot. The supplied configuration runs the official
SnowLuma image (not a modified distribution), disables telemetry, and accepts
its bundled EULA/privacy terms for unattended startup. Review those terms
before deploying. Pin `SNOWLUMA_IMAGE` to the pulled digest for reproducibility.
The image needs `SYS_PTRACE`, `seccomp=unconfined` and a 1 GiB shared-memory mount.
It persists gateway data separately from QQ configuration and local data.

WebUI: port 5099. The password-protected noVNC desktop is bound to localhost
on port 6081; for QR login use `ssh -L 6081:127.0.0.1:6081 YOUR_SERVER`, then
open `http://localhost:6081/`. Initial WebUI and desktop passwords are generated
by the official image. Do not commit credentials or copy NapCat configuration
files into SnowLuma. Configure a reverse WebSocket client in SnowLuma and log
in to the bot QQ account. Never run both gateways against this account at once.

History uses SnowLuma's `message_id` cursor with `reverse_order=true` for older
pages. SnowLuma IDs are stored as `snowluma:<id>` in archives, requests and helper
arguments; the bridge strips the prefix only when calling SnowLuma. Existing
NapCat rows remain readable and distinct. Old gateway IDs are never used to
fetch/transcribe/react through the new gateway. A resumed task referencing an
old message sends its answer without a quote. Cached images remain readable;
uncached old images may need to be resent. Backfill can contain the same real
message under both gateways' IDs; do not interpret these as distinct statements.

### Concorde rollback

Migration branch: `migration/snowluma`. Original source is preserved on
`rollback/napcat-20261013`. On the server, the pre-migration bot directory was
archived at `~/services/qq-dsh-bot/backups/pre-snowluma-20261013/bot.tar.gz`.
The stopped NapCat container retains its volumes and has restart disabled;
its writable image layer was saved as `napcat:rollback-20261013`. The previous
bot image is `qq-dsh-bot:rollback-napcat-20261013`.

For a normal rollback, first check `/health` and let active work finish, then run:

```sh
bash ~/services/qq-dsh-bot/deploy/rollback-napcat.sh
```

This stops SnowLuma, changes the current bridge to NapCat, and starts the preserved
NapCat container. It keeps all history/memories/skills created during the trial.
The preserved original image/source and archive are additional emergency recovery
points; do not overwrite live `state/` with the old snapshot during a normal rollback.
A restored NapCat session may still require QR login at port 6099.

## Behavior

Only explicit OneBot `at` segments addressed to the bot trigger work. Private messages, untrusted groups, @all, text resembling an @mention, and the bot's own messages do not trigger tasks. Live messages more than five minutes old are archived without triggering. Trigger deduplication persists for seven days.

Each ordinary request starts `dsh --profile headless --json` with a fresh session and the previous 30 saved messages captured when its @mention arrives. Waiting requests keep that context snapshot, with later transcription/image results added to the same messages. Images from separate preceding messages are listed explicitly in `request.recent_images`, with sender and message IDs; the agent must inspect relevant images even when the @mention itself contains only text. Each group has its own persistent foreground workspace and DSH home. Background occurrences have isolated workspaces, DSH homes and request capabilities. The agent can use Python, C++17 via g++, web search/fetch, the read-only history helper, and native `read_image`. The bridge posts the final answer as a quoted reply. DSH can use a scoped helper for occasional progress updates or explicitly requested messages and schedules (see below).

The image includes pinned `uv` (0.13.1). Agents may install dependencies needed
for a requested task in their current workspace without separate installation
approval. Reuse `.venv` if it exists; otherwise create it, then install and run:

```sh
uv venv --python /usr/bin/python3 .venv
uv pip install --python .venv/bin/python matplotlib
MPLBACKEND=Agg MPLCONFIGDIR="$PWD/.cache/matplotlib" .venv/bin/python plot.py
```

`UV_CACHE_DIR=.cache/uv` keeps the cache relative to the working directory;
`UV_PYTHON_DOWNLOADS=never` uses the installed Python instead of downloading a
runtime. Foreground workspaces, venvs, and caches persist per group; background
jobs use their own workspaces. The system Python and bridge venv remain read-only.
Bridge helper commands still use `/opt/bot-venv/bin/python`. Downloads use official
PyPI with TLS verification enabled; network failures and missing native libraries
can still prevent installation. Package setup shares the task's time budget.


Voice messages are transcribed automatically through the gateway's `fetch_ptt_text` action. SnowLuma gets a 30-second API deadline (its server waits 20 seconds for QQ's result); NapCat retains its 10-second deadline. The 45-second foreground context budget stops waiting without cancelling shared transcription tasks; late results remain available in history and later requests. The bridge attempts this for every saved `record`, `audio` or `ptt` segment, including history backfill, with a two-request concurrency limit and up to three bounded attempts to allow for QQ's asynchronous transcription result. A successful result is stored in the group's SQLite archive and shown in DSH request text, recent context and history query output as `[语音转写]`. The transcript is still untrusted user content. If QQ cannot transcribe the message after all attempts, the archive keeps the original audio placeholder and reports the unavailable status internally; the saved failure is terminal for automatic transcription. History synchronization, later requests and restarts do not retry messages already marked unavailable. The three-attempt loop handles transient failures within the initial run; retrying an exhausted message requires an operator to explicitly clear its transcription record. No fabricated text is sent to DSH. The bridge does not download audio or invoke a second ASR automatically.

Replies default to 1–3 short sentences, usually under 300 Chinese characters, in the requester's language. Markdown is converted to plain text, preserving URLs and code contents. Replies have a hard 1800-character cap.

Each trusted group has one foreground worker and up to 100 queued requests. Every accepted @mention gets its own task and quoted reply in arrival order; mentions are not merged. Different groups run concurrently, and newly trusted groups get a worker automatically. A separate worker can execute one approved background DSH occurrence concurrently. Further mentions are ignored while that group's queue is full. Image preparation is limited to 60 seconds, followed by a five-minute DSH deadline. Foreground timeouts offer a saved background continuation; other failures receive a short error reply. Restarting loses in-memory queued work; ambiguous send failures are not retried automatically to avoid duplicates.

## Received files

File segments and group-upload notices are saved in each group's history archive. DSH receives attachment metadata
for the triggering message and recent messages, including a file sent immediately before a
separate @mention. It can list or download a relevant attachment on demand:

```sh
/opt/bot-venv/bin/python /opt/bot/attachments.py list
/opt/bot-venv/bin/python /opt/bot/attachments.py list --message-id MESSAGE_ID
/opt/bot-venv/bin/python /opt/bot/attachments.py download MESSAGE_ID --position 0
```

Downloads are scoped to the current group, capped at 32 MiB per file and 512 MiB of
per-group cache, and stored under `state/groups/<group-id>/attachments/`. The helper uses
NapCat's `get_group_file_url`; it does not expose NapCat credentials or accept arbitrary
group IDs. Files are untrusted input and are never executed automatically.

## Received-message reaction

Accepted @mentions in trusted groups get an 👀 reaction immediately, including queued requests and `/status`, `/continue` and `/cancel` commands. The bridge calls NapCat's [`set_msg_emoji_like`](https://napcat.apifox.cn/226659104e0) with Unicode emoji ID `128064` and `set=true`, then `set=false` after the quoted reply is acknowledged. Error replies and timeout continuation offers also clear the reaction. Background progress/final messages do not add reactions to unrelated messages.

Reaction calls run outside the WebSocket receive loop, with a three-second timeout and no model involvement. Adding the reaction runs alongside normal processing; fast replies wait for that attempt to finish so add/remove cannot be sent in reverse order. Reaction errors are logged and never prevent an answer. Ignored, duplicate, stale, untrusted and queue-overflow messages are not marked received. Failed/unconfirmed replies keep the receipt. Reactions are best-effort: a disconnection, failed removal or container restart can leave an old reaction visible; pending reaction operations are not persisted or retried automatically.

## Status command

In a trusted group, @mention the bot with `/status` anywhere in the message text (case-sensitive), for example `@bot 请检查 /status 现在怎么样`. The bridge replies directly with a plain-text status snapshot, without starting DSH, calling an LLM, preparing images for a model, or executing other instructions in that message. The usual real @mention, group trust, freshness and deduplication checks still apply. Quoted messages and attachment metadata do not trigger the command.

Status reports the bridge uptime, verified NapCat connection, the current group's foreground queue and worker health, container memory, free space on the state filesystem, and the current group's background jobs, active schedules and last finished foreground diagnostics. It never reports other groups' identities, task contents, raw logs or credentials. Metrics unavailable on the host are marked unavailable.

A dedicated status worker replies even when the foreground worker is busy or its queue is full. Its own queue accepts up to 20 pending status requests; excess status requests are dropped without falling back to DSH. Replies use the normal quoted-message delivery and archive path. Connection or delivery failures are logged and never retried blindly. Restarting clears pending status requests.

## Group memory

Each group has persistent memory under `state/groups/<group-id>/memory/`, independent
of foreground workspaces and background runs. `MEMORY.md` holds essential group rules
and a topic index; `topics/*.md` holds details, including personal preferences in
`members.md`. A private `journal.jsonl` records changes with before/after values,
QQ author, source message ID and time. `memory.sqlite3` is the authoritative store:
entries, audit records and retry results commit together. Markdown and journal views
are rebuilt on startup and after changes; edit memory through the helper, not these
generated files. The whole directory is persistent runtime state and is not committed
to Git. Back it up with the rest of `state/`.

Examples (with an @mention):

- “记住：本群以后不要说‘xxx’。” saves an essential group rule.
- “记住我喜欢用 C++ 解算法题。” saves a preference for that QQ user in this group.
- “你记住了本群的哪些规则？” recalls entries.
- “把我的语言偏好改成 Python。” updates the existing preference.
- “忘掉我喜欢 C++ 这条。” removes it from active memory. The private audit journal
  and original chat archive still retain the prior text; this is not a data-erasure command.

Any member can manage their own preferences. Group rules, group facts, and changes to
someone else's entries require live QQ owner/admin status or an explicit per-group
`memory_managers` entry in `config/config.json`, for example:

```json
"memory_managers": {"529987256": [1234567890]}
```

Use actual authorized QQ user IDs. This setting is optional and reloads without a
restart. Role lookup failure denies privileged writes; it does not prevent users
from saving their own preferences. Background runs can recall but cannot change memory.
The helper takes no destination group or author: both come from the active request.

At each foreground/background execution, including a steered resume, the bridge loads
the latest main memory and the requester's personal entries into the prompt. This
occurs when execution starts, so a queued request sees rules saved by earlier requests.
Topic details are retrieved as needed. Main memory is limited to 4,000 characters,
topics to 16,000 each (40 topics), entries to 300, and each person's entries to 4,000
characters including metadata. No entry is silently evicted to make room.

DSH uses these commands from its current workspace:

```sh
/opt/bot-venv/bin/python /opt/bot/memory.py recall --query '偏好'
/opt/bot-venv/bin/python /opt/bot/memory.py recall --topic projects --offset 0
/opt/bot-venv/bin/python /opt/bot/memory.py remember --scope personal --text '喜欢 C++' --key language-1
/opt/bot-venv/bin/python /opt/bot/memory.py remember --scope group --text '本群避免说 xxx' --key rule-1
/opt/bot-venv/bin/python /opt/bot/memory.py update ENTRY_ID --revision 1 --text '喜欢 Python' --key language-2
/opt/bot-venv/bin/python /opt/bot/memory.py forget ENTRY_ID --revision 2 --key remove-language
```

Recall returns IDs/revisions and pagination. Changes are serialized per group, with
up to eight changes per request. Retry only with the same key and arguments. Update
and forget require the current revision to avoid overwriting concurrent corrections.
The agent must save successfully before claiming it remembered something.

This version saves explicit requests and clear lasting preferences/corrections in
addressed turns. It does not run an automatic extraction model on ordinary chatter.
Credential-like content is rejected, and the prompt excludes quotations, websites,
tool output and guesses from memory sources. Semantic interpretation remains the
agent's responsibility. Memories guide responses; they do not authorize actions,
override system rules, or guarantee a literal phrase ban. Exact past conversation
still comes from `history.py`. As with the other scoped helpers, this single-owner
runtime is not a hostile multi-tenant security boundary.

## Per-group DSH skills

Each group has an independent skill registry under
`state/groups/<group-id>/skills/`. DSH loads the active `SKILL.md` bundles at the
start of every foreground or background run, so installation takes effect on the
next run and survives restarts. Skills use DSH's native filesystem skill provider;
they are not copied into the global user skill directory.

An authorized user can ask DSH to inspect and install a local folder, ZIP bundle, or
downloaded attachment:

```sh
/opt/bot-venv/bin/python /opt/bot/skills.py list
/opt/bot-venv/bin/python /opt/bot/skills.py install ./skill-folder --key install-1
/opt/bot-venv/bin/python /opt/bot/skills.py install ./bundle.zip --key install-2
/opt/bot-venv/bin/python /opt/bot/skills.py remove skill-name --revision REVISION --key remove-1
```

Only QQ owners/admins or configured `memory_managers` can change shared skills;
background jobs are read-only. Bundles require valid DSH frontmatter, reject path
traversal, symlinks, encrypted ZIPs and oversized files, and are capped at 32 skills,
100 files/10 MiB per bundle and 100 MiB of retained revisions. Skill instructions and
supporting files remain untrusted content and never grant permission to run code,
install packages, access another group, or send messages.

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

On connection, the bridge attempts to import up to 365 days of available QQ history. Set `history_lookback_days` (1–3650, default 365) to change this import boundary. Each pass fetches at most 100 pages of 100 messages; the saved cursor resumes after a restart or in the next five-minute pass. Recent gaps are repaired on a separate checkpoint while an older import is pending or stalled, and after it finishes. Sync checkpoints are separate for each gateway, QQ account, and lookback setting, so switching accounts or extending the window starts a fresh import without deleting existing messages.

Archives remain in `state/groups/<group_id>/history.sqlite3` and are shared across accounts. Switching back to the original bot keeps all imported history, including messages that only the temporary account could fetch. There is no age-based deletion: the lookback setting limits fetching, not retention. Preserve the `state/` volume when redeploying or rolling back; account rollback must change config only.

QQ/SnowLuma may expose less than the requested period, so this does not guarantee a complete archive. `incremental_caught_up` describes the sync cutoff; `pagination_stalled` means the gateway repeated a cursor. Neither proves that older messages do not exist. `history.py` reports the configured lookback and current sync account with archive coverage. Set `history_sync` to `false` to disable imports while retaining live recording.

A background worker downloads trusted-group images even without @mentions and resumes pending work after restart. Images are saved by content hash within each group. Missing/expired URLs are refreshed with NapCat `get_msg`, with group and message identity checks. Triggering messages, recent context, quoted messages outside recent context, and history results expose saved paths to DSH's native `read_image` tool. Unavailable images are marked explicitly, never described from a placeholder. Failed downloads retry hourly and can retry sooner for an active request.

Downloads accept QQ media endpoints only, validate public DNS addresses and redirect targets, time out after 20 seconds, and cap each image at 16 MiB. PNG, JPEG, GIF and WebP are validated, with large pixel counts rejected. Animated-image interpretation depends on Harness/model support. Old or deleted QQ media may be unrecoverable. Other non-text message types remain placeholders.

### History analysis and exports

These generic tools are available to any installed group skill; the bot has no built-in Distilly dependency.
All commands use `/opt/bot-venv/bin/python /opt/bot/history.py` inside the current group's workspace:

```sh
# Recorded members/aliases, not a live membership roster; clarify duplicate nicknames.
history.py --members --name '昵称' --limit 100 --offset 0
history.py --stats --user 123456 --start 2026-10-01 --end 2026-10-13
# Recent-first pages; the default remains oldest-first for compatibility.
history.py --user 123456 --order desc --limit 100
# Seeded sample balanced across Shanghai calendar days, with conversation context.
history.py --user 123456 --start 2026-10-01 --end 2026-10-13 --sample 120 --seed profile-v1 --context 2 --output ./evidence.jsonl
history.py --message-id snowluma:123 --context 3
```

Use the full interpreter/helper path above in actual shell commands. `--members` and `--stats` accept the same
sender/date/search/exclusion filters and bounded pagination. `--name` searches historical aliases and may
return several QQ IDs. `--stats` reports filtered message count, unique senders, active days, first/last timestamps
and newest-first daily counts. The separate `coverage` object always describes the whole group archive.
`--contains` searches message text and ready voice transcripts. Repeat `--exclude-user QQ_ID` to omit senders
from the selection; surrounding context still includes other speakers when needed.

`--sample N` selects up to 200 messages using seeded daily round-robin: one message per day before second
messages, with seeded day selection if there are more days than slots. It cannot be combined with `--offset`.
It is reproducible for an unchanged archive and seed, not an exhaustive report or a frequency estimate.
`--context 0..5` adds adjacent records each side and recorded reply targets, separately attributed in
`context_messages` and linked by `context_ids`; context may lie outside query dates/senders. `reply_to`
retains original IDs even without context. Missing reply records and truncated context are flagged.

Pages retain the 60,000-character combined text budget; ordinary message text is capped at 2000 characters,
single-ID lookups at 16,000. Follow `next_offset` for ordinary pages and inspect `text_truncated`,
`selection.text_budget_truncated`, and `context_truncated`. Sampling does not return a continuation cursor.

`--output ./new-file.jsonl` writes exactly this bounded result to a new file in the current workspace,
without overwriting existing files or allowing paths outside the workspace. `--format text` produces a readable
alternative. JSONL starts with coverage/selection metadata and group ID, followed by selected/context records
with QQ IDs, message IDs and timestamps. The CLI reports an export summary instead of the entire dataset.
Exports are limited to 4 MiB, include archive limitations, and do not download images/audio/files. Analyze
chunks or collect additional bounded queries when needed; never label a sample a complete group history.

Messages and cached images have no automatic expiry. Monitor disk usage and back up the complete state directory.

## Inline image replies

To answer a request with a picture, DSH uses:

```bash
/opt/bot-venv/bin/python /opt/bot/send_image.py ./plot.png --caption 'The requested plot'
```

PNG, JPEG, WebP and GIF files are validated before sending (20 MiB maximum, three image attempts per request, caption up to 1000 characters). Files must be inside the active request's group workspace, including an approved background job's isolated workspace. The destination is fixed by the shared request capability. Pictures are sent with `send_group_msg` and an `image` segment through a temporary internal transfer URL; no group-file upload or shared QQ filesystem mount is required. Use `upload.py` instead when the user explicitly requests delivery to group files.

Only `status=sent` confirms delivery and includes the gateway message ID. Identical bytes/caption reuse a durable result within the same request, even if the filename changes. Unknown acknowledgements are never blindly retried; restarts mark interrupted sends unknown. Confirmed images and captions enter group history, with image bytes retained in the group's media directory for future `read_image` calls. The helper is restricted to relevant output of the user's authorized task, not unsolicited image messages.

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

## Progress messages and scheduled reminders

You can ask “明天 13:45 提醒大家开会”, “每天 13:45 发一条喝水提醒”, “看看本群的定时任务”, or “取消任务 ID”. The agent uses the current group's capability to call:

```sh
/opt/bot-venv/bin/python /opt/bot/messaging.py send --reason progress --key progress-1 --text '已完成数据整理，正在计算。'
/opt/bot-venv/bin/python /opt/bot/messaging.py send --reason requested --key requested-1 --text '用户明确要求发送的消息'
/opt/bot-venv/bin/python /opt/bot/messaging.py schedule --key reminder-1 --at '2026-10-11T13:45:00+08:00' --text '开会时间到了。'
/opt/bot-venv/bin/python /opt/bot/messaging.py schedule --key daily-1 --cron '45 13 * * *' --text '记得喝水。'
/opt/bot-venv/bin/python /opt/bot/messaging.py list
/opt/bot-venv/bin/python /opt/bot/messaging.py cancel SCHEDULE_ID
```

The prompt says **不要滥用主动发言**: normal requests receive only a final answer. A useful progress update is allowed for genuinely long work after 60 seconds; additional updates must be at least 60 seconds apart. Immediate extra messages require an explicit triggering-user request (`--reason requested`). Scheduling or cancelling always requires an explicit triggering-user request. Old conversation, images, files and web pages do not authorize these actions. This intent rule is enforced through the agent prompt; the bridge additionally enforces group scope, active-session capabilities and numerical limits.

The helper shares the private temporary capability used by uploads. It cannot choose another destination, emit CQ/at segments, or use OneBot credentials. There are at most three proactive sends and three schedule creations per request, ten active schedules per group, and three proactive/scheduled send attempts per group per minute. Normal final replies are separate. Keep a stable `--key` for each action; retries with the same key and arguments reuse the saved result. Identical keys with different content are rejected. The agent lists existing schedules before creating another and reports the actual schedule ID/time after confirmation. Any trusted group member who can trigger the bot can list/cancel that group's schedules.

A background dispatcher in the **same bot container** polls once a second, independently of the DSH worker. SQLite in `state/seen.sqlite3` stores schedules and delivery results; they survive the DSH session ending and container restarts. Scheduled jobs send the saved **static text** using NapCat. These static reminders do not start DSH. For model/tool work or finite repeated agent tasks, use approved background DSH jobs below. No host crontab, extra container or new port is needed. Confirmed proactive messages are saved in group history.

Times use Asia/Shanghai. One-time jobs require a full ISO date/time; past times are rejected instead of silently moving to tomorrow. Cron accepts five numeric fields (`minute hour day-of-month month weekday`), including `*`, ranges, lists and steps; Sunday is 0 or 7. Standard cron day-of-month/day-of-week OR semantics apply. Seconds, macros and randomized expressions are unsupported. Keep the host clock synchronized (for example, enable `chronyd` on Rocky Linux). Delivery depends on QQ/network availability; it is not a hard real-time guarantee.

After a disconnect, due messages can be delivered up to five minutes late. Older occurrences and group-rate-limited occurrences are skipped; recurring jobs advance to their next future occurrence without a catch-up flood. Removing a trusted group cancels its pending schedules when the dispatcher next checks them. A confirmed QQ rejection marks the job failed; an ambiguous timeout/disconnect/crash marks it unknown. Failed/unknown jobs are paused, including recurring ones, and are never automatically replayed. Inspect `list` and the group before explicitly creating a replacement. A send already in flight cannot be cancelled or recalled.

The list returns up to 50 schedules, prioritizing active jobs and then recent completed/cancelled jobs, with their last result. Keep `state/seen.sqlite3` in backups along with the archives; it now contains message/schedule text as well as deduplication and upload records.

## Background DSH jobs and user confirmation

The bot can ask clarification questions when scope, timing or desired output is unclear. For lengthy work it explains the proposed background execution and asks permission. **A proposal does not run.** Only the same user, in a later @mention clearly agreeing to the proposal, can launch it. The prompt instructs DSH to clarify ambiguous confirmations and scope changes; the bridge enforces a persisted proposal, requester identity, group scope and a different triggering message ID. Semantic interpretation of “yes” remains the agent's responsibility. Ordinary short questions need no extra approval.

Example group conversation:

```text
User: @bot 接下来3分钟，每分钟往群里发一条信息。
Bot: 确认后我会在后台每分钟发一条，共3条，不影响回答其他消息。要开始吗？请 @ 我回复“开始”。
User: @bot 开始。
Bot: 已启动后台任务 ID，每分钟一条，共3条。
```

The first occurrence is due one minute after confirmation, followed by the second and third at two and three minutes. Each starts a fresh DSH session and posts its generated final answer. Other @mentions continue through the usual sequential foreground queue while background work runs. Model latency can delay actual delivery; for exact fixed text, use static reminders instead.

The foreground agent uses:

```sh
/opt/bot-venv/bin/python /opt/bot/background.py propose --key three-updates --every 60 --count 3 --prompt 'Produce one brief message for this occurrence'
/opt/bot-venv/bin/python /opt/bot/background.py propose --key analysis --timeout 43200 --prompt 'Analyze the requested data and report the result'
/opt/bot-venv/bin/python /opt/bot/background.py list
# Only after the requesting user explicitly agrees in a later @mention:
/opt/bot-venv/bin/python /opt/bot/background.py confirm JOB_ID
/opt/bot-venv/bin/python /opt/bot/background.py cancel JOB_ID
```

A one-shot job starts immediately after confirmation unless `--delay SECONDS` is set. Repeated jobs default to delaying the first run by `--every SECONDS`. Limits are 1–60 occurrences, intervals of 60–86400 seconds, a seven-day total span, three pending/running jobs per group, twenty globally, and three proposals per triggering request. Per-occurrence runtime defaults to 300 seconds, up to 43200 seconds (12 hours) with `--timeout`. One background DSH process runs globally alongside the per-group foreground processes, inside the existing container and its resource limits. Jobs never overlap their own occurrences or recursively spawn children. Background sessions may issue up to two useful progress updates, reserving the third message for their automatically posted final answer; all proactive sends share the existing per-group rate limit.

SQLite stores proposals, confirmations, pending jobs, run outcomes and timing. State lives in `state/seen.sqlite3`; occurrence files are under `state/groups/<group>/background/<job>/runs/<occurrence>/{workspace,home}`. The history helper still reads the same group archive, while upload/messaging capability files belong to each isolated run. A later occurrence gets fresh group context, not a reused DSH session; put the necessary task instructions into the job prompt.

Pending proposals/jobs survive restarts. Work interrupted while running is marked `interrupted` and is not automatically replayed, because tools may already have produced effects. Failed or uncertain results pause the job. List jobs to inspect their last result and request a replacement explicitly when appropriate. Offline/overlapping slots are skipped rather than producing a burst of catch-up messages. Cancellation terminates the running process group and prevents future occurrences; it cannot recall an in-flight or already-sent message. Removing group trust cancels its pending/running jobs. A job's requester must confirm it; any trusted member who can trigger the bot may list/cancel jobs in that group.

## Timeout continuation and background instructions

After a foreground timeout, the bridge saves a pending continuation and asks the original requester whether to continue in the background for up to twelve hours. Nothing starts until confirmation. @mention the bot with `/continue JOB_ID` to confirm or `/cancel JOB_ID` to cancel; these exact commands bypass DSH and the foreground queue. Natural-language confirmation can also be handled by the foreground agent through `background.py confirm`. Group trust, same requester, later-message confirmation and deduplication still apply.

A continuation adopts the recorded DSH session with `--session-id` in the exact original working directory and DSH home. Files and persisted session events remain available. The timed-out process is stopped: this is continuation from saved progress, not a suspended CPU/network operation. A currently running tool or model request may be interrupted; inspect results before repeating external effects. If no session ID was recorded, the offer says so and continuation uses saved files and original context in a fresh session. Missing/corrupt recorded sessions fail explicitly rather than silently restarting the task.

DSH requires the same cwd for adoption. The bridge therefore reserves a timed-out foreground workspace for that continuation and rotates subsequent ordinary requests to a new persistent `state/groups/<group>/foreground/<id>/{workspace,home}`. The old files stay in their original location; they are not copied into the new workspace. This prevents background and foreground work sharing writable files or request grants. Workspace selection, proposals and resume metadata survive restarts. Foreground diagnostics remain under the group's `diagnostics/`; continuation diagnostics use the background occurrence directory. Pending continuations count toward the existing job quotas.

The original requester can send corrections to an existing pending or running job:

```sh
/opt/bot-venv/bin/python /opt/bot/background.py steer JOB_ID --key use-cpp --message 'Use C++17 instead of Python; compile and test it'
```

The foreground agent should list jobs, identify the intended job and steer it rather than create a duplicate. Ambiguous targets require clarification. A pending proposal still needs confirmation; an explicit correction to an already-authorized job needs no new launch permission. Only the original requester in the same group may steer, and background children cannot steer other tasks. C++17 compilation is installed in the image via `g++`.

Instructions are persisted and idempotent per request/key (up to 20 instructions per job, 2000 characters each). `queued` means saved for delivery; `applied` means included in a runner attempt, not proof that the model has obeyed. List returns the instruction history. During execution, the bridge stops and reaps the current process group, then adopts the same saved session/workspace with the new instructions. Newer corrections take precedence. This does not reset the occurrence deadline or create another job, and only the updated run's final result is delivered. It cannot recall earlier uploads/progress messages or an already-in-flight final delivery. Completed/cancelled jobs and jobs already delivering their final result reject new instructions. On container restart, running jobs still become interrupted and require operator review; pending instructions remain saved.

## Model limits and failure diagnostics

The main model has a 32768-token response budget, including reasoning, with high reasoning effort. The short QQ reply policy is independent of that budget. Foreground work retains its five-minute deadline; approved background occurrences can request up to twelve hours. Longer runtime does not remove the model's per-response token limit.

If DSH ends a turn with `max-tokens`, the bot explains that it exhausted the reasoning/output budget before completing an answer. It does not pretend a partial output is complete or automatically repeat tool actions. Background jobs retain this specific failure reason in their last result and pause.

Every DSH run writes structured `dsh_result` metadata to container logs and private JSON files in `<run-root>/diagnostics/`. A foreground run's root is `state/groups/<group>`; each background occurrence uses its isolated run root. Records include session/message identity, elapsed time, timeout, process exit code, stop reason, available per-step token totals and the last step's usage. `usage_steps` versus `step_count` identifies missing usage reports; absent values mean unreported, not zero. They exclude prompts, answers, reasoning, raw errors and stderr content. Stderr is drained and only its byte count is recorded. Atomic, mode-600 JSON files survive container recreation; `last.json` refers to the last finished run, while message-ID hashes retain individual records.

From the current workspace, DSH can inspect a prior failure using:

```sh
/opt/bot-venv/bin/python /opt/bot/diagnostics.py --message-id TRIGGER_MESSAGE_ID
/opt/bot-venv/bin/python /opt/bot/diagnostics.py
```

Records start with this version; earlier failures still require inspecting private DSH session logs. Keep these diagnostics with private state backups.

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
  qq-dsh-bot:test -m unittest -v test_bot test_history test_media test_upload test_messaging test_background test_runtime
```

GitHub Actions builds the standalone image, runs the tests as the unprivileged runtime user, and validates Compose configuration.
