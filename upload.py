"""Request-scoped group-file upload service and DSH command-line helper."""
import argparse
import asyncio
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import time
import urllib.error
import urllib.request

from aiohttp import web
from history import current_archive

MAX_BYTES = 20 * 1024 * 1024
MAX_FILES = 3
UPLOAD_TIMEOUT = 180


class OneBotError(RuntimeError):
    def __init__(self, action, code):
        self.code = code
        super().__init__(f"OneBot action failed: {action} (retcode={code})")


@dataclass
class Grant:
    group: int
    message_id: str
    workspace: Path
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def file_snapshot(workspace, requested, name=None):
    """Read a bounded, regular workspace file; reject paths escaping via symlinks."""
    workspace = workspace.resolve(strict=True)
    path = Path(requested)
    path = (workspace / path).resolve(strict=True) if not path.is_absolute() else path.resolve(strict=True)
    if not path.is_relative_to(workspace):
        raise ValueError("File must be inside this group's workspace")
    name = path.name if name is None else name
    if (not isinstance(name, str) or not name or name in {".", ".."}
            or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in name)
            or len(name.encode()) > 200):
        raise ValueError("Use a plain filename, at most 200 UTF-8 bytes, without path separators")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("Only regular files can be uploaded")
        # On Linux, verify the opened inode's path too, closing parent-symlink races.
        if Path("/proc/self/fd").exists():
            actual = Path(f"/proc/self/fd/{source.fileno()}").resolve(strict=True)
            if not actual.is_relative_to(workspace):
                raise ValueError("File must be inside this group's workspace")
        if info.st_size > MAX_BYTES:
            raise ValueError("File exceeds the 20 MiB upload limit")
        data = source.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise ValueError("File exceeds the 20 MiB upload limit")
        if not data:
            raise ValueError("Cannot upload an empty file")
    return name, data


class UploadService:
    def __init__(self, db, state, trusted, api, transfer_base="http://qq-dsh-bot:8080"):
        self.db, self.state, self.trusted, self.api = db, Path(state), trusted, api
        self.transfer_base = transfer_base.rstrip("/")
        self.grants, self.transfers = {}, {}
        db.execute("""CREATE TABLE IF NOT EXISTS uploads (
            group_id INTEGER NOT NULL, message_id TEXT NOT NULL, digest TEXT NOT NULL,
            name TEXT NOT NULL, result TEXT NOT NULL, updated_at REAL NOT NULL,
            PRIMARY KEY(group_id,message_id,digest,name))""")
        # A restart during a send leaves its delivery ambiguous; never resend blindly.
        unknown = json.dumps({"status": "unknown", "message": "Upload interrupted. Check QQ group files before trying again; it may already have arrived."})
        db.execute("UPDATE uploads SET result=? WHERE json_extract(result,'$.status')='uploading'", (unknown,))
        db.commit()

    def begin(self, job):
        token = secrets.token_urlsafe(32)
        workspace = self.state / "groups" / str(job.group) / "workspace"
        workspace.mkdir(parents=True, exist_ok=True)
        self.grants[token] = Grant(job.group, str(job.message_id), workspace)
        # DSH deliberately strips credential-shaped environment variables from
        # shell tools. Publish only this scoped grant outside its writable workspace.
        path = workspace.parent / ".upload-request.json"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as target:
            json.dump({"token": token, "url": "http://127.0.0.1:8080/agent/upload"}, target)
        return token

    def end(self, token):
        grant = self.grants.pop(token, None)
        if grant:
            path = grant.workspace.parent / ".upload-request.json"
            try:
                if json.loads(path.read_text()).get("token") == token:
                    path.unlink()
            except (OSError, ValueError):
                pass

    def prune(self):
        for token, (deadline, _) in list(self.transfers.items()):
            if deadline < time.monotonic():
                del self.transfers[token]

    async def download(self, request):
        self.prune()
        item = self.transfers.get(request.match_info["token"])
        if item is None:
            raise web.HTTPNotFound()
        return web.Response(body=item[1], content_type="application/octet-stream",
                            headers={"Cache-Control": "no-store"})

    def result(self, key, result):
        self.db.execute("INSERT OR REPLACE INTO uploads VALUES (?,?,?,?,?,?)", (*key, json.dumps(result, ensure_ascii=False), time.time()))
        self.db.commit()
        return result

    async def handle(self, request):
        header = request.headers.get("Authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        grant = self.grants.get(token)
        if grant is None:
            return web.json_response({"status": "denied", "message": "Upload access is only available during an active bot request"}, status=403)
        async with grant.lock:
            if self.grants.get(token) is not grant or not self.trusted(grant.group):
                return web.json_response({"status": "denied", "message": "Request ended or group is no longer trusted"}, status=403)
            try:
                body = await request.json()
                if not isinstance(body, dict) or set(body) - {"file_path", "name"} or not isinstance(body.get("file_path"), str):
                    raise ValueError("Expected file_path and optional name; destination group is fixed by the request")
                name, data = file_snapshot(grant.workspace, body["file_path"], body.get("name"))
            except (ValueError, TypeError, OSError):
                return web.json_response({"status": "failed", "message": "Invalid file: use a regular nonempty file inside this group's workspace, at most 20 MiB, and a plain filename of at most 200 UTF-8 bytes"}, status=400)
            key = (grant.group, grant.message_id, hashlib.sha256(data).hexdigest(), name)
            previous = self.db.execute("SELECT result FROM uploads WHERE group_id=? AND message_id=? AND digest=? AND name=?", key).fetchone()
            if previous:
                return web.json_response({**json.loads(previous[0]), "reused_result": True})
            count = self.db.execute("SELECT count(*) FROM uploads WHERE group_id=? AND message_id=?", key[:2]).fetchone()[0]
            if count >= MAX_FILES:
                return web.json_response({"status": "denied", "message": "At most three file upload attempts are allowed per request"}, status=429)
            self.result(key, {"status": "uploading", "name": name})
            self.prune()
            transfer = secrets.token_urlsafe(32)
            self.transfers[transfer] = (time.monotonic() + UPLOAD_TIMEOUT + 60, data)
            asyncio.get_running_loop().call_later(UPLOAD_TIMEOUT + 60, self.transfers.pop, transfer, None)
            try:
                response = await self.api("upload_group_file", {
                    "group_id": str(grant.group), "file": self.transfer_base + "/transfers/" + transfer,
                    "name": name,
                }, timeout=UPLOAD_TIMEOUT)
                file_id = response.get("file_id") if isinstance(response, dict) else None
                if file_id:
                    outcome = {"status": "uploaded", "name": name, "file_id": str(file_id), "size": len(data)}
                    self.transfers.pop(transfer, None)
                else:
                    outcome = {"status": "unknown", "name": name, "message": "NapCat did not confirm a file ID. Check QQ group files; do not automatically retry."}
            except OneBotError as exc:
                outcome = {"status": "failed", "name": name, "retcode": exc.code,
                           "message": "NapCat/QQ rejected the upload. Check group file permissions, space and account restrictions; the cause is not established by this error alone."}
                self.transfers.pop(transfer, None)
            except (ConnectionError, asyncio.TimeoutError):
                outcome = {"status": "unknown", "name": name,
                           "message": "Upload acknowledgement was lost or timed out. Check QQ group files before trying again; the file may already have arrived."}
            except Exception:
                outcome = {"status": "unknown", "name": name, "message": "Upload confirmation failed. Check QQ group files before trying again."}
            return web.json_response(self.result(key, outcome))


def capability():
    # Explicit environment support is useful for non-DSH integrations and tests.
    if os.environ.get("QQ_UPLOAD_TOKEN"):
        return {"token": os.environ["QQ_UPLOAD_TOKEN"], "url": os.environ.get("QQ_UPLOAD_URL", "http://127.0.0.1:8080/agent/upload")}
    try:
        return json.loads((current_archive().parent / ".upload-request.json").read_text())
    except (OSError, ValueError):
        return {}


def main():
    parser = argparse.ArgumentParser(description="Upload a local workspace file to the current request's QQ group (20 MiB max).")
    parser.add_argument("file_path")
    parser.add_argument("--name", help="Filename shown in QQ group files")
    args = parser.parse_args()
    access = capability()
    token = access.get("token")
    if not token:
        parser.exit(1, "Upload access is only available during an active bot request.\n")
    payload = {"file_path": str(Path(args.file_path).absolute())}
    if args.name is not None:
        payload["name"] = args.name
    request = urllib.request.Request(access["url"],
                                     data=json.dumps(payload).encode(),
                                     headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    # Bypass ambient proxies for this container-local capability endpoint.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        try:
            response = opener.open(request, timeout=UPLOAD_TIMEOUT + 30)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            result = json.load(response)
    except (OSError, ValueError):
        result = {"status": "unknown", "message": "Could not obtain upload status. Check group files; do not blindly retry."}
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if result.get("status") == "uploaded" else 1)


if __name__ == "__main__":
    main()
