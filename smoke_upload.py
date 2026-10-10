"""Opt-in LIVE test: ask DSH to upload hello.txt, or verify an existing upload."""
import argparse
import asyncio
import json
import logging
import os
import time
import uuid

from aiohttp import ClientSession, ClientTimeout, web
from bot import Bridge, Job


async def verify(bridge, group, result, uploaded_at):
    # NapCat can return a raw QQ UUID on upload, but random opaque IDs in listings.
    # Confirm the bot's matching recent file, then download and compare its bytes.
    for attempt in range(6):
        listing = await bridge.api("get_group_root_files", {"group_id": str(group), "file_count": 1000}, timeout=30)
        for item in (listing or {}).get("files", []):
            timestamp = float(item.get("upload_time", 0))
            if timestamp > 100_000_000_000:
                timestamp /= 1000
            if not (item.get("file_name") == "hello.txt" and int(item.get("file_size", item.get("size", 0))) == 23
                    and int(item.get("uploader", 0)) == bridge.config["bot_qq"]
                    and abs(timestamp - uploaded_at) <= 60):
                continue
            link = await bridge.api("get_group_file_url", {"group_id": str(group), "file_id": item["file_id"], "busid": item.get("busid", 102)}, timeout=30)
            async with ClientSession(timeout=ClientTimeout(total=30)) as client:
                async with client.get(link["url"]) as response:
                    response.raise_for_status()
                    content = await response.content.read(1024)
            if content != b"Hello from qq-dsh-bot!\n":
                raise RuntimeError("Listed hello.txt did not contain the expected bytes")
            print(json.dumps({"visible_in_group_files": True, "downloaded_content_matches": True,
                              "file_id": result["file_id"], "bytes": len(content)}), flush=True)
            return
        if attempt < 5:
            await asyncio.sleep(2)
    raise RuntimeError("QQ acknowledged upload but a matching recent hello.txt was not found in the root listing")


async def run(group, verify_file_id=None):
    bridge = Bridge(os.environ.get("BOT_CONFIG", "/etc/bot/config.json"),
                    os.environ.get("BOT_STATE", "/state"), os.environ["ONEBOT_TOKEN"])
    if not bridge.trusted(group):
        raise ValueError("The live test group must be trusted")
    mid = "upload-smoke-" + uuid.uuid4().hex
    job = Job(group, mid, bridge.config["bot_qq"],
              f"This is an authorized live group-file upload test. Create {mid}/hello.txt inside the current workspace containing exactly 'Hello from qq-dsh-bot!\\n' (a final newline), then upload it with /opt/bot-venv/bin/python /opt/bot/upload.py ./{mid}/hello.txt --name hello.txt. Do not upload any other file. If the result is unknown or failed, stop and report it; do not retry. Reply briefly with the result.", [])
    final = asyncio.get_running_loop().create_future()
    original_reply = bridge.reply

    async def reply(current, text):
        if current.message_id == mid:
            final.set_result(text)  # This test posts the file, not an extra chat reply.
        else:
            await original_reply(current, text)

    bridge.reply = reply
    runner = web.AppRunner(bridge.app(), access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "0.0.0.0", 8080).start()
        await asyncio.wait_for(bridge.connected.wait(), 90)
        if verify_file_id:
            row = bridge.db.execute("SELECT result,updated_at FROM uploads WHERE group_id=? AND json_extract(result,'$.file_id')=? ORDER BY updated_at DESC LIMIT 1", (group, verify_file_id)).fetchone()
            if not row or json.loads(row[0]).get("status") != "uploaded":
                raise ValueError("No successful upload audit entry for that group/file ID")
            result, uploaded_at = json.loads(row[0]), row[1]
        else:
            bridge.queue.put_nowait(job)
            answer = await asyncio.wait_for(final, 380)
            await asyncio.wait_for(bridge.queue.join(), 380)
            rows = bridge.db.execute("SELECT result,updated_at FROM uploads WHERE group_id=? AND message_id=?", (group, mid)).fetchall()
            results = [json.loads(row[0]) for row in rows]
            print(json.dumps({"request_id": mid, "results": results, "agent_answer": answer}, ensure_ascii=False), flush=True)
            if len(results) != 1 or results[0].get("status") != "uploaded" or results[0].get("name") != "hello.txt":
                raise RuntimeError("Live upload was not confirmed; inspect group files before retrying")
            result, uploaded_at = results[0], rows[0][1]
        await verify(bridge, group, result, uploaded_at)
    finally:
        if bridge.ws:
            await bridge.ws.close()
        await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", required=True, type=int)
    parser.add_argument("--verify-file-id", help="Only verify an existing successful upload; do not create or send another file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(args.group, args.verify_file_id))
