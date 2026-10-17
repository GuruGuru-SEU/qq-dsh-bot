"""Fetch QQ image bytes into durable per-group storage for DSH's read_image tool."""
import asyncio
import hashlib
import ipaddress
import io
import logging
from pathlib import Path
import warnings

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiohttp.resolver import ThreadedResolver
from PIL import Image
from yarl import URL

MAX_BYTES = 16 * 1024 * 1024
LOG = logging.getLogger("qq-dsh")
Image.MAX_IMAGE_PIXELS = 40_000_000


def checked_url(value):
    url = URL(value)
    host = (url.host or "").lower()
    if (url.scheme not in {"http", "https"} or url.user or url.password
            or url.port not in {80, 443}
            or not any(host == domain or host.endswith("." + domain)
                       for domain in ("qq.com", "qpic.cn", "qlogo.cn", "multimedia.nt.qq.com.cn"))):
        raise ValueError("Image URL is not a QQ media endpoint")
    return url


class ImageDNSRejected(ValueError):
    """A proxy fake IP or private address is not safe for media downloads."""


class PublicResolver(ThreadedResolver):
    async def resolve(self, host, port=0, family=0):
        addresses = await super().resolve(host, port, family)
        if not addresses or any(not ipaddress.ip_address(a["host"]).is_global for a in addresses):
            raise ImageDNSRejected("Image endpoint resolved to a non-public address")
        return addresses


def image_extension(data):
    with warnings.catch_warnings():
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(data)) as image:
            extension = {"JPEG": "jpg", "PNG": "png", "WEBP": "webp", "GIF": "gif"}.get(image.format)
            if not extension:
                raise ValueError("Unsupported image format")
            image.verify()
            return extension


async def download_image(url):
    # No bot/API credentials or ambient proxy configuration go to media servers.
    async with ClientSession(timeout=ClientTimeout(total=20),
                             connector=TCPConnector(resolver=PublicResolver()),
                             trust_env=False) as session:
        for _ in range(4):
            async with session.get(checked_url(url), allow_redirects=False) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    url = str(response.url.join(URL(response.headers["Location"])))
                    continue
                response.raise_for_status()
                if response.content_length and response.content_length > MAX_BYTES:
                    raise ValueError("Image exceeds 16 MiB")
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > MAX_BYTES:
                        raise ValueError("Image exceeds 16 MiB")
                image_extension(data)
                return bytes(data)
        raise ValueError("Too many image redirects")


class ImageCache:
    def __init__(self, api, trusted):
        self.api, self.trusted = api, trusted
        self.lock = asyncio.Lock()

    async def cache_message(self, archive, group, mid):
        if not archive.images(mid):
            return
        async with self.lock:
            if not self.trusted(group):
                return
            segments = archive.segments(mid)
            refreshed = None
            for image in archive.images(mid):
                if image["status"] == "ready" and Path(image["file_path"]).is_file():
                    continue
                position = image["position"]
                data = segments[position].get("data", {})
                raw = None
                download_error = None
                if data.get("url"):
                    try:
                        raw = await download_image(data["url"])
                    except Exception as exc:
                        download_error = type(exc).__name__
                try:
                    phase = "refresh_message"
                    if raw is None:
                        # History may omit URLs, or a recorded signed URL may have expired.
                        # get_image returns a path inside NapCat's separate container; get_msg
                        # regenerates a downloadable URL without mounting QQ's private files.
                        if refreshed is None:
                            candidate = await self.api("get_msg", {"message_id": mid})
                            if (int(candidate.get("group_id", 0)) != group
                                    or str(candidate.get("message_id")) != str(mid)):
                                raise ValueError("Refreshed image does not belong to this message/group")
                            refreshed = candidate
                        fresh = refreshed["message"][position]
                        if fresh.get("type") != "image":
                            raise ValueError("Refreshed message changed image positions")
                        phase = "download_refreshed_image"
                        raw = await download_image(fresh["data"]["url"])
                    if not self.trusted(group):
                        return
                    phase = "validate_and_save"
                    extension = image_extension(raw)
                    directory = archive.path.parent / "media"
                    directory.mkdir(mode=0o700, exist_ok=True)
                    path = directory / (hashlib.sha256(raw).hexdigest() + "." + extension)
                    if not path.exists():
                        temporary = path.with_suffix(".part")
                        temporary.write_bytes(raw)
                        temporary.chmod(0o600)
                        temporary.replace(path)
                    archive.image_result(mid, position, "ready", path)
                except Exception as exc:
                    # Never expose signed URLs or exception contents in logs/prompts.
                    LOG.warning("image_unavailable group=%s message=%s position=%s phase=%s kind=%s initial_download_kind=%s http_status=%s",
                                group, mid, position, phase, type(exc).__name__, download_error, getattr(exc, "status", None))
                    detail = "Image could not be downloaded or validated; ask the sender to resend if needed."
                    if isinstance(exc, ImageDNSRejected) or download_error == "ImageDNSRejected":
                        detail = "QQ image DNS resolved to a non-public address (possibly proxy fake-IP); the server's DNS configuration needs repair."
                    archive.image_result(mid, position, "unavailable", detail=detail)
