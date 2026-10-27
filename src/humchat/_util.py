"""Small helpers that reproduce JavaScript semantics the wire contract depends on."""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any
from urllib.parse import SplitResult, urlsplit

MAX_SAFE_INTEGER = 9_007_199_254_740_991
MAX_TIMER_MS = 2_147_483_647

# ECMAScript WhiteSpace + LineTerminator, the set String.prototype.trim() removes.
JS_WHITESPACE = "".join(
    chr(code)
    for code in (
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0xA0, 0x1680, *range(0x2000, 0x200B),
        0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF,
    )
)  # fmt: skip
_JS_WS_SET = frozenset(JS_WHITESPACE)


def js_trim(value: str) -> str:
    return value.strip(JS_WHITESPACE)


def has_js_whitespace_or_control(value: str) -> bool:
    """``/[\\s\\p{Cc}]/u``."""
    return any(ch in _JS_WS_SET or unicodedata.category(ch) == "Cc" for ch in value)


def utf8_len(value: str) -> int:
    """``Buffer.byteLength(value, 'utf8')``; lone surrogates count as U+FFFD (3 bytes)."""
    return len(value.encode("utf-8", "surrogatepass"))


def utf16_len(value: str) -> int:
    """JavaScript ``string.length`` (UTF-16 code units)."""
    return sum(2 if ord(ch) > 0xFFFF else 1 for ch in value)


def is_int(value: Any) -> bool:
    """A JSON number that is a JavaScript integer (``1.0`` counts; booleans never do)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and value.is_integer()


def is_safe_int(value: Any) -> bool:
    return is_int(value) and -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER


def as_int(value: Any) -> int:
    return int(value)


def is_str(value: Any) -> bool:
    return isinstance(value, str)


def _reject_constant(_: str) -> Any:
    raise ValueError("Non-standard JSON constant")


def json_loads(data: bytes | str) -> Any:
    """Strict UTF-8 JSON (``JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(...))``)."""
    text = data.decode("utf-8", "strict") if isinstance(data, (bytes, bytearray, memoryview)) else data
    if isinstance(text, (bytearray, memoryview)):  # pragma: no cover - defensive
        text = bytes(text).decode("utf-8", "strict")
    return json.loads(text, parse_constant=_reject_constant)


def json_dumps(value: Any) -> str:
    """Compact ``JSON.stringify`` equivalent preserving key insertion order."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


_DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443}


class ParsedUrl:
    """The subset of WHATWG URL parsing the SDK relies on."""

    __slots__ = ("hash", "hostname", "password", "pathname", "port", "scheme", "search", "username")

    def __init__(self, parts: SplitResult, raw: str) -> None:
        self.scheme = parts.scheme.lower()
        self.username = parts.username or ""
        self.password = parts.password or ""
        host = parts.hostname or ""
        if parts.netloc.startswith("[") or (parts.netloc.rsplit("@", 1)[-1].startswith("[")):
            host = f"[{host}]"
        self.hostname = host.lower()
        port = parts.port
        self.port = "" if port is None or _DEFAULT_PORTS.get(self.scheme) == port else str(port)
        self.pathname = parts.path or "/"
        self.search = "?" + parts.query if parts.query else ""
        self.hash = "#" + parts.fragment if parts.fragment else ""

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.hostname}{':' + self.port if self.port else ''}"

    @property
    def href(self) -> str:
        auth = ""
        if self.username or self.password:
            auth = self.username + (":" + self.password if self.password else "") + "@"
        return f"{self.scheme}://{auth}{self.hostname}{':' + self.port if self.port else ''}{self.pathname}{self.search}{self.hash}"


_C0_OR_SPACE = "".join(chr(code) for code in range(0x21))


def parse_url(raw: str) -> ParsedUrl:
    """Parse an absolute special URL or raise ``ValueError``."""
    if not isinstance(raw, str):
        raise ValueError("URL must be a string")
    value = raw.strip(_C0_OR_SPACE).replace("\t", "").replace("\n", "").replace("\r", "")
    parts = urlsplit(value)
    if not parts.scheme or not re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]*", parts.scheme):
        raise ValueError("Relative URL")
    scheme = parts.scheme.lower()
    if scheme in _DEFAULT_PORTS:
        if not value[len(parts.scheme) + 1 :].startswith("//") or not parts.hostname:
            raise ValueError("Special URL requires a host")
        try:
            parts.port  # noqa: B018 - validates the port
        except ValueError as error:
            raise ValueError("Invalid port") from error
        host = parts.hostname or ""
        if any(ch in host for ch in " <>^|%") and not host.startswith("["):
            raise ValueError("Invalid host")
    return ParsedUrl(parts, value)
