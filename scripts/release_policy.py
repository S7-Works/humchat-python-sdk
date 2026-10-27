"""Local private-alpha distribution policy (port of scripts/release-policy.mjs).

This module never contacts a package index. It validates project metadata, the exact
sdist and wheel file lists, shipped text for credential-shaped content, documentation
links, and source stability during validation.
"""

from __future__ import annotations

import json
import posixpath
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import unquote

PACKAGE = "humchat-sdk"
MODULE = "humchat"
PRIVATE_CLASSIFIER = "Private :: Do Not Upload"
MAX_FILE_BYTES = 2 * 1024 * 1024

REQUIRED_DOCS = [
    "docs/AUTHENTICATION.md", "docs/RELIABILITY.md", "docs/API.md", "docs/MIGRATION.md", "docs/TROUBLESHOOTING.md",
    "docs/RELEASING.md", "docs/SUPPORT.md", "docs/MULTIPLEX.md", "docs/WEBHOOKS.md", "docs/DIRECT_MESSAGES.md",
]  # fmt: skip
REQUIRED_EXAMPLES = [
    "examples/README.md", "examples/multiplex_worker.py", "examples/webhook_receiver.py", "examples/webhook_inbox.py", "examples/dm_responder.py",
]  # fmt: skip
#: Paths relative to the sdist root (``humchat_sdk-<version>/``).
REQUIRED_FILES = [
    "pyproject.toml", "PKG-INFO", "README.md", "CHANGELOG.md", "LICENSE",
    "src/humchat/__init__.py", "src/humchat/py.typed", "src/humchat/_version.py",
    *REQUIRED_DOCS, *REQUIRED_EXAMPLES,
]  # fmt: skip

_SDIST_ALLOWED = re.compile(
    r"^(?:pyproject\.toml|PKG-INFO|README\.md|CHANGELOG\.md|LICENSE|\.gitignore"
    r"|src/humchat/(?:[a-z_][a-z0-9_]*/)*[a-z_][a-z0-9_]*\.py|src/humchat/py\.typed"
    r"|docs/[A-Z][A-Z0-9_]*\.md|examples/(?:README\.md|[a-z][a-z0-9_]*\.py))$"
)
_WHEEL_ALLOWED = re.compile(
    r"^(?:humchat/(?:[a-z_][a-z0-9_]*/)*[a-z_][a-z0-9_]*\.py|humchat/py\.typed"
    r"|humchat_sdk-0\.1\.0a[0-9]+\.dist-info/(?:METADATA|WHEEL|RECORD|licenses/LICENSE))$"
)
_INTERNAL_DOC = re.compile(r"^docs/SDK_KEY_AUTH_(?:CONTRACT|PHASE[0-9]+_REPORT)\.md$")
_BOUNDED_REQUIREMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(?:\[[a-z0-9,_-]+\])?>=\d+(?:\.\d+)*,<\d+(?:\.\d+)*$")
_CREDENTIALS = [
    re.compile(r"bb_live_(?:sk|cid)_[a-f0-9]{16,}", re.IGNORECASE),
    # HumChat generates 43 base64url characters after hsk_. A 32-character floor also
    # catches nearby opaque formats without rejecting the documented prefix.
    re.compile(r"hsk_[A-Za-z0-9_-]{32,}"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"),
    re.compile(r"npm_[a-zA-Z0-9]{30,}"),
    re.compile(r"pypi-[A-Za-z0-9_-]{50,}"),
    re.compile(r"whsec_[0-9a-f]{64}"),
]


class PolicyError(Exception):
    """A release policy violation. Messages never echo the offending content."""


def validate_metadata(pyproject: Mapping[str, Any], version: str, changelog: str, license_text: str) -> None:
    project = pyproject.get("project")
    if not isinstance(project, Mapping):
        raise PolicyError("Missing [project] metadata")
    if project.get("name") != PACKAGE:
        raise PolicyError("Package name requires a reviewed policy update")
    if not re.fullmatch(r"0\.1\.0a[0-9]+", version):
        raise PolicyError("Only private alpha versions are permitted by this release policy")
    if PRIVATE_CLASSIFIER not in (project.get("classifiers") or []):
        raise PolicyError("Private distribution classifier removed; review release decisions first")
    if project.get("license") != {"file": "LICENSE"} or not license_text.startswith("UNLICENSED"):
        raise PolicyError("Private alpha/license policy changed; review release decisions first")
    for key in ("scripts", "gui-scripts", "entry-points"):
        if project.get(key):
            raise PolicyError("Unexpected executable or plugin entry point metadata")
    build = pyproject.get("tool", {}).get("hatch", {}).get("build", {})
    if build.get("hooks") or any(target.get("hooks") for target in build.get("targets", {}).values() if isinstance(target, Mapping)):
        raise PolicyError("Unexpected build-time hook")
    if project.get("requires-python") != ">=3.10":
        raise PolicyError("Python compatibility policy changed")
    if project.get("dynamic") != ["version"] and project.get("version") != version:
        raise PolicyError("Project version and module version differ")
    for requirement in project.get("dependencies") or []:
        if not isinstance(requirement, str) or not _BOUNDED_REQUIREMENT.fullmatch(requirement.replace(" ", "")):
            raise PolicyError("Release dependencies must have explicit lower and upper bounds")
    if not re.search(rf"^##\s+\[?{re.escape(version)}\]?(?:\s|$)", changelog, re.MULTILINE):
        raise PolicyError("CHANGELOG has no entry for the package version")


def _check_path(path: Any, seen: set[str]) -> str:
    if (
        not isinstance(path, str)
        or not path
        or path != posixpath.normpath(path)
        or path.startswith("/")
        or "\\" in path
        or any(part in ("..", "") or (part.startswith(".") and path != ".gitignore") for part in path.split("/"))
        or path in seen
    ):
        raise PolicyError("Unsafe or duplicate packed path")
    return path


def _check_size(size: Any) -> None:
    if not isinstance(size, int) or isinstance(size, bool) or size < 0 or size > MAX_FILE_BYTES:
        raise PolicyError("Invalid packed file size")


def validate_pack_files(files: Iterable[Mapping[str, Any]]) -> set[str]:
    """Validate an sdist file list (paths relative to the sdist root)."""
    seen: set[str] = set()
    for item in files:
        path = _check_path(item.get("path"), seen)
        if _INTERNAL_DOC.fullmatch(path):
            raise PolicyError("Internal documentation cannot be included in the package")
        if not _SDIST_ALLOWED.fullmatch(path):
            raise PolicyError("Unexpected packed file: " + path)
        _check_size(item.get("size"))
        seen.add(path)
    for path in REQUIRED_FILES:
        if path not in seen:
            raise PolicyError("Missing required package file: " + path)
    return seen


def validate_wheel_files(files: Iterable[Mapping[str, Any]]) -> set[str]:
    seen: set[str] = set()
    for item in files:
        path = _check_path(item.get("path"), seen)
        if not _WHEEL_ALLOWED.fullmatch(path):
            raise PolicyError("Unexpected wheel file: " + path)
        _check_size(item.get("size"))
        seen.add(path)
    for path in ("humchat/__init__.py", "humchat/py.typed"):
        if path not in seen:
            raise PolicyError("Missing required wheel file: " + path)
    return seen


def inspect_text(path: str, text: str) -> None:
    """Known credential shapes and signing-key material, not a general secret detector."""
    if any(pattern.search(text) for pattern in _CREDENTIALS):
        raise PolicyError("Credential-shaped content detected in " + path)
    if re.search(r"sourceMappingURL\s*=", text):
        raise PolicyError("Unexpected source map reference in " + path)


def validate_doc_links(path: str, text: str, packed_paths: set[str]) -> None:
    for match in re.finditer(r"\]\(([^\s)]+)(?:\s+\"[^\"]*\")?\)", text):
        target = re.sub(r"^<|>$", "", match.group(1))
        if re.match(r"^(?:https?://|mailto:|#)", target, re.IGNORECASE):
            continue
        target = unquote(target.split("#")[0])
        if not target:
            continue
        resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), target))
        if resolved not in packed_paths:
            raise PolicyError("Package documentation links outside its shipped files: " + path + " -> " + target)


def assert_source_stable(before: Sequence[Any], after: Sequence[Any]) -> None:
    if json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True):
        raise PolicyError("Release sources changed during validation; prepare a new bundle")
