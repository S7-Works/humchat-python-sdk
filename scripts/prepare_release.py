"""Build and verify a local private-alpha sdist and wheel (port of scripts/prepare-release.mjs).

No upload, login, tag or remote mutation. Usage (no arguments):  python scripts/prepare_release.py
Requires the dev extras plus ``build`` in the running interpreter's environment.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_policy import (
    assert_source_stable,
    inspect_text,
    validate_doc_links,
    validate_metadata,
    validate_pack_files,
    validate_wheel_files,
)

ROOT = Path(__file__).resolve().parent.parent
SOURCE_ROOTS = ["src", "scripts", "tests", "examples", "docs", "contract"]
OPEN_GATES = [
    "production direct-message send/reply and HumChat app verification",
    "server delegated-token administrative restrictions",
    "server expiry-companion build and deployment acceptance",
    "staging and agreed 24-hour pilot",
    "package index ownership and access decision",
    "license and support/security ownership",
    "external-team acceptance",
]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_manifest() -> list[dict[str, Any]]:
    paths = [p for p in ("pyproject.toml", "README.md", "CHANGELOG.md", "LICENSE") if (ROOT / p).is_file()]
    for directory in SOURCE_ROOTS:
        base = ROOT / directory
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if "__pycache__" in path.parts or ".pytest_cache" in path.parts:
                continue
            if path.is_symlink():
                raise RuntimeError("Release source symlink is unsupported: " + path.relative_to(ROOT).as_posix())
            if path.is_file():
                paths.append(path.relative_to(ROOT).as_posix())
    records = []
    for relative in sorted(paths):
        data = (ROOT / relative).read_bytes()
        records.append({"path": relative, "bytes": len(data), "sha256": sha256(data)})
    return records


def main() -> int:
    if len(sys.argv) != 1:
        raise SystemExit("prepare_release takes no arguments and cannot publish")
    artifacts = ROOT / "target" / "release"
    artifacts.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="release-", dir=artifacts))
    (output / "logs").mkdir()
    report: dict[str, Any] = {
        "schema": 1, "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "passed": False,
        "python": platform.python_version(), "platform": sys.platform, "arch": platform.machine(), "checks": [],
        "publication": {"enabled": False, "distribution": "private local sdist and wheel", "open_gates": OPEN_GATES},
    }  # fmt: skip

    def command(name: str, args: list[str], env: dict[str, str] | None = None) -> str:
        print("Validating: " + name, flush=True)
        log = output / "logs" / f"{name}.log"
        started = time.monotonic()
        try:
            result = subprocess.run(args, cwd=ROOT, env={**os.environ, **(env or {})}, capture_output=True, text=True, timeout=15 * 60)
            code, stdout, stderr, timed_out = result.returncode, result.stdout, result.stderr, False
        except subprocess.TimeoutExpired as expired:
            code, stdout, stderr, timed_out = None, str(expired.stdout or ""), str(expired.stderr or ""), True
        log.write_text(stdout + stderr, encoding="utf-8")
        report["checks"].append({"name": name, "log": f"logs/{name}.log", "exit_code": code, "timed_out": timed_out,
                                 "duration_ms": round((time.monotonic() - started) * 1000)})  # fmt: skip
        if timed_out or code != 0:
            raise RuntimeError(f"Validation failed: {name}; see logs/{name}.log")
        return stdout

    python = sys.executable
    try:
        report["source"] = source_manifest()
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        version = (ROOT / "src" / "humchat" / "_version.py").read_text(encoding="utf-8").split('"')[1]
        validate_metadata(pyproject, version, (ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), (ROOT / "LICENSE").read_text(encoding="utf-8"))
        report["package"], report["version"] = pyproject["project"]["name"], version
        report["git_head"] = command("git-head", ["git", "rev-parse", "HEAD"]).strip()
        report["working_tree"] = command("git-status", ["git", "status", "--porcelain", "--", "."]).strip()
        command("ruff", [python, "-m", "ruff", "check", "src", "tests", "scripts", "examples"])
        command("mypy", [python, "-m", "mypy"])
        for suite in ("unit", "integration", "release"):
            command(f"test-{suite}", [python, "-m", "pytest", f"tests/{suite}", "-q"])
        command("build", [python, "-m", "build", "--sdist", "--wheel", "--outdir", str(output)])
        sdists = list(output.glob("humchat_sdk-*.tar.gz"))
        wheels = list(output.glob("humchat_sdk-*.whl"))
        if len(sdists) != 1 or len(wheels) != 1 or sdists[0].name != f"humchat_sdk-{version}.tar.gz":
            raise RuntimeError("Unexpected build result")
        sdist, wheel = sdists[0], wheels[0]
        prefix = f"humchat_sdk-{version}/"
        with tarfile.open(sdist, "r:gz") as archive:
            members = archive.getmembers()
            if any(not member.isfile() or not member.name.startswith(prefix) for member in members):
                raise RuntimeError("sdist contains links, directories or foreign roots")
            entries = [{"path": member.name[len(prefix) :], "size": member.size} for member in members]
            paths = validate_pack_files(entries)
            files = []
            for member in members:
                extracted = archive.extractfile(member)
                assert extracted is not None
                data = extracted.read()
                relative = member.name[len(prefix) :]
                text = data.decode("utf-8")
                inspect_text(relative, text)
                if relative.endswith(".md"):
                    validate_doc_links(relative, text, paths)
                source = ROOT / relative
                if relative not in ("PKG-INFO",) and (not source.is_file() or sha256(source.read_bytes()) != sha256(data)):
                    raise RuntimeError("Packed source file changed or differs: " + relative)
                files.append({"path": relative, "bytes": len(data), "sha256": sha256(data)})
        with zipfile.ZipFile(wheel) as archive:
            validate_wheel_files([{"path": info.filename, "size": info.file_size} for info in archive.infolist()])
            for info in archive.infolist():
                inspect_text(info.filename, archive.read(info).decode("utf-8"))
        report["artifacts"] = {
            "sdist": {"filename": sdist.name, "bytes": sdist.stat().st_size, "sha256": sha256(sdist.read_bytes()), "files": files},
            "wheel": {"filename": wheel.name, "bytes": wheel.stat().st_size, "sha256": sha256(wheel.read_bytes())},
        }
        command("test-package", [python, "-m", "pytest", "tests/package", "-q"],
                {"HUMCHAT_SDK_PACKAGE_WHEEL": str(wheel), "HUMCHAT_SDK_PACKAGE_SDIST": str(sdist),
                 "HUMCHAT_SDK_ARTIFACTS_DIR": str(output / "consumers")})  # fmt: skip
        consumer = json.loads((output / "consumers" / "consumer-result.json").read_text(encoding="utf-8"))
        if consumer.get("passed") is not True or consumer.get("wheel", {}).get("sha256") != report["artifacts"]["wheel"]["sha256"]:
            raise RuntimeError("Consumer evidence does not match the validated wheel")
        if consumer.get("installed_version") != version:
            raise RuntimeError("Consumer installed a different version")
        report["consumer_evidence"] = "consumers/consumer-result.json"
        assert_source_stable(report["source"], source_manifest())
        (output / "SHA256SUMS").write_text(
            "".join(f"{report['artifacts'][kind]['sha256']}  {report['artifacts'][kind]['filename']}\n" for kind in ("sdist", "wheel")), encoding="utf-8"
        )
        report["passed"] = True
    except Exception as error:
        report["failure"] = str(error) if isinstance(error, RuntimeError) or type(error).__name__ == "PolicyError" else "Release preparation failed"
    finally:
        report["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        (output / "release-manifest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        summary = {"passed": report["passed"], "package": report.get("package"), "version": report.get("version"),
                   "output": str(output), "publication_enabled": False, **({"failure": report["failure"]} if "failure" in report else {})}  # fmt: skip
        print(json.dumps(summary), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
