"""Port of test/release/policy.test.mjs, adapted to Python packaging."""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10 needs the tomli backport for this release-only check
    tomllib = pytest.importorskip("tomli")

ROOT = Path(__file__).resolve().parent.parent.parent
_spec = importlib.util.spec_from_file_location("release_policy", ROOT / "scripts" / "release_policy.py")
assert _spec is not None and _spec.loader is not None
policy = importlib.util.module_from_spec(_spec)
sys.modules["release_policy"] = policy
_spec.loader.exec_module(policy)
PolicyError = policy.PolicyError

PYPROJECT: dict[str, Any] = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
VERSION = (ROOT / "src" / "humchat" / "_version.py").read_text(encoding="utf-8").split('"')[1]
CHANGELOG = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8") if (ROOT / "CHANGELOG.md").exists() else ""
LICENSE = (ROOT / "LICENSE").read_text(encoding="utf-8")
FILES = [{"path": path, "size": 12} for path in policy.REQUIRED_FILES]


def changed(**project: Any) -> dict[str, Any]:
    value = copy.deepcopy(PYPROJECT)
    value["project"].update(project)
    return value


def test_current_private_package_and_bounded_dependencies_satisfy_local_release_policy() -> None:
    policy.validate_metadata(PYPROJECT, VERSION, CHANGELOG, LICENSE)


def test_accidental_public_or_stable_metadata_entry_points_or_build_hooks_fail_closed() -> None:
    public = [c for c in PYPROJECT["project"]["classifiers"] if c != policy.PRIVATE_CLASSIFIER]
    hooked = copy.deepcopy(PYPROJECT)
    hooked["tool"]["hatch"]["build"]["hooks"] = {"custom": {}}
    for pyproject, version, license_text in [
        (changed(classifiers=public), VERSION, LICENSE),
        (PYPROJECT, "1.0.0", LICENSE),
        (changed(license={"text": "MIT"}), VERSION, LICENSE),
        (PYPROJECT, VERSION, "MIT License"),
        (changed(scripts={"humchat": "humchat.cli:main"}), VERSION, LICENSE),
        (hooked, VERSION, LICENSE),
    ]:
        with pytest.raises(PolicyError):
            policy.validate_metadata(pyproject, version, CHANGELOG.replace(VERSION, version), license_text)


def test_changelog_mismatch_and_unbounded_dependency_cannot_pass() -> None:
    with pytest.raises(PolicyError):
        policy.validate_metadata(PYPROJECT, "0.1.0a999", CHANGELOG, LICENSE)
    with pytest.raises(PolicyError):
        policy.validate_metadata(changed(dependencies=["httpx>=0.27", "websockets>=13.0,<18"]), VERSION, CHANGELOG, LICENSE)


def test_allowlist_rejects_stale_build_outputs_caches_credentials_and_development_files() -> None:
    for path in ["src/humchat/__pycache__/client.cpython-313.pyc", "src/humchat/client.py.map", ".env", "examples/.env.local",
                 "tests/test_private.py", "contract/fixtures/http-ws.json", "scripts/release_policy.py", "docs/private.txt",
                 "contract/README.md", "target/release/manifest.json"]:  # fmt: skip
        with pytest.raises(PolicyError):
            policy.validate_pack_files([*FILES, {"path": path, "size": 1}])
    assert len(policy.validate_pack_files(FILES)) == len(policy.REQUIRED_FILES)


def test_numbered_uppercase_documentation_names_are_allowed_without_widening_other_packed_paths() -> None:
    for path in ["docs/API_V2.md", "docs/HTTP2.md"]:
        assert path in policy.validate_pack_files([*FILES, {"path": path, "size": 1}])
    for path in ["docs/2_PHASE.md", "docs/PHASE2_report.md", "docs/PHASE-2.md", "docs/PHASE2.txt", "docs/PHASE2/REPORT.md", "docs/.PHASE2.md"]:
        with pytest.raises(PolicyError):
            policy.validate_pack_files([*FILES, {"path": path, "size": 1}])


def test_internal_authentication_reports_cannot_ship_even_when_restored_locally() -> None:
    for name in ["CONTRACT", *[f"PHASE{index + 2}_REPORT" for index in range(6)]]:
        with pytest.raises(PolicyError, match="Internal documentation cannot be included"):
            policy.validate_pack_files([*FILES, {"path": f"docs/SDK_KEY_AUTH_{name}.md", "size": 1}])


def test_paths_must_be_canonical_unique_relative_and_bounded() -> None:
    for path in ["../README.md", "/README.md", "docs/../README.md", "docs\\API.md", "README.md"]:
        with pytest.raises(PolicyError):
            policy.validate_pack_files([*FILES, {"path": path, "size": 1}])
    with pytest.raises(PolicyError):
        policy.validate_pack_files([{**item, "size": 3 * 1024 * 1024} if index == 0 else item for index, item in enumerate(FILES)])
    with pytest.raises(PolicyError):
        policy.validate_pack_files(FILES[1:])


def test_credential_marker_scan_rejects_signing_keys_and_tokens_without_echoing_the_value() -> None:
    for text in ["bb_live_sk_" + "a" * 32, "-----BEGIN PRIVATE KEY-----", "npm_" + "x" * 36, "pypi-" + "A" * 60,
                 "eyJ" + "a" * 30 + "." + "b" * 30 + "." + "c" * 30, "//# sourceMappingURL=old.js.map", "whsec_" + "ab" * 32]:  # fmt: skip
        with pytest.raises(PolicyError) as raised:
            policy.inspect_text("README.md", text)
        assert text not in str(raised.value)
    policy.inspect_text("README.md", "Read the token from os.environ['HUMCHAT_TOKEN']; never commit credentials.")


def test_credential_marker_scan_rejects_humchat_sdk_keys_in_docs_and_code_without_exposing_them() -> None:
    candidates = ["hsk_" + "A" * 43, "hsk_" + "aB09_-" * 7 + "A", "hsk_" + "z" * 32, "hsk_" + "x" * 124]
    for key in candidates:
        for content in [key, f"HUMCHAT_SDK_KEY={key}", json.dumps({"sdk_key": key}), f"sdk_key = '{key}'"]:
            with pytest.raises(PolicyError) as raised:
                policy.inspect_text("docs/AUTHENTICATION.md", content)
            assert str(raised.value) == "Credential-shaped content detected in docs/AUTHENTICATION.md"
            assert key not in repr(raised.value) and key not in str(raised.value.args)


def test_sdk_key_prefix_documentation_and_environment_based_examples_remain_allowed() -> None:
    for content in ["Keys start with `hsk_`.", "HUMCHAT_SDK_KEY=<your-sdk-key>",
                    'auth={"sdk_key": os.environ["HUMCHAT_SDK_KEY"], "act_as": os.environ["HUMCHAT_ACT_AS"]}']:  # fmt: skip
        policy.inspect_text("README.md", content)


def test_installed_docs_links_cannot_depend_on_repository_only_files() -> None:
    paths = set(policy.REQUIRED_FILES)
    policy.validate_doc_links("docs/API.md", "[README](../README.md) [Auth](AUTHENTICATION.md#tokens) [PyPI](https://pypi.org/)", paths)
    with pytest.raises(PolicyError):
        policy.validate_doc_links("README.md", "[report](../../docs/HUMCHAT_PYTHON_SDK_PLAN.md)", paths)
    with pytest.raises(PolicyError):
        policy.validate_doc_links("README.md", "[missing](docs/MISSING.md)", paths)


def test_changes_during_validation_invalidate_the_release_bundle() -> None:
    before = [{"path": "README.md", "sha256": "old"}]
    policy.assert_source_stable(before, copy.deepcopy(before))
    with pytest.raises(PolicyError):
        policy.assert_source_stable(before, [{"path": "README.md", "sha256": "new"}])
    with pytest.raises(PolicyError):
        policy.assert_source_stable(before, [*before, {"path": "examples/new.py", "sha256": "new"}])
