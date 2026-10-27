# Release preparation and publication

The current package version is `0.1.0a1`, a review preview that ports `@humchat/node` 0.1.0-alpha.8. No package-index release exists: `pyproject.toml` carries the `Private :: Do Not Upload` classifier, which PyPI rejects, and the license is `UNLICENSED`. Production deployment validation and a sustained pilot remain open. A passing local artifact check does not establish production readiness or grant an open-source license.

## Prepare a local artifact

From the repository root, with the dev extras installed:

```sh
python -m pip install -e ".[dev]"
python scripts/prepare_release.py
```

The command accepts no arguments and performs no upload, login, tag or other remote mutation. It runs lint, type checks and the test suite, validates the release policy (`scripts/release_policy.py`), builds the sdist and wheel once, checks their exact file lists, scans shipped text for credential-shaped content (for example `hsk_` followed by 32 or more key characters), verifies documentation links, and installs the built wheel into a fresh virtual environment to import every public export and type-check a consumer. The credential scan is deliberately limited; it does not replace review.

It writes a fresh `target/release/release-*` directory containing:

| Output | Purpose |
| --- | --- |
| `humchat_sdk-0.1.0a1-py3-none-any.whl`, `humchat_sdk-0.1.0a1.tar.gz` | Installable wheel and source distribution with documentation and examples. |
| `SHA256SUMS` | Hashes for transfer verification. |
| `release-manifest.json` | Runtime, package identity, file/source hashes, check results and open publication gates. `passed: true` means local validation passed. |

On failure the manifest records `passed: false`; do not distribute those files. Every run uses a new directory. A dirty checkout is recorded rather than refused, so the manifest is evidence about local validation, not a clean-checkout attestation or a publisher signature.

To use a supplied artifact, compare its SHA-256 with `SHA256SUMS`, then `python -m pip install /path/to/humchat_sdk-0.1.0a1-py3-none-any.whl`. Do not `pip install humchat-sdk` from an index: no release exists, and a package of that name on a public index would not be this SDK.

## Repository workflow

`.github/workflows/ci.yml` runs `ruff check .`, `mypy` and `pytest` on Python 3.10, 3.11, 3.12 and 3.13 across Ubuntu, Windows and macOS for every push and pull request. It uses only synthetic loopback peers, never production credentials, and never publishes. Check the [Actions results](https://github.com/S7-Works/humchat-python-sdk/actions) for actual outcomes; a configured workflow is not passing evidence.

Validation against a real HumChat deployment is a separate, opt-in step: the [live message runner](SDK_KEY_MESSAGE_ACCEPTANCE.md) requires an authorized sender/recipient pair and reads credentials only from the environment.

## Decisions required before any index publication

| Decision / evidence | Current state |
| --- | --- |
| Distribution name and index ownership | `humchat-sdk` is provisional; no index project is reserved. |
| Public versus restricted distribution | Undecided. The private classifier blocks accidental uploads and must be removed deliberately. |
| License | `UNLICENSED`; owner decision pending. |
| Support/security owner | See [support](SUPPORT.md) and the [private security reporting route](https://github.com/S7-Works/humchat-python-sdk/security/advisories/new); no named owner or SLA. |
| Production member messaging | Run the designated two-member DM/reply acceptance and confirm both messages in the HumChat app. |
| Delegated administrative boundary | Verify that the target server denies delegated sessions on administrative routes. |
| Socket expiry recovery | Verify proactive renewal and any advertised `TOKEN_EXPIRED` behavior on the target deployment. |
| Staging and sustained pilot | Required before a release candidate. |

After those gates close, make a reviewed release change: bump `src/humchat/_version.py`, update the [changelog](../CHANGELOG.md) and [migration guide](MIGRATION.md), record the supported SDK/server pair, resolve license/support metadata and remove the private classifier only as part of that reviewed change. The release policy rejects stable versions and changed publication metadata until the policy itself is updated.

## Future index workflow

Use a protected release environment and trusted publishing (OIDC) configured for this exact repository/workflow; do not store long-lived index tokens in the repository or examples. Publish pre-releases (`aN`, `bN`, `rcN`) before a stable version, install the exact published version in a clean environment and run the smoke examples before promotion. Never overwrite a published file; yank a defective version and publish a corrected one.

## Version policy

SDK versions follow PEP 440 and are independent of message-state and durable-event wire versions. Each Python release states which Node SDK release it corresponds to. After 1.0, breaking public API changes, a higher minimum Python version or weaker delivery guarantees require a major release.
