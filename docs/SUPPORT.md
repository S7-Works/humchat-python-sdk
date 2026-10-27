# Support and security during the alpha

The source repository is public at [S7-Works/humchat-python-sdk](https://github.com/S7-Works/humchat-python-sdk). Use its [issue tracker](https://github.com/S7-Works/humchat-python-sdk/issues) for nonsensitive bugs and questions. This is an alpha; no support SLA or response-time commitment is offered.

For a reproducible bug, include the package version (`humchat.__version__`), Python version, server capabilities, the failing method, the typed error code/status/request ID, and a minimal example with synthetic message content. For recovery problems, include the recovery mode, channel count, whether a persistent store is in use, and whether the worker restarted. Exclude tokens, SDK keys, message bodies, checkpoint/outbox files, and personally identifying data. See [troubleshooting](TROUBLESHOOTING.md).

For a suspected security defect, [submit a private vulnerability report](https://github.com/S7-Works/humchat-python-sdk/security/advisories/new) to the repository maintainers. Do not post exploit details, credentials or private deployment information in public issues.

The current [license](../LICENSE) is `UNLICENSED`; public source visibility does not grant an open-source license. The `Private :: Do Not Upload` classifier prevents accidental package-index uploads.
