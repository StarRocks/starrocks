# AGENTS.md - be/src/common

This directory contains core common infrastructure. Follow the BE-wide rules from [`be/AGENTS.md`](../../AGENTS.md)
and the config workflow in [`README.md`](./README.md).

## Module boundary

- `be/src/common` may depend only on `base/*`, `gutil/*`, `gen_cpp/*`, system headers, and third-party libraries.
- Do not add dependencies on `util/*`, `runtime/*`, `storage/*`, `exec/*`, `service/*`, `http/*`, `connector/*`, or
  other higher-level BE modules.

## Config files

- `common/config.h` is the only source of truth for config declarations, defaults, and comments.
- `common/config.cpp` owns config definitions and must keep including `configbase_impl.h` before `config.h`.
- `common/config_<domain>_fwd.h` files are generated. Never edit them by hand.
- If a config needs to be visible through a forward header, update
  `common/config_fwd_headers_manifest.json` and run `python3 build-support/gen_config_fwd_headers.py`.

## Include policy

- Prefer `common/config_<domain>_fwd.h` over `common/config.h`.
- Treat new direct `common/config.h` includes as exceptions. Check whether an existing forward header can be reused or
  whether the manifest should grow first.
- Remember that the guardrail in `build-support/check_common_config_header_includes.sh` scans `be/src` and `be/test`
  for direct includes.

## Generator behavior

- Default generation is timestamp-stable: if content is unchanged, files are not rewritten.
- Use `--force` only when you intentionally want to rewrite generated headers.
- Before finishing config-related work, run:

```bash
python3 build-support/gen_config_fwd_headers.py --check
bash build-support/check_common_config_header_includes.sh
```
