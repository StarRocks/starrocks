# AGENTS.md - be/src/common

`be/src/common` follows the BE harness in [`be/AGENTS.md`](../../AGENTS.md) and adds the config-forward-header workflow from [`README.md`](./README.md).

## Local Rules

- `common/config.h` is the single source of truth for config declarations, defaults, and comments.
- `common/config.cpp` owns config definitions and must keep including `configbase_impl.h` before `config.h`.
- Generated `common/config_<domain>_fwd.h` headers must not be edited by hand.
- Prefer `common/config_<domain>_fwd.h` over direct `common/config.h` includes.
- New direct `common/config.h` includes are exceptions and must survive `build-support/check_common_config_header_includes.sh`.

## Commands

```bash
python3 build-support/gen_config_fwd_headers.py --check
bash build-support/check_common_config_header_includes.sh
python3 build-support/check_be_module_boundaries.py --mode full
```

If a config needs a new forward header, update `common/config_fwd_headers_manifest.json` and regenerate through the existing generator instead of creating ad hoc declaration headers.
