# common

## Config Forward Headers

`common/config.h` remains the single source of truth for BE config declarations:

- config names
- config types
- default values
- comments that describe the config

The generated `config_<domain>_fwd.h` headers exist only to expose selected `CONF_*` declarations to consumers
without forcing them to include all of `common/config.h`.

### Why it works this way

We do this for two reasons:

1. `common/config.h` is large and widely included. Any direct include expands rebuild fanout when the file changes.
2. We do not want multiple hand-maintained config declaration files. That would split the source of truth and make
   defaults, types, and comments drift.

The current design keeps `config.h` authoritative and derives smaller forward headers from it:

- `common/config.h`: authoritative declarations
- `common/config.cpp`: definitions
- `build-support/config_fwd_headers_manifest.json`: selects which configs belong to which forward header
- `build-support/gen_config_fwd_headers.py`: generates the committed `config_<domain>_fwd.h` files

The generator preserves comments, declaration order, and surrounding preprocessor guards from `config.h`. By default it
only rewrites a generated file when the content actually changes, so unchanged forward headers keep their timestamps
and do not trigger needless recompiles. Use `--force` only when you intentionally want to rewrite the generated files.

### Rules

- Do not hand-edit `config_<domain>_fwd.h`.
- Add or modify config declarations only in `common/config.h` or a config fragment it includes.
- Prefer including `common/config_<domain>_fwd.h` instead of `common/config.h` in headers and reusable `.cpp` files.
- Reuse an existing domain header when the config fits that domain; otherwise add a new generated domain header through
  the manifest instead of adding an ad-hoc manual declaration header.
- `common/config.cpp` is the definition owner and should continue to include `configbase_impl.h` before `config.h`.

### How to add a new config

From the repo root:

1. Add the new `CONF_*` entry in `be/src/common/config.h` or an existing local config fragment.
2. Decide whether the config should be exposed through an existing `config_<domain>_fwd.h`.
3. If needed, update `build-support/config_fwd_headers_manifest.json`.
   - Add the config name to an existing header entry if the domain already fits.
   - Add a new header entry only when the config does not fit an existing domain cleanly.
4. Regenerate forward headers:

```bash
python3 build-support/gen_config_fwd_headers.py
```

5. Update consumers to include the appropriate `common/config_<domain>_fwd.h` instead of `common/config.h`.
6. Run the guardrails:

```bash
python3 build-support/gen_config_fwd_headers.py --check
bash build-support/check_common_config_header_includes.sh
```

### When a direct `common/config.h` include is acceptable

The preferred answer is "almost never" for new code.

Current legacy direct includes are tracked in the allowlist enforced by
`build-support/check_common_config_header_includes.sh`, but new direct includes should be treated as exceptions that
need strong justification. If you think a new direct include is necessary, first check whether:

- an existing `config_<domain>_fwd.h` can be reused
- the manifest can be extended
- a new generated domain header would be cleaner

The migration path is to keep shrinking the allowlist over time, not to add new long-term direct includes.
