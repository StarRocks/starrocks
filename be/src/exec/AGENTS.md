# AGENTS.md - StarRocks Exec

Local guide for work under `be/src/exec`.

## Layering Rules

- Preserve acyclic dependency direction. Lower exec modules must not include factory/adaptor code or concrete implementation families above them.
- Prefer target ownership and include cleanup before physical file moves. Move files only after the destination target boundary is clear.
- Do not add new production includes of `runtime/exec_env.h` or new `ExecEnv::GetInstance()` call sites under `be/src/exec`; existing uses are shrink-only.
- Keep registry/plugin work above concrete implementation modules. Do not add self-registration or registry dependencies to lower layers.
- Add new `be/module_boundary_manifest.json` entries only when the target is clean enough for the harness to enforce honestly.

## Current Direction

- Keep `Exec` as the temporary compatibility umbrella while extracted modules move out of it.
- Treat existing targets such as `ExecCore`, `ExecSinkCore`, `SpillCore`, `ExecSchemaScannerCore`, `ExecSchemaScanners`, `ExecSortingCore`, and `ExecJoinCore` as the model for future extractions.
- Keep local or checkout-specific roadmap notes outside the committed guide unless they become shared repository policy.

## Validation

```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/render_be_agents.py --check
```

Run the smallest affected BE unit-test target before broader BE builds.
