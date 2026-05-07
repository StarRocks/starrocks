# Backend Domain

## Purpose

Map the BE development surface for execution, storage, runtime, services, and the current structural harness work.

## Entrypoints

- [`be/AGENTS.md`](../../be/AGENTS.md)
- [`handbook/architecture/be-boundary-harness.md`](../architecture/be-boundary-harness.md)
- [`be/module_boundary_manifest.json`](../../be/module_boundary_manifest.json)
- [`build-support/check_be_module_boundaries.py`](../../build-support/check_be_module_boundaries.py)

## Commands

- `./build.sh --be`
- `./run-be-ut.sh --build-target <test_binary> --module <test_binary> --without-java-ext`
- `python3 build-support/check_be_module_boundaries.py --mode full`
- `python3 build-support/render_be_agents.py --check`

## Guardrails

- The BE module boundary manifest is the source of truth for the current architectural lattice.
- Reviewed legacy debt in `build-support/be_module_boundary_baseline.json` is shrink-only.
- BE config or metric changes must update matching public docs.

## Metrics Ownership

- `be/src/base/metrics.h` owns only low-level metric primitives such as `Metric`, `MetricRegistry`, labels, visitors, and hooks.
- `be/src/common/metrics/process_metrics_registry.h` is the dependency-neutral owner for BE/CN process metric registries. Keep it free of concrete storage, exec, runtime, service, HTTP, cache, and connector includes.
- New module metrics should be defined in the owning module and installed by top-level composition code.

## Test and Validation

- Prefer the smallest relevant core test binary before broader `run-be-ut.sh`.
- Run the boundary harness whenever BE layering, owned files, or generated AGENTS content changes.
- Use `be/src/common/AGENTS.md` for the config-forward-header workflow.
- Run `bash build-support/check_be_metrics_header_includes.sh` when metric ownership or metric header includes change.

## Open Gaps

- FE-style structural boundaries do not exist yet.
- Eval registration and observability evidence are not standardized.
- Change-to-suite selection still depends on human judgment outside BE boundary checks.
