# ExecEnv Decomposition Multi-PR Rollout

- Status: active
- Owner: Backend Runtime
- Last Updated: 2026-04-04

## Summary

Stage the `ExecEnv` decomposition across a sequence of behavior-preserving PRs. Keep `ExecEnv` as a temporary composition root during the rollout, but stop using it as the general singleton access path. The end state is that `ExecEnv` owns a small set of internal service groups for wiring and lifetime management, while production code depends on `QueryExecutionServices`, `AdminServices`, or exact services. `ExecEnv` remains bootstrap and teardown glue only.

## Context Model

- Internal owner groups inside `ExecEnv`: `ExecutionEnv`, `RpcServices`, `LakeServices`, `RuntimeServices`, and `AgentServices`.
- Public composite dependency objects for broad call surfaces: `QueryExecutionServices` and `AdminServices`.
- Leaf infrastructure should prefer exact dependencies over any composite context.
- Do not replace `ExecEnv` with a single large public service context.

## Acceptance Criteria

- PR1 introduces internal service-group scaffolding, public composite-context scaffolding, and a mechanical guardrail that blocks new production uses of `runtime/exec_env.h` and `ExecEnv::GetInstance()`.
- PR2 removes reverse references inside shared infra leaf services.
- PR3 moves `RuntimeState` and query setup from `ExecEnv*` to `QueryExecutionServices*`.
- PR4 migrates query-path consumers off `RuntimeState::exec_env()`.
- PR5 migrates storage, lake, cache, connector, fs, and related util code off `ExecEnv` to exact dependencies, with `LakeServices` and `RpcServices` used only as internal grouping and migration glue where needed.
- PR6 migrates service, http, and agent surfaces to `AdminServices` and exact dependencies.
- PR7 leaves `ExecEnv` bootstrap-only and tightens the guardrail and baseline.
- Each PR is behavior-preserving, passes the BE boundary checks, and does not expand the `exec_env` allowlist or baseline.

## PR Breakdown

### PR1: Add ExecEnv guardrails and context scaffolding

Add the internal owner groups `ExecutionEnv`, `RpcServices`, `LakeServices`, `RuntimeServices`, and `AgentServices`, plus the public composites `QueryExecutionServices` and `AdminServices`. Wire them in `exec_env.*`. Extend the BE boundary tooling with a focused `exec_env` usage check and shrink-only allowlists.

### PR2: Remove reverse refs inside shared infra services

Convert shared infra leaves to exact dependencies instead of `ExecEnv`: `BrpcStubCache`, `ProfileReportWorker`, default workgroup initialization, and `LoadChannelMgr`.

### PR3: Introduce QueryExecutionServices into runtime state

Change `RuntimeState`, `QueryContext`, fragment setup, and related construction paths so query execution carries `QueryExecutionServices*` instead of `ExecEnv*`.

### PR4: Migrate exec/runtime query consumers

Update query-path consumers in `be/src/exec/**` and query-execution code in `be/src/runtime/**` to use `QueryExecutionServices`. Remove production use of `state->exec_env()` except any temporary compatibility points explicitly needed by later PRs.

### PR5: Migrate storage-side dependencies

Move storage, lake, cache, connector, fs, and related util code from `ExecEnv` access to exact dependencies. Use `LakeServices` and `RpcServices` as internal grouping and migration glue, not as the default public dependency type. Eliminate direct `ExecEnv` usage for lake managers, BRPC stubs, PK-index pools, spill-dir access, and store-path access.

### PR6: Migrate service, http, and agent surfaces

Replace stored `ExecEnv*` in service, http, and agent code with `AdminServices&` plus exact service references where needed. Keep startup wiring in service bootstrap code.

### PR7: Tighten enforcement and trim ExecEnv

Remove compatibility accessors that are no longer needed. Shrink the checker baseline to bootstrap and tests only. Keep `ExecEnv` responsible only for construction order, lifetime ordering, and teardown.

## Common Verification

- `python3 build-support/check_be_module_boundaries.py --mode full`
- `python3 build-support/render_be_agents.py --check`
- `./build.sh --be`

Additional verification for PR3 and PR4:

- `./run-be-ut.sh --build-target runtime_core_test --module runtime_core_test --without-java-ext`
- `./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext`

## Decision Log

- 2026-04-04: Use an incremental multi-PR rollout instead of a hard cut to keep each PR behavior-preserving and reviewable.
- 2026-04-04: Do not replace `ExecEnv` with another mega-registry; keep owner service groups internal and expose only `QueryExecutionServices`, `AdminServices`, or exact services to production code.
- 2026-04-04: Add mechanical `exec_env` guardrails in PR1 because the current BE harness does not yet cover all higher-level directories.
