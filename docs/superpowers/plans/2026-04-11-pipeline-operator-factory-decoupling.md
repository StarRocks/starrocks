# Pipeline Operator/Factory Decoupling Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents are explicitly authorized) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the pipeline operator stack so `OperatorFactory` becomes a builder/shared-state owner, while `Operator` and `SourceOperator` runtime instances no longer depend on their factories after construction.

**Architecture:** Use an additive-to-subtractive rollout. First introduce primitive value types and runtime-owned hook interfaces next to the current classes, then migrate the base `Operator` and `SourceOperator` contracts plus low-risk operator families onto those seams, and only after the adoption path is proven remove `_factory` / `_source_factory()` reach-back and tighten boundary rules. Keep this work inside stage 2 of the existing pipeline-layering rollout; do not widen into graph ownership, fragment/query lifetime redesign, or broad family-state rewrites unless a concrete compile blocker forces a narrow follow-up seam.

**Tech Stack:** C++, StarRocks BE pipeline engine, CMake/Ninja, BE boundary harness

---

## File Map

### Create
- `be/src/exec/pipeline/primitives/execution_context.h` — define per-driver execution context passed into operators after construction
- `be/src/exec/pipeline/primitives/operator_state.h` — define `OperatorSpec`, `SourceOperatorSpec`, and shared-state handle types
- `be/src/exec/pipeline/primitives/runtime_services.h` — define narrow runtime-owned service interfaces for runtime filters, runtime state access, spill/profile helpers, and source notifications
- `be/src/exec/pipeline/operator_runtime_services.h` — declare runtime-owned hook implementations/adapters that satisfy `primitives/runtime_services.h`
- `be/src/exec/pipeline/operator_runtime_services.cpp` — implement hook/adaptor objects without changing operator semantics

### Modify (base contracts)
- `be/src/exec/pipeline/operator.h`
- `be/src/exec/pipeline/operator.cpp`
- `be/src/exec/pipeline/operator_factory.h`
- `be/src/exec/pipeline/operator_factory.cpp`
- `be/src/exec/pipeline/source_operator.h`
- `be/src/exec/pipeline/source_operator.cpp`
- `be/src/exec/pipeline/pipeline_fwd.h`
- `be/src/exec/CMakeLists.txt`

### Modify (runtime/filter plumbing)
- `be/src/exec/exec_node.h`
- `be/src/exec/exec_node_executor.cpp`
- `be/src/exec/pipeline/pipeline_driver.cpp`
- `be/src/exec/pipeline/pipeline.cpp`
- `be/src/exec/pipeline/fragment_executor.cpp`

### Modify (first proving slice: simple operators)
- `be/src/exec/pipeline/limit_operator.h`
- `be/src/exec/pipeline/offset_operator.h`
- `be/src/exec/pipeline/noop_sink_operator.h`
- `be/src/exec/pipeline/project_operator.cpp`
- `be/src/exec/pipeline/project_operator.h`
- `be/src/exec/pipeline/select_operator.cpp`
- `be/src/exec/pipeline/select_operator.h`
- `be/src/exec/pipeline/chunk_accumulate_operator.cpp`
- `be/src/exec/pipeline/chunk_accumulate_operator.h`
- `be/src/exec/pipeline/assert_num_rows_operator.h`

### Modify (source and wrapper proving slice)
- `be/src/exec/pipeline/fetch_sink_operator.h`
- `be/src/exec/pipeline/lookup_operator.h`
- `be/src/exec/pipeline/wait_operator.cpp`
- `be/src/exec/pipeline/bucket_process_operator.h`
- `be/src/exec/pipeline/bucket_process_operator.cpp`
- `be/src/exec/pipeline/scan/scan_operator.h`
- `be/src/exec/pipeline/scan/scan_operator.cpp`
- `be/src/exec/pipeline/scan/chunk_source.cpp`

### Modify (stateful families after seam is proven)
- `be/src/exec/pipeline/exchange/*`
- `be/src/exec/pipeline/scan/*`
- `be/src/exec/pipeline/sink/*`
- `be/src/exec/pipeline/set/*`
- `be/src/exec/pipeline/hashjoin/*`
- `be/src/exec/pipeline/nljoin/*`
- `be/src/exec/pipeline/aggregate/*`
- `be/src/exec/pipeline/sort/*`
- `be/src/exec/query_cache/*`
- `be/src/exec/spill/operator_mem_resource_manager.cpp`

### Modify (boundary activation and sync)
- `be/module_boundary_manifest.json`
- `be/AGENTS.md`

### Verification
- `./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext`
- `python3 build-support/check_be_module_boundaries.py --mode full`
- `python3 build-support/render_be_agents.py --check`
- Preferred broad build: `./build.sh --be`
- Environment fallback when the known Darwin Arrow Flight header gap blocks the broad build: `ninja -C be/build_Release PipelineRuntimeCompat PipelineOperatorsBatch PipelineExtensions Exec -j10`

## Guardrails

- Do not remove `OperatorFactory` until its shared-state and hook-population responsibilities have exact replacements.
- Do not push source-specific group/morsel/adaptive state into generic `Operator` primitives.
- Do not force a flag day across all operator families; keep the compatibility shim until the proving slices are green.
- Do not activate strict pipeline boundary rules until `_factory` and `_source_factory()` are gone from the primitive surface.
- Treat any remaining runtime dependency gap as a missing hook interface, not as a reason to let operators reach back into `QueryContext`, `FragmentContext`, or the factory again.

---

### Task 1: Introduce Primitive Execution Types

**Files:**
- Create: `be/src/exec/pipeline/primitives/execution_context.h`
- Create: `be/src/exec/pipeline/primitives/operator_state.h`
- Create: `be/src/exec/pipeline/primitives/runtime_services.h`
- Modify: `be/src/exec/pipeline/pipeline_fwd.h`
- Modify: `be/src/exec/CMakeLists.txt`

- [ ] **Step 1: Define `OperatorSpec` and source-specific metadata**

Add data-only structs for the immutable metadata currently split between operator constructors and factories:
- `OperatorSpec` with `id`, `name`, `plan_node_id`, `is_subordinate`
- `SourceOperatorSpec` for source-only immutable metadata that truly belongs with the runtime instance, not the factory
- small enums or tags only when they remove branching from callers; do not invent a taxonomy the code does not need

- [ ] **Step 2: Define per-driver execution context**

Add a small per-driver context type with `driver_sequence`, `degree_of_parallelism`, and other data that should be available after construction without asking the factory again.

- [ ] **Step 3: Define shared-state and runtime-service handles**

In `primitives/operator_state.h` and `primitives/runtime_services.h`, define the base types that will replace runtime factory reach-back:
- generic shared-state handle base for operator-family cross-driver state
- runtime-service interfaces for runtime filters, runtime state access, spill/profile helpers, and source notifications

- [ ] **Step 4: Keep the new headers forward-declaration friendly**

Update `pipeline_fwd.h` and includes so the primitive headers do not pull in `query_context.h`, `fragment_context.h`, scheduler observer headers, or scan/workgroup headers.

- [ ] **Step 5: Compile the primitive layer**

Run: `ninja -C be/build_Release PipelineRuntimeCompat Exec -j10`

Expected: the new primitive headers compile cleanly and no heavy runtime headers leak back into the primitive include path.

- [ ] **Step 6: Commit**

```bash
git add be/src/exec/pipeline/primitives/execution_context.h \
  be/src/exec/pipeline/primitives/operator_state.h \
  be/src/exec/pipeline/primitives/runtime_services.h \
  be/src/exec/pipeline/pipeline_fwd.h \
  be/src/exec/CMakeLists.txt
git commit -m "introduce pipeline primitive execution types"
```

---

### Task 2: Extract Runtime-Owned Hook Implementations

**Files:**
- Create: `be/src/exec/pipeline/operator_runtime_services.h`
- Create: `be/src/exec/pipeline/operator_runtime_services.cpp`
- Modify: `be/src/exec/pipeline/operator_factory.h`
- Modify: `be/src/exec/pipeline/operator_factory.cpp`
- Modify: `be/src/exec/exec_node.h`
- Modify: `be/src/exec/exec_node_executor.cpp`

- [ ] **Step 1: Add runtime hook implementations that wrap current factory-owned behavior**

Move runtime-filter caching/binding, runtime-state access, and any other runtime-only helper logic behind implementations of the new `primitives/runtime_services.h` interfaces. Keep the logic behavior-preserving by delegating to existing code paths first.

- [ ] **Step 2: Move runtime-filter initialization off the public factory contract**

Replace `ExecNode::init_runtime_filter_for_operator(OperatorFactory* ...)` with a path that initializes either:
- explicit shared runtime-filter state, or
- runtime hook objects created by the factory at build time

The end state for this task is: `ExecNode` no longer teaches the execution tree that runtime-filter state lives on factories.

- [ ] **Step 3: Limit factory responsibility to build-time population**

Update `OperatorFactory` so it prepares shared state and builds the hook bundle used by operators, instead of exposing broad runtime accessors for arbitrary runtime calls.

- [ ] **Step 4: Preserve compatibility for still-unmigrated operators**

Keep a temporary compatibility path so existing operators can still be constructed while later tasks migrate their runtime accesses. This shim must be explicitly temporary and isolated to factory-side creation.

- [ ] **Step 5: Compile the runtime-hook extraction**

Run: `ninja -C be/build_Release PipelineRuntimeCompat Exec -j10`

Expected: runtime-filter and runtime-state plumbing still compiles, and the new hook interfaces satisfy the old behavior.

- [ ] **Step 6: Commit**

```bash
git add be/src/exec/pipeline/operator_runtime_services.h \
  be/src/exec/pipeline/operator_runtime_services.cpp \
  be/src/exec/pipeline/operator_factory.h \
  be/src/exec/pipeline/operator_factory.cpp \
  be/src/exec/exec_node.h \
  be/src/exec/exec_node_executor.cpp
git commit -m "extract runtime-owned pipeline operator hooks"
```

---

### Task 3: Slim `Operator` to Consume Injected State and Hooks

**Files:**
- Modify: `be/src/exec/pipeline/operator.h`
- Modify: `be/src/exec/pipeline/operator.cpp`
- Modify: `be/src/exec/pipeline/pipeline.cpp`
- Modify: `be/src/exec/pipeline/pipeline_driver.cpp`
- Modify: `be/src/exec/spill/operator_mem_resource_manager.cpp`

- [ ] **Step 1: Add constructor input bundle for operators**

Change `Operator` construction to take explicit immutable spec, per-driver execution context, shared-state handle, and runtime-service interface references (directly or through a small bundle struct).

- [ ] **Step 2: Move `Operator` runtime logic off `_factory`**

Convert the current `Operator` base implementation to use injected runtime hooks for:
- runtime-filter wait/bind work
- runtime bloom-filter access
- filter-null-value metadata
- runtime-state access used by generic helpers
- spill/profile helper access

- [ ] **Step 3: Stop driver code from depending on `op->get_factory()` for base metadata**

Update `PipelineDriver` and any shared helper code to read dependency/filter metadata through explicit operator state or hooks rather than `op->get_factory()`.

- [ ] **Step 4: Keep a temporary compatibility accessor only where migration is incomplete**

If `_factory` must survive this task for later family adoption, mark it compatibility-only and remove all uses from the base class implementation itself. No new callers should be added after this step lands.

- [ ] **Step 5: Verify compile and focused UT**

Run:
```bash
ninja -C be/build_Release PipelineRuntimeCompat PipelineOperatorsBatch Exec -j10
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
```

Expected: base operator/runtime paths compile and the focused core UT loop remains green.

- [ ] **Step 6: Commit**

```bash
git add be/src/exec/pipeline/operator.h \
  be/src/exec/pipeline/operator.cpp \
  be/src/exec/pipeline/pipeline.cpp \
  be/src/exec/pipeline/pipeline_driver.cpp \
  be/src/exec/spill/operator_mem_resource_manager.cpp
git commit -m "slim pipeline operator base away from factory reach-back"
```

---

### Task 4: Slim `SourceOperator` and `SourceOperatorFactory`

**Files:**
- Modify: `be/src/exec/pipeline/source_operator.h`
- Modify: `be/src/exec/pipeline/source_operator.cpp`
- Modify: `be/src/exec/pipeline/fragment_executor.cpp`
- Modify: `be/src/exec/pipeline/scan/scan_operator.h`
- Modify: `be/src/exec/pipeline/scan/scan_operator.cpp`
- Modify: `be/src/exec/pipeline/scan/chunk_source.cpp`

- [ ] **Step 1: Separate source runtime metadata from source factory ownership**

Move DOP, grouped-pipeline dependencies, source observables, and morsel/runtime source notifications into explicit source runtime state/hooks. Keep only build-time source configuration on `SourceOperatorFactory`.

- [ ] **Step 2: Remove `_source_factory()` from `SourceOperator` runtime logic**

Update `SourceOperator` so `prepare()`, observer registration, `degree_of_parallelism()`, and `group_dependent_pipelines()` operate through injected source runtime state/hooks rather than casting `_factory`.

- [ ] **Step 3: Convert scan-side callers first**

Adopt the new source runtime seam in `scan_operator.*` and `chunk_source.cpp`, because those are the heaviest current users of `_source_factory()` and runtime factory access.

- [ ] **Step 4: Update fragment/pipeline bootstrap to populate source runtime state**

Adjust the pipeline bootstrap path so source runtime metadata is created once during pipeline instantiation and attached to each source operator instance explicitly.

- [ ] **Step 5: Verify compile and focused UT**

Run:
```bash
ninja -C be/build_Release PipelineRuntimeCompat PipelineOperatorsBatch Exec -j10
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
```

Expected: `SourceOperator` no longer needs `_source_factory()` in its base implementation, and scan/source compilation stays green.

- [ ] **Step 6: Commit**

```bash
git add be/src/exec/pipeline/source_operator.h \
  be/src/exec/pipeline/source_operator.cpp \
  be/src/exec/pipeline/fragment_executor.cpp \
  be/src/exec/pipeline/scan/scan_operator.h \
  be/src/exec/pipeline/scan/scan_operator.cpp \
  be/src/exec/pipeline/scan/chunk_source.cpp
git commit -m "separate source runtime state from source factories"
```

---

### Task 5: Prove the Seam on Simple Core Operators

**Files:**
- Modify: `be/src/exec/pipeline/limit_operator.h`
- Modify: `be/src/exec/pipeline/offset_operator.h`
- Modify: `be/src/exec/pipeline/noop_sink_operator.h`
- Modify: `be/src/exec/pipeline/project_operator.h`
- Modify: `be/src/exec/pipeline/project_operator.cpp`
- Modify: `be/src/exec/pipeline/select_operator.h`
- Modify: `be/src/exec/pipeline/select_operator.cpp`
- Modify: `be/src/exec/pipeline/chunk_accumulate_operator.h`
- Modify: `be/src/exec/pipeline/chunk_accumulate_operator.cpp`
- Modify: `be/src/exec/pipeline/assert_num_rows_operator.h`

- [ ] **Step 1: Convert constructors to the injected operator context**

Update the low-risk operators so they consume explicit spec, shared state, and runtime hooks. Use `LimitOperator` as the reference pattern for shared cross-driver state, but move that shared state behind the common shared-state seam rather than a raw factory field.

- [ ] **Step 2: Remove `get_factory()` use from this slice**

Any operator in this slice should be fully off `get_factory()` by the end of the task. If a missing capability appears, add the narrowest hook needed and update `primitives/runtime_services.h`.

- [ ] **Step 3: Keep factories as pure builders in this slice**

Update the matching factories so they:
- prepare shared state
- create operator instances with explicit constructor inputs
- stop exposing runtime data that the migrated operators no longer need

- [ ] **Step 4: Verify the proving slice**

Run:
```bash
ninja -C be/build_Release PipelineOperatorsBatch Exec -j10
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
```

Expected: the simple-core slice compiles and runs without any remaining factory reach-back.

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/limit_operator.h \
  be/src/exec/pipeline/offset_operator.h \
  be/src/exec/pipeline/noop_sink_operator.h \
  be/src/exec/pipeline/project_operator.h \
  be/src/exec/pipeline/project_operator.cpp \
  be/src/exec/pipeline/select_operator.h \
  be/src/exec/pipeline/select_operator.cpp \
  be/src/exec/pipeline/chunk_accumulate_operator.h \
  be/src/exec/pipeline/chunk_accumulate_operator.cpp \
  be/src/exec/pipeline/assert_num_rows_operator.h
git commit -m "migrate simple pipeline operators to injected runtime seams"
```

---

### Task 6: Adopt Source/Wrapper and Runtime-Sensitive Families

**Files:**
- Modify: `be/src/exec/pipeline/fetch_sink_operator.h`
- Modify: `be/src/exec/pipeline/lookup_operator.h`
- Modify: `be/src/exec/pipeline/wait_operator.cpp`
- Modify: `be/src/exec/pipeline/bucket_process_operator.h`
- Modify: `be/src/exec/pipeline/bucket_process_operator.cpp`
- Modify: `be/src/exec/pipeline/exchange/*`
- Modify: `be/src/exec/pipeline/scan/*`
- Modify: `be/src/exec/pipeline/sink/*`

- [ ] **Step 1: Migrate wrapper and combinatorial operators**

Convert `wait`, `fetch`, `lookup`, and `bucket_process` to explicit operator/shared-state/hook inputs. These are the first places to prove that child-operator composition can work without runtime factory reach-back.

- [ ] **Step 2: Migrate source and sink families that still need runtime services**

Convert exchange, scan, and sink operators to use injected runtime hooks for runtime state, runtime filters, observer wakeups, and source notifications.

- [ ] **Step 3: Remove compatibility-only uses in this slice**

As each family migrates, delete its local use of `get_factory()` or `_source_factory()` rather than leaving stale shims behind.

- [ ] **Step 4: Verify compile**

Run:
```bash
ninja -C be/build_Release PipelineRuntimeCompat PipelineOperatorsBatch PipelineExtensions Exec -j10
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
```

Expected: all source/wrapper/runtime-sensitive families build against the injected seams.

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/fetch_sink_operator.h \
  be/src/exec/pipeline/lookup_operator.h \
  be/src/exec/pipeline/wait_operator.cpp \
  be/src/exec/pipeline/bucket_process_operator.h \
  be/src/exec/pipeline/bucket_process_operator.cpp \
  be/src/exec/pipeline/exchange \
  be/src/exec/pipeline/scan \
  be/src/exec/pipeline/sink
git commit -m "migrate runtime-sensitive pipeline operators off factory reach-back"
```

---

### Task 7: Finish Stateful Family Adoption and Remove Compatibility Shims

**Files:**
- Modify: `be/src/exec/pipeline/set/*`
- Modify: `be/src/exec/pipeline/hashjoin/*`
- Modify: `be/src/exec/pipeline/nljoin/*`
- Modify: `be/src/exec/pipeline/aggregate/*`
- Modify: `be/src/exec/pipeline/sort/*`
- Modify: `be/src/exec/query_cache/*`
- Modify: `be/src/exec/spill/operator_mem_resource_manager.cpp`
- Modify: `be/src/exec/pipeline/operator.h`
- Modify: `be/src/exec/pipeline/source_operator.h`
- Modify: `be/src/exec/pipeline/operator_factory.h`

- [ ] **Step 1: Migrate the remaining stateful families**

Adopt the new seams across join, set, aggregate, sort, spill-aware, and cache-adjacent families. Record any family-specific state that still belongs outside the base contract and keep it in family state/context objects.

- [ ] **Step 2: Delete `_factory` and `_source_factory()` from runtime base classes**

Once the migrated slices are clear, remove the compatibility backpointers and helper casts from the base runtime classes.

- [ ] **Step 3: Delete transitional factory runtime accessors that are no longer needed**

After the last caller is gone, remove any factory methods that existed only to support runtime reach-back.

- [ ] **Step 4: Verify shim removal**

Run:
```bash
rg -n "get_factory\\(|_source_factory\\(" be/src/exec/pipeline be/src/exec | sed -n '1,200p'
```

Expected: no remaining hits in migrated runtime base paths; any remaining family-specific uses must be explicitly called out for a follow-up seam before merge.

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/set \
  be/src/exec/pipeline/hashjoin \
  be/src/exec/pipeline/nljoin \
  be/src/exec/pipeline/aggregate \
  be/src/exec/pipeline/sort \
  be/src/exec/query_cache \
  be/src/exec/spill/operator_mem_resource_manager.cpp \
  be/src/exec/pipeline/operator.h \
  be/src/exec/pipeline/source_operator.h \
  be/src/exec/pipeline/operator_factory.h
git commit -m "remove pipeline operator factory reverse dependencies"
```

---

### Task 8: Activate Honest Boundary Rules and Verify

**Files:**
- Modify: `be/module_boundary_manifest.json`
- Modify: `be/AGENTS.md`

- [ ] **Step 1: Add the first honest pipeline primitive boundary rules**

Model the extracted lowest-layer shape honestly in `be/module_boundary_manifest.json`. `PipelinePrimitives` must reject direct dependency on `query_context.h`, `fragment_context.h`, `runtime/exec_env.h`, and legacy `exec/` planning headers.

- [ ] **Step 2: Regenerate and verify AGENTS sync**

Run:
```bash
python3 build-support/render_be_agents.py --write
python3 build-support/render_be_agents.py --check
```

- [ ] **Step 3: Run the boundary harness**

Run:
```bash
python3 build-support/check_be_module_boundaries.py --mode full
```

Expected: the new primitive boundary rules are enforceable against the post-migration code.

- [ ] **Step 4: Run preferred and fallback build verification**

Run preferred:
```bash
./build.sh --be
```

If the known local Arrow Flight header gap still blocks the full build, record that explicitly and run:
```bash
ninja -C be/build_Release PipelineRuntimeCompat PipelineOperatorsBatch PipelineExtensions Exec -j10
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
```

- [ ] **Step 5: Commit**

```bash
git add be/module_boundary_manifest.json be/AGENTS.md
git commit -m "activate pipeline primitive boundary rules"
```

---

## Acceptance Criteria

- `Operator` base runtime code no longer uses `OperatorFactory` after construction.
- `SourceOperator` base runtime code no longer casts back to `SourceOperatorFactory`.
- Runtime filters, runtime state access, spill/profile helpers, and source notifications flow through narrow runtime-owned interfaces or explicit shared state rather than factory reach-back.
- `OperatorFactory` is reduced to build-time shared-state preparation and operator construction.
- `ExecNode` no longer initializes runtime-filter behavior by mutating factory-owned runtime state.
- The first strict `PipelinePrimitives` boundary is active and passes the BE boundary harness.

## Known Risks To Watch

- `SourceOperatorFactory` cleanup can silently reintroduce graph/runtime coupling if grouped-pipeline state is moved to the wrong layer.
- Scan, exchange, and spill-aware families use runtime-state access heavily; if a runtime hook becomes too broad, stop and split it instead of turning the hook bundle into a hidden replacement for the old factory.
- Query-cache and combinatorial wrappers may expose missing shared-state semantics. Treat those as family-specific state follow-ups, not as reasons to widen the generic primitive contract.
