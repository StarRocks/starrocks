# Pipeline Operator Runtime Access Introduction Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents are explicitly authorized) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a minimal `OperatorRuntimeAccess` seam so the base `Operator` implementation and `PipelineDriver` stop depending directly on `OperatorFactory`, while deferring `OperatorSpec`, `OperatorBinding`, and `SourceOperator` cleanup to later stage-2 leaves.

**Architecture:** Start from the rebased baseline instead of the pre-rebase primitive-header sketch. `be/src/exec/pipeline/operator_factory.h/.cpp` already exists as a separate home for factory-owned behavior, and `runtime_filter_core_types.h` already carries the observer-free RF core types. This leaf should add one small runtime-access interface that covers only the runtime-filter and filter-null-value capabilities currently consumed by `operator.cpp` and `pipeline_driver.cpp`. Make `OperatorFactory` implement that interface without moving `_tuple_ids`, `_row_desc`, `_tuple_slot_mappings`, `_state`, or source-specific plumbing yet. Then route base `Operator` logic and `PipelineDriver` through the new seam, while keeping `get_factory()` as a temporary compatibility path for unmigrated subclasses.

**Tech Stack:** C++, StarRocks BE pipeline engine, CMake/Ninja

---

## File Map

### Create
- `be/src/exec/pipeline/primitives/operator_runtime_access.h` — narrow runtime-access seam for base `Operator` and `PipelineDriver`

### Modify
- `be/src/exec/pipeline/operator.h` — store and expose the narrow runtime-access seam for base behavior
- `be/src/exec/pipeline/operator.cpp` — move base runtime-filter and filter-null-value logic off direct `_factory` reach-back
- `be/src/exec/pipeline/operator_factory.h` — implement `OperatorRuntimeAccess` while keeping binding metadata and `RuntimeState` ownership where it is today
- `be/src/exec/pipeline/pipeline_driver.cpp` — stop reading RF wait state and bloom-filter descriptors through `op->get_factory()`

### Modify Only If Compile Forces It
- `be/src/exec/pipeline/operator_factory.cpp`
- `be/src/exec/query_cache/conjugate_operator.h`
- `be/src/exec/query_cache/conjugate_operator.cpp`
- `be/src/exec/query_cache/multilane_operator.h`
- `be/src/exec/query_cache/multilane_operator.cpp`

### Do Not Modify In This Leaf Unless Compile Forces It
- `be/src/exec/pipeline/source_operator.h`
- `be/src/exec/pipeline/source_operator.cpp`
- `be/src/exec/exec_node.h`
- `be/src/exec/exec_node_executor.cpp`
- `be/src/exec/pipeline/scan/*`
- `be/src/exec/spill/operator_mem_resource_manager.cpp`

### Verification
- `rg -n "get_factory\\(|_factory->" be/src/exec/pipeline/operator.cpp be/src/exec/pipeline/pipeline_driver.cpp`
- `ninja -C be/build_Release pipeline_primitives_exec_objs pipeline_runtime_compat_exec_objs -j10`
- Preferred broad validation: `./build.sh --be`
- Known local fallback note: if the existing Darwin Arrow Flight header gap blocks the broad build, record that and rely on the focused Ninja target result for this leaf

## Contract Rules For This Leaf

- `OperatorRuntimeAccess` must stay minimal. It is not a renamed `OperatorFactory`.
- Include only the runtime-owned capabilities currently needed by base `Operator` and `PipelineDriver`:
  - `rf_waiting_set()`
  - `prepare_runtime_in_filters(RuntimeState*)`
  - `get_colocate_runtime_in_filters(size_t)`
  - `get_runtime_in_filters()`
  - `get_runtime_bloom_filters()`
  - `get_filter_null_value_columns()`
- Do not include `runtime_state()`, `row_desc()`, `_tuple_ids`, `_tuple_slot_mappings`, or other binding metadata in this leaf. Those belong to a later `OperatorBinding`-style cleanup.
- Do not introduce `OperatorSpec` yet. Keep `id`, `name`, `plan_node_id`, `is_subordinate`, and `driver_sequence` flowing through existing constructor arguments for now.
- Do not widen into `SourceOperatorFactory` concerns such as DOP, grouped-pipeline state, observer plumbing, adaptive state, or morsel ownership.
- Do not remove `get_factory()` or delete `_factory` storage unless all existing subclass callers in this checkout have been migrated. In this leaf, `_factory` is compatibility-only.

---

### Task 1: Define the Exact `OperatorRuntimeAccess` Surface

**Files:**
- Create: `be/src/exec/pipeline/primitives/operator_runtime_access.h`
- Modify: `be/src/exec/pipeline/operator_factory.h`

- [ ] **Step 1: Lock the minimal method set from current call sites**

Use the existing rebased call sites as the source of truth:
- `be/src/exec/pipeline/operator.cpp`
- `be/src/exec/pipeline/pipeline_driver.cpp`

The interface should cover only what those files consume today for runtime-filter and filter-null-value behavior. Explicitly exclude:
- `runtime_state()`
- `row_desc()`
- tuple-id and tuple-slot-mapping access
- source-specific DOP, observer, grouped-pipeline, or morsel APIs

- [ ] **Step 2: Create `primitives/operator_runtime_access.h`**

Add a small pure-virtual interface. Keep the first-cut method names aligned with the current factory surface so the migration stays review-sized.

Recommended shape:
```cpp
#pragma once

#include <cstddef>
#include <vector>

#include "exec/pipeline/runtime_filter_core_types.h"

namespace starrocks {
class ExprContext;
class RuntimeState;

namespace pipeline {

using LocalRFWaitingSet = std::set<TPlanNodeId>;

class OperatorRuntimeAccess {
public:
    virtual ~OperatorRuntimeAccess() = default;

    virtual const LocalRFWaitingSet& rf_waiting_set() const = 0;
    virtual std::vector<ExprContext*> get_colocate_runtime_in_filters(size_t driver_sequence) = 0;
    virtual void prepare_runtime_in_filters(RuntimeState* state) = 0;
    virtual const std::vector<ExprContext*>& get_runtime_in_filters() const = 0;
    virtual RuntimeFilterProbeCollector* get_runtime_bloom_filters() = 0;
    virtual const RuntimeFilterProbeCollector* get_runtime_bloom_filters() const = 0;
    virtual const std::vector<SlotId>& get_filter_null_value_columns() const = 0;
};

} // namespace pipeline
} // namespace starrocks
```

If one of these declarations needs lighter includes or a forward declaration adjustment, keep that change local to the interface rather than widening the seam.

- [ ] **Step 3: Make `OperatorFactory` implement the interface**

Update `operator_factory.h` so:
- `OperatorFactory` inherits `OperatorRuntimeAccess`
- the existing RF/filter-null methods satisfy the interface directly where possible
- `get_runtime_in_filters()` has a const-returning path suitable for the interface

Do not move `_tuple_ids`, `_row_desc`, `_tuple_slot_mappings`, `_runtime_filter_hub`, `_runtime_filter_collector`, or `_state` out of `OperatorFactory` in this task. This leaf is only about the access seam.

- [ ] **Step 4: Commit**

```bash
git add be/src/exec/pipeline/primitives/operator_runtime_access.h \
  be/src/exec/pipeline/operator_factory.h
git commit -m "add pipeline operator runtime access seam"
```

---

### Task 2: Route Base `Operator` Through `OperatorRuntimeAccess`

**Files:**
- Modify: `be/src/exec/pipeline/operator.h`
- Modify: `be/src/exec/pipeline/operator.cpp`

- [ ] **Step 1: Add runtime-access storage without deleting compatibility storage**

Update `Operator` so the base class owns a narrow runtime-access pointer for base behavior.

Required outcome:
- base methods no longer call `_factory->...` for runtime-filter or filter-null-value behavior
- `_factory` may remain as compatibility-only storage for unmigrated subclasses

Do not attempt to remove `get_factory()` in this task.

- [ ] **Step 2: Keep constructor arguments scalar for now**

Do not introduce `OperatorSpec` or `OperatorBinding`.

Keep these constructor inputs as they are today:
- `id`
- `name`
- `plan_node_id`
- `is_subordinate`
- `driver_sequence`

The runtime-access seam should be layered around the current constructor flow, not coupled to a larger metadata repackaging.

- [ ] **Step 3: Move base runtime-filter logic onto the seam**

Update these methods in `operator.cpp` to use the runtime-access pointer instead of `_factory`:
- `set_precondition_ready(RuntimeState*)`
- `close(RuntimeState*)`
- `global_rf_wait_timeout_ns() const`
- `eval_runtime_bloom_filters(Chunk*)`

If the driver migration is cleaner with a couple of narrow `Operator` wrapper accessors, add only the smallest helpers needed:
- `rf_waiting_set() const`
- `runtime_bloom_filters() const`

Do not reintroduce broad accessors such as `runtime_state()` or `row_desc()`.

- [ ] **Step 4: Keep the base contract honest**

After the refactor, inspect `operator.h` and `operator.cpp` to ensure:
- base `Operator` logic no longer depends on concrete `OperatorFactory`
- no new source-specific or binding-metadata APIs leaked into the base class
- `get_factory()` is explicitly compatibility-only in comments or nearby documentation

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/operator.h \
  be/src/exec/pipeline/operator.cpp
git commit -m "route pipeline operator base through runtime access"
```

---

### Task 3: Remove `PipelineDriver` Reach-Back Through `get_factory()`

**Files:**
- Modify: `be/src/exec/pipeline/pipeline_driver.cpp`

- [ ] **Step 1: Replace direct factory reach-back in driver preparation**

Update the driver setup path so it no longer reads:
- `op->get_factory()->rf_waiting_set()`
- `op->get_factory()->get_runtime_bloom_filters()`

Use the narrow operator/runtime-access seam added in Task 2 instead.

- [ ] **Step 2: Keep runtime-filter observer behavior unchanged**

Do not alter:
- local RF holder subscription behavior
- global RF descriptor observer registration
- wait-timeout computation semantics

This task is a dependency-direction cleanup only.

- [ ] **Step 3: Leave subclass and family call sites for later leaves**

Do not widen into:
- scan operators still using `get_factory()->runtime_state()`
- spill or query-cache wrappers
- source-operator factory plumbing

If a compile fix outside `pipeline_driver.cpp` becomes necessary, keep it minimal and note why it was required.

- [ ] **Step 4: Commit**

```bash
git add be/src/exec/pipeline/pipeline_driver.cpp
git commit -m "stop pipeline driver reaching back through operator factories"
```

---

### Task 4: Run Focused Verification and Close the Leaf

**Files:**
- Create/Modify: none beyond the files above unless compile fixes forced minimal follow-ups

- [ ] **Step 1: Verify the intended dependency cut**

Run:
```bash
rg -n "get_factory\\(|_factory->" \
  be/src/exec/pipeline/operator.cpp \
  be/src/exec/pipeline/pipeline_driver.cpp
```

Expected:
- no remaining `_factory->` calls in `operator.cpp`
- no remaining `op->get_factory()` calls in `pipeline_driver.cpp`

`operator.h` may still expose `get_factory()` as compatibility-only in this leaf.

- [ ] **Step 2: Build the focused pipeline targets**

Run:
```bash
ninja -C be/build_Release pipeline_primitives_exec_objs pipeline_runtime_compat_exec_objs -j10
```

Expected: the rebased staged target graph still compiles after the base seam change.

- [ ] **Step 3: Run the preferred broad build if the environment supports it**

Run:
```bash
./build.sh --be
```

If the known local Arrow Flight header gap still blocks this build, record that explicitly and do not widen the leaf to chase unrelated service build fallout.

- [ ] **Step 4: Commit final leaf state**

```bash
git add be/src/exec/pipeline/primitives/operator_runtime_access.h \
  be/src/exec/pipeline/operator.h \
  be/src/exec/pipeline/operator.cpp \
  be/src/exec/pipeline/operator_factory.h \
  be/src/exec/pipeline/pipeline_driver.cpp
git commit -m "introduce pipeline operator runtime access seam"
```

---

## Acceptance Criteria

- `be/src/exec/pipeline/primitives/operator_runtime_access.h` exists and defines only the minimal runtime-access surface used by base `Operator` and `PipelineDriver`.
- `OperatorFactory` implements that interface without moving binding metadata or `RuntimeState` ownership in this leaf.
- Base `Operator` logic in `operator.cpp` no longer reaches runtime-filter or filter-null-value behavior through `_factory`.
- `PipelineDriver` no longer reads RF wait state or bloom-filter descriptors through `op->get_factory()`.
- `get_factory()` may remain present, but only as a temporary compatibility path for subclass and family code outside this leaf.
- `OperatorSpec`, `OperatorBinding`, `SourceOperator` cleanup, and `runtime_state()`/`row_desc()` decoupling remain deferred.

## Known Review Risks

- If `OperatorRuntimeAccess` grows to include `runtime_state()`, `row_desc()`, tuple bindings, or source plumbing in this leaf, it becomes a renamed `OperatorFactory` and defeats the cut.
- Deleting `_factory` storage too early will force broad scan/spill/query-cache rewrites and make this opening leaf non-reviewable.
- Reopening `SourceOperatorFactory` ownership in this leaf will mix base runtime-access cleanup with grouped-pipeline, observer, adaptive, and morsel redesign.
