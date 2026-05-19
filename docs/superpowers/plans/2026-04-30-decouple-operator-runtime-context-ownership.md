# Decouple Operator Runtime Context Ownership Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `OperatorRuntimeContext` ownership out of `OperatorFactory` so factories only reference runtime context managed by the pipeline runtime layer.

**Architecture:** `FragmentContext` owns an `OperatorRuntimeContextRegistry` whose lifetime matches pipelines, drivers, runtime filters, and the runtime filter hub. `OperatorFactory` keeps a non-owning `OperatorRuntimeContext*` and forwards legacy APIs during migration. `PipelineBuilderContext` attaches contexts centrally when building pipelines and when initializing runtime filters, so factory constructors do not become a big-bang edit.

**Tech Stack:** StarRocks BE C++20, pipeline runtime, existing CMake module split, focused BE unit tests.

---

### Task 1: Add Runtime Context Registry

**Files:**
- Create: `be/src/exec/pipeline/operator_runtime_context_registry.h`
- Create: `be/src/exec/pipeline/operator_runtime_context_registry.cpp`
- Modify: `be/src/exec/CMakeLists.txt`
- Test: `be/test/exec/pipeline/pipeline_control_flow_test.cpp`

- [ ] **Step 1: Add failing registry test**

Add a test that proves contexts are owned outside the factory:

```cpp
TEST(OperatorRuntimeContextRegistryTest, test_get_or_create_reuses_context_by_operator_id) {
    OperatorRuntimeContextRegistry registry;

    auto& first = registry.get_or_create(11, 22);
    auto& second = registry.get_or_create(11, 22);
    auto& third = registry.get_or_create(12, 22);

    ASSERT_EQ(&first, &second);
    ASSERT_NE(&first, &third);
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run:

```bash
./run-be-ut.sh --build-target starrocks_test --module starrocks_test --without-java-ext --gtest_filter 'OperatorRuntimeContextRegistryTest.*'
```

Expected: compile failure because `OperatorRuntimeContextRegistry` does not exist.

- [ ] **Step 3: Implement the registry**

Create `operator_runtime_context_registry.h`:

```cpp
#pragma once

#include <memory>
#include <unordered_map>

#include "exec/pipeline/operator_runtime_context.h"

namespace starrocks::pipeline {

class OperatorRuntimeContextRegistry {
public:
    OperatorRuntimeContext& get_or_create(int32_t operator_id, int32_t plan_node_id);
    OperatorRuntimeContext* find(int32_t operator_id);

private:
    std::unordered_map<int32_t, std::unique_ptr<OperatorRuntimeContext>> _contexts;
};

} // namespace starrocks::pipeline
```

Create `operator_runtime_context_registry.cpp`:

```cpp
#include "exec/pipeline/operator_runtime_context_registry.h"

namespace starrocks::pipeline {

OperatorRuntimeContext& OperatorRuntimeContextRegistry::get_or_create(int32_t operator_id, int32_t plan_node_id) {
    auto [it, inserted] = _contexts.emplace(operator_id, nullptr);
    if (inserted) {
        it->second = std::make_unique<OperatorRuntimeContext>(plan_node_id);
    }
    return *it->second;
}

OperatorRuntimeContext* OperatorRuntimeContextRegistry::find(int32_t operator_id) {
    auto it = _contexts.find(operator_id);
    return it == _contexts.end() ? nullptr : it->second.get();
}

} // namespace starrocks::pipeline
```

- [ ] **Step 4: Add registry source to CMake**

Add `pipeline/operator_runtime_context_registry.cpp` next to `pipeline/operator_runtime_context.cpp` in `PIPELINE_RUNTIME_SRCS`.

- [ ] **Step 5: Run focused registry test**

Run:

```bash
./run-be-ut.sh --build-target starrocks_test --module starrocks_test --without-java-ext --gtest_filter 'OperatorRuntimeContextRegistryTest.*'
```

Expected: PASS.

### Task 2: Make FragmentContext Own Runtime Contexts

**Files:**
- Modify: `be/src/exec/pipeline/fragment_context.h`
- Modify: `be/src/exec/pipeline/fragment_context.cpp` if include placement requires it

- [ ] **Step 1: Add registry to FragmentContext**

In `fragment_context.h`, include or forward declare `OperatorRuntimeContextRegistry`, then add:

```cpp
OperatorRuntimeContextRegistry& operator_runtime_contexts() { return _operator_runtime_contexts; }
const OperatorRuntimeContextRegistry& operator_runtime_contexts() const { return _operator_runtime_contexts; }
```

Add the member near `_runtime_filter_hub`:

```cpp
OperatorRuntimeContextRegistry _operator_runtime_contexts;
```

- [ ] **Step 2: Run syntax compile for FragmentContext**

Run:

```bash
ninja -C be/build_Release Exec -j10
```

Expected: either compile succeeds, or the current local ORC CMake blocker appears before compilation. If ORC blocks, run a syntax-only compile for `fragment_context.cpp` using the build tree compile command.

### Task 3: Convert OperatorFactory to Non-Owning Runtime Context

**Files:**
- Modify: `be/src/exec/pipeline/operator_factory.h`
- Modify: `be/src/exec/pipeline/operator_factory.cpp`
- Test: `be/test/exec/pipeline/pipeline_control_flow_test.cpp`

- [ ] **Step 1: Add failing test for external context attachment**

Add a test with `RuntimeAccessProbeFactory`:

```cpp
TEST(OperatorRuntimeAccessTest, test_factory_uses_attached_runtime_context) {
    RuntimeAccessProbeFactory factory(1, 2);
    OperatorRuntimeContext external_context(factory.plan_node_id());

    factory.set_runtime_context(&external_context);

    ASSERT_EQ(&external_context, &factory.runtime_context());
}
```

Expected failure before implementation: no `set_runtime_context`, or `runtime_context()` returns the factory-owned member.

- [ ] **Step 2: Replace owned member with pointer**

In `operator_factory.h`, replace:

```cpp
OperatorRuntimeContext _runtime_context;
```

with:

```cpp
OperatorRuntimeContext* _runtime_context = nullptr;
```

Add:

```cpp
void set_runtime_context(OperatorRuntimeContext* runtime_context);
bool has_runtime_context() const { return _runtime_context != nullptr; }
OperatorRuntimeContext& runtime_context();
const OperatorRuntimeContext& runtime_context() const;
```

Move forwarding method bodies to `operator_factory.cpp` where possible, so `operator_factory.h` can forward declare `OperatorRuntimeContext` and stop including `operator_runtime_context.h`.

- [ ] **Step 3: Update factory constructor and methods**

In `operator_factory.cpp`, remove `_runtime_context(plan_node_id)` from the constructor initializer.

Implement:

```cpp
void OperatorFactory::set_runtime_context(OperatorRuntimeContext* runtime_context) {
    DCHECK(runtime_context != nullptr);
    _runtime_context = runtime_context;
}

OperatorRuntimeContext& OperatorFactory::runtime_context() {
    DCHECK(_runtime_context != nullptr);
    return *_runtime_context;
}

const OperatorRuntimeContext& OperatorFactory::runtime_context() const {
    DCHECK(_runtime_context != nullptr);
    return *_runtime_context;
}
```

Forwarding methods should call `runtime_context()` instead of `_runtime_context`.

- [ ] **Step 4: Run factory-focused test**

Run:

```bash
./run-be-ut.sh --build-target starrocks_test --module starrocks_test --without-java-ext --gtest_filter 'OperatorRuntimeAccessTest.*'
```

Expected: PASS after pipeline wiring is complete. Before Task 4, direct factory tests that call `set_runtime_context()` should pass, while integration-style tests may expose missing attachments.

### Task 4: Attach Contexts Centrally During Pipeline Build

**Files:**
- Modify: `be/src/exec/pipeline/pipeline_builder.h`
- Modify: `be/src/exec/pipeline/pipeline_builder.cpp`
- Modify: `be/src/exec/pipeline/exec_node_pipeline_adapter.cpp`

- [ ] **Step 1: Add PipelineBuilderContext helper**

In `pipeline_builder.h`, add:

```cpp
OperatorRuntimeContext& attach_runtime_context(OperatorFactory* factory);
void attach_runtime_contexts(const OpFactories& operators);
```

In `pipeline_builder.cpp`, implement:

```cpp
OperatorRuntimeContext& PipelineBuilderContext::attach_runtime_context(OperatorFactory* factory) {
    auto& context = _fragment_context->operator_runtime_contexts().get_or_create(factory->id(), factory->plan_node_id());
    factory->set_runtime_context(&context);
    return context;
}

void PipelineBuilderContext::attach_runtime_contexts(const OpFactories& operators) {
    for (const auto& factory : operators) {
        attach_runtime_context(factory.get());
    }
}
```

- [ ] **Step 2: Attach before storing every pipeline**

At the start of `PipelineBuilderContext::add_pipeline(const OpFactories& operators, ExecutionGroupRawPtr execution_group)`:

```cpp
attach_runtime_contexts(operators);
```

This covers interpolated exchange/cache/debug operators because all final operator chains pass through `add_pipeline`.

- [ ] **Step 3: Attach before runtime filter initialization**

In `init_runtime_filter_for_operator`, replace direct `op->runtime_context()` access with:

```cpp
auto& runtime_context = context->attach_runtime_context(op);
runtime_context.init_runtime_filter(...);
```

This covers ExecNode-derived factories before they are added to a pipeline.

- [ ] **Step 4: Run syntax compile for builder and adapter**

Run:

```bash
ninja -C be/build_Release Exec -j10
```

Expected: either compile succeeds, or the current local ORC CMake blocker appears before compilation. If ORC blocks, use syntax-only compiles for `pipeline_builder.cpp` and `exec_node_pipeline_adapter.cpp`.

### Task 5: Preserve Wrapper Factory Compatibility

**Files:**
- Inspect: `be/src/exec/query_cache/conjugate_operator.h`
- Inspect: `be/src/exec/query_cache/conjugate_operator.cpp`
- Inspect: `be/src/exec/query_cache/multilane_operator.h`
- Inspect: `be/src/exec/query_cache/multilane_operator.cpp`

- [ ] **Step 1: Keep factory forwarding APIs virtual**

Do not remove these virtual methods yet:

```cpp
rf_waiting_set()
bind_runtime_in_filters(...)
get_runtime_bloom_filters()
get_filter_null_value_columns()
has_runtime_filters()
has_topn_filter()
```

Reason: wrapper factories still delegate these calls to wrapped source/sink factories. Removing them is a later migration after wrappers receive explicit runtime access objects.

- [ ] **Step 2: Run syntax compile for wrapper factories**

Run syntax-only compiles or focused build for:

```bash
be/src/exec/query_cache/conjugate_operator.cpp
be/src/exec/query_cache/multilane_operator.cpp
```

Expected: no `override` errors and no missing runtime-context attachment errors.

### Task 6: Strengthen Ownership Invariants

**Files:**
- Modify: `be/src/exec/pipeline/operator_factory.cpp`
- Modify: `be/src/exec/pipeline/pipeline_builder.cpp`
- Test: `be/test/exec/pipeline/pipeline_control_flow_test.cpp`

- [ ] **Step 1: Add DCHECKs**

In `OperatorFactory::prepare`, `close`, `runtime_context`, and default `Operator` constructor path, assert a context is attached:

```cpp
DCHECK(has_runtime_context()) << get_name();
```

- [ ] **Step 2: Add test for builder attachment**

Create two simple factories, add them through `PipelineBuilderContext::add_pipeline`, and assert both have runtime contexts after the call. If constructing `PipelineBuilderContext` is too heavy, keep this as a narrow test against `attach_runtime_contexts`.

- [ ] **Step 3: Run control-flow tests**

Run:

```bash
./run-be-ut.sh --build-target starrocks_test --module starrocks_test --without-java-ext --gtest_filter 'OperatorRuntimeAccessTest.*:OperatorRuntimeContextRegistryTest.*'
```

Expected: PASS.

### Task 7: Verification and Boundary Checks

**Files:**
- No source edits unless checks fail.

- [ ] **Step 1: Format touched C++ files**

Run:

```bash
clang-format -i \
  be/src/exec/pipeline/operator_factory.h \
  be/src/exec/pipeline/operator_factory.cpp \
  be/src/exec/pipeline/operator_runtime_context_registry.h \
  be/src/exec/pipeline/operator_runtime_context_registry.cpp \
  be/src/exec/pipeline/fragment_context.h \
  be/src/exec/pipeline/pipeline_builder.h \
  be/src/exec/pipeline/pipeline_builder.cpp \
  be/src/exec/pipeline/exec_node_pipeline_adapter.cpp \
  be/test/exec/pipeline/pipeline_control_flow_test.cpp
```

- [ ] **Step 2: Run boundary checks**

Run:

```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/render_be_agents.py --check
git diff --check
```

Expected: all pass.

- [ ] **Step 3: Run focused build/test**

Run:

```bash
STARROCKS_THIRDPARTY=/Users/az/workspaces/starrocks-mac/thirdparty \
STARROCKS_LLVM_HOME=/opt/homebrew/opt/llvm \
ninja -C be/build_Release Exec -j10
```

Then run:

```bash
./run-be-ut.sh --build-target starrocks_test --module starrocks_test --without-java-ext --gtest_filter 'OperatorRuntimeAccessTest.*:OperatorRuntimeContextRegistryTest.*'
```

Expected: build and tests pass. If the local ORC CMake blocker appears before compilation, record it and run syntax-only compiles for every touched `.cpp` using `compile_commands.json`.

### Later Cleanup, Not This Step

- Remove `OperatorFactory` runtime-filter forwarding methods once wrapper factories no longer need them.
- Move plugin-mode registration behind a dedicated registry interface after the runtime-context registry is stable.
- Consider splitting runtime-filter context from generic runtime state if `OperatorRuntimeContext` starts accumulating non-filter responsibilities.
