# Pipeline and Workgroup Core ExecEnv Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `ExecEnv::GetInstance()` from pipeline scheduling, workgroup scheduling, and query-context shared state for workgroup_manager, global_spill_manager, profile_report_worker, and query_context_mgr lookups.

**Architecture:** Two migration layers — (A) replace singleton calls with existing `QueryExecutionServices` accessors in files that already carry query context, and (B) plumb exact service pointers (`WorkGroupManager*`, `QueryContextManager*`, `ProfileReportWorker*`) through constructor chains for scheduling infrastructure that operates below query level.

**Tech Stack:** C++, StarRocks BE pipeline/workgroup scheduling

---

## File Map

### Modified (infrastructure plumbing)
- `be/src/exec/pipeline/pipeline_driver_queue.h` — add `WorkGroupManager*` to `WorkGroupDriverQueue`
- `be/src/exec/pipeline/pipeline_driver_queue.cpp` — use stored `_wg_manager`
- `be/src/exec/pipeline/pipeline_driver_poller.h` — add `QueryContextManager*` to `PipelineDriverPoller`
- `be/src/exec/pipeline/pipeline_driver_poller.cpp` — use stored `_query_context_mgr`
- `be/src/exec/workgroup/scan_task_queue.h` — add `WorkGroupManager*` to `WorkGroupScanTaskQueue`
- `be/src/exec/workgroup/scan_task_queue.cpp` — use stored `_wg_manager`
- `be/src/exec/pipeline/pipeline_driver_executor.h` — add params to `GlobalDriverExecutor` constructor
- `be/src/exec/pipeline/pipeline_driver_executor.cpp` — pass through to queue/poller
- `be/src/exec/workgroup/pipeline_executor_set.h` — add params to `PipelineExecutorSet::start()`
- `be/src/exec/workgroup/pipeline_executor_set.cpp` — pass through to driver executor and scan queues
- `be/src/exec/workgroup/pipeline_executor_set_manager.h` — add params to `start_shared_executors_unlocked()` and `maybe_create_exclusive_executors_unlocked()`
- `be/src/exec/workgroup/pipeline_executor_set_manager.cpp` — pass through
- `be/src/exec/workgroup/work_group.h` — add `QueryContextManager*` to `WorkGroupManager`, change `start()` signature
- `be/src/exec/workgroup/work_group.cpp` — store and pass `QueryContextManager*`
- `be/src/runtime/exec_env.cpp` — pass `_query_context_mgr` to `_workgroup_manager->start()`

### Modified (call site migration)
- `be/src/exec/pipeline/pipeline_driver.cpp` — replace `ExecEnv::GetInstance()->global_spill_manager()` with query services
- `be/src/exec/pipeline/fragment_executor.cpp` — replace `ExecEnv::GetInstance()->workgroup_manager()` with query services
- `be/src/exec/pipeline/fragment_context.cpp` — replace workgroup_manager singleton with query services
- `be/src/exec/pipeline/query_context.cpp` — tighten fallbacks for query_context_mgr, global_spill_manager, profile_report_worker
- `be/src/exec/pipeline/query_context.h` — add `ProfileReportWorker*` to `QueryContextManager`

### Updated
- `build-support/exec_env_singleton_allowlist.txt` — remove migrated files

## Out of Scope (documented remaining callers)
- `fragment_executor.cpp:755` — `_calc_sink_dop(ExecEnv::GetInstance(), request)` — sink DOP calculation, belongs to sink/writer leaf

---

### Task 1: Plumb WorkGroupManager* through driver queue infrastructure

**Files:**
- Modify: `be/src/exec/pipeline/pipeline_driver_queue.h:155-227`
- Modify: `be/src/exec/pipeline/pipeline_driver_queue.cpp:280,330,360`
- Modify: `be/src/exec/pipeline/pipeline_driver_executor.h:77-78`
- Modify: `be/src/exec/pipeline/pipeline_driver_executor.cpp:39-52`
- Modify: `be/src/exec/workgroup/pipeline_executor_set.h:54`
- Modify: `be/src/exec/workgroup/pipeline_executor_set.cpp:87-104`
- Modify: `be/src/exec/workgroup/pipeline_executor_set_manager.h:50,58`
- Modify: `be/src/exec/workgroup/pipeline_executor_set_manager.cpp:36-37,113-123`
- Modify: `be/src/exec/workgroup/work_group.h:284`
- Modify: `be/src/exec/workgroup/work_group.cpp:600,668-669`
- Modify: `be/src/runtime/exec_env.cpp:587`

- [ ] **Step 1: Add WorkGroupManager* to WorkGroupDriverQueue**

In `pipeline_driver_queue.h`, add a `WorkGroupManager*` member and constructor param to `WorkGroupDriverQueue`:

```cpp
// Constructor change
WorkGroupDriverQueue(DriverQueueMetrics* metrics, workgroup::WorkGroupManager* wg_manager)
    : FactoryMethod(metrics), _wg_manager(wg_manager) {}

// Add member (private section, after _local_queue_cntl)
workgroup::WorkGroupManager* _wg_manager;
```

In `pipeline_driver_queue.cpp`, replace the three `ExecEnv::GetInstance()->workgroup_manager()` calls:
- Line 280: `ExecEnv::GetInstance()->workgroup_manager()->should_yield(...)` → `_wg_manager->should_yield(...)`
- Line 330: `ExecEnv::GetInstance()->workgroup_manager()->num_workgroups()` → `_wg_manager->num_workgroups()`
- Line 360: `ExecEnv::GetInstance()->workgroup_manager()->should_yield(...)` → `_wg_manager->should_yield(...)`

Remove the `#include "runtime/exec_env.h"` from `pipeline_driver_queue.cpp` if no other usage remains. Add forward decl `namespace workgroup { class WorkGroupManager; }` to the header if needed.

- [ ] **Step 2: Add WorkGroupManager* to GlobalDriverExecutor constructor**

In `pipeline_driver_executor.h`, change constructor:
```cpp
GlobalDriverExecutor(const std::string& name, std::unique_ptr<ThreadPool> thread_pool, bool enable_resource_group,
                     const CpuUtil::CpuIds& cpuids, PipelineExecutorMetrics* metrics,
                     workgroup::WorkGroupManager* wg_manager, QueryContextManager* query_context_mgr);
```

In `pipeline_driver_executor.cpp`, update constructor to pass `wg_manager` to `WorkGroupDriverQueue` and `query_context_mgr` to `PipelineDriverPoller`:
```cpp
GlobalDriverExecutor::GlobalDriverExecutor(const std::string& name, std::unique_ptr<ThreadPool> thread_pool,
                                           bool enable_resource_group, const CpuUtil::CpuIds& cpuids,
                                           PipelineExecutorMetrics* metrics,
                                           workgroup::WorkGroupManager* wg_manager,
                                           QueryContextManager* query_context_mgr)
        : Base("pip_exec_" + name),
          _driver_queue(enable_resource_group
                                ? std::unique_ptr<DriverQueue>(
                                          std::make_unique<WorkGroupDriverQueue>(metrics->get_driver_queue_metrics(),
                                                                                 wg_manager))
                                : std::make_unique<QuerySharedDriverQueue>(metrics->get_driver_queue_metrics())),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(
                  new PipelineDriverPoller(name, _driver_queue.get(), cpuids, metrics->get_poller_metrics(),
                                           query_context_mgr)),
          ...
```

- [ ] **Step 3: Thread through PipelineExecutorSet::start()**

In `pipeline_executor_set.h`, change signature:
```cpp
Status start(workgroup::WorkGroupManager* wg_manager, pipeline::QueryContextManager* query_context_mgr);
```

Add forward declaration at top:
```cpp
namespace starrocks::pipeline { class QueryContextManager; }
```

In `pipeline_executor_set.cpp`, update `start()` to pass both pointers to `GlobalDriverExecutor`:
```cpp
Status PipelineExecutorSet::start(workgroup::WorkGroupManager* wg_manager,
                                   pipeline::QueryContextManager* query_context_mgr) {
    ...
    _driver_executor = std::make_unique<pipeline::GlobalDriverExecutor>(
        _name, std::move(driver_executor_thread_pool), true, _cpuids, _conf.metrics,
        wg_manager, query_context_mgr);
    ...
}
```

- [ ] **Step 4: Thread through ExecutorsManager**

In `pipeline_executor_set_manager.h`, change signatures:
```cpp
Status start_shared_executors_unlocked(pipeline::QueryContextManager* query_context_mgr) const;
std::unique_ptr<PipelineExecutorSet> maybe_create_exclusive_executors_unlocked(
    WorkGroup* wg, const CpuUtil::CpuIds& cpuids, pipeline::QueryContextManager* query_context_mgr) const;
```

In `pipeline_executor_set_manager.cpp`, pass through:
```cpp
Status ExecutorsManager::start_shared_executors_unlocked(pipeline::QueryContextManager* query_context_mgr) const {
    return _shared_executors->start(_parent, query_context_mgr);
}

std::unique_ptr<PipelineExecutorSet> ExecutorsManager::maybe_create_exclusive_executors_unlocked(
        WorkGroup* wg, const CpuUtil::CpuIds& cpuids, pipeline::QueryContextManager* query_context_mgr) const {
    ...
    if (const Status status = executors->start(_parent, query_context_mgr); !status.ok()) {
    ...
}
```

- [ ] **Step 5: Thread through WorkGroupManager and ExecEnv**

In `work_group.h`, change `start()` and add storage:
```cpp
Status start(pipeline::QueryContextManager* query_context_mgr);
private:
    pipeline::QueryContextManager* _query_context_mgr = nullptr;
```

In `work_group.cpp`:
```cpp
Status WorkGroupManager::start(pipeline::QueryContextManager* query_context_mgr) {
    _query_context_mgr = query_context_mgr;
    return _executors_manager.start_shared_executors_unlocked(query_context_mgr);
}
```

Update `create_workgroup_unlocked` to pass `_query_context_mgr`:
```cpp
exclusive_executors = _executors_manager.maybe_create_exclusive_executors_unlocked(
    wg.get(), cpuids, _query_context_mgr);
```

In `exec_env.cpp:587`:
```cpp
RETURN_IF_ERROR(_workgroup_manager->start(_query_context_mgr));
```

- [ ] **Step 6: Compile check**

Run: `cd /Users/az/workspaces/starrocks-mac && cmake --build be/ut_build_ASAN --target pipeline_driver_queue.o pipeline_driver_executor.o pipeline_executor_set.o pipeline_executor_set_manager.o work_group.o -j$(nproc) 2>&1 | tail -30`

Expected: compiles with no errors (or fix forward-declaration issues).

- [ ] **Step 7: Commit**

```bash
git add be/src/exec/pipeline/pipeline_driver_queue.h be/src/exec/pipeline/pipeline_driver_queue.cpp \
  be/src/exec/pipeline/pipeline_driver_executor.h be/src/exec/pipeline/pipeline_driver_executor.cpp \
  be/src/exec/workgroup/pipeline_executor_set.h be/src/exec/workgroup/pipeline_executor_set.cpp \
  be/src/exec/workgroup/pipeline_executor_set_manager.h be/src/exec/workgroup/pipeline_executor_set_manager.cpp \
  be/src/exec/workgroup/work_group.h be/src/exec/workgroup/work_group.cpp \
  be/src/runtime/exec_env.cpp
git commit -m "plumb WorkGroupManager and QueryContextManager through pipeline scheduling constructors"
```

---

### Task 2: Plumb WorkGroupManager* to WorkGroupScanTaskQueue

**Files:**
- Modify: `be/src/exec/workgroup/scan_task_queue.h:170-172`
- Modify: `be/src/exec/workgroup/scan_task_queue.cpp:89,174`
- Modify: `be/src/exec/workgroup/pipeline_executor_set.cpp:116,131`

- [ ] **Step 1: Add WorkGroupManager* to WorkGroupScanTaskQueue**

In `scan_task_queue.h`:
```cpp
WorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type, WorkGroupManager* wg_manager)
    : _sched_entity_type(sched_entity_type), _wg_manager(wg_manager) {}

// Add member in private section:
WorkGroupManager* _wg_manager;
```

In `scan_task_queue.cpp`, replace:
- Line 89: `ExecEnv::GetInstance()->workgroup_manager()->should_yield(...)` → `_wg_manager->should_yield(...)`
- Line 174: `ExecEnv::GetInstance()->workgroup_manager()->should_yield(wg)` → `_wg_manager->should_yield(wg)`

Remove `#include "runtime/exec_env.h"` from `scan_task_queue.cpp` if no other usage remains.

- [ ] **Step 2: Update PipelineExecutorSet::start() to pass wg_manager**

In `pipeline_executor_set.cpp`, update the two `WorkGroupScanTaskQueue` construction sites:
```cpp
_scan_executor = std::make_unique<ScanExecutor>(
    std::move(scan_thread_pool),
    std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::OLAP, wg_manager),
    _conf.metrics->get_scan_executor_metrics());

_connector_scan_executor = std::make_unique<ScanExecutor>(
    std::move(connector_scan_thread_pool),
    std::make_unique<WorkGroupScanTaskQueue>(ScanSchedEntityType::CONNECTOR, wg_manager),
    _conf.metrics->get_connector_scan_executor_metrics());
```

- [ ] **Step 3: Compile check and commit**

Run: `cd /Users/az/workspaces/starrocks-mac && cmake --build be/ut_build_ASAN --target scan_task_queue.o pipeline_executor_set.o -j$(nproc) 2>&1 | tail -20`

```bash
git add be/src/exec/workgroup/scan_task_queue.h be/src/exec/workgroup/scan_task_queue.cpp \
  be/src/exec/workgroup/pipeline_executor_set.cpp
git commit -m "plumb WorkGroupManager to WorkGroupScanTaskQueue"
```

---

### Task 3: Migrate PipelineDriverPoller to use stored QueryContextManager*

**Files:**
- Modify: `be/src/exec/pipeline/pipeline_driver_poller.h:38-46`
- Modify: `be/src/exec/pipeline/pipeline_driver_poller.cpp:272-283`

- [ ] **Step 1: Add QueryContextManager* to PipelineDriverPoller**

In `pipeline_driver_poller.h`, update constructor and add member:
```cpp
explicit PipelineDriverPoller(std::string name, DriverQueue* driver_queue, CpuUtil::CpuIds cpuids,
                              PollerMetrics* metrics, QueryContextManager* query_context_mgr)
        : _name(std::move(name)),
          _cpud_ids(std::move(cpuids)),
          _driver_queue(driver_queue),
          _polling_thread(nullptr),
          _is_polling_thread_initialized(false),
          _is_shutdown(false),
          _metrics(metrics),
          _query_context_mgr(query_context_mgr) {}

// Add member in private section:
QueryContextManager* _query_context_mgr;
```

- [ ] **Step 2: Replace ExecEnv call in for_each_driver**

In `pipeline_driver_poller.cpp:272-283`:
```cpp
void PipelineDriverPoller::for_each_driver(const ConstDriverConsumer& call) const {
    _query_context_mgr->for_each_active_ctx([&call](const QueryContextPtr& ctx) {
        ctx->fragment_mgr()->for_each_fragment([&call](const FragmentContextPtr& fragment) {
            fragment->iterate_drivers([&call](const std::shared_ptr<PipelineDriver>& driver) {
                if (driver->is_in_blocked()) {
                    call(driver.get());
                }
            });
        });
    });
    ...
}
```

Remove `#include "runtime/exec_env.h"` from `pipeline_driver_poller.cpp` if no other usage remains.

- [ ] **Step 3: Compile check and commit**

```bash
git add be/src/exec/pipeline/pipeline_driver_poller.h be/src/exec/pipeline/pipeline_driver_poller.cpp
git commit -m "migrate PipelineDriverPoller from ExecEnv singleton to stored QueryContextManager"
```

---

### Task 4: Migrate pipeline_driver.cpp global_spill_manager calls

**Files:**
- Modify: `be/src/exec/pipeline/pipeline_driver.cpp:672,702`

- [ ] **Step 1: Replace ExecEnv calls with query services**

`PipelineDriver` has `_query_ctx` (QueryContext*) which has `query_execution_services()`.

Line 672:
```cpp
size_t shared_reserved = _query_ctx->query_execution_services()->runtime->global_spill_manager->spill_expected_reserved_bytes();
```

Line 702:
```cpp
size_t shared_reserved = _query_ctx->query_execution_services()->runtime->global_spill_manager->spill_expected_reserved_bytes();
```

Remove `#include "runtime/exec_env.h"` from `pipeline_driver.cpp` if no other usage remains.

- [ ] **Step 2: Compile check and commit**

```bash
git add be/src/exec/pipeline/pipeline_driver.cpp
git commit -m "migrate pipeline_driver global_spill_manager from ExecEnv singleton to query services"
```

---

### Task 5: Migrate fragment_executor.cpp workgroup_manager calls

**Files:**
- Modify: `be/src/exec/pipeline/fragment_executor.cpp:194-203`

- [ ] **Step 1: Replace ExecEnv calls in _prepare_workgroup**

`FragmentExecutor` has `_query_ctx` which has `query_execution_services()`.

```cpp
Status FragmentExecutor::_prepare_workgroup(const UnifiedExecPlanFragmentParams& request) {
    auto* wg_manager = _query_ctx->query_execution_services()->execution->workgroup_manager;
    WorkGroupPtr wg;
    if (!request.common().__isset.workgroup || request.common().workgroup.id == WorkGroup::DEFAULT_WG_ID) {
        wg = wg_manager->get_default_workgroup();
    } else if (request.common().workgroup.id == WorkGroup::DEFAULT_MV_WG_ID) {
        wg = wg_manager->get_default_mv_workgroup();
    } else {
        wg = std::make_shared<WorkGroup>(request.common().workgroup);
        wg = wg_manager->add_workgroup(wg);
    }
    ...
```

Note: `fragment_executor.cpp:755` (`_calc_sink_dop(ExecEnv::GetInstance(), request)`) is OUT OF SCOPE — it's a sink DOP calculation, not workgroup/spill/profile. The file remains on the singleton allowlist for this remaining caller.

- [ ] **Step 2: Compile check and commit**

```bash
git add be/src/exec/pipeline/fragment_executor.cpp
git commit -m "migrate fragment_executor workgroup_manager from ExecEnv singleton to query services"
```

---

### Task 6: Migrate fragment_context.cpp workgroup_manager calls

**Files:**
- Modify: `be/src/exec/pipeline/fragment_context.cpp:261-263,358`

- [ ] **Step 1: Replace ExecEnv call in set_final_status (line 263)**

The file already has a namespace-local helper `execution_services(const RuntimeState* state)`. Use it:

```cpp
const auto* executors = _workgroup != nullptr
                                ? _workgroup->executors()
                                : execution_services(_runtime_state.get()).workgroup_manager->shared_executors();
```

- [ ] **Step 2: Replace ExecEnv call in FragmentContextManager::get_or_register (line 358)**

`FragmentContextManager` doesn't carry services. Since this method has no callers in production (confirmed by grep), the simplest fix is to pass the default workgroup through query services. Add a `const QueryExecutionServices*` member to `FragmentContextManager`:

In `fragment_context.h`:
```cpp
class FragmentContextManager {
public:
    void set_query_execution_services(const QueryExecutionServices* services) { _services = services; }
    ...
private:
    const QueryExecutionServices* _services = nullptr;
};
```

In `query_context.h`, update `set_query_execution_services`:
```cpp
void set_query_execution_services(const QueryExecutionServices* query_execution_services) {
    _query_execution_services = query_execution_services;
    _fragment_mgr->set_query_execution_services(query_execution_services);
}
```

In `fragment_context.cpp:358`:
```cpp
raw_ctx->set_workgroup(_services->execution->workgroup_manager->get_default_workgroup());
```

Remove `#include "runtime/exec_env.h"` from `fragment_context.cpp` if no other usage remains.

- [ ] **Step 3: Compile check and commit**

```bash
git add be/src/exec/pipeline/fragment_context.h be/src/exec/pipeline/fragment_context.cpp \
  be/src/exec/pipeline/query_context.h
git commit -m "migrate fragment_context workgroup_manager from ExecEnv singleton to query services"
```

---

### Task 7: Migrate query_context.cpp ExecEnv calls

**Files:**
- Modify: `be/src/exec/pipeline/query_context.h:418-462`
- Modify: `be/src/exec/pipeline/query_context.cpp:128,212,620,720,790`

- [ ] **Step 1: Tighten count_down_fragments fallback (line 128)**

The existing code falls back to `ExecEnv::GetInstance()->query_context_mgr()` when `_query_execution_services` is null. Add a DCHECK and keep the fallback as a safety net:

```cpp
void QueryContext::count_down_fragments() {
    if (auto* services = runtime_services(_query_execution_services); services != nullptr) {
        return this->count_down_fragments(services->query_context_mgr);
    }
    DCHECK(false) << "QueryExecutionServices should be set before count_down_fragments";
    return this->count_down_fragments(ExecEnv::GetInstance()->query_context_mgr());
}
```

- [ ] **Step 2: Tighten init_spill_manager fallback (line 212)**

Same pattern:

```cpp
auto* g_spill_manager = services != nullptr ? services->global_spill_manager : nullptr;
DCHECK(g_spill_manager != nullptr) << "QueryExecutionServices should be set before init_spill_manager";
if (g_spill_manager == nullptr) {
    g_spill_manager = ExecEnv::GetInstance()->global_spill_manager();
}
```

- [ ] **Step 3: Add ProfileReportWorker* to QueryContextManager**

In `query_context.h`:
```cpp
class QueryContextManager {
public:
    QueryContextManager(size_t log2_num_slots);
    void set_profile_report_worker(ProfileReportWorker* worker) { _profile_report_worker = worker; }
    ...
private:
    ProfileReportWorker* _profile_report_worker = nullptr;
};
```

- [ ] **Step 4: Set ProfileReportWorker in ExecEnv**

In `exec_env.cpp`, after `_profile_report_worker` creation (line 656):
```cpp
_profile_report_worker = new ProfileReportWorker(_fragment_mgr, _query_context_mgr);
_query_context_mgr->set_profile_report_worker(_profile_report_worker);
```

- [ ] **Step 5: Replace ExecEnv calls in report_fragments (lines 620, 720, 790)**

For lines 620 and 720 (inside fragment iteration where query context is available):
```cpp
auto* profile_worker = need_report_query_ctx[i]->query_execution_services()
                            ? need_report_query_ctx[i]->query_execution_services()->runtime->profile_report_worker
                            : _profile_report_worker;
profile_worker->unregister_pipeline_load(fragment_ctx->query_id(), fragment_ctx->fragment_instance_id());
```

For line 790 (fragment_context_non_exist case, no query context):
```cpp
_profile_report_worker->unregister_pipeline_load(key.query_id, key.fragment_instance_id);
```

- [ ] **Step 6: Compile check and commit**

```bash
git add be/src/exec/pipeline/query_context.h be/src/exec/pipeline/query_context.cpp \
  be/src/runtime/exec_env.cpp
git commit -m "migrate query_context ExecEnv calls to query services and stored ProfileReportWorker"
```

---

### Task 8: Update singleton allowlist and run full verification

**Files:**
- Modify: `build-support/exec_env_singleton_allowlist.txt`

- [ ] **Step 1: Remove fully migrated files from allowlist**

Remove these lines from `exec_env_singleton_allowlist.txt`:
- `src/exec/pipeline/pipeline_driver.cpp`
- `src/exec/pipeline/pipeline_driver_poller.cpp`
- `src/exec/pipeline/pipeline_driver_queue.cpp`
- `src/exec/pipeline/query_context.cpp`
- `src/exec/workgroup/scan_task_queue.cpp`

Keep (has remaining out-of-scope ExecEnv call):
- `src/exec/pipeline/fragment_executor.cpp` (sink DOP)
- `src/exec/pipeline/fragment_context.cpp` (check if any remain — may also be removable)
- `src/exec/pipeline/query_context.cpp` (has DCHECK-guarded fallbacks — keep until fallbacks are removed)

Actually, `query_context.cpp` still has `ExecEnv::GetInstance()` in the DCHECK-guarded fallbacks. Keep it on the allowlist until those are fully removed.

- [ ] **Step 2: Run full verification**

```bash
python3 build-support/check_be_module_boundaries.py --mode full
python3 build-support/render_be_agents.py --check
./run-be-ut.sh --build-target exec_core_test --module exec_core_test --without-java-ext
./run-be-ut.sh --build-target runtime_core_test --module runtime_core_test --without-java-ext
```

- [ ] **Step 3: Final commit**

```bash
git add build-support/exec_env_singleton_allowlist.txt
git commit -m "shrink ExecEnv singleton allowlist after pipeline/workgroup core migration"
```
