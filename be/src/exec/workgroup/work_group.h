// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group_fwd.h"
#include "pipeline_executor_set_manager.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_define.h"
#include "util/priority_thread_pool.hpp"
#include "util/starrocks_metrics.h"

namespace starrocks {

class TWorkGroup;

namespace workgroup {

using seconds = std::chrono::seconds;
using milliseconds = std::chrono::microseconds;
using steady_clock = std::chrono::steady_clock;
using std::chrono::duration_cast;

using pipeline::QueryContext;
using WorkGroupType = TWorkGroupType::type;
struct WorkGroupMetrics;
using WorkGroupMetricsPtr = std::shared_ptr<WorkGroupMetrics>;

template <typename Q>
class WorkGroupSchedEntity {
public:
    explicit WorkGroupSchedEntity(WorkGroup* workgroup);
    ~WorkGroupSchedEntity();

    WorkGroup* workgroup() { return _workgroup; }

    Q* queue() { return _my_queue.get(); }
    void set_queue(std::unique_ptr<Q> my_queue);

    Q* in_queue() { return _in_queue; }
    const Q* in_queue() const { return _in_queue; }
    void set_in_queue(Q* in_queue) { _in_queue = in_queue; }

    int64_t cpu_weight() const;

    int64_t vruntime_ns() const { return _vruntime_ns; }
    int64_t runtime_ns() const { return _vruntime_ns * cpu_weight(); }

    /// Return the growth runtime in the range [last, curr].
    /// For example:
    ///     mark_curr_runtime_ns();           // Move curr to latest.
    ///     auto value = growth_runtime_ns;   // Get growth value in [curr, last] multiple times.
    ///     auto value = growth_runtime_ns;
    ///     mark_last_runtime_ns();           // Move last to curr.
    int64_t growth_runtime_ns() const { return _curr_unadjusted_runtime_ns - _last_unadjusted_runtime_ns; }
    /// Update curr runtime to the latest runtime.
    void mark_curr_runtime_ns() { _curr_unadjusted_runtime_ns = _unadjusted_runtime_ns; }
    /// Update last runtime to the curr runtime.
    void mark_last_runtime_ns() { _last_unadjusted_runtime_ns = _curr_unadjusted_runtime_ns; }

    int64_t unadjusted_runtime_ns() const { return _unadjusted_runtime_ns; }

    void incr_runtime_ns(int64_t runtime_ns);
    void adjust_runtime_ns(int64_t runtime_ns);

private:
    WorkGroup* _workgroup; // The workgroup owning this entity.

    std::unique_ptr<Q> _my_queue; // The queue owned by this group.
    Q* _in_queue = nullptr;       // The queue on which this entity is queued.

    int64_t _vruntime_ns = 0;

    int64_t _unadjusted_runtime_ns = 0;
    int64_t _curr_unadjusted_runtime_ns = 0;
    int64_t _last_unadjusted_runtime_ns = 0;
};

using WorkGroupDriverSchedEntity = WorkGroupSchedEntity<pipeline::DriverQueue>;
using WorkGroupScanSchedEntity = WorkGroupSchedEntity<ScanTaskQueue>;

struct RunningQueryToken {
    explicit RunningQueryToken(WorkGroupPtr wg) : wg(std::move(wg)) {}
    ~RunningQueryToken();

private:
    WorkGroupPtr wg;
};
using RunningQueryTokenPtr = std::unique_ptr<RunningQueryToken>;

// WorkGroup is the unit of resource isolation, it has {CPU, Memory, Concurrency} quotas which limit the
// resource usage of the queries belonging to the WorkGroup. Each user has be bound to a WorkGroup, when
// the user issues a query, then the corresponding WorkGroup is chosen to manage the query.
class WorkGroup : public std::enable_shared_from_this<WorkGroup> {
public:
    WorkGroup(std::string name, int64_t id, int64_t version, size_t cpu_weight, double memory_limit, size_t concurrency,
              double spill_mem_limit_threshold, WorkGroupType type);
    explicit WorkGroup(const TWorkGroup& twg);
    ~WorkGroup() = default;

    void init();

    TWorkGroup to_thrift() const;
    TWorkGroup to_thrift_verbose() const;
    std::string to_string() const;

    // Copy metrics from the other work group
    void copy_metrics(const WorkGroup& rhs);

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    const MemTracker* mem_tracker() const { return _mem_tracker.get(); }
    MemTracker* connector_scan_mem_tracker() { return _connector_scan_mem_tracker.get(); }

    int64_t id() const { return _id; }
    int64_t version() const { return _version; }
    const std::string& name() const { return _name; }
    size_t cpu_weight() const { return _cpu_weight; }
    size_t exclusive_cpu_cores() const { return _exclusive_cpu_cores; }
    size_t mem_limit() const { return _memory_limit; }
    int64_t mem_limit_bytes() const { return _memory_limit_bytes; }

    int64_t mem_consumption_bytes() const { return _mem_tracker == nullptr ? 0L : _mem_tracker->consumption(); }

    WorkGroupDriverSchedEntity* driver_sched_entity() { return &_driver_sched_entity; }
    const WorkGroupDriverSchedEntity* driver_sched_entity() const { return &_driver_sched_entity; }
    WorkGroupScanSchedEntity* scan_sched_entity() { return &_scan_sched_entity; }
    const WorkGroupScanSchedEntity* scan_sched_entity() const { return &_scan_sched_entity; }
    WorkGroupScanSchedEntity* connector_scan_sched_entity() { return &_connector_scan_sched_entity; }
    const WorkGroupScanSchedEntity* connector_scan_sched_entity() const { return &_connector_scan_sched_entity; }

    void incr_num_running_drivers();
    void decr_num_running_drivers();
    int num_running_drivers() const { return _num_running_drivers; }

    // mark the workgroup is deleted, but at the present, it can not be removed from WorkGroupManager, because
    // 1. there exists pending drivers
    // 2. there is a race condition that a driver is attached to the workgroup after it is marked del.
    void mark_del() {
        bool expect_false = false;
        if (_is_marked_del.compare_exchange_strong(expect_false, true)) {
            static constexpr seconds expire_seconds{120};
            _vacuum_ttl = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + expire_seconds).count();
        }
    }
    // no drivers shall be added to this workgroup
    bool is_marked_del() const { return _is_marked_del.load(std::memory_order_acquire); }
    // a workgroup should wait several seconds to be cleaned safely.
    bool is_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _vacuum_ttl;
    }

    // return true if current workgroup is removable:
    // 1. is already marked del
    // 2. no pending drivers exists
    // 3. wait for a period of vacuum_ttl to prevent race condition
    bool is_removable() const {
        return is_marked_del() && _num_running_drivers.load(std::memory_order_acquire) == 0 && is_expired();
    }

    int128_t unique_id() const { return create_unique_id(_id, _version); }
    static int128_t create_unique_id(int64_t id, int64_t version) { return (((int128_t)version) << 64) | id; }

    Status check_big_query(const QueryContext& query_context);
    StatusOr<RunningQueryTokenPtr> acquire_running_query_token(bool enable_group_level_query_queue);
    void decr_num_queries();
    int64_t num_running_queries() const { return _num_running_queries; }
    int64_t num_total_queries() const { return _num_total_queries; }
    int64_t concurrency_overflow_count() const { return _concurrency_overflow_count; }
    int64_t bigquery_count() const { return _bigquery_count; }

    int64_t big_query_mem_limit() const { return _big_query_mem_limit; }
    bool use_big_query_mem_limit() const {
        return 0 < _big_query_mem_limit && _big_query_mem_limit <= _mem_tracker->limit();
    }
    int64_t big_query_cpu_second_limit() const { return _big_query_cpu_nanos_limit / NANOS_PER_SEC; }
    int64_t big_query_scan_rows_limit() const { return _big_query_scan_rows_limit; }
    void incr_cpu_runtime_ns(int64_t delta_ns) { _cpu_runtime_ns += delta_ns; }
    int64_t cpu_runtime_ns() const { return _cpu_runtime_ns; }

    void set_executors(PipelineExecutorSet* executors) { _executors = executors; }
    void set_exclusive_executors(std::unique_ptr<PipelineExecutorSet> executors) {
        _exclusive_executors = std::move(executors);
        _executors = _exclusive_executors.get();
    }

    PipelineExecutorSet* exclusive_executors() const { return _exclusive_executors.get(); }
    PipelineExecutorSet* executors() const { return _executors; }

    static constexpr int64 DEFAULT_WG_ID = 0;
    static constexpr int64 DEFAULT_MV_WG_ID = 1;
    static constexpr int64 DEFAULT_VERSION = 0;
    static constexpr int64 DEFAULT_MV_VERSION = 1;

    // Yield scan io task when maximum time in nano-seconds has spent in current execution round.
    static constexpr int64_t YIELD_MAX_TIME_SPENT = 100'000'000L;
    // Yield scan io task when maximum time in nano-seconds has spent in current execution round,
    // if it runs in the worker thread owned by other workgroup, which has running drivers.
    static constexpr int64_t YIELD_PREEMPT_MAX_TIME_SPENT = 5'000'000L;

private:
    static constexpr double ABSENT_MEMORY_LIMIT = -1;
    static constexpr size_t ABSENT_CONCURRENCY_LIMIT = 0;

    std::string _name;
    int64_t _id;
    int64_t _version = 0;
    WorkGroupType _type = WorkGroupType::WG_NORMAL;

    // Specified limitations
    size_t _cpu_weight = 1;
    size_t _exclusive_cpu_cores = 0;
    double _memory_limit = ABSENT_MEMORY_LIMIT;
    int64_t _memory_limit_bytes = -1;
    size_t _concurrency_limit = ABSENT_CONCURRENCY_LIMIT;
    int64_t _big_query_mem_limit = 0;
    int64_t _big_query_scan_rows_limit = 0;
    int64_t _big_query_cpu_nanos_limit = 0;
    double _spill_mem_limit_threshold = 1.0;
    int64_t _spill_mem_limit_bytes = -1;

    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    std::shared_ptr<MemTracker> _connector_scan_mem_tracker = nullptr;

    WorkGroupDriverSchedEntity _driver_sched_entity;
    WorkGroupScanSchedEntity _scan_sched_entity;
    WorkGroupScanSchedEntity _connector_scan_sched_entity;

    std::atomic<bool> _is_marked_del = false;

    std::atomic<size_t> _num_running_drivers = 0;
    std::atomic<size_t> _acc_num_drivers = 0;
    int64_t _vacuum_ttl = std::numeric_limits<int64_t>::max();

    // Metrics of this workgroup
    std::atomic<int64_t> _num_running_queries = 0;
    std::atomic<int64_t> _num_total_queries = 0;
    std::atomic<int64_t> _concurrency_overflow_count = 0;
    std::atomic<int64_t> _bigquery_count = 0;
    /// The total CPU runtime cost in nanos unit, including driver execution time, and the cpu execution time of
    /// other threads including Source and Sink threads.
    std::atomic<int64_t> _cpu_runtime_ns = 0;

    std::unique_ptr<PipelineExecutorSet> _exclusive_executors;
    PipelineExecutorSet* _executors = nullptr;
};

// WorkGroupManager is a singleton used to manage WorkGroup instances in BE, it has an io queue and a cpu queues for
// pick next workgroup for computation and launching io tasks.
class WorkGroupManager {
public:
    explicit WorkGroupManager(PipelineExecutorSetConfig executors_manager_conf);

    ~WorkGroupManager();

    Status start();

    // add a new workgroup to WorkGroupManger
    WorkGroupPtr add_workgroup(const WorkGroupPtr& wg);
    // return reserved beforehand default workgroup for query is not bound to any workgroup
    WorkGroupPtr get_default_workgroup();
    // return reserved beforehand default mv workgroup for MV query is not bound to any workgroup
    WorkGroupPtr get_default_mv_workgroup();

    void close();
    // destruct workgroups
    void destroy();

    void apply(const std::vector<TWorkGroupOp>& ops);
    std::vector<TWorkGroup> list_workgroups();

    using WorkGroupConsumer = std::function<void(const WorkGroup&)>;
    void for_each_workgroup(const WorkGroupConsumer& consumer) const;

    void update_metrics();

    bool should_yield(const WorkGroup* wg) const;
    PipelineExecutorSet* shared_executors() const { return _executors_manager.shared_executors(); }
    void for_each_executors(const ExecutorsManager::ExecutorsConsumer& consumer) const;
    void change_num_connector_scan_threads(uint32_t num_connector_scan_threads);
    void change_enable_resource_group_cpu_borrowing(bool val);

private:
    using MutexType = std::shared_mutex;
    using UniqueLockType = std::unique_lock<MutexType>;
    using SharedLockType = std::shared_lock<MutexType>;

    // {create, alter,delete}_workgroup_unlocked is used to replay WorkGroupOps.
    // WorkGroupManager::_mutex is held when invoking these method.
    void create_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void alter_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void delete_workgroup_unlocked(const WorkGroupPtr& wg);
    void add_metrics_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock);
    void update_metrics_unlocked();
    WorkGroupPtr get_default_workgroup_unlocked();

private:
    friend class ExecutorsManager;

    mutable std::shared_mutex _mutex;
    // Place it before _workgroups to ensure the shared executors is destructed after all the dedicated executors for
    // workgroups, since _executors_manager owns the shared executors, and WorkGroup owns the dedicated executors.
    ExecutorsManager _executors_manager;

    std::unordered_map<int128_t, WorkGroupPtr> _workgroups;
    std::unordered_map<int64_t, int64_t> _workgroup_versions;
    std::list<int128_t> _workgroup_expired_versions;

    std::atomic<size_t> _sum_cpu_weight = 0;

    std::once_flag init_metrics_once_flag;
    std::unordered_map<std::string, WorkGroupMetricsPtr> _wg_metrics;
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization();

    // create or renew default group
    std::shared_ptr<WorkGroup> create_default_workgroup();
    // create or renew default mv group
    std::shared_ptr<WorkGroup> create_default_mv_workgroup();
};

} // namespace workgroup
} // namespace starrocks
