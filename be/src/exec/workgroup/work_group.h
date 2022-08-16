// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/scan_task_queue.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_define.h"
#include "util/blocking_queue.hpp"
#include "util/metrics.h"
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

class WorkGroup;
class WorkGroupManager;
using WorkGroupPtr = std::shared_ptr<WorkGroup>;
using WorkGroupPtrSet = std::unordered_set<WorkGroupPtr>;
using WorkGroupType = TWorkGroupType::type;

// WorkGroup is the unit of resource isolation, it has {CPU, Memory, Concurrency} quotas which limit the
// resource usage of the queries belonging to the WorkGroup. Each user has be bound to a WorkGroup, when
// the user issues a query, then the corresponding WorkGroup is chosen to manage the query.
class WorkGroup {
public:
    WorkGroup(const std::string& name, int64_t id, int64_t version, size_t cpu_limit, double memory_limit,
              size_t concurrency, WorkGroupType type);
    WorkGroup(const TWorkGroup& twg);
    ~WorkGroup() = default;

    TWorkGroup to_thrift() const;
    TWorkGroup to_thrift_verbose() const;
    void init();

    // Copy metrics from the other work group
    void copy_metrics(const WorkGroup& rhs);

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    const MemTracker* mem_tracker() const { return _mem_tracker.get(); }
    double get_mem_limit() const { return _memory_limit; }
    pipeline::DriverQueue* driver_queue() { return _driver_queue.get(); }
    ScanTaskQueue* scan_task_queue() { return _scan_task_queue.get(); }

    int64_t id() const { return _id; }

    int64_t version() const { return _version; }

    const std::string& name() const { return _name; }

    std::string to_string() const;

    size_t cpu_limit() const { return _cpu_limit; }

    int64_t vruntime_ns() const { return _vruntime_ns; }
    int64_t real_runtime_ns() const { return _vruntime_ns * _cpu_limit; }

    int64_t growth_real_runtime_ns() const { return _vruntime_ns * _cpu_limit - _last_vruntime_ns; }
    void update_last_real_runtime_ns(int64_t last_vruntime_ns) { _last_vruntime_ns = last_vruntime_ns; }

    // Accumulate virtual runtime divided by _cpu_limit, so that the larger _cpu_limit,
    // the more cpu time can be consumed proportionally.
    void increment_real_runtime_ns(int64_t real_runtime_ns) { _vruntime_ns += real_runtime_ns / _cpu_limit; }
    void set_vruntime_ns(int64_t vruntime_ns) { _vruntime_ns = vruntime_ns; }

    double get_cpu_expected_use_ratio() const;
    double get_cpu_actual_use_ratio() const;
    void set_cpu_actual_use_ratio(double ratio) { _cpu_actual_use_ratio = ratio; }

    // If the scan layer generates data, then this interface should be called
    void incr_period_scaned_chunk_num(int32_t chunk_num);

    // This interface should be called if a request for a chunk is made
    // Whether successful or not
    void incr_period_ask_chunk_num(int32_t chunk_num);

    void estimate_trend_factor_period();
    double get_expect_factor() const;
    double get_diff_factor() const;
    double get_select_factor() const;
    void set_select_factor(double value);
    void update_select_factor(double value);
    double get_cur_select_factor() const;
    void update_cur_select_factor(double value);

    // increase num_driver when the driver is attached to the workgroup
    void increase_num_drivers() {
        ++_num_drivers;
        ++_acc_num_drivers;
    }
    // decrease num_driver when the driver is detached from the workgroup
    void decrease_num_drivers() {
        int64_t old = _num_drivers.fetch_sub(1);
        DCHECK_GT(old, 0);
    }

    int num_drivers() const { return _num_drivers; }

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
    bool is_expired() {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _vacuum_ttl;
    }

    int64_t total_cpu_cost() const { return _total_cpu_cost.load(); }
    void incr_total_cpu_cost(int64_t cpu_cost) { _total_cpu_cost.fetch_add(cpu_cost); }

    Status check_big_query(const QueryContext& query_context);
    Status try_incr_num_queries();
    void decr_num_queries();
    int64_t num_running_queries() const { return _num_running_queries; }
    int64_t num_total_queries() const { return _num_total_queries; }
    int64_t concurrency_overflow_count() const { return _concurrency_overflow_count; }
    int64_t bigquery_count() const { return _bigquery_count; }

    int64_t big_query_mem_limit() const { return _big_query_mem_limit; }
    bool use_big_query_mem_limit() const {
        return 0 < _big_query_mem_limit && _big_query_mem_limit <= _mem_tracker->limit();
    }
    int64_t big_query_cpu_second_limit() const { return _big_query_cpu_second_limit; }
    int64_t big_query_scan_rows_limit() const { return _big_query_scan_rows_limit; }

    // return true if current workgroup is removable:
    // 1. is already marked del
    // 2. no pending drivers exists
    // 3. wait for a period of vacuum_ttl to prevent race condition
    bool is_removable() { return is_marked_del() && _num_drivers.load(std::memory_order_acquire) == 0 && is_expired(); }

    int128_t unique_id() const { return create_unique_id(_id, _version); }
    static int128_t create_unique_id(int64_t id, int64_t version) { return (((int128_t)version) << 64) | id; }

    static constexpr int64 DEFAULT_WG_ID = 0;
    static constexpr int64 DEFAULT_VERSION = 0;

    int64_t mem_limit() const;

private:
    std::string _name;
    int64_t _id;
    int64_t _version;
    WorkGroupType _type;

    // Specified limitations
    size_t _cpu_limit;
    double _memory_limit;
    int64_t _memory_limit_bytes = -1;
    size_t _concurrency_limit = 0;
    int64_t _big_query_mem_limit = 0;
    int64_t _big_query_scan_rows_limit = 0;
    int64_t _big_query_cpu_second_limit = 0;

    std::shared_ptr<starrocks::MemTracker> _mem_tracker = nullptr;

    pipeline::DriverQueuePtr _driver_queue = nullptr;
    int64_t _vruntime_ns = 0;
    int64_t _last_vruntime_ns = 0;

    std::atomic<bool> _is_marked_del = false;
    std::atomic<size_t> _num_drivers = 0;
    std::atomic<size_t> _acc_num_drivers = 0;
    int64_t _vacuum_ttl = std::numeric_limits<int64_t>::max();

    std::unique_ptr<ScanTaskQueue> _scan_task_queue = nullptr;

    //  some variables for io schedule
    std::atomic<size_t> _period_scaned_chunk_num = 1;
    std::atomic<size_t> _period_ask_chunk_num = 1;

    double _expect_factor = 0; // the factor which should be selected to run by scheduler
    double _diff_factor = 0;
    double _select_factor = 0;
    double _cur_select_factor = 0;

    // Metrics of this workgroup
    std::atomic<int64_t> _total_cpu_cost = 0;
    std::atomic<double> _cpu_actual_use_ratio = 0;
    std::atomic<int64_t> _num_running_queries = 0;
    std::atomic<int64_t> _num_total_queries = 0;
    std::atomic<int64_t> _concurrency_overflow_count = 0;
    std::atomic<int64_t> _bigquery_count = 0;
};

class WorkerOwnerManager {
public:
    explicit WorkerOwnerManager(int num_total_workers);
    ~WorkerOwnerManager() = default;

    // Disable copy/move ctor and assignment.
    WorkerOwnerManager(const WorkerOwnerManager&) = delete;
    WorkerOwnerManager& operator=(const WorkerOwnerManager&) = delete;
    WorkerOwnerManager(WorkerOwnerManager&&) = delete;
    WorkerOwnerManager& operator=(WorkerOwnerManager&&) = delete;

    int num_total_workers() const { return _num_total_workers; }

    std::shared_ptr<WorkGroupPtrSet> get_owners(int worker_id) const {
        return _worker_id2owner_wgs[_index % 2][worker_id];
    }

    // Labels which workgroups each worker thread belongs to based on the cpu limit of each workgroup.
    void reassign_to_wgs(const std::unordered_map<int128_t, WorkGroupPtr>& workgroups, int sum_cpu_limit);

    // Return true, when the worker thread is running the workgroup which it doesn't belong to,
    // and any owner workgroups of it has running drivers.
    bool should_yield(int worker_id, const WorkGroupPtr& running_wg) const;

private:
    const int _num_total_workers;
    // Use two _worker_id2owner_wgs and _index to insulate read and write.
    std::vector<std::shared_ptr<WorkGroupPtrSet>> _worker_id2owner_wgs[2]{};
    std::atomic<size_t> _index = 0;
};

// WorkGroupManager is a singleton used to manage WorkGroup instances in BE, it has an io queue and a cpu queues for
// pick next workgroup for computation and launching io tasks.
class WorkGroupManager {
    DECLARE_SINGLETON(WorkGroupManager);

public:
    // add a new workgroup to WorkGroupManger
    WorkGroupPtr add_workgroup(const WorkGroupPtr& wg);
    // return reserved beforehand default workgroup for query is not bound to any workgroup
    WorkGroupPtr get_default_workgroup();
    // destruct workgroups
    void destroy();

    size_t sum_cpu_limit() const { return _sum_cpu_limit; }
    void increment_cpu_runtime_ns(int64_t cpu_runtime_ns) { _sum_cpu_runtime_ns += cpu_runtime_ns; }
    int64_t sum_cpu_runtime_ns() const { return _sum_cpu_runtime_ns; }
    void apply(const std::vector<TWorkGroupOp>& ops);
    std::vector<TWorkGroup> list_workgroups();
    std::vector<TWorkGroup> list_all_workgroups();

    std::shared_ptr<WorkGroupPtrSet> get_owners_of_driver_worker(int worker_id);
    bool should_yield_driver_worker(int worker_id, const WorkGroupPtr& running_wg);

    std::shared_ptr<WorkGroupPtrSet> get_owners_of_scan_worker(ScanExecutorType type, int worker_id);
    bool should_yield_scan_worker(ScanExecutorType type, int worker_id, const WorkGroupPtr& running_wg);

    int num_total_driver_workers() const { return _driver_worker_owner_manager->num_total_workers(); }

    void update_metrics();

private:
    using MutexType = std::shared_mutex;
    using UniqueLockType = std::unique_lock<MutexType>;
    using SharedLockType = std::shared_lock<MutexType>;

    // {create, alter,delete}_workgroup_unlocked is used to replay WorkGroupOps.
    // WorkGroupManager::_mutex is held when invoking these method.
    void create_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void alter_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void delete_workgroup_unlocked(const WorkGroupPtr& wg);

    // Label each executor thread to a specific workgroup by cpu limit.
    // WorkGroupManager::_mutex is held when invoking this method.
    void reassign_worker_to_wgs();

    std::shared_mutex _mutex;
    std::unordered_map<int128_t, WorkGroupPtr> _workgroups;
    std::unordered_map<int64_t, int64_t> _workgroup_versions;
    std::list<int128_t> _workgroup_expired_versions;

    std::atomic<size_t> _sum_cpu_limit = 0;
    std::atomic<int64_t> _sum_cpu_runtime_ns = 0;

    std::unique_ptr<WorkerOwnerManager> _driver_worker_owner_manager;
    std::unique_ptr<WorkerOwnerManager> _scan_worker_owner_manager;
    std::unique_ptr<WorkerOwnerManager> _connector_scan_worker_owner_manager;

    std::once_flag init_metrics_once_flag;
    std::unordered_map<std::string, int128_t> _wg_metrics;

    std::unordered_map<std::string, std::unique_ptr<starrocks::DoubleGauge>> _wg_cpu_limit_metrics;
    std::unordered_map<std::string, std::unique_ptr<starrocks::DoubleGauge>> _wg_cpu_metrics;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_mem_limit_metrics;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_mem_metrics;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_running_queries;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_total_queries;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_concurrency_overflow_count;
    std::unordered_map<std::string, std::unique_ptr<starrocks::IntGauge>> _wg_bigquery_count;

    void add_metrics_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock);
    void update_metrics_unlocked();
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization();
};

} // namespace workgroup
} // namespace starrocks
