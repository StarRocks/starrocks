// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "exec/pipeline/pipeline_driver_queue.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_define.h"
#include "util/blocking_queue.hpp"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class TWorkGroup;

namespace workgroup {

using seconds = std::chrono::seconds;
using milliseconds = std::chrono::microseconds;
using steady_clock = std::chrono::steady_clock;
using std::chrono::duration_cast;
class WorkGroup;
class WorkGroupManager;
using WorkGroupPtr = std::shared_ptr<WorkGroup>;

class WorkGroupQueue {
public:
    WorkGroupQueue() = default;
    virtual ~WorkGroupQueue() = default;

    virtual void add(const WorkGroupPtr& wg) = 0;
    virtual void remove(const WorkGroupPtr& wg) = 0;
    virtual WorkGroupPtr pick_next() = 0;

    virtual void close() = 0;
};

class IoWorkGroupQueue final : public WorkGroupQueue {
public:
    IoWorkGroupQueue() = default;
    ~IoWorkGroupQueue() override = default;

    void add(const WorkGroupPtr& wg) override {}
    void remove(const WorkGroupPtr& wg) override {}
    WorkGroupPtr pick_next() override { return nullptr; };

    StatusOr<PriorityThreadPool::Task> pick_next_task();
    bool try_offer_io_task(WorkGroupPtr wg, const PriorityThreadPool::Task& task);

    // for real_time wg
    bool try_offer_real_time_io_task(const PriorityThreadPool::Task& task);
    StatusOr<PriorityThreadPool::Task> pick_next_real_time_task();

    void close() override;

private:
    void _maybe_adjust_weight();
    WorkGroupPtr _select_next_wg();

    std::mutex _global_io_mutex;
    std::condition_variable _cv;

    bool _is_closed = false;

    std::unordered_set<WorkGroupPtr> _ready_wgs;
    std::atomic<size_t> _total_task_num = 0;

    const int _max_schedule_num_period = config::pipeline_max_io_schedule_num_period;
    // Adjust select factor of each wg after every `min(_max_schedule_num_period, num_tasks)` schedule times.
    int _remaining_schedule_num_period = 0;

    // for realtime workgroup
    std::mutex _realtime_io_mutex;
    std::condition_variable _realtime_io_cv;
    WorkGroupPtr _real_time_wg;
};

class WorkGroupManager;
<<<<<<< HEAD
using WorkGroupType = TWorkGroupType::type;
=======

enum WorkGroupType {
    WG_NORMAL = 0,   // normal work group, maybe added to the BE dynamically
    WG_DEFAULT = 1,  // default work group
    WG_REALTIME = 2, // realtime work group, maybe reserved beforehand
};

>>>>>>> 3c7f327e (support low latency wg)
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

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    pipeline::DriverQueue* driver_queue() { return _driver_queue.get(); }

    int64_t id() const { return _id; }

    int64_t version() const { return _version; }

    const std::string& name() const { return _name; }

    size_t get_cpu_limit() const { return _cpu_limit; }
    int64_t get_vruntime_ns() const { return _vruntime_ns; }
    int64_t get_real_runtime_ns() const { return _vruntime_ns * _cpu_limit; }
    // Accumulate virtual runtime divided by _cpu_limit, so that the larger _cpu_limit,
    // the more cpu time can be consumed proportionally.
    void increment_real_runtime_ns(int64_t real_runtime_ns) {
        _vruntime_ns += real_runtime_ns / _cpu_limit;
        _unadjusted_real_runtime_ns += real_runtime_ns;
    }
    void set_vruntime_ns(int64_t vruntime_ns) { _vruntime_ns = vruntime_ns; }

    double get_cpu_expected_use_ratio() const;
    double get_cpu_actual_use_ratio() const;
    double get_cpu_unadjusted_actual_use_ratio() const;

    static constexpr int64 DEFAULT_WG_ID = 0;
    static constexpr int64 DEFAULT_VERSION = 0;
    bool try_offer_io_task(const PriorityThreadPool::Task& task);
    PriorityThreadPool::Task pick_io_task();

    WorkGroupType type() { return _type; }

public:
    // Return current io task queue size
    // need be lock when invoking
    size_t io_task_queue_size();

    // should be call read chunk from disk
    void increase_chunk_num(int32_t chunk_num);

    // should be call while comsume chunk from calculate thread
    void decrease_chunk_num(int32_t chunk_num);

    void estimate_trend_factor_period();
    double get_expect_factor() const;
    double get_diff_factor() const;
    double get_select_factor() const;
    int64_t get_cur_hold_total_chunk_num() const { return _cur_hold_total_chunk_num; }
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
    void decrease_num_drivers() { --_num_drivers; }

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
    // return true if current workgroup is removable:
    // 1. is already marked del
    // 2. no pending drivers exists
    // 3. wait for a period of vacuum_ttl to prevent race condition
    bool is_removable() { return is_marked_del() && _num_drivers.load(std::memory_order_acquire) == 0 && is_expired(); }

    int128_t unique_id() const { return create_unique_id(_id, _version); }
    static int128_t create_unique_id(int64_t id, int64_t version) { return (((int128_t)version) << 64) | id; }

private:
    std::string _name;
    int64_t _id;
    int64_t _version;

    size_t _cpu_limit;
    double _memory_limit;
    size_t _concurrency;
    WorkGroupType _type;

    std::shared_ptr<starrocks::MemTracker> _mem_tracker = nullptr;

    pipeline::DriverQueuePtr _driver_queue = nullptr;
    int64_t _vruntime_ns = 0;
    // vruntime and real_runtime is adjusted when the driver put back to ready_wgs,
    // _unadjusted_real_runtime_ns is used to record the unadjusted real runtime.
    int64_t _unadjusted_real_runtime_ns = 0;

    // it's proper to define Context as a Thrift or protobuf struct.
    // WorkGroupContext _context;

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    // BlockingPriorityQueue<PriorityThreadPool::Task> _io_work_queue;
    std::queue<PriorityThreadPool::Task> _io_work_queue;

    //  some variables for io schedule
    std::atomic<int64_t> _cur_hold_total_chunk_num = 0; // total chunk num wait for consume
    std::atomic<size_t> _increase_chunk_num_period = 1;
    std::atomic<size_t> _decrease_chunk_num_period = 1;

    double _expect_factor = 0; // the factor which should be selected to run by scheduler
    double _diff_factor = 0;
    double _select_factor = 0;
    double _cur_select_factor = 0;

    std::atomic<bool> _is_marked_del = false;
    std::atomic<size_t> _num_drivers = 0;
    std::atomic<size_t> _acc_num_drivers = 0;
    int64_t _vacuum_ttl = std::numeric_limits<int64_t>::max();
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
    void close();

    // get next workgroup for io
    StatusOr<PriorityThreadPool::Task> pick_next_task_for_io(bool is_real_time_type = false);
    bool try_offer_io_task(WorkGroupPtr wg, const PriorityThreadPool::Task& task);

    // for real time workgroup
    bool try_offer_real_time_io_task(const PriorityThreadPool::Task& task);

    WorkGroupQueue& get_cpu_queue();

    WorkGroupQueue& get_io_queue();

    size_t get_sum_cpu_limit() const { return _sum_cpu_limit; }
    void increment_cpu_runtime_ns(int64_t cpu_runtime_ns) { _sum_cpu_runtime_ns += cpu_runtime_ns; }
    int64_t get_sum_cpu_runtime_ns() const { return _sum_cpu_runtime_ns; }
    void increment_sum_unadjusted_cpu_runtime_ns(int64_t cpu_runtime_ns) {
        _sum_unadjusted_cpu_runtime_ns += cpu_runtime_ns;
    }
    int64_t get_sum_unadjusted_cpu_runtime_ns() const { return _sum_unadjusted_cpu_runtime_ns; }

    void apply(const std::vector<TWorkGroupOp>& ops);
    std::vector<TWorkGroup> list_workgroups();
    std::vector<TWorkGroup> list_all_workgroups();

private:
    // {create, alter,delete}_workgroup_unlocked is used to replay WorkGroupOps.
    // WorkGroupManager::_mutex is held when invoking these method.
    void create_workgroup_unlocked(const WorkGroupPtr& wg);
    void alter_workgroup_unlocked(const WorkGroupPtr& wg);
    void delete_workgroup_unlocked(const WorkGroupPtr& wg);
    std::shared_mutex _mutex;
    std::unordered_map<int128_t, WorkGroupPtr> _workgroups;
    std::unordered_map<int64_t, int64_t> _workgroup_versions;
    std::list<int128_t> _workgroup_expired_versions;
    IoWorkGroupQueue _wg_io_queue;

    std::atomic<size_t> _sum_cpu_limit = 0;
    std::atomic<int64_t> _sum_cpu_runtime_ns = 0;
    std::atomic<int64_t> _sum_unadjusted_cpu_runtime_ns = 0;
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization();
};

} // namespace workgroup
} // namespace starrocks
