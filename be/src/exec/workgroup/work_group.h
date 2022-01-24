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

    void close() override;

private:
    void _adjust_weight();

    WorkGroupPtr _select_next_wg();

private:
    std::mutex _global_io_mutex;
    std::condition_variable _cv;

    bool _is_closed = false;

    std::unordered_set<WorkGroupPtr> _ready_wgs;
    std::atomic<size_t> _total_task_num = 0;
};

class WorkGroupManager;

enum WorkGroupType {
    WG_NORMAL = 0,   // normal work group, maybe added to the BE dynamically
    WG_DEFAULT = 1,  // default work group
    WG_REALTIME = 2, // realtime work group, maybe reserved beforehand
};
// WorkGroup is the unit of resource isolation, it has {CPU, Memory, Concurrency} quotas which limit the
// resource usage of the queries belonging to the WorkGroup. Each user has be bound to a WorkGroup, when
// the user issues a query, then the corresponding WorkGroup is chosen to manage the query.
class WorkGroup {
public:
    WorkGroup(const std::string& name, int id, size_t cpu_limit, size_t memory_limit, size_t concurrency,
              WorkGroupType type);
    WorkGroup(const TWorkGroup& twg);
    ~WorkGroup() = default;

    void init();

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    pipeline::DriverQueue* driver_queue() { return _driver_queue.get(); }

    int id() const { return _id; }

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

    static constexpr int DEFAULT_WG_ID = 0;
    bool try_offer_io_task(const PriorityThreadPool::Task& task);
    PriorityThreadPool::Task pick_io_task();

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

    // TODO(ZiheLiu): remove them. They are just for test.
    std::atomic<int> num_selected_io_times = 0;

private:
    std::string _name;
    int _id;
    WorkGroupType _type;

    size_t _cpu_limit;
    size_t _memory_limit;
    size_t _concurrency;

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
    // remove already-existing workgroup from WorkGroupManager
    void remove_workgroup(int wg_id);

    void close();

    // get next workgroup for io
    StatusOr<PriorityThreadPool::Task> pick_next_task_for_io();
    bool try_offer_io_task(WorkGroupPtr wg, const PriorityThreadPool::Task& task);

    size_t get_sum_cpu_limit() const { return _sum_cpu_limit; }
    void increment_cpu_runtime_ns(int64_t cpu_runtime_ns) { _sum_cpu_runtime_ns += cpu_runtime_ns; }
    int64_t get_sum_cpu_runtime_ns() const { return _sum_cpu_runtime_ns; }
    void increment_sum_unadjusted_cpu_runtime_ns(int64_t cpu_runtime_ns) {
        _sum_unadjusted_cpu_runtime_ns += cpu_runtime_ns;
    }
    int64_t get_sum_unadjusted_cpu_runtime_ns() const { return _sum_unadjusted_cpu_runtime_ns; }

    // TODO(ZiheLiu): remove it. It is just for test.
    void log_cpu();

private:
    std::mutex _mutex;
    std::unordered_map<int, WorkGroupPtr> _workgroups;
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
