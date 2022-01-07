// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "exec/pipeline/pipeline_driver_queue.h"
#include "runtime/mem_tracker.h"
#include "storage/olap_define.h"

namespace starrocks {

class TWorkGroup;

namespace workgroup {

class WorkGroup;
using WorkGroupPtr = std::shared_ptr<WorkGroup>;

class WorkGroupQueue {
public:
    WorkGroupQueue() = default;
    virtual ~WorkGroupQueue() = default;
    virtual void add(const WorkGroupPtr& wg) = 0;
    virtual void remove(const WorkGroupPtr& wg) = 0;
    virtual WorkGroupPtr pick_next() = 0;
};

class IoWorkGroupQueue final : public WorkGroupQueue {
public:
    IoWorkGroupQueue() = default;
    ~IoWorkGroupQueue() = default;
    void add(const WorkGroupPtr& wg) override {}
    void remove(const WorkGroupPtr& wg) override {}
    WorkGroupPtr pick_next() override { return nullptr; }
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

    int get_io_priority() {
        // TODO: implement io priority computation
        return 0;
    }

    static constexpr int DEFAULT_WG_ID = 0;

    size_t get_cpu_limit() const { return _cpu_limit; }

    int64_t get_vruntime_ns() const { return _vruntime_ns; }

    int64_t get_real_runtime_ns() const { return _vruntime_ns * _cpu_limit; }

    // Accumulate virtual runtime divided by _cpu_limit, so that the larger _cpu_limit,
    // the more cpu time can be consumed proportionally.
    void increment_real_runtime(int64_t real_runtime_ns) { _vruntime_ns += real_runtime_ns / _cpu_limit; }

    void set_vruntime_ns(int64_t vruntime_ns) { _vruntime_ns = vruntime_ns; }

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

    // it's proper to define Context as a Thrift or protobuf struct.
    // WorkGroupContext _context;
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

    // get next workgroup for io
    WorkGroupPtr pick_next_wg_for_io();
    WorkGroupQueue& get_io_queue();

private:
    std::mutex _mutex;
    std::unordered_map<int, WorkGroupPtr> _workgroups;
    IoWorkGroupQueue _wg_io_queue;
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization();
};

} // namespace workgroup
} // namespace starrocks