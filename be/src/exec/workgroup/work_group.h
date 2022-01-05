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
namespace workgroup {

class WorkGroup;
using WorkGroupPtr = std::shared_ptr<WorkGroup>;
template <typename Compare>
using WorkGroupQueue = std::priority_queue<WorkGroupPtr, std::vector<WorkGroupPtr>, Compare>;
class CpuPriorityComparator;
class IoPriorityComparator;
using CpuWorkGroupQueue = WorkGroupQueue<CpuPriorityComparator>;
using IoWorkQroupQueue = WorkGroupQueue<IoPriorityComparator>;

class WorkGroupManager;

enum WorkGroupType {
    WG_NORMAL = 0, // normal work group, maybe added to the BE dynamically
    WG_DEFAULT = 1, // default work group
    WG_REALTIME = 2, // realtime work group, maybe reserved beforehand
};
// WorkGroup is the unit of resource isolation, it has {CPU, Memory, Concurrency} quotas which limit the
// resource usage of the queries belonging to the WorkGroup. Each user has be bound to a WorkGroup, when
// the user issues a query, then the corresponding WorkGroup is chosen to manage the query.
class WorkGroup {
public:
    WorkGroup(const std::string& name, int id, size_t cpu_limit, size_t memory_limit, size_t concurrency,
              WorkGroupType type);
    ~WorkGroup() = default;

    starrocks::MemTracker* mem_tracker() { return _mem_tracker.get(); }
    starrocks::pipeline::DriverQueue* driver_queue() { return _driver_queue.get(); }

    int id() const { return _id; }
    bool is_mark_del() { return _mark_del.load(std::memory_order_acquire); }
    void mark_del() { return _mark_del.store(true, std::memory_order_release); }
    int get_cpu_priority() {
        // TODO: implement cpu priority computation
        return 0;
    }
    int get_io_priority() {
        // TODO: implement io priority computation
        return 0;
    }

private:
    std::string _name;
    int _id;
    size_t _cpu_limit;
    size_t _memory_limit;
    size_t _concurrency;
    WorkGroupType _type;
    std::atomic<bool> _mark_del{false};
    std::shared_ptr<starrocks::MemTracker> _mem_tracker;
    starrocks::pipeline::DriverQueuePtr _driver_queue;
    // it's proper to define Context as a Thrift or protobuf struct.
    // WorkGroupContext _context;
};

class CpuPriorityComparator {
public:
    bool operator()(const WorkGroupPtr& lhs, const WorkGroupPtr& rhs) const {
        if (lhs->is_mark_del()) {
            return true;
        }
        if (rhs->is_mark_del()) {
            return false;
        }
        return lhs->get_cpu_priority() < rhs->get_cpu_priority();
    }
};

class IoPriorityComparator {
public:
    bool operator()(const WorkGroupPtr& lhs, const WorkGroupPtr& rhs) const {
        if (lhs->is_mark_del()) {
            return true;
        }
        if (rhs->is_mark_del()) {
            return false;
        }
        return lhs->get_io_priority() < rhs->get_io_priority();
    }
};

// WorkGroupManager is a singleton used to manage WorkGroup instances in BE, it has an io queue and a cpu queues for
// pick next workgroup for computation and launching io tasks.
class WorkGroupManager {
    DECLARE_SINGLETON(WorkGroupManager);

public:
    // add a new workgroup to WorkGroupManger
    void add_workgroup(const WorkGroupPtr& wg);
    // remove already-existing workgroup from WorkGroupManager
    void remove_workgroup(int wg_id);
    // get next workgroup for computation
    WorkGroupPtr pick_next_wg_for_cpu();
    // get next workgroup for io
    WorkGroupPtr pick_next_wg_for_io();

private:
    std::mutex _mutex;
    std::unordered_map<int, WorkGroupPtr> _workgroups;
    CpuWorkGroupQueue _wg_cpu_queue;
    IoWorkQroupQueue _wg_io_queue;
};

} // namespace workgroup
} // namespace starrocks