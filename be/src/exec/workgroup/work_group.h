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

class CpuWorkGroupQueue final : public WorkGroupQueue {
public:
    CpuWorkGroupQueue() = default;
    ~CpuWorkGroupQueue() = default;
    void add(const WorkGroupPtr& wg) override {}
    void remove(const WorkGroupPtr& wg) override {}
    WorkGroupPtr pick_next() override { return nullptr; }
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

    starrocks::MemTracker* mem_tracker() { return _mem_tracker.get(); }
    starrocks::pipeline::DriverQueue* driver_queue() { return _driver_queue.get(); }

    int id() const { return _id; }
    int get_cpu_priority() {
        // TODO: implement cpu priority computation
        return 0;
    }
    int get_io_priority() {
        // TODO: implement io priority computation
        return 0;
    }

    static constexpr int DEFAULT_WG_ID = 0;

private:
    std::string _name;
    int _id;
    size_t _cpu_limit;
    size_t _memory_limit;
    size_t _concurrency;
    WorkGroupType _type;
    std::shared_ptr<starrocks::MemTracker> _mem_tracker;
    starrocks::pipeline::DriverQueuePtr _driver_queue;
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
    // get next workgroup for computation
    WorkGroupPtr pick_next_wg_for_cpu();
    // get next workgroup for io
    WorkGroupPtr pick_next_wg_for_io();

    WorkGroupQueue& get_cpu_queue();

    WorkGroupQueue& get_io_queue();

private:
    std::mutex _mutex;
    std::unordered_map<int, WorkGroupPtr> _workgroups;
    CpuWorkGroupQueue _wg_cpu_queue;
    IoWorkGroupQueue _wg_io_queue;
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization();
};

} // namespace workgroup
} // namespace starrocks