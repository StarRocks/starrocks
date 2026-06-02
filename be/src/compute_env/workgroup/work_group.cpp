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

#include "compute_env/workgroup/work_group.h"

#include <fmt/format.h>

#include <algorithm>
#include <utility>

#include "common/system/cpu_info.h"
#include "compute_env/workgroup/pipeline_executor_set.h"
#include "compute_env/workgroup/scan_task_queue_factory.h"
#include "exec/pipeline/primitives/driver_queue.h"
#include "glog/logging.h"
#include "runtime/env/global_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks::workgroup {

// ------------------------------------------------------------------------------------
// WorkGroupSchedEntity
// ------------------------------------------------------------------------------------

void WorkGroupSchedState::incr_runtime_ns(int64_t runtime_ns, size_t cpu_weight) {
    _vruntime_ns.fetch_add(runtime_ns / int64_t(cpu_weight), std::memory_order_relaxed);
    _unadjusted_runtime_ns.fetch_add(runtime_ns, std::memory_order_relaxed);
}

void WorkGroupSchedState::adjust_runtime_ns(int64_t runtime_ns, size_t cpu_weight) {
    _vruntime_ns.fetch_add(runtime_ns / int64_t(cpu_weight), std::memory_order_relaxed);
}

template <typename Q>
WorkGroupSchedEntity<Q>::WorkGroupSchedEntity(WorkGroup* workgroup, WorkGroupSchedState* sched_state)
        : _workgroup(workgroup), _sched_state(sched_state) {}

template <typename Q>
WorkGroupSchedEntity<Q>::~WorkGroupSchedEntity() = default;

template <typename Q>
void WorkGroupSchedEntity<Q>::set_queue(std::unique_ptr<Q> my_queue) {
    _my_queue = std::move(my_queue);
}

template <typename Q>
int64_t WorkGroupSchedEntity<Q>::cpu_weight() const {
    return _workgroup->cpu_weight();
}

template <typename Q>
void WorkGroupSchedEntity<Q>::incr_runtime_ns(int64_t runtime_ns) {
    _sched_state->incr_runtime_ns(runtime_ns, cpu_weight());
}

template <typename Q>
void WorkGroupSchedEntity<Q>::adjust_runtime_ns(int64_t runtime_ns) {
    _sched_state->adjust_runtime_ns(runtime_ns, cpu_weight());
}

template class WorkGroupSchedEntity<pipeline::DriverQueue>;
template class WorkGroupSchedEntity<ScanTaskQueue>;

// ------------------------------------------------------------------------------------
// WorkGroup
// ------------------------------------------------------------------------------------

RunningQueryToken::~RunningQueryToken() {
    wg->decr_num_queries();
}

WorkGroup::WorkGroup(std::string name, int64_t id, int64_t version, size_t cpu_limit, double memory_limit,
                     size_t concurrency, double spill_mem_limit_threshold, WorkGroupType type, std::string mem_pool)
        : _name(std::move(name)),
          _id(id),
          _version(version),
          _type(type),
          _cpu_weight(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency_limit(concurrency),
          _spill_mem_limit_threshold(spill_mem_limit_threshold),
          _mem_pool(std::move(mem_pool)),
          _driver_sched_entity(this, &_driver_sched_state),
          _scan_sched_entity(this, &_scan_sched_state),
          _connector_scan_sched_entity(this, &_connector_scan_sched_state) {}

WorkGroup::~WorkGroup() = default;

WorkGroup::WorkGroup(const TWorkGroup& twg)
        : _name(twg.name),
          _id(twg.id),
          _driver_sched_entity(this, &_driver_sched_state),
          _scan_sched_entity(this, &_scan_sched_state),
          _connector_scan_sched_entity(this, &_connector_scan_sched_state) {
    const int num_cores = CpuInfo::num_cores();
    if (twg.__isset.cpu_weight_percent && twg.cpu_weight_percent > 0) {
        _cpu_weight = std::max<size_t>(1, num_cores * twg.cpu_weight_percent / 100);
    } else if (twg.__isset.cpu_core_limit && twg.cpu_core_limit > 0) {
        _cpu_weight = twg.cpu_core_limit;
    }

    if (twg.__isset.exclusive_cpu_percent && twg.exclusive_cpu_percent > 0) {
        const size_t exclusive_cpu_cores = num_cores * twg.exclusive_cpu_percent / 100;
        if (exclusive_cpu_cores > 0) {
            _exclusive_cpu_cores = exclusive_cpu_cores;
        } else {
            _cpu_weight = 1;
        }
    } else if (twg.__isset.exclusive_cpu_cores) {
        _exclusive_cpu_cores = twg.exclusive_cpu_cores;
    }

    if (twg.__isset.inactive && twg.inactive) {
        _exclusive_cpu_cores = 0;
        _cpu_weight = 1;
    }

    if (twg.__isset.mem_limit) {
        _memory_limit = twg.mem_limit;
    }

    if (twg.__isset.concurrency_limit) {
        _concurrency_limit = twg.concurrency_limit;
    }

    if (twg.__isset.workgroup_type) {
        _type = twg.workgroup_type;
    }

    if (twg.__isset.version) {
        _version = twg.version;
    }

    if (twg.__isset.big_query_mem_limit) {
        _big_query_mem_limit = twg.big_query_mem_limit;
    }

    if (twg.__isset.big_query_scan_rows_limit) {
        _big_query_scan_rows_limit = twg.big_query_scan_rows_limit;
    }

    if (twg.__isset.big_query_cpu_second_limit) {
        _big_query_cpu_nanos_limit = twg.big_query_cpu_second_limit * NANOS_PER_SEC;
    }

    if (twg.__isset.spill_mem_limit_threshold) {
        _spill_mem_limit_threshold = twg.spill_mem_limit_threshold;
    }
    if (twg.__isset.mem_pool) {
        _mem_pool = twg.mem_pool;
    } else {
        _mem_pool = DEFAULT_MEM_POOL;
    }
}

void WorkGroup::set_exclusive_executors(std::unique_ptr<PipelineExecutorSet> executors) {
    _exclusive_executors = std::move(executors);
    _executors = _exclusive_executors.get();
}

TWorkGroup WorkGroup::to_thrift() const {
    TWorkGroup twg;
    twg.__set_id(_id);
    twg.__set_version(_version);
    return twg;
}

void WorkGroup::init(std::shared_ptr<MemTracker>& parent_mem_tracker, pipeline::DriverQueuePtr driver_queue) {
    DCHECK(driver_queue != nullptr);

    if (parent_mem_tracker->type() == MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL) {
        _memory_limit_bytes = parent_mem_tracker->limit();
        _shared_mem_tracker = parent_mem_tracker;
    } else {
        _memory_limit_bytes = _memory_limit == ABSENT_MEMORY_LIMIT ? parent_mem_tracker->limit()
                                                                   : parent_mem_tracker->limit() * _memory_limit;
    }

    _spill_mem_limit_bytes = _spill_mem_limit_threshold * _memory_limit_bytes;

    //todo (m.bogusz) MemTracker can only handle raw ptr parent so we need to add parent_mem_tracker as member to workgroup
    _mem_tracker = std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, _memory_limit_bytes, _name,
                                                parent_mem_tracker.get());
    _mem_tracker->set_reserve_limit(_spill_mem_limit_bytes);

    _driver_sched_entity.set_queue(std::move(driver_queue));
    _scan_sched_entity.set_queue(workgroup::create_scan_task_queue());
    _connector_scan_sched_entity.set_queue(workgroup::create_scan_task_queue());

    _connector_scan_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP, _memory_limit_bytes, _name + "/connector_scan",
                                         GlobalEnv::GetInstance()->connector_scan_pool_mem_tracker());
}

std::string WorkGroup::to_string() const {
    return fmt::format(
            "(id:{}, name:{}, version:{}, "
            "cpu_weight:{}, exclusive_cpu_cores:{}, mem_limit:{}, concurrency_limit:{}, "
            "bigquery: (cpu_second_limit:{}, mem_limit:{}, scan_rows_limit:{}), "
            "spill_mem_limit_threshold:{}"
            ")",
            _id, _name, _version, _cpu_weight, _exclusive_cpu_cores, _memory_limit_bytes, _concurrency_limit,
            big_query_cpu_second_limit(), _big_query_mem_limit, _big_query_scan_rows_limit, _spill_mem_limit_threshold);
}

void WorkGroup::incr_num_running_drivers() {
    ++_num_running_drivers;
    ++_acc_num_drivers;
}

void WorkGroup::decr_num_running_drivers() {
    int64_t old = _num_running_drivers.fetch_sub(1);
    DCHECK_GT(old, 0);
}

StatusOr<RunningQueryTokenPtr> WorkGroup::acquire_running_query_token(bool enable_group_level_query_queue) {
    int64_t old = _num_running_queries.fetch_add(1);
    if (!enable_group_level_query_queue && _concurrency_limit != ABSENT_CONCURRENCY_LIMIT &&
        old >= _concurrency_limit) {
        _num_running_queries.fetch_sub(1);
        _concurrency_overflow_count++;
        return Status::TooManyTasks(fmt::format("Exceed concurrency limit: {}", _concurrency_limit));
    }
    _num_total_queries++;
    return std::make_unique<RunningQueryToken>(shared_from_this());
}

void WorkGroup::decr_num_queries() {
    int64_t old = _num_running_queries.fetch_sub(1);
    DCHECK_GT(old, 0);
}

Status WorkGroup::check_big_query(const WorkGroupQueryStats& query_stats) {
    // Check big query run time
    if (_big_query_cpu_nanos_limit) {
        int64_t query_runtime_ns = query_stats.cpu_runtime_ns;
        if (query_runtime_ns > _big_query_cpu_nanos_limit) {
            _bigquery_count++;
            return Status::BigQueryCpuSecondLimitExceeded(
                    fmt::format("exceed big query cpu limit: current is {}ns but limit is {}ns", query_runtime_ns,
                                _big_query_cpu_nanos_limit));
        }
    }

    // Check scan rows number
    int64_t bigquery_scan_limit =
            query_stats.scan_rows_limit > 0 ? query_stats.scan_rows_limit : _big_query_scan_rows_limit;
    if (_big_query_scan_rows_limit && query_stats.scan_rows > bigquery_scan_limit) {
        _bigquery_count++;
        return Status::BigQueryScanRowsLimitExceeded(
                fmt::format("exceed big query scan_rows limit: current is {} but limit is {}", query_stats.scan_rows,
                            _big_query_scan_rows_limit));
    }

    return Status::OK();
}

void WorkGroup::copy_metrics(const WorkGroup& rhs) {
    _num_total_queries = rhs.num_total_queries();
    _concurrency_overflow_count = rhs.concurrency_overflow_count();
    _bigquery_count = rhs.bigquery_count();
}

} // namespace starrocks::workgroup
