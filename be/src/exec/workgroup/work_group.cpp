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

#include "exec/workgroup/work_group.h"

#include <utility>

#include "common/config.h"
#include "exec/workgroup/work_group_fwd.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"
#include "util/cpu_info.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks::workgroup {

/// WorkGroupSchedEntity.
template <typename Q>
int64_t WorkGroupSchedEntity<Q>::cpu_limit() const {
    return _workgroup->cpu_limit();
}

template <typename Q>
bool WorkGroupSchedEntity<Q>::is_sq_wg() const {
    return _workgroup->is_sq_wg();
}

template <typename Q>
void WorkGroupSchedEntity<Q>::incr_runtime_ns(int64_t runtime_ns) {
    _vruntime_ns += runtime_ns / cpu_limit();
    _unadjusted_runtime_ns += runtime_ns;
}

template <typename Q>
void WorkGroupSchedEntity<Q>::adjust_runtime_ns(int64_t runtime_ns) {
    _vruntime_ns += runtime_ns / cpu_limit();
}

template class WorkGroupSchedEntity<pipeline::DriverQueue>;
template class WorkGroupSchedEntity<ScanTaskQueue>;

/// WorkGroup.
RunningQueryToken::~RunningQueryToken() {
    wg->decr_num_queries();
}

WorkGroup::WorkGroup(std::string name, int64_t id, int64_t version, size_t cpu_limit, double memory_limit,
                     size_t concurrency, WorkGroupType type)
        : _name(std::move(name)),
          _id(id),
          _version(version),
          _type(type),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency_limit(concurrency),
          _driver_sched_entity(this),
          _scan_sched_entity(this),
          _connector_scan_sched_entity(this) {}

WorkGroup::WorkGroup(const TWorkGroup& twg)
        : _name(twg.name),
          _id(twg.id),
          _driver_sched_entity(this),
          _scan_sched_entity(this),
          _connector_scan_sched_entity(this) {
    if (twg.__isset.cpu_core_limit) {
        _cpu_limit = twg.cpu_core_limit;
    } else {
        _cpu_limit = -1;
    }
    if (twg.__isset.mem_limit) {
        _memory_limit = twg.mem_limit;
    } else {
        _memory_limit = ABSENT_MEMORY_LIMIT;
    }

    if (twg.__isset.concurrency_limit) {
        _concurrency_limit = twg.concurrency_limit;
    } else {
        _concurrency_limit = ABSENT_CONCURRENCY_LIMIT;
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
}

TWorkGroup WorkGroup::to_thrift() const {
    TWorkGroup twg;
    twg.__set_id(_id);
    twg.__set_version(_version);
    return twg;
}

TWorkGroup WorkGroup::to_thrift_verbose() const {
    TWorkGroup twg;
    twg.__set_id(_id);
    twg.__set_name(_name);
    twg.__set_version(_version);
    twg.__set_workgroup_type(_type);
    std::string state = is_marked_del() ? "dead" : "alive";
    twg.__set_state(state);
    twg.__set_cpu_core_limit(_cpu_limit);
    twg.__set_mem_limit(_memory_limit);
    twg.__set_concurrency_limit(_concurrency_limit);
    twg.__set_num_drivers(_acc_num_drivers);
    twg.__set_big_query_mem_limit(_big_query_mem_limit);
    twg.__set_big_query_scan_rows_limit(_big_query_scan_rows_limit);
    twg.__set_big_query_cpu_second_limit(big_query_cpu_second_limit());
    return twg;
}

void WorkGroup::init() {
    _memory_limit_bytes = _memory_limit == ABSENT_MEMORY_LIMIT
                                  ? ExecEnv::GetInstance()->query_pool_mem_tracker()->limit()
                                  : ExecEnv::GetInstance()->query_pool_mem_tracker()->limit() * _memory_limit;
    _mem_tracker = std::make_shared<starrocks::MemTracker>(_memory_limit_bytes, _name,
                                                           ExecEnv::GetInstance()->query_pool_mem_tracker());
    _driver_sched_entity.set_queue(std::make_unique<pipeline::QuerySharedDriverQueue>());
    _scan_sched_entity.set_queue(workgroup::create_scan_task_queue());
    _connector_scan_sched_entity.set_queue(workgroup::create_scan_task_queue());
}

std::string WorkGroup::to_string() const {
    return fmt::format(
            "(id:{}, name:{}, version:{}, "
            "cpu_limit:{}, mem_limit:{}, concurrency_limit:{}, "
            "bigquery: (cpu_second_limit:{}, mem_limit:{}, scan_rows_limit:{})"
            ")",
            _id, _name, _version, _cpu_limit, _memory_limit_bytes, _concurrency_limit, big_query_cpu_second_limit(),
            _big_query_mem_limit, _big_query_scan_rows_limit);
}

void WorkGroup::incr_num_running_drivers() {
    ++_num_running_drivers;
    ++_acc_num_drivers;

    if (is_sq_wg()) {
        WorkGroupManager::instance()->incr_num_running_sq_drivers();
    }
}

void WorkGroup::decr_num_running_drivers() {
    int64_t old = _num_running_drivers.fetch_sub(1);
    DCHECK_GT(old, 0);

    if (is_sq_wg()) {
        WorkGroupManager::instance()->decr_num_running_sq_drivers();
    }
}

StatusOr<RunningQueryTokenPtr> WorkGroup::acquire_running_query_token() {
    int64_t old = _num_running_queries.fetch_add(1);
    if (_concurrency_limit != ABSENT_CONCURRENCY_LIMIT && old >= _concurrency_limit) {
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

Status WorkGroup::check_big_query(const QueryContext& query_context) {
    // Check big query run time
    if (_big_query_cpu_nanos_limit) {
        int64_t query_runtime_ns = query_context.cpu_cost();
        if (query_runtime_ns > _big_query_cpu_nanos_limit) {
            _bigquery_count++;
            return Status::Cancelled(fmt::format("exceed big query cpu limit: current is {}ns but limit is {}ns",
                                                 query_runtime_ns, _big_query_cpu_nanos_limit));
        }
    }

    // Check scan rows number
    int64_t bigquery_scan_limit =
            query_context.get_scan_limit() > 0 ? query_context.get_scan_limit() : _big_query_scan_rows_limit;
    if (_big_query_scan_rows_limit && query_context.cur_scan_rows_num() > bigquery_scan_limit) {
        _bigquery_count++;
        return Status::Cancelled(fmt::format("exceed big query scan_rows limit: current is {} but limit is {}",
                                             query_context.cur_scan_rows_num(), _big_query_scan_rows_limit));
    }

    return Status::OK();
}

void WorkGroup::copy_metrics(const WorkGroup& rhs) {
    _num_total_queries = rhs.num_total_queries();
    _concurrency_overflow_count = rhs.concurrency_overflow_count();
    _bigquery_count = rhs.bigquery_count();
}

/// WorkGroupManager.
WorkGroupManager::WorkGroupManager() = default;

WorkGroupManager::~WorkGroupManager() = default;
void WorkGroupManager::destroy() {
    std::unique_lock write_lock(_mutex);

    update_metrics_unlocked();
    _workgroups.clear();
}

WorkGroupPtr WorkGroupManager::add_workgroup(const WorkGroupPtr& wg) {
    std::unique_lock write_lock(_mutex);
    auto unique_id = wg->unique_id();
    create_workgroup_unlocked(wg, write_lock);
    if (_workgroup_versions.count(wg->id()) && _workgroup_versions[wg->id()] == wg->version()) {
        return _workgroups[unique_id];
    } else {
        return get_default_workgroup();
    }
}

void WorkGroupManager::add_metrics_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock) {
    std::call_once(init_metrics_once_flag, []() {
        StarRocksMetrics::instance()->metrics()->register_hook("work_group_metrics_hook",
                                                               [] { WorkGroupManager::instance()->update_metrics(); });
    });

    if (_wg_metrics.count(wg->name()) == 0) {
        // Unlock when register_metric to avoid deadlock, since update_metric would take the MetricRegistry::mutex then WorkGroupManager::mutex
        unique_lock.unlock();

        // cpu limit.
        auto resource_group_cpu_limit_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool cpu_limit_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_cpu_limit_ratio", MetricLabels().add("name", wg->name()),
                resource_group_cpu_limit_ratio.get());
        // cpu use ratio.
        auto resource_group_cpu_use_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool cpu_ratio_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_cpu_use_ratio", MetricLabels().add("name", wg->name()),
                resource_group_cpu_use_ratio.get());
        // scan use ratio.
        auto resource_group_scan_use_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool scan_ratio_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_scan_use_ratio", MetricLabels().add("name", wg->name()),
                resource_group_scan_use_ratio.get());
        // connector scan use ratio.
        auto resource_group_connector_scan_use_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool connector_scan_ratio_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_connector_scan_use_ratio", MetricLabels().add("name", wg->name()),
                resource_group_connector_scan_use_ratio.get());
        // mem limit.
        auto resource_group_mem_limit_bytes = std::make_unique<starrocks::IntGauge>(MetricUnit::BYTES);
        bool mem_limit_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_mem_limit_bytes", MetricLabels().add("name", wg->name()),
                resource_group_mem_limit_bytes.get());
        // mem use bytes.
        auto resource_group_mem_allocated_bytes = std::make_unique<starrocks::IntGauge>(MetricUnit::BYTES);
        bool mem_inuse_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_mem_inuse_bytes", MetricLabels().add("name", wg->name()),
                resource_group_mem_allocated_bytes.get());
        // running queries
        auto resource_group_running_queries = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
        bool running_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_running_queries", MetricLabels().add("name", wg->name()),
                resource_group_running_queries.get());

        // total queries
        auto resource_group_total_queries = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
        bool total_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_total_queries", MetricLabels().add("name", wg->name()),
                resource_group_total_queries.get());

        // concurrency overflow
        auto resource_group_concurrency_overflow = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
        bool concurrency_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_concurrency_overflow_count", MetricLabels().add("name", wg->name()),
                resource_group_concurrency_overflow.get());

        // bigquery count
        auto resource_group_bigquery_count = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
        bool bigquery_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_bigquery_count", MetricLabels().add("name", wg->name()),
                resource_group_bigquery_count.get());

        unique_lock.lock();
        if (cpu_limit_registered) _wg_cpu_limit_metrics.emplace(wg->name(), std::move(resource_group_cpu_limit_ratio));
        if (cpu_ratio_registered) _wg_cpu_metrics.emplace(wg->name(), std::move(resource_group_cpu_use_ratio));
        if (scan_ratio_registered) _wg_scan_metrics.emplace(wg->name(), std::move(resource_group_scan_use_ratio));
        if (connector_scan_ratio_registered)
            _wg_connector_scan_metrics.emplace(wg->name(), std::move(resource_group_connector_scan_use_ratio));
        if (mem_limit_registered) _wg_mem_limit_metrics.emplace(wg->name(), std::move(resource_group_mem_limit_bytes));
        if (mem_inuse_registered) _wg_mem_metrics.emplace(wg->name(), std::move(resource_group_mem_allocated_bytes));
        if (running_registered) _wg_running_queries.emplace(wg->name(), std::move(resource_group_running_queries));
        if (total_registered) _wg_total_queries.emplace(wg->name(), std::move(resource_group_total_queries));
        if (concurrency_registered)
            _wg_concurrency_overflow_count.emplace(wg->name(), std::move(resource_group_concurrency_overflow));
        if (bigquery_registered) _wg_bigquery_count.emplace(wg->name(), std::move(resource_group_bigquery_count));
    }
    _wg_metrics[wg->name()] = wg->unique_id();
}

double _calculate_ratio(int64_t curr_value, int64_t sum_value) {
    if (sum_value <= 0) {
        return 0;
    }
    return double(curr_value) / sum_value;
}

void WorkGroupManager::update_metrics_unlocked() {
    int64_t sum_cpu_runtime_ns = 0;
    int64_t sum_scan_runtime_ns = 0;
    int64_t sum_connector_scan_runtime_ns = 0;
    for (const auto& [_, wg] : _workgroups) {
        wg->driver_sched_entity()->mark_curr_runtime_ns();
        wg->scan_sched_entity()->mark_curr_runtime_ns();
        wg->connector_scan_sched_entity()->mark_curr_runtime_ns();

        sum_cpu_runtime_ns += wg->driver_sched_entity()->growth_runtime_ns();
        sum_scan_runtime_ns += wg->scan_sched_entity()->growth_runtime_ns();
        sum_connector_scan_runtime_ns += wg->connector_scan_sched_entity()->growth_runtime_ns();
    }
    DeferOp mark_last_runtime_op([this] {
        for (const auto& [_, wg] : _workgroups) {
            wg->driver_sched_entity()->mark_last_runtime_ns();
            wg->scan_sched_entity()->mark_last_runtime_ns();
            wg->connector_scan_sched_entity()->mark_last_runtime_ns();
        }
    });

    for (const auto& [name, wg_id] : _wg_metrics) {
        auto wg_it = _workgroups.find(wg_id);
        if (wg_it != _workgroups.end()) {
            const auto& wg = wg_it->second;
            VLOG(2) << "workgroup update_metrics " << name;

            double cpu_expected_use_ratio = _calculate_ratio(wg->cpu_limit(), _sum_cpu_limit);
            double cpu_use_ratio = _calculate_ratio(wg->driver_sched_entity()->growth_runtime_ns(), sum_cpu_runtime_ns);
            double scan_use_ratio = _calculate_ratio(wg->scan_sched_entity()->growth_runtime_ns(), sum_scan_runtime_ns);
            double connector_scan_use_ratio = _calculate_ratio(wg->connector_scan_sched_entity()->growth_runtime_ns(),
                                                               sum_connector_scan_runtime_ns);

            _wg_cpu_limit_metrics[name]->set_value(cpu_expected_use_ratio);
            _wg_cpu_metrics[name]->set_value(cpu_use_ratio);
            _wg_scan_metrics[name]->set_value(scan_use_ratio);
            _wg_connector_scan_metrics[name]->set_value(connector_scan_use_ratio);
            _wg_mem_limit_metrics[name]->set_value(wg->mem_limit_bytes());
            _wg_mem_metrics[name]->set_value(wg->mem_tracker()->consumption());
            _wg_running_queries[name]->set_value(wg->num_running_queries());
            _wg_total_queries[name]->set_value(wg->num_total_queries());
            _wg_concurrency_overflow_count[name]->set_value(wg->concurrency_overflow_count());
            _wg_bigquery_count[name]->set_value(wg->bigquery_count());
        } else {
            VLOG(2) << "workgroup update_metrics " << name << ", workgroup not exists so cleanup metrics";

            _wg_cpu_limit_metrics[name]->set_value(0);
            _wg_cpu_metrics[name]->set_value(0);
            _wg_scan_metrics[name]->set_value(0);
            _wg_connector_scan_metrics[name]->set_value(0);
            _wg_mem_limit_metrics[name]->set_value(0);
            _wg_mem_metrics[name]->set_value(0);
            _wg_running_queries[name]->set_value(0);
            _wg_total_queries[name]->set_value(0);
            _wg_concurrency_overflow_count[name]->set_value(0);
            _wg_bigquery_count[name]->set_value(0);
        }
    }
}

void WorkGroupManager::update_metrics() {
    std::unique_lock write_lock(_mutex);
    update_metrics_unlocked();
}

WorkGroupPtr WorkGroupManager::get_default_workgroup() {
    std::shared_lock read_lock(_mutex);
    auto unique_id = WorkGroup::create_unique_id(WorkGroup::DEFAULT_VERSION, WorkGroup::DEFAULT_WG_ID);
    DCHECK(_workgroups.count(unique_id));
    return _workgroups[unique_id];
}

void WorkGroupManager::apply(const std::vector<TWorkGroupOp>& ops) {
    std::unique_lock write_lock(_mutex);

    auto it = _workgroup_expired_versions.begin();
    // collect removable workgroups
    while (it != _workgroup_expired_versions.end()) {
        auto wg_it = _workgroups.find(*it);
        if (wg_it != _workgroups.end() && wg_it->second->is_removable()) {
            int128_t wg_id = *it;
            _sum_cpu_limit -= wg_it->second->cpu_limit();
            _workgroups.erase(wg_it);
            _workgroup_expired_versions.erase(it++);
            LOG(INFO) << "cleanup expired workgroup version:  " << (int64_t)(wg_id >> 64) << "," << (int64_t)wg_id;
        } else {
            ++it;
        }
    }

    for (const auto& op : ops) {
        auto op_type = op.op_type;
        auto wg = std::make_shared<WorkGroup>(op.workgroup);
        switch (op_type) {
        case TWorkGroupOpType::WORKGROUP_OP_CREATE:
            create_workgroup_unlocked(wg, write_lock);
            break;
        case TWorkGroupOpType::WORKGROUP_OP_ALTER:
            alter_workgroup_unlocked(wg, write_lock);
            break;
        case TWorkGroupOpType::WORKGROUP_OP_DELETE:
            delete_workgroup_unlocked(wg);
            break;
        }
    }
}

void WorkGroupManager::create_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock) {
    auto unique_id = wg->unique_id();
    // only current version not exists or current version is older than wg->version(), then create a new WorkGroup
    if (_workgroup_versions.count(wg->id()) && _workgroup_versions[wg->id()] >= wg->version()) {
        return;
    }
    wg->init();
    _workgroups[unique_id] = wg;

    _sum_cpu_limit += wg->cpu_limit();
    if (wg->is_sq_wg()) {
        _rt_cpu_limit = wg->cpu_limit();
    }

    // old version exists, so mark the stale version delete
    if (_workgroup_versions.count(wg->id())) {
        auto stale_version = _workgroup_versions[wg->id()];
        DCHECK(stale_version < wg->version());
        auto old_unique_id = WorkGroup::create_unique_id(wg->id(), stale_version);
        if (_workgroups.count(old_unique_id)) {
            _workgroups[old_unique_id]->mark_del();
            _workgroup_expired_versions.push_back(old_unique_id);
            LOG(INFO) << "workgroup expired version: " << wg->name() << "(" << wg->id() << "," << stale_version << ")";

            // Copy metrics from old version work-group
            auto& old_wg = _workgroups[old_unique_id];
            wg->copy_metrics(*old_wg);
        }
    }
    // install new version
    _workgroup_versions[wg->id()] = wg->version();

    // Update metrics
    add_metrics_unlocked(wg, unique_lock);
}

void WorkGroupManager::alter_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock) {
    create_workgroup_unlocked(wg, unique_lock);
    LOG(INFO) << "alter workgroup " << wg->to_string();
}

void WorkGroupManager::delete_workgroup_unlocked(const WorkGroupPtr& wg) {
    auto id = wg->id();
    auto version_it = _workgroup_versions.find(id);
    if (version_it == _workgroup_versions.end()) {
        return;
    }

    auto curr_version = version_it->second;
    if (wg->version() <= curr_version) {
        LOG(WARNING) << "try to delete workgroup with fresher version: "
                     << "[delete_version=" << wg->version() << "] "
                     << "[curr_version=" << curr_version << "]";
        return;
    }

    auto unique_id = WorkGroup::create_unique_id(id, curr_version);
    auto wg_it = _workgroups.find(unique_id);
    if (wg_it != _workgroups.end()) {
        wg_it->second->mark_del();
        _workgroup_expired_versions.push_back(unique_id);
        LOG(INFO) << "workgroup expired version: " << wg->name() << "(" << wg->id() << "," << curr_version << ")";
    }
    LOG(INFO) << "delete workgroup " << wg->name();
}

std::vector<TWorkGroup> WorkGroupManager::list_workgroups() {
    std::shared_lock read_lock(_mutex);
    std::vector<TWorkGroup> alive_workgroups;
    for (auto& [_, wg] : _workgroups) {
        if (wg->version() != WorkGroup::DEFAULT_VERSION) {
            alive_workgroups.push_back(wg->to_thrift());
        }
    }
    return alive_workgroups;
}

std::vector<TWorkGroup> WorkGroupManager::list_all_workgroups() {
    std::vector<TWorkGroup> workgroups;
    {
        std::shared_lock read_lock(_mutex);
        workgroups.reserve(_workgroups.size());
        for (auto& _workgroup : _workgroups) {
            const auto& wg = _workgroup.second;
            auto twg = wg->to_thrift_verbose();
            workgroups.push_back(twg);
        }
    }
    std::sort(workgroups.begin(), workgroups.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.name < rhs.name; });
    return workgroups;
}

size_t WorkGroupManager::normal_workgroup_cpu_hard_limit() const {
    static int num_hardware_cores = CpuInfo::num_cores();
    return std::max<int>(1, num_hardware_cores - _rt_cpu_limit);
}

/// DefaultWorkGroupInitialization.
DefaultWorkGroupInitialization::DefaultWorkGroupInitialization() {
    // The default workgroup can use all the resources of CPU and memory,
    // so set cpu_limit to max_executor_threads and memory_limit to 100%.
    int64_t cpu_limit = ExecEnv::GetInstance()->max_executor_threads();
    double memory_limit = 1.0;
    auto default_wg = std::make_shared<WorkGroup>("default_wg", WorkGroup::DEFAULT_WG_ID, WorkGroup::DEFAULT_VERSION,
                                                  cpu_limit, memory_limit, 0, WorkGroupType::WG_DEFAULT);
    WorkGroupManager::instance()->add_workgroup(default_wg);
}

} // namespace starrocks::workgroup
