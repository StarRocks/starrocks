// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "common/config.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gen_cpp/internal_service.pb.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"
#include "util/time.h"

namespace starrocks {
namespace workgroup {

WorkGroup::WorkGroup(const std::string& name, int64_t id, int64_t version, size_t cpu_limit, double memory_limit,
                     size_t concurrency, WorkGroupType type)
        : _name(name),
          _id(id),
          _version(version),
          _type(type),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency_limit(concurrency) {}

WorkGroup::WorkGroup(const TWorkGroup& twg) : _name(twg.name), _id(twg.id) {
    if (twg.__isset.cpu_core_limit) {
        _cpu_limit = twg.cpu_core_limit;
    } else {
        _cpu_limit = -1;
    }
    if (twg.__isset.mem_limit) {
        _memory_limit = twg.mem_limit;
    } else {
        _memory_limit = -1;
    }

    if (twg.__isset.concurrency_limit) {
        _concurrency_limit = twg.concurrency_limit;
    } else {
        _concurrency_limit = 0;
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
        _big_query_cpu_second_limit = twg.big_query_cpu_second_limit * NANOS_PER_SEC;
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
    twg.__set_big_query_cpu_second_limit(_big_query_cpu_second_limit);
    return twg;
}

void WorkGroup::init() {
    _memory_limit_bytes = ExecEnv::GetInstance()->query_pool_mem_tracker()->limit() * _memory_limit;
    _mem_tracker = std::make_shared<starrocks::MemTracker>(_memory_limit_bytes, _name,
                                                           ExecEnv::GetInstance()->query_pool_mem_tracker());
    _driver_queue = std::make_unique<pipeline::QuerySharedDriverQueueWithoutLock>();
    _scan_task_queue = std::make_unique<PriorityScanTaskQueue>(config::pipeline_scan_thread_pool_queue_size);
}

std::string WorkGroup::to_string() const {
    return fmt::format(
            "(id:{}, name:{}, version:{}, "
            "cpu_limit:{}, mem_limit:{}, concurrency_limit:{}, "
            "bigquery: (cpu_second_limit:{}, mem_limit:{}, scan_rows_limit:{})"
            ")",
            _id, _name, _version, _cpu_limit, _memory_limit_bytes, _concurrency_limit, _big_query_cpu_second_limit,
            _big_query_mem_limit, _big_query_scan_rows_limit);
}

double WorkGroup::get_cpu_expected_use_ratio() const {
    return static_cast<double>(_cpu_limit) / WorkGroupManager::instance()->sum_cpu_limit();
}

double WorkGroup::get_cpu_actual_use_ratio() const {
    return _cpu_actual_use_ratio;
}

int64_t WorkGroup::mem_limit() const {
    return _memory_limit_bytes;
}

WorkGroupManager::WorkGroupManager() {
    _driver_worker_owner_manager = std::make_unique<WorkerOwnerManager>(
            config::pipeline_exec_thread_pool_thread_num > 0 ? config::pipeline_exec_thread_pool_thread_num
                                                             : std::thread::hardware_concurrency());
    _scan_worker_owner_manager = std::make_unique<WorkerOwnerManager>(
            config::pipeline_scan_thread_pool_thread_num > 0 ? config::pipeline_scan_thread_pool_thread_num
                                                             : std::thread::hardware_concurrency());
    _connector_scan_worker_owner_manager =
            std::make_unique<WorkerOwnerManager>(config::pipeline_hdfs_scan_thread_pool_thread_num);
}

WorkGroupManager::~WorkGroupManager() {}
void WorkGroupManager::destroy() {
    std::unique_lock write_lock(_mutex);

    _driver_worker_owner_manager.reset(nullptr);
    _scan_worker_owner_manager.reset(nullptr);
    _connector_scan_worker_owner_manager.reset(nullptr);
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

        //cpu limit
        auto resource_group_cpu_limit_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool cpu_limit_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_cpu_limit_ratio", MetricLabels().add("name", wg->name()),
                resource_group_cpu_limit_ratio.get());
        //cpu concurrent
        auto resource_group_cpu_use_ratio = std::make_unique<starrocks::DoubleGauge>(MetricUnit::PERCENT);
        bool cpu_ratio_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_cpu_use_ratio", MetricLabels().add("name", wg->name()),
                resource_group_cpu_use_ratio.get());
        //mem limit
        auto resource_group_mem_limit_bytes = std::make_unique<starrocks::IntGauge>(MetricUnit::BYTES);
        bool mem_limit_registered = StarRocksMetrics::instance()->metrics()->register_metric(
                "resource_group_mem_limit_bytes", MetricLabels().add("name", wg->name()),
                resource_group_mem_limit_bytes.get());
        //mem concurrent
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

void WorkGroup::copy_metrics(const WorkGroup& rhs) {
    _cpu_actual_use_ratio = rhs.get_cpu_expected_use_ratio();
    _num_total_queries = rhs.num_total_queries();
    _concurrency_overflow_count = rhs.concurrency_overflow_count();
    _bigquery_count = rhs.bigquery_count();
}

void WorkGroupManager::update_metrics_unlocked() {
    for (auto& wg_metric : _wg_metrics) {
        auto wg = _workgroups.find(wg_metric.second);
        auto& name = wg_metric.first;
        if (wg != _workgroups.end()) {
            VLOG(2) << "workgroup update_metrics " << name;

            _wg_cpu_limit_metrics.find(wg_metric.first)->second->set_value(wg->second->get_cpu_expected_use_ratio());
            _wg_cpu_metrics.find(wg_metric.first)->second->set_value(wg->second->get_cpu_actual_use_ratio());
            _wg_mem_limit_metrics.find(wg_metric.first)->second->set_value(wg->second->mem_limit());
            _wg_mem_metrics.find(wg_metric.first)->second->set_value(wg->second->mem_tracker()->consumption());
            _wg_running_queries[name]->set_value(wg->second->num_running_queries());
            _wg_total_queries[name]->set_value(wg->second->num_total_queries());
            _wg_concurrency_overflow_count[name]->set_value(wg->second->concurrency_overflow_count());
            _wg_bigquery_count[name]->set_value(wg->second->bigquery_count());
        } else {
            VLOG(2) << "workgroup update_metrics " << name << ", workgroup not exists so cleanup metrics";

            _wg_cpu_limit_metrics[name]->set_value(0);
            _wg_cpu_metrics[name]->set_value(0);
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

// Should be call when read chunk_num from disk.
void WorkGroup::incr_period_scaned_chunk_num(int32_t chunk_num) {
    _period_scaned_chunk_num += chunk_num;
}

void WorkGroup::incr_period_ask_chunk_num(int32_t chunk_num) {
    _period_ask_chunk_num += chunk_num;
}

double WorkGroup::get_expect_factor() const {
    return _expect_factor;
}

double WorkGroup::get_diff_factor() const {
    return _diff_factor;
}

double WorkGroup::get_select_factor() const {
    return _select_factor;
}

void WorkGroup::set_select_factor(double value) {
    _select_factor = value;
}

void WorkGroup::update_select_factor(double value) {
    _select_factor += value;
}

double WorkGroup::get_cur_select_factor() const {
    return _cur_select_factor;
}

void WorkGroup::update_cur_select_factor(double value) {
    _cur_select_factor += value;
}

void WorkGroup::estimate_trend_factor_period() {
    // As an example
    // During the execution period, This WorkGroup actually consumes 70 chunks and uses 80% of the cpu resources
    // But in reality, the WorkGroup's cpu resources are limited to 70%
    // At the same time, the WorkGroup reads 200 chunks from io_threads
    // Therefore, we calculate the correlation factor as follows
    // decrease_factor = 70 / 0.8 * (0.7 / 0.8)
    // increase_factor = 200 / 0.7
    // _expect_factor indicates the percentage of io resources we expect the WorkGroup to use so that it meets the 70% cpu limit
    // so _expect_factor = decrease_factor / increase_factor * 0.7

    double decrease_factor = _period_ask_chunk_num / get_cpu_actual_use_ratio() * get_cpu_expected_use_ratio() /
                             get_cpu_actual_use_ratio();

    double increase_factor = _period_scaned_chunk_num / get_cpu_actual_use_ratio();

    _expect_factor = decrease_factor / increase_factor * get_cpu_actual_use_ratio();

    // diff_factor indicates the difference between the actual percentage of io resources used and the limited cpu percentage
    // If it is negative, it means that there are not enough resources and more resources are needed
    // If it is positive, it means that its resources are sufficient and can be reduced by a fraction
    //_diff_factor = _select_factor - _expect_factor;
    _diff_factor = _select_factor - _expect_factor;

    _period_scaned_chunk_num = 1;
    _period_ask_chunk_num = 1;
}

Status WorkGroup::try_incr_num_queries() {
    int64_t old = _num_running_queries.fetch_add(1);
    if (_concurrency_limit > 0 && old >= _concurrency_limit) {
        _num_running_queries.fetch_sub(1);
        _concurrency_overflow_count++;
        return Status::TooManyTasks(fmt::format("Exceed concurrency limit: {}", _concurrency_limit));
    }
    _num_total_queries++;
    return Status::OK();
}

void WorkGroup::decr_num_queries() {
    int64_t old = _num_running_queries.fetch_sub(1);
    DCHECK_GT(old, 0);
}

Status WorkGroup::check_big_query(const QueryContext& query_context) {
    // Check big query run time
    if (_big_query_cpu_second_limit) {
        int64_t wg_growth_cpu_use_cost = total_cpu_cost() - query_context.init_wg_cpu_cost();
        if (wg_growth_cpu_use_cost > _big_query_cpu_second_limit) {
            _bigquery_count++;
            return Status::Cancelled(fmt::format("exceed big query cpu limit: current is {} but limit is {}",
                                                 wg_growth_cpu_use_cost, _big_query_cpu_second_limit));
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

void WorkGroupManager::apply(const std::vector<TWorkGroupOp>& ops) {
    std::unique_lock write_lock(_mutex);

    size_t original_num_workgroups = _workgroups.size();
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
    if (original_num_workgroups != _workgroups.size()) {
        reassign_worker_to_wgs();
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
    reassign_worker_to_wgs();

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
    auto version_id = version_it->second;
    DCHECK(version_id < wg->version());
    auto unique_id = WorkGroup::create_unique_id(id, version_id);
    auto wg_it = _workgroups.find(unique_id);
    if (wg_it != _workgroups.end()) {
        wg_it->second->mark_del();
        _workgroup_expired_versions.push_back(unique_id);
        LOG(INFO) << "workgroup expired version: " << wg->name() << "(" << wg->id() << "," << version_id << ")";
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
        for (auto it = _workgroups.begin(); it != _workgroups.end(); ++it) {
            const auto& wg = it->second;
            auto twg = wg->to_thrift_verbose();
            workgroups.push_back(twg);
        }
    }
    std::sort(workgroups.begin(), workgroups.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.name < rhs.name; });
    return workgroups;
}

void WorkGroupManager::reassign_worker_to_wgs() {
    _driver_worker_owner_manager->reassign_to_wgs(_workgroups, _sum_cpu_limit);
    _scan_worker_owner_manager->reassign_to_wgs(_workgroups, _sum_cpu_limit);
    _connector_scan_worker_owner_manager->reassign_to_wgs(_workgroups, _sum_cpu_limit);
}

std::shared_ptr<WorkGroupPtrSet> WorkGroupManager::get_owners_of_driver_worker(int worker_id) {
    return _driver_worker_owner_manager->get_owners(worker_id);
}

bool WorkGroupManager::should_yield_driver_worker(int worker_id, const WorkGroupPtr& running_wg) {
    return _driver_worker_owner_manager->should_yield(worker_id, running_wg);
}

std::shared_ptr<WorkGroupPtrSet> WorkGroupManager::get_owners_of_scan_worker(ScanExecutorType type, int worker_id) {
    return type == TypeOlapScanExecutor ? _scan_worker_owner_manager->get_owners(worker_id)
                                        : _connector_scan_worker_owner_manager->get_owners(worker_id);
}

bool WorkGroupManager::should_yield_scan_worker(ScanExecutorType type, int worker_id, const WorkGroupPtr& running_wg) {
    return type == TypeOlapScanExecutor ? _scan_worker_owner_manager->should_yield(worker_id, running_wg)
                                        : _connector_scan_worker_owner_manager->should_yield(worker_id, running_wg);
}

DefaultWorkGroupInitialization::DefaultWorkGroupInitialization() {
    // The default workgroup can use all the resources of CPU and memory,
    // so set cpu_limit to max_executor_threads and memory_limit to 100%.
    int64_t cpu_limit = ExecEnv::GetInstance()->max_executor_threads();
    double memory_limit = 1.0;
    auto default_wg = std::make_shared<WorkGroup>("default_wg", WorkGroup::DEFAULT_WG_ID, WorkGroup::DEFAULT_VERSION,
                                                  cpu_limit, memory_limit, 0, WorkGroupType::WG_DEFAULT);
    WorkGroupManager::instance()->add_workgroup(default_wg);
}

WorkerOwnerManager::WorkerOwnerManager(int num_total_workers) : _num_total_workers(num_total_workers) {
    for (int i = 0; i < 2; ++i) {
        _worker_id2owner_wgs[i] = std::vector<std::shared_ptr<WorkGroupPtrSet>>(_num_total_workers);
    }
}

void WorkerOwnerManager::reassign_to_wgs(const std::unordered_map<int128_t, WorkGroupPtr>& workgroups,
                                         int sum_cpu_limit) {
    auto& worker_id2owner_wgs = _worker_id2owner_wgs[(_index + 1) % 2];
    for (auto& owner_wgs : worker_id2owner_wgs) {
        owner_wgs = std::make_shared<WorkGroupPtrSet>();
    }

    int worker_id = 0;
    for (const auto& [_, wg] : workgroups) {
        int num_num_workers = std::max(1, _num_total_workers * int(wg->cpu_limit()) / sum_cpu_limit);
        for (int i = 0; i < num_num_workers; ++i) {
            worker_id2owner_wgs[(worker_id++) % _num_total_workers]->emplace(wg);
        }
    }

    _index++;
}

bool WorkerOwnerManager::should_yield(int worker_id, const WorkGroupPtr& running_wg) const {
    if (worker_id >= _num_total_workers) {
        return false;
    }

    auto wgs = _worker_id2owner_wgs[_index % 2][worker_id];
    // This worker thread doesn't belong to any workgroup, or running_wg is the owner of it.
    if (wgs == nullptr || wgs->empty() || wgs->find(running_wg) != wgs->end()) {
        return false;
    }

    // Any owner of this worker thread has running drivers.
    for (const auto& wg : *wgs) {
        if (wg->num_drivers() > 0) {
            return true;
        }
    }

    return false;
}

} // namespace workgroup
} // namespace starrocks
