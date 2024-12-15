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

#include "storage/compaction_manager.h"

#include <chrono>
#include <thread>

#include "compaction_manager.h"
#include "storage/data_dir.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"

using namespace std::chrono_literals;

namespace starrocks {

CompactionManager::CompactionManager() : _next_task_id(0) {}

void CompactionManager::stop() {
    _stop.store(true, std::memory_order_release);
    if (_scheduler_thread.joinable()) {
        _scheduler_thread.join();
    }
    if (_compaction_pool) {
        _compaction_pool->shutdown();
    }
    if (_dispatch_update_candidate_thread.joinable()) {
        _dispatch_update_candidate_thread.join();
    }
    if (_update_candidate_pool) {
        _update_candidate_pool->shutdown();
    }
}

void CompactionManager::schedule() {
    auto st = ThreadPoolBuilder("up_candidates")
                      .set_min_threads(1)
                      .set_max_threads(5)
                      .set_max_queue_size(100000)
                      .build(&_update_candidate_pool);
    DCHECK(st.ok());

    _dispatch_update_candidate_thread = std::thread([this] { _dispatch_worker(); });
    Thread::set_thread_name(_dispatch_update_candidate_thread, "dispatch_candidate");

    st = ThreadPoolBuilder("compact_pool")
                 .set_min_threads(1)
                 .set_max_threads(std::max(1, _max_task_num))
                 .set_max_queue_size(1000)
                 .build(&_compaction_pool);
    DCHECK(st.ok());
    REGISTER_THREAD_POOL_METRICS(compact_pool, _compaction_pool);

    _scheduler_thread = std::thread([this] { _schedule(); });
    Thread::set_thread_name(_scheduler_thread, "compact_sched");
}

void CompactionManager::_schedule() {
    LOG(INFO) << "start compaction scheduler";
    while (!_stop.load(std::memory_order_consume)) {
        ++_round;
        _wait_to_run();
        CompactionCandidate compaction_candidate;

        if (!pick_candidate(&compaction_candidate)) {
            std::unique_lock<std::mutex> lk(_mutex);
            _cv.wait_for(lk, 1000ms);
        } else {
            submit_compaction_task(compaction_candidate);
        }
    }
}

void CompactionManager::submit_compaction_task(const CompactionCandidate& compaction_candidate) {
    if (compaction_candidate.type == CompactionType::BASE_COMPACTION) {
        StarRocksMetrics::instance()->tablet_base_max_compaction_score.set_value(compaction_candidate.score);
    } else {
        StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.set_value(compaction_candidate.score);
    }

    auto task_id = next_compaction_task_id();
    LOG(INFO) << "submit task to compaction pool"
              << ", task_id:" << task_id << ", tablet_id:" << compaction_candidate.tablet->tablet_id()
              << ", compaction_type:" << starrocks::to_string(compaction_candidate.type)
              << ", compaction_score:" << compaction_candidate.score << " for round:" << _round
              << ", candidates_size:" << candidates_size();
    auto manager = this;
    auto tablet = std::move(compaction_candidate.tablet);
    auto st = _compaction_pool->submit_func([tablet, task_id, manager] {
        auto compaction_task = tablet->create_compaction_task();
        if (compaction_task != nullptr) {
            CompactionCandidate candidate;
            candidate.type = compaction_task->compaction_type();
            candidate.tablet = tablet;
            if (manager->check_compaction_disabled(candidate)) {
                LOG(INFO) << "skip base compaction task " << task_id << " for tablet " << tablet->tablet_id();
                return;
            }
            compaction_task->set_task_id(task_id);
            compaction_task->start();
        }
    });
    if (!st.ok()) {
        LOG(WARNING) << "submit compaction task " << task_id << " to compaction pool failed. status:" << st.to_string();
        update_tablet_async(tablet);
    }
}

void CompactionManager::_notify() {
    std::unique_lock<std::mutex> lk(_mutex);
    _cv.notify_one();
}

bool CompactionManager::_can_schedule_next() {
    return (!check_if_exceed_max_task_num() && candidates_size() > 0) || _stop.load(std::memory_order_consume);
}

void CompactionManager::_wait_to_run() {
    std::unique_lock<std::mutex> lk(_mutex);
    // check _can_schedule_next every five second to avoid deadlock and support modifying config online
    while (!_cv.wait_for(lk, 100ms, [this] { return _can_schedule_next(); })) {
    }
}

std::shared_ptr<CompactionTask> CompactionManager::_try_get_next_compaction_task() {
    VLOG(2) << "try to get next qualified tablet for round:" << _round
            << ", current candidates size:" << candidates_size();
    CompactionCandidate compaction_candidate;
    std::shared_ptr<CompactionTask> compaction_task = nullptr;

    if (pick_candidate(&compaction_candidate)) {
        compaction_task = compaction_candidate.tablet->create_compaction_task();
    }

    return compaction_task;
}

void CompactionManager::init_max_task_num(int32_t num) {
    _max_task_num = num;
}

void CompactionManager::update_candidates(std::vector<CompactionCandidate> candidates) {
    {
        std::lock_guard lg(_candidates_mutex);
        // TODO(meegoo): This is very inefficient to implement, just to fix bug, it will refactor later
        for (auto iter = _compaction_candidates.begin(); iter != _compaction_candidates.end();) {
            bool has_erase = false;
            for (auto& candidate : candidates) {
                if (candidate.tablet->tablet_id() == iter->tablet->tablet_id()) {
                    if (iter->type == CompactionType::BASE_COMPACTION) {
                        StarRocksMetrics::instance()->wait_base_compaction_task_num.increment(-1);
                    } else {
                        StarRocksMetrics::instance()->wait_cumulative_compaction_task_num.increment(-1);
                    }
                    iter = _compaction_candidates.erase(iter);
                    has_erase = true;
                    break;
                }
            }
            if (!has_erase) {
                iter++;
            }
        }
        for (auto& candidate : candidates) {
            if (_check_compaction_disabled(candidate)) {
                continue;
            }
            if (candidate.tablet->enable_compaction()) {
                VLOG(2) << "update candidate " << candidate.tablet->tablet_id() << " type "
                        << starrocks::to_string(candidate.type) << " score " << candidate.score;
                if (candidate.type == CompactionType::BASE_COMPACTION) {
                    StarRocksMetrics::instance()->wait_base_compaction_task_num.increment(1);
                } else {
                    StarRocksMetrics::instance()->wait_cumulative_compaction_task_num.increment(1);
                }
                _compaction_candidates.emplace(std::move(candidate));
            }
        }
        // if candidates size exceed max, remove the last one which has the lowest score
        // too many candidates will cause too many resources occupied and make priority queue adjust too slow
        while (_compaction_candidates.size() > config::max_compaction_candidate_num &&
               !_compaction_candidates.empty()) {
            _compaction_candidates.erase(std::prev(_compaction_candidates.end()));
        }
    }
    _notify();
}

void CompactionManager::remove_candidate(int64_t tablet_id) {
    std::lock_guard lg(_candidates_mutex);
    for (auto iter = _compaction_candidates.begin(); iter != _compaction_candidates.end();) {
        if (tablet_id == iter->tablet->tablet_id()) {
            if (iter->type == CompactionType::BASE_COMPACTION) {
                StarRocksMetrics::instance()->wait_base_compaction_task_num.increment(-1);
            } else {
                StarRocksMetrics::instance()->wait_cumulative_compaction_task_num.increment(-1);
            }
            iter = _compaction_candidates.erase(iter);
            break;
        } else {
            iter++;
        }
    }
}

bool CompactionManager::check_compaction_disabled(const CompactionCandidate& candidate) {
    std::lock_guard lg(_candidates_mutex);
    return _check_compaction_disabled(candidate);
}

bool CompactionManager::_check_compaction_disabled(const CompactionCandidate& candidate) {
    if (candidate.type == CompactionType::BASE_COMPACTION &&
        _table_to_disable_deadline_map.find(candidate.tablet->tablet_meta()->table_id()) !=
                _table_to_disable_deadline_map.end()) {
        int64_t deadline = _table_to_disable_deadline_map[candidate.tablet->tablet_meta()->table_id()];
        if (deadline > 0 && UnixSeconds() < deadline) {
            return true;
        } else {
            // disable compaction deadline has passed, remove it from map
            _table_to_disable_deadline_map.erase(candidate.tablet->tablet_meta()->table_id());
            // check if the tablet should compact now after the deadline
            update_tablet_async(candidate.tablet);
            LOG(INFO) << "remove disable table compaction, table_id:" << candidate.tablet->tablet_meta()->table_id()
                      << ", deadline:" << deadline;
        }
    }
    return false;
}

bool CompactionManager::_check_precondition(const CompactionCandidate& candidate) {
    if (!candidate.tablet) {
        LOG(WARNING) << "candidate with null tablet";
        return false;
    }
    const TabletSharedPtr& tablet = candidate.tablet;
    if (tablet->tablet_state() != TABLET_RUNNING) {
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because tablet state is:" << tablet->tablet_state()
                << ", not RUNNING";
        return false;
    }

    int64_t last_failure_ts = 0;
    DataDir* data_dir = tablet->data_dir();
    if (candidate.type == CUMULATIVE_COMPACTION) {
        std::shared_lock lk(tablet->get_cumulative_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id() << " for cumulative lock";
            return false;
        }
        // control the concurrent running tasks's limit
        // allow overruns up to twice the configured limit
        uint16_t num = running_cumulative_tasks_num_for_dir(data_dir);
        if (config::cumulative_compaction_num_threads_per_disk > 0 &&
            num >= config::cumulative_compaction_num_threads_per_disk * 2) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id()
                    << " for limit of cumulative compaction task per disk. disk path:" << data_dir->path()
                    << ", running num:" << num;
            return false;
        }
        last_failure_ts = tablet->last_cumu_compaction_failure_time();
    } else if (candidate.type == BASE_COMPACTION) {
        std::unique_lock lk(tablet->get_base_lock(), std::try_to_lock);
        if (!lk.owns_lock()) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id() << " for base lock";
            return false;
        }
        uint16_t num = running_base_tasks_num_for_dir(data_dir);
        if (config::base_compaction_num_threads_per_disk > 0 && num >= config::base_compaction_num_threads_per_disk) {
            VLOG(2) << "skip tablet:" << tablet->tablet_id()
                    << " for limit of base compaction task per disk. disk path:" << data_dir->path()
                    << ", running num:" << num;
            return false;
        }
        last_failure_ts = tablet->last_base_compaction_failure_time();
    }

    int64_t now_ms = UnixMillis();
    if (candidate.type == CompactionType::CUMULATIVE_COMPACTION) {
        if (now_ms - last_failure_ts <= config::min_cumulative_compaction_failure_interval_sec * 1000) {
            VLOG(2) << "Too often to schedule failure compaction, skip it."
                    << "compaction_type=" << starrocks::to_string(candidate.type)
                    << ", min_cumulative_compaction_failure_interval_sec="
                    << config::min_cumulative_compaction_failure_interval_sec
                    << ", last_failure_timestamp=" << last_failure_ts / 1000 << ", tablet_id=" << tablet->tablet_id();
            return false;
        }
    } else if (candidate.type == CompactionType::BASE_COMPACTION) {
        if (now_ms - last_failure_ts <= config::min_compaction_failure_interval_sec * 1000) {
            VLOG(2) << "Too often to schedule failure compaction, skip it."
                    << "compaction_type=" << starrocks::to_string(candidate.type)
                    << ", min_compaction_failure_interval_sec=" << config::min_compaction_failure_interval_sec
                    << ", last_failure_timestamp=" << last_failure_ts / 1000 << ", tablet_id=" << tablet->tablet_id();
            return false;
        }
    }

    return true;
}

bool CompactionManager::pick_candidate(CompactionCandidate* candidate) {
    std::lock_guard lg(_candidates_mutex);
    if (_compaction_candidates.empty()) {
        return false;
    }

    auto iter = _compaction_candidates.begin();
    while (iter != _compaction_candidates.end()) {
        if (_check_compaction_disabled(*iter)) {
            _compaction_candidates.erase(iter++);
            continue;
        }
        if (_check_precondition(*iter)) {
            *candidate = *iter;
            _compaction_candidates.erase(iter);
            _last_score = candidate->score;
            if (candidate->type == CompactionType::BASE_COMPACTION) {
                StarRocksMetrics::instance()->wait_base_compaction_task_num.increment(-1);
            } else {
                StarRocksMetrics::instance()->wait_cumulative_compaction_task_num.increment(-1);
            }
            return true;
        }
        iter++;
    }

    return false;
}

void CompactionManager::_dispatch_worker() {
    while (!_stop.load(std::memory_order_consume)) {
        {
            std::lock_guard lock(_dispatch_mutex);
            if (!_dispatch_map.empty()) {
                for (auto& [id, tablet_pair] : _dispatch_map) {
                    auto& tablet = tablet_pair.first;
                    Status st = _update_candidate_pool->submit_func([tablet, this] { update_tablet(tablet); });
                    if (!st.ok()) {
                        LOG(WARNING) << "update candidate tablet " << id << "failed. status:" << st.to_string();
                    }
                }
                _dispatch_map.clear();
            }
        }
        int32_t left_seconds = 10;
        do {
            sleep(1);
            --left_seconds;
        } while (!_stop.load(std::memory_order_consume) && left_seconds > 0 &&
                 _max_dispatch_count < config::min_cumulative_compaction_num_singleton_deltas &&
                 _dispatch_map.size() < 10240);
    }
}

void CompactionManager::update_tablet_async(const TabletSharedPtr& tablet) {
    std::lock_guard lock(_dispatch_mutex);
    auto iter = _dispatch_map.find(tablet->tablet_id());
    if (iter != _dispatch_map.end()) {
        iter->second.first = tablet;
        iter->second.second++;
        if (iter->second.second > _max_dispatch_count) {
            _max_dispatch_count = iter->second.second;
        }
    } else {
        _dispatch_map.emplace(tablet->tablet_id(), std::make_pair(tablet, 0));
    }
}

void CompactionManager::update_tablet(const TabletSharedPtr& tablet) {
    if (tablet == nullptr) {
        return;
    }
    if (_disable_update_tablet) {
        return;
    }
    VLOG(2) << "update tablet " << tablet->tablet_id();
    if (tablet->need_compaction()) {
        CompactionCandidate candidate;
        candidate.tablet = tablet;
        candidate.score = tablet->compaction_score();
        candidate.type = tablet->compaction_type();
        update_candidates({candidate});
    }
}

bool CompactionManager::register_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return false;
    }
    std::lock_guard lg(_tasks_mutex);
    TabletSharedPtr& tablet = compaction_task->tablet();
    DataDir* data_dir = tablet->data_dir();
    bool success;
    auto iter = _running_tasks.find(tablet->tablet_id());
    if (iter == _running_tasks.end()) {
        std::unordered_set<CompactionTask*> task_set;
        task_set.emplace(compaction_task);
        _running_tasks.emplace(tablet->tablet_id(), task_set);
        success = true;
    } else {
        success = iter->second.emplace(compaction_task).second;
    }
    if (!success) {
        // duplicate task
        LOG(WARNING) << "duplicate task, compaction_task:" << compaction_task->task_id()
                     << ", tablet:" << tablet->tablet_id();
        return false;
    }
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _data_dir_to_cumulative_task_num_map[data_dir]++;
        _cumulative_compaction_concurrency++;
        StarRocksMetrics::instance()->cumulative_compaction_request_total.increment(1);
        StarRocksMetrics::instance()->running_cumulative_compaction_task_num.increment(1);
    } else {
        _data_dir_to_base_task_num_map[data_dir]++;
        _base_compaction_concurrency++;
        StarRocksMetrics::instance()->base_compaction_request_total.increment(1);
        StarRocksMetrics::instance()->running_base_compaction_task_num.increment(1);
    }
    return true;
}

void CompactionManager::unregister_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return;
    }
    {
        std::lock_guard lg(_tasks_mutex);
        auto iter = _running_tasks.find(compaction_task->tablet()->tablet_id());
        if (iter != _running_tasks.end()) {
            auto size = iter->second.erase(compaction_task);
            if (size > 0) {
                TabletSharedPtr& tablet = compaction_task->tablet();
                DataDir* data_dir = tablet->data_dir();
                if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
                    _data_dir_to_cumulative_task_num_map[data_dir]--;
                    _cumulative_compaction_concurrency--;
                    StarRocksMetrics::instance()->running_cumulative_compaction_task_num.increment(-1);
                } else {
                    _data_dir_to_base_task_num_map[data_dir]--;
                    _base_compaction_concurrency--;
                    StarRocksMetrics::instance()->running_base_compaction_task_num.increment(-1);
                }
            }
            if (iter->second.empty()) {
                _running_tasks.erase(iter);
            }
        }
    }
    compaction_task->tablet()->reset_compaction_status();
}

void CompactionManager::clear_tasks() {
    std::lock_guard lg(_tasks_mutex);
    _running_tasks.clear();
    _data_dir_to_cumulative_task_num_map.clear();
    _data_dir_to_base_task_num_map.clear();
    _base_compaction_concurrency = 0;
    _cumulative_compaction_concurrency = 0;
}

// for http action
void CompactionManager::get_running_status(std::string* json_result) {
    int32_t max_task_num;
    int64_t base_task_num;
    int64_t cumulative_task_num;
    int64_t running_task_num;
    int64_t candidate_num;
    std::unordered_map<std::string, uint16_t> data_dir_to_base_task_num;
    std::unordered_map<std::string, uint16_t> data_dir_to_cumulative_task_num;
    vector<int64_t> running_tablet_ids;
    {
        std::lock_guard lg(_tasks_mutex);
        max_task_num = _max_task_num;
        base_task_num = _base_compaction_concurrency;
        cumulative_task_num = _cumulative_compaction_concurrency;
        running_task_num = 0;
        running_tablet_ids.reserve(_running_tasks.size());
        for (const auto& it : _running_tasks) {
            running_tablet_ids.push_back(it.first);
            running_task_num += it.second.size();
        }
        for (auto it : _data_dir_to_base_task_num_map) {
            data_dir_to_base_task_num[it.first->path()] = it.second;
        }
        for (auto it : _data_dir_to_cumulative_task_num_map) {
            data_dir_to_cumulative_task_num[it.first->path()] = it.second;
        }
        candidate_num = candidates_size();
    }

    rapidjson::Document root;
    root.SetObject();

    rapidjson::Value max_task_num_value;
    max_task_num_value.SetInt(max_task_num);
    root.AddMember("max_task_num", max_task_num_value, root.GetAllocator());

    rapidjson::Value base_task_num_value;
    base_task_num_value.SetInt64(base_task_num);
    root.AddMember("base_task_num", base_task_num_value, root.GetAllocator());

    rapidjson::Value cumulative_task_num_value;
    cumulative_task_num_value.SetInt64(cumulative_task_num);
    root.AddMember("cumulative_task_num", cumulative_task_num_value, root.GetAllocator());

    rapidjson::Value running_task_num_value;
    running_task_num_value.SetInt64(running_task_num);
    root.AddMember("running_task_num", running_task_num_value, root.GetAllocator());

    rapidjson::Value base_task_num_detail;
    base_task_num_detail.SetArray();
    for (const auto& it : data_dir_to_base_task_num) {
        rapidjson::Value value;
        value.SetObject();

        rapidjson::Value path;
        path.SetString(it.first.c_str(), it.first.size(), root.GetAllocator());
        value.AddMember("path", path, root.GetAllocator());

        rapidjson::Value task_num;
        task_num.SetUint64(it.second);
        value.AddMember("base_task_num", task_num, root.GetAllocator());

        base_task_num_detail.PushBack(value, root.GetAllocator());
    }
    root.AddMember("base_task_num_detail", base_task_num_detail, root.GetAllocator());

    rapidjson::Value cumulative_task_num_detail;
    cumulative_task_num_detail.SetArray();
    for (const auto& it : data_dir_to_cumulative_task_num) {
        rapidjson::Value value;
        value.SetObject();

        rapidjson::Value path;
        path.SetString(it.first.c_str(), it.first.size(), root.GetAllocator());
        value.AddMember("path", path, root.GetAllocator());

        rapidjson::Value task_num;
        task_num.SetUint64(it.second);
        value.AddMember("cumulative_task_num", task_num, root.GetAllocator());

        cumulative_task_num_detail.PushBack(value, root.GetAllocator());
    }
    root.AddMember("cumulative_task_num_detail", cumulative_task_num_detail, root.GetAllocator());

    rapidjson::Value tablet_num;
    tablet_num.SetInt(running_tablet_ids.size());
    root.AddMember("tablet_num", tablet_num, root.GetAllocator());

    rapidjson::Value running_tablet_list;
    running_tablet_list.SetArray();
    for (auto it : running_tablet_ids) {
        rapidjson::Value value;
        value.SetInt64(it);
        running_tablet_list.PushBack(value, root.GetAllocator());
    }
    root.AddMember("running_tablet_list", running_tablet_list, root.GetAllocator());

    rapidjson::Value candidate_num_value;
    candidate_num_value.SetInt64(candidate_num);
    root.AddMember("candidate_num", candidate_num_value, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

bool CompactionManager::has_running_task(const TabletSharedPtr& tablet) {
    std::lock_guard lg(_tasks_mutex);
    auto iter = _running_tasks.find(tablet->tablet_id());
    return iter != _running_tasks.end() && !iter->second.empty();
}

void CompactionManager::stop_compaction(const TabletSharedPtr& tablet) {
    std::lock_guard lg(_tasks_mutex);
    auto iter = _running_tasks.find(tablet->tablet_id());
    if (iter != _running_tasks.end()) {
        for (auto task : iter->second) {
            task->stop();
        }
    }
}

std::unordered_set<CompactionTask*> CompactionManager::get_running_task(const TabletSharedPtr& tablet) {
    std::lock_guard lg(_tasks_mutex);
    std::unordered_set<CompactionTask*> res;
    auto iter = _running_tasks.find(tablet->tablet_id());
    if (iter != _running_tasks.end()) {
        res = iter->second;
    }
    return res;
}

int32_t CompactionManager::compute_max_compaction_task_num() const {
    int32_t max_task_num = 0;
    // new compaction framework
    if (config::base_compaction_num_threads_per_disk >= 0 && config::cumulative_compaction_num_threads_per_disk >= 0) {
        max_task_num = static_cast<int32_t>(
                StorageEngine::instance()->get_store_num() *
                (config::cumulative_compaction_num_threads_per_disk + config::base_compaction_num_threads_per_disk));
    } else {
        // When cumulative_compaction_num_threads_per_disk or config::base_compaction_num_threads_per_disk is less than 0,
        // there is no limit to _max_task_num if max_compaction_concurrency is also less than 0, and here we set maximum value to be 20.
        max_task_num = std::min(20, static_cast<int32_t>(StorageEngine::instance()->get_store_num() * 5));
    }

    {
        std::lock_guard lg(_compact_threads_mutex);
        if (_max_compaction_concurrency > 0 && _max_compaction_concurrency < max_task_num) {
            max_task_num = _max_compaction_concurrency;
        }
    }

    return max_task_num;
}

void CompactionManager::set_max_compaction_concurrency(int threads_num) {
    std::lock_guard lg(_compact_threads_mutex);
    _max_compaction_concurrency = threads_num;
}

Status CompactionManager::update_max_threads(int max_threads) {
    if (_compaction_pool != nullptr) {
        int32 max_thread_num = 0;
        set_max_compaction_concurrency(max_threads);
        {
            std::lock_guard lg(_tasks_mutex);
            if (max_threads == 0) {
                _max_task_num = 0;
                return Status::OK();
            }

            _max_task_num = compute_max_compaction_task_num();
            max_thread_num = _max_task_num;
        }

        return _compaction_pool->update_max_threads(std::max(1, max_thread_num));
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

double CompactionManager::max_score() {
    std::lock_guard lg(_candidates_mutex);
    if (_compaction_candidates.empty()) {
        return 0;
    }

    return _compaction_candidates.begin()->score;
}

double CompactionManager::last_score() {
    std::lock_guard lg(_candidates_mutex);
    return _last_score;
}

int64_t CompactionManager::base_compaction_concurrency() {
    std::lock_guard lg(_tasks_mutex);
    return _base_compaction_concurrency;
}

int64_t CompactionManager::cumulative_compaction_concurrency() {
    std::lock_guard lg(_tasks_mutex);
    return _cumulative_compaction_concurrency;
}

int CompactionManager::get_waiting_task_num() {
    return _compaction_candidates.size();
}

void CompactionManager::disable_table_compaction(int64_t table_id, int64_t deadline) {
    std::lock_guard lg(_candidates_mutex);
    if (_table_to_disable_deadline_map.find(table_id) == _table_to_disable_deadline_map.end()) {
        LOG(INFO) << "start disable table compaction, table_id:" << table_id << ", deadline:" << deadline;
    }
    _table_to_disable_deadline_map[table_id] = deadline;
    VLOG(2) << "disable table compaction, table_id:" << table_id << ", deadline:" << deadline;
}

} // namespace starrocks
