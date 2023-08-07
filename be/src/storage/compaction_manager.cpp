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

#include "storage/data_dir.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"

using namespace std::chrono_literals;

namespace starrocks {

CompactionManager::CompactionManager() : _next_task_id(0) {}

CompactionManager::~CompactionManager() {
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
                 .set_max_threads(std::max(1, max_task_num()))
                 .set_max_queue_size(1000)
                 .build(&_compaction_pool);
    DCHECK(st.ok());

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
            if (compaction_candidate.type == CompactionType::BASE_COMPACTION) {
                StarRocksMetrics::instance()->tablet_base_max_compaction_score.set_value(compaction_candidate.score);
            } else {
                StarRocksMetrics::instance()->tablet_cumulative_max_compaction_score.set_value(
                        compaction_candidate.score);
            }

            auto task_id = next_compaction_task_id();
            LOG(INFO) << "submit task to compaction pool"
                      << ", task_id:" << task_id << ", tablet_id:" << compaction_candidate.tablet->tablet_id()
                      << ", compaction_type:" << starrocks::to_string(compaction_candidate.type)
                      << ", compaction_score:" << compaction_candidate.score << " for round:" << _round
                      << ", task_queue_size:" << candidates_size();
            auto st = _compaction_pool->submit_func([compaction_candidate, task_id] {
                auto compaction_task = compaction_candidate.tablet->create_compaction_task();
                if (compaction_task != nullptr) {
                    compaction_task->set_task_id(task_id);
                    compaction_task->start();
                }
            });
            if (!st.ok()) {
                LOG(WARNING) << "submit compaction task " << task_id
                             << " to compaction pool failed. status:" << st.to_string();
                update_tablet_async(compaction_candidate.tablet);
            }
        }
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
            if (candidate.tablet->enable_compaction()) {
                VLOG(1) << "update candidate " << candidate.tablet->tablet_id() << " type "
                        << starrocks::to_string(candidate.type) << " score " << candidate.score;
                _compaction_candidates.emplace(std::move(candidate));
            }
        }
    }
    _notify();
}

void CompactionManager::remove_candidate(int64_t tablet_id) {
    std::lock_guard lg(_candidates_mutex);
    for (auto iter = _compaction_candidates.begin(); iter != _compaction_candidates.end();) {
        if (tablet_id == iter->tablet->tablet_id()) {
            iter = _compaction_candidates.erase(iter);
            break;
        } else {
            iter++;
        }
    }
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

    if (tablet->has_compaction_task()) {
        // tablet already has a running compaction task, skip it
        VLOG(2) << "skip tablet:" << tablet->tablet_id() << " because there is another running compaction task.";
        return false;
    }

    int64_t last_failure_ts = 0;
    DataDir* data_dir = tablet->data_dir();
    if (candidate.type == CUMULATIVE_COMPACTION) {
        std::unique_lock lk(tablet->get_cumulative_lock(), std::try_to_lock);
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
        if (config::base_compaction_num_threads_per_disk > 0 &&
            num >= config::base_compaction_num_threads_per_disk * 2) {
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
            VLOG(1) << "Too often to schedule failure compaction, skip it."
                    << "compaction_type=" << starrocks::to_string(candidate.type)
                    << ", min_cumulative_compaction_failure_interval_sec="
                    << config::min_cumulative_compaction_failure_interval_sec
                    << ", last_failure_timestamp=" << last_failure_ts / 1000 << ", tablet_id=" << tablet->tablet_id();
            return false;
        }
    } else if (candidate.type == CompactionType::BASE_COMPACTION) {
        if (now_ms - last_failure_ts <= config::min_compaction_failure_interval_sec * 1000) {
            VLOG(1) << "Too often to schedule failure compaction, skip it."
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
        if (_check_precondition(*iter)) {
            *candidate = *iter;
            _compaction_candidates.erase(iter);
            _last_score = candidate->score;
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
    VLOG(1) << "update tablet " << tablet->tablet_id();
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
    auto p = _running_tasks.insert(compaction_task);
    if (!p.second) {
        // duplicate task
        LOG(WARNING) << "duplicate task, compaction_task:" << compaction_task->task_id()
                     << ", tablet:" << tablet->tablet_id();
        return false;
    }
    if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
        _data_dir_to_cumulative_task_num_map[data_dir]++;
        _cumulative_compaction_concurrency++;
    } else {
        _data_dir_to_base_task_num_map[data_dir]++;
        _base_compaction_concurrency++;
    }
    return true;
}

void CompactionManager::unregister_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return;
    }
    std::lock_guard lg(_tasks_mutex);
    auto size = _running_tasks.erase(compaction_task);
    if (size > 0) {
        TabletSharedPtr& tablet = compaction_task->tablet();
        DataDir* data_dir = tablet->data_dir();
        if (compaction_task->compaction_type() == CUMULATIVE_COMPACTION) {
            _data_dir_to_cumulative_task_num_map[data_dir]--;
            _cumulative_compaction_concurrency--;
        } else {
            _data_dir_to_base_task_num_map[data_dir]--;
            _base_compaction_concurrency--;
        }
    }
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
        running_task_num = _running_tasks.size();
        running_tablet_ids.reserve(running_task_num);
        for (auto it : _running_tasks) {
            running_tablet_ids.push_back(it->tablet()->tablet_id());
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

Status CompactionManager::update_max_threads(int max_threads) {
    if (_compaction_pool != nullptr) {
        return _compaction_pool->update_max_threads(max_threads);
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

} // namespace starrocks
