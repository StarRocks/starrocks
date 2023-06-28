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

#include "publish_version_manager.h"

#include "agent/finish_task.h"
#include "agent/task_signatures_manager.h"
#include "common/config.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "util/cpu_info.h"

namespace starrocks {

Status PublishVersionManager::init() {
    int max_thread_count = config::transaction_publish_version_worker_count;
    if (max_thread_count <= 0) {
        max_thread_count = CpuInfo::num_cores();
    }
    RETURN_IF_ERROR(ThreadPoolBuilder("finish_publish_version")
                            .set_max_threads(max_thread_count)
                            .build(&_finish_publish_version_thread_pool));
    return Status::OK();
}

PublishVersionManager::~PublishVersionManager() {
    if (_finish_publish_version_thread_pool) {
        _finish_publish_version_thread_pool->shutdown();
    }
    _finish_task_requests.clear();
    _waitting_finish_task_requests.clear();
    _unapplied_tablet_by_txn.clear();
}

// should under lock
bool PublishVersionManager::_all_task_applied(TFinishTaskRequest& finish_task_request) {
    if (finish_task_request.task_status.status_code != TStatusCode::OK) {
        return true;
    }
    auto& tablet_versions = finish_task_request.tablet_publish_versions;
    bool all_task_applied = true;
    std::set<std::pair<int64_t, int64_t>> unapplied_tablet;
    for (int i = 0; i < tablet_versions.size(); i++) {
        TTabletVersionPair tablet_version = tablet_versions[i];
        int64_t tablet_id = tablet_version.tablet_id;
        int64_t request_version = tablet_version.version;

        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet != nullptr)
            if (tablet->keys_type() != KeysType::PRIMARY_KEYS) {
                return true;
            }
        if (tablet->max_readable_version() < request_version) {
            all_task_applied = false;
            unapplied_tablet.insert(std::make_pair(tablet_id, request_version));
        }
        VLOG(1) << "tablet: " << tablet->tablet_id() << " max_readable_version is " << tablet->max_readable_version()
                << ", request_version is " << request_version;
    }

    if (!all_task_applied) {
        _unapplied_tablet_by_txn[finish_task_request.signature] = std::move(unapplied_tablet);
    }
    return all_task_applied;
}

bool PublishVersionManager::_left_task_applied(TFinishTaskRequest* finish_task_request) {
    bool applied = true;
    int64_t signature = finish_task_request->signature;
    std::set<std::pair<int64_t, int64_t>> unapplied_tablet;
    auto iter = _unapplied_tablet_by_txn.find(signature);
    if (iter == _unapplied_tablet_by_txn.end()) {
        return true;
    }
    for (auto& tablet_pair : iter->second) {
        int64_t tablet_id = tablet_pair.first;
        int64_t request_version = tablet_pair.second;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet != nullptr) {
            DCHECK(tablet->keys_type() == KeysType::PRIMARY_KEYS);
            if (tablet->max_readable_version() < request_version) {
                applied = false;
                unapplied_tablet.insert(std::make_pair(tablet_id, request_version));
            }
            VLOG(1) << "tablet: " << tablet->tablet_id() << " max_readable_version is "
                    << tablet->max_readable_version() << ", request_version is " << request_version;
        }
    }
    if (!applied) {
        iter->second.swap(unapplied_tablet);
    } else {
        _unapplied_tablet_by_txn.erase(signature);
    }
    return applied;
}

Status PublishVersionManager::finish_publish_task(std::vector<TFinishTaskRequest> finish_task_requests) {
    std::lock_guard wl(_lock);
    for (auto& finish_task_request : finish_task_requests) {
        if (_all_task_applied(finish_task_request)) {
            _finish_task_requests[finish_task_request.signature] =
                    std::move(std::make_shared<TFinishTaskRequest>(finish_task_request));
        } else {
            _waitting_finish_task_requests[finish_task_request.signature] =
                    std::move(std::make_shared<TFinishTaskRequest>(finish_task_request));
        }
    }
    return Status::OK();
}

void PublishVersionManager::update_tablet_version(TFinishTaskRequest* finish_task_request) {
    auto& tablet_versions = finish_task_request->tablet_versions;
    for (int32_t i = 0; i < tablet_versions.size(); i++) {
        int64_t tablet_id = tablet_versions[i].tablet_id;
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet != nullptr) {
            tablet_versions[i].__set_version(tablet->max_continuous_version());
        }
    }
}

Status PublishVersionManager::submit_finish_task() {
    auto token = _finish_publish_version_thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::vector<int64_t> erase_finish_task_signature;
    std::vector<int64_t> erase_waitting_finish_task_signature;
    {
        std::lock_guard wl(_lock);
        for (auto& [signature, finish_task_request_ptr] : _finish_task_requests) {
            // submit finish task
            token->submit_func([this, finish_task_request_ptr]() mutable {
                //TFinishTaskRequest request = std::move(finish_task_request);
                update_tablet_version(finish_task_request_ptr.get());
#ifndef BE_TEST
                finish_task(finish_task_request_ptr.get());
#endif
                remove_task_info(finish_task_request_ptr->task_type, finish_task_request_ptr->signature);
            });
            erase_finish_task_signature.emplace_back(signature);
        }

        std::vector<int64_t> clear_txn;
        for (auto& [signature, finish_task_request_ptr] : _waitting_finish_task_requests) {
            if (_left_task_applied(finish_task_request_ptr.get())) {
                token->submit_func([this, finish_task_request_ptr]() mutable {
                    //TFinishTaskRequest request = std::move(finish_task_request);
                    update_tablet_version(finish_task_request_ptr.get());
#ifndef BE_TEST
                    finish_task(finish_task_request_ptr.get());
#endif
                    remove_task_info(finish_task_request_ptr->task_type, finish_task_request_ptr->signature);
                });
                erase_waitting_finish_task_signature.emplace_back(signature);
            }
        }
        size_t finish_size_before = _finish_task_requests.size();
        size_t waitting_finish_size_before = _waitting_finish_task_requests.size();
        size_t erase_finish = erase_finish_task_signature.size();
        size_t erase_waitting = erase_waitting_finish_task_signature.size();
        for (auto& signature : erase_finish_task_signature) {
            _finish_task_requests.erase(signature);
        }
        for (auto& signature : erase_waitting_finish_task_signature) {
            _waitting_finish_task_requests.erase(signature);
            _unapplied_tablet_by_txn.erase(signature);
        }
    }
    return Status::OK();
}

} // namespace starrocks