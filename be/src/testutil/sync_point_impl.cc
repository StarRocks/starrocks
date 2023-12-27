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
//
// The code in this file was modified from the RocksDB project at the following URL:
// https://github.com/facebook/rocksdb/blob/fb63d9b4ee26afc81810bd5398bda4f2b351b26b/test_util/sync_point_impl.cc
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "testutil/sync_point_impl.h"

#include <sys/types.h>
#include <unistd.h>

#include <csignal>

#if !defined(NDEBUG) || defined(BE_TEST)
namespace starrocks {
KillPoint* KillPoint::GetInstance() {
    static KillPoint kp;
    return &kp;
}

void KillPoint::TestKillRandom(std::string kill_point, int odds_weight, const std::string& srcfile, int srcline) {
    if (starrock_skill_odds <= 0) {
        return;
    }
    int odds = starrock_skill_odds * odds_weight;
    for (auto& p : starrockskill_exclude_prefixes) {
        if (kill_point.substr(0, p.length()) == p) {
            return;
        }
    }

    assert(odds > 0);
    if (odds % 7 == 0) {
        // class Random uses multiplier 16807, which is 7^5. If odds are
        // multiplier of 7, there might be limited values generated.
        odds++;
    }
    auto* r = Random::GetTLSInstance();
    bool crash = r->OneIn(odds);
    if (crash) {
        fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
        fflush(stdout);
        kill(getpid(), SIGTERM);
    }
}

void SyncPoint::Data::LoadDependency(const std::vector<SyncPointPair>& dependencies) {
    std::lock_guard<std::mutex> lock(mutex_);
    successors_.clear();
    predecessors_.clear();
    cleared_points_.clear();
    for (const auto& dependency : dependencies) {
        successors_[dependency.predecessor].push_back(dependency.successor);
        predecessors_[dependency.successor].push_back(dependency.predecessor);
    }
    cv_.notify_all();
}

void SyncPoint::Data::LoadDependencyAndMarkers(const std::vector<SyncPointPair>& dependencies,
                                               const std::vector<SyncPointPair>& markers) {
    std::lock_guard<std::mutex> lock(mutex_);
    successors_.clear();
    predecessors_.clear();
    cleared_points_.clear();
    markers_.clear();
    marked_thread_id_.clear();
    for (const auto& dependency : dependencies) {
        successors_[dependency.predecessor].push_back(dependency.successor);
        predecessors_[dependency.successor].push_back(dependency.predecessor);
    }
    for (const auto& marker : markers) {
        successors_[marker.predecessor].push_back(marker.successor);
        predecessors_[marker.successor].push_back(marker.predecessor);
        markers_[marker.predecessor].push_back(marker.successor);
    }
    cv_.notify_all();
}

bool SyncPoint::Data::PredecessorsAllCleared(const std::string& point) {
    for (const auto& pred : predecessors_[point]) {
        if (cleared_points_.count(pred) == 0) {
            return false;
        }
    }
    return true;
}

void SyncPoint::Data::ClearCallBack(const std::string& point) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_callbacks_running_ > 0) {
        cv_.wait(lock);
    }
    callbacks_.erase(point);
}

void SyncPoint::Data::ClearAllCallBacks() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_callbacks_running_ > 0) {
        cv_.wait(lock);
    }
    callbacks_.clear();
}

void SyncPoint::Data::Process(const std::string_view& point, void* cb_arg) {
    if (!enabled_) {
        return;
    }

    // Must convert to std::string for remaining work.  Take
    //  heap hit.
    std::string point_string(point);
    std::unique_lock<std::mutex> lock(mutex_);
    auto thread_id = std::this_thread::get_id();

    auto marker_iter = markers_.find(point_string);
    if (marker_iter != markers_.end()) {
        for (auto& marked_point : marker_iter->second) {
            marked_thread_id_.emplace(marked_point, thread_id);
        }
    }

    if (DisabledByMarker(point_string, thread_id)) {
        return;
    }

    while (!PredecessorsAllCleared(point_string)) {
        cv_.wait(lock);
        if (DisabledByMarker(point_string, thread_id)) {
            return;
        }
    }

    auto callback_pair = callbacks_.find(point_string);
    if (callback_pair != callbacks_.end()) {
        num_callbacks_running_++;
        mutex_.unlock();
        callback_pair->second(cb_arg);
        mutex_.lock();
        num_callbacks_running_--;
    }
    cleared_points_.insert(point_string);
    cv_.notify_all();
}
} // namespace starrocks
#endif
