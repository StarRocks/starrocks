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

#include "data_workflows/load/tablet_writer/add_chunk_probe.h"

#include <set>
#include <sstream>
#include <vector>

#include "base/time/time.h"
#include "common/logging.h"

namespace starrocks {

namespace {

// Registry order is registry lock then probe lock; probes never take the registry lock while
// holding their own.
bthread::Mutex g_registry_lock;
std::set<AddChunkProbe*>& registry() {
    static auto* probes = new std::set<AddChunkProbe*>();
    return *probes;
}

constexpr int64_t kNanosPerSec = 1000L * 1000 * 1000;

} // namespace

AddChunkProbe::AddChunkProbe(int64_t txn_id, int64_t index_id, int32_t sender_id, bool eos)
        : _txn_id(txn_id),
          _index_id(index_id),
          _sender_id(sender_id),
          _eos(eos),
          _start_ns(MonotonicNanos()),
          _phase("entered") {
    std::lock_guard l(g_registry_lock);
    registry().insert(this);
}

AddChunkProbe::~AddChunkProbe() {
    std::lock_guard l(g_registry_lock);
    registry().erase(this);
}

void AddChunkProbe::watch_latch(const Latch* latch, uint64_t expected) {
    _latch_expected.store(expected, std::memory_order_relaxed);
    _latch.store(latch, std::memory_order_release);
}

void AddChunkProbe::owed_by(int64_t tablet_id) {
    std::lock_guard l(_owed_lock);
    _owed.insert(tablet_id);
}

void AddChunkProbe::settled_by(int64_t tablet_id) {
    std::lock_guard l(_owed_lock);
    _owed.erase(tablet_id);
}

std::string AddChunkProbe::describe(int64_t now_ns) const {
    std::ostringstream os;
    os << "txn_id=" << _txn_id << " index_id=" << _index_id << " sender_id=" << _sender_id
       << " eos=" << (_eos ? 1 : 0) << " running=" << (now_ns - _start_ns) / kNanosPerSec << "s"
       << " phase=" << _phase.load(std::memory_order_relaxed);

    if (const Latch* latch = _latch.load(std::memory_order_acquire); latch != nullptr) {
        os << " latch=" << latch->count() << "/" << _latch_expected.load(std::memory_order_relaxed);
    } else {
        os << " latch=-";
    }

    std::lock_guard l(_owed_lock);
    os << " owed_by=[";
    int printed = 0;
    for (auto tablet_id : _owed) {
        if (printed > 0) {
            os << ",";
        }
        // The list names the writers that never reported; a dozen is plenty to chase.
        if (printed == 12) {
            os << "+" << (_owed.size() - printed) << " more";
            break;
        }
        os << tablet_id;
        ++printed;
    }
    os << "]";
    return os.str();
}

void AddChunkProbe::log_stuck_calls(int64_t threshold_seconds) {
    const int64_t now_ns = MonotonicNanos();
    const int64_t threshold_ns = threshold_seconds * kNanosPerSec;

    std::vector<std::string> stuck;
    {
        std::lock_guard l(g_registry_lock);
        for (const auto* probe : registry()) {
            if (now_ns - probe->_start_ns >= threshold_ns) {
                stuck.push_back(probe->describe(now_ns));
            }
        }
    }
    for (const auto& line : stuck) {
        LOG(WARNING) << "TabletsChannel::add_chunk has not returned: " << line;
    }
}

} // namespace starrocks
