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

#pragma once

#include <atomic>
#include <deque>
#include <mutex>
#include <utility>

#include "exec/pipeline/scan/morsel_queue.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"

namespace starrocks::pipeline {

class DynamicMorselQueue final : public MorselQueue, public TicketedMorselQueue {
public:
    explicit DynamicMorselQueue(Morsels&& morsels, bool has_more) {
        (void)append_morsels(std::move(morsels));
        _size = _num_morsels = _queue.size();
        _degree_of_parallelism = _num_morsels;
        _has_more_scan_ranges = has_more;
    }

    ~DynamicMorselQueue() override = default;
    bool empty() const override { return _size.load(std::memory_order_relaxed) == 0; }
    StatusOr<MorselPtr> try_get() override;
    void unget(MorselPtr&& morsel) override;
    std::string name() const override { return "dynamic_morsel_queue"; }
    Status append_morsels(Morsels&& morsels) override;
    void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() const override { return true; }
    Type type() const override { return DYNAMIC; }

    void set_max_degree_of_parallelism(size_t degree_of_parallelism) { _degree_of_parallelism = degree_of_parallelism; }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }

private:
    std::atomic<int64_t> _size = 0;
    std::deque<MorselPtr> _queue;
    std::mutex _mutex;
    query_cache::TicketCheckerPtr _ticket_checker;
    size_t _degree_of_parallelism;
};

} // namespace starrocks::pipeline
