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
#include <memory>
#include <mutex>
#include <string>

#include "common/statusor.h"
#include "exec/pipeline/scan/scan_morsel.h"

namespace starrocks::query_cache {
class TicketChecker;
using TicketCheckerPtr = std::shared_ptr<TicketChecker>;
} // namespace starrocks::query_cache

namespace starrocks::pipeline {

class MorselQueue {
public:
    enum Type {
        FIXED,
        DYNAMIC,
        SPLIT,
        LOGICAL_SPLIT,
        PHYSICAL_SPLIT,
        BUCKET_SEQUENCE,
    };

    MorselQueue() = default;
    MorselQueue(Morsels&& morsels) : _morsels(std::move(morsels)), _num_morsels(_morsels.size()) {}
    virtual ~MorselQueue() = default;

    // NOTE: some subclasses of MorselQueue nest another MorselQueue, such as BucketSequenceMorselQueue.
    // When adding a new virtual method, DO NOT forget to invoke it on the nested MorselQueue as well.

    virtual void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) {}
    virtual bool could_attch_ticket_checker() const { return false; }

    virtual size_t num_original_morsels() const { return _num_morsels; }
    virtual size_t max_degree_of_parallelism() const { return _num_morsels; }
    virtual bool empty() const = 0;
    virtual StatusOr<MorselPtr> try_get() = 0;
    virtual void unget(MorselPtr&& morsel);
    virtual std::string name() const = 0;
    virtual StatusOr<bool> ready_for_next() const { return true; }
    virtual Status append_morsels(Morsels&& morsels);
    virtual Type type() const = 0;

    // is there any more scan ranges delivered from FE to be processed?
    bool has_more() const { return _has_more_scan_ranges || _has_more_from_split; }
    bool has_more_scan_ranges() const { return _has_more_scan_ranges; }
    bool has_more_from_split() const { return _has_more_from_split; }
    void set_has_more_scan_ranges(bool v) { _has_more_scan_ranges = v; }
    void set_has_more_from_split(bool v) { _has_more_from_split = v; }
    // do scan operator emit enough rows that we can stop processing scan ranges?
    void set_reach_limit(bool v) { _reach_limit = v; }
    bool reach_limit() const { return _reach_limit; }

protected:
    std::atomic<bool> _has_more_scan_ranges = false;
    std::atomic<bool> _has_more_from_split = false;
    std::atomic<bool> _reach_limit = false;
    Morsels _morsels;
    size_t _num_morsels = 0;
    MorselPtr _unget_morsel = nullptr;
};

class DynamicMorselQueue final : public MorselQueue {
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

MorselQueuePtr create_empty_morsel_queue();

} // namespace starrocks::pipeline
