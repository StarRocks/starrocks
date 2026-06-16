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
#include <cstdint>
#include <mutex>
#include <set>

#include "exec/pipeline/scan/morsel_queue.h"
#include "exec/pipeline/scan/morsel_queue_builder.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"

namespace starrocks::pipeline {

// Morsel queue for ORDER BY <col> [ASC|DESC] LIMIT k over Iceberg. It serves morsels in the order
// of each file's min/max on the sort column, best file first, so the TopN runtime filter gets its
// bound early and the other files are skipped.
//
// The key is the raw int64 bound from THdfsScanRange.min_max_values (min for ASC, max for DESC).
// It is not decoded: the raw Iceberg encodings (int, date as days, timestamp as micros) are
// monotonic, so raw int64 order is the same as decoded order. The key is computed when a morsel is
// added, from its scan range, so morsels created later in the fragment executor are ordered too.
//
// Split pieces of one file share a scan range, so they get the same key and stay next to each
// other. That keeps the per-owner_id grouping the ticket checker needs.
class PriorityMorselQueue final : public MorselQueue, public TicketedMorselQueue {
public:
    PriorityMorselQueue(Morsels&& morsels, bool has_more, int32_t reorder_slot_id, bool desc, bool nulls_first)
            : _reorder_slot_id(reorder_slot_id), _desc(desc), _nulls_first(nulls_first), _set(Cmp{desc}) {
        _has_more_scan_ranges = has_more;
        (void)append_morsels(std::move(morsels));
        _num_morsels = _size.load(std::memory_order_relaxed);
        _degree_of_parallelism = _num_morsels;
    }
    ~PriorityMorselQueue() override = default;

    bool empty() const override { return _size.load(std::memory_order_relaxed) == 0; }
    StatusOr<MorselPtr> try_get() override;
    void unget(MorselPtr&& morsel) override;
    std::string name() const override { return "priority_morsel_queue"; }
    Status append_morsels(Morsels&& morsels) override;
    Type type() const override { return DYNAMIC; }

    void set_ticket_checker(const SplitMorselTicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() const override { return true; }

    void set_max_degree_of_parallelism(size_t dop) { _degree_of_parallelism = dop; }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }

    int64_t reorder_eligible_morsels() const override { return _eligible_morsels.load(std::memory_order_relaxed); }
    int64_t reorder_no_bound_morsels() const override { return _no_bound_morsels.load(std::memory_order_relaxed); }

private:
    // rank 0: nulls lead (NULLS FIRST and the file has nulls) -> serve first;
    // rank 1: has a numeric bound -> serve ordered by key;
    // rank 2: no usable bound -> serve last.
    struct Entry {
        int rank = 2;
        int64_t key = 0;
        uint64_t seq = 0;
        mutable MorselPtr morsel;
    };
    struct Cmp {
        bool desc;
        bool operator()(const Entry& a, const Entry& b) const {
            if (a.rank != b.rank) {
                return a.rank < b.rank;
            }
            if (a.rank == 1 && a.key != b.key) {
                return desc ? a.key > b.key : a.key < b.key;
            }
            return a.seq < b.seq; // stable tiebreak (and total order for non-bound ranks)
        }
    };

    Entry make_entry(MorselPtr&& morsel);

    const int32_t _reorder_slot_id;
    const bool _desc;
    const bool _nulls_first;
    std::mutex _mutex;
    std::multiset<Entry, Cmp> _set;
    std::atomic<int64_t> _size = 0;
    // Reorder diagnostics; bumped in make_entry (under _mutex), read lock-free by the scan operator.
    std::atomic<int64_t> _eligible_morsels = 0;
    std::atomic<int64_t> _no_bound_morsels = 0;
    uint64_t _seq = 0;
    SplitMorselTicketCheckerPtr _ticket_checker;
    size_t _degree_of_parallelism = 0;
};

MorselQueueBuilderPtr make_priority_morsel_queue_builder(Morsels&& morsels, bool has_more_scan_ranges, size_t max_dop,
                                                         int32_t reorder_slot_id, bool desc, bool nulls_first);

} // namespace starrocks::pipeline
