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
#include <cstddef>
#include <cstdint>

namespace starrocks {

// The cache blocks the lake query scans of a node may hold ahead of their reads, all scans together
// (lake_scan_read_ahead_max_blocks; 0 means no limit).
//
// A segment reader that reads ahead is a Member of the budget for as long as it does, and asks for blocks with
// acquire(). What it gets is a Grant, which gives its blocks back when it is released or destroyed, so a reader that
// stops for any reason -- consumed, cancelled, failed -- returns what it held. The members share the limit equally:
// each holds at most the limit divided by their number (at least one block), so a scan that starts later still gets
// its share.
//
// A budget built with its own limit (a lake compaction task's) works the same way, but holds that many blocks and
// reports nothing to the node metrics.
class ReadAheadBudget {
public:
    // The node's query budget: lake_scan_read_ahead_max_blocks, reported to the node metrics.
    ReadAheadBudget() = default;
    // A budget of its own with a fixed limit of `limit` blocks (positive).
    explicit ReadAheadBudget(int64_t limit) : _fixed_limit(limit) {}

    class Grant {
    public:
        Grant() = default;
        Grant(Grant&& other) noexcept : _budget(other._budget), _blocks(other._blocks) { other._blocks = 0; }
        Grant& operator=(Grant&& other) noexcept;
        Grant(const Grant&) = delete;
        Grant& operator=(const Grant&) = delete;
        ~Grant() { release(); }

        size_t blocks() const { return _blocks; }
        // Gives back all its blocks.
        void release();

    private:
        friend class ReadAheadBudget;
        Grant(ReadAheadBudget* budget, size_t blocks) : _budget(budget), _blocks(blocks) {}

        ReadAheadBudget* _budget = nullptr;
        size_t _blocks = 0;
    };

    class Member {
    public:
        Member() = default;
        Member(Member&& other) noexcept : _budget(other._budget) { other._budget = nullptr; }
        Member& operator=(Member&& other) noexcept;
        Member(const Member&) = delete;
        Member& operator=(const Member&) = delete;
        ~Member() { leave(); }

        void leave();

    private:
        friend class ReadAheadBudget;
        explicit Member(ReadAheadBudget* budget) : _budget(budget) {}

        ReadAheadBudget* _budget = nullptr;
    };

    static ReadAheadBudget* instance();

    Member join();
    // Up to `want` blocks for a member that holds `held` blocks of the budget and may keep `own_limit` ahead by
    // itself: no more than its share and what the node has left. An empty grant when there is nothing. Asking for more
    // than the limit lets have is counted (lake_scan_read_ahead_capped_total).
    Grant acquire(size_t want, size_t held, size_t own_limit);
    // Whether the node holds fewer blocks than the limit; when it does not, the caller gives something up, which is
    // counted.
    bool has_room();

    // The blocks a member may hold: `own_limit`, or less, its share of the limit.
    size_t share(size_t own_limit) const;
    int64_t held_blocks() const { return _held.load(std::memory_order_relaxed); }
    int64_t members() const { return _members.load(std::memory_order_relaxed); }

private:
    int64_t limit() const;
    bool reports_metrics() const { return _fixed_limit <= 0; }
    void give_back(size_t blocks);
    void count_capped();

    // Positive for a budget with a limit of its own; otherwise the node's limit applies.
    const int64_t _fixed_limit = 0;
    std::atomic<int64_t> _held{0};
    std::atomic<int64_t> _members{0};
};

} // namespace starrocks
