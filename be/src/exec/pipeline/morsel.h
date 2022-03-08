// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <optional>

#include "gen_cpp/InternalService_types.h"
#include "storage/olap_common.h"

namespace starrocks {
namespace pipeline {
class Morsel;
using MorselPtr = std::unique_ptr<Morsel>;
using Morsels = std::vector<MorselPtr>;
class MorselQueue;
using MorselQueuePtr = std::unique_ptr<MorselQueue>;
using MorselQueueMap = std::unordered_map<int32_t, MorselQueuePtr>;

class Morsel {
public:
    Morsel(int32_t plan_node_id) : _plan_node_id(plan_node_id) {}
    virtual ~Morsel() = default;
    int32_t get_plan_node_id() const { return _plan_node_id; }

private:
    int32_t _plan_node_id;
};

class ScanMorsel final : public Morsel {
public:
    ScanMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range) : Morsel(plan_node_id) {
        _scan_range = std::make_unique<TScanRange>(scan_range.scan_range);
    }

    TScanRange* get_scan_range() { return _scan_range.get(); }

    TInternalScanRange* get_olap_scan_range() { return &(_scan_range->internal_scan_range); }

private:
    std::unique_ptr<TScanRange> _scan_range;
};

class MorselQueue {
public:
    MorselQueue(Morsels&& morsels) : _morsels(std::move(morsels)), _num_morsels(_morsels.size()), _pop_index(0) {}

    const Morsels& morsels() const { return _morsels; }

    size_t num_morsels() const { return _num_morsels; }

    std::optional<MorselPtr> try_get() {
        auto idx = _pop_index.load();
        // prevent _num_morsels from superfluous addition
        if (idx >= _num_morsels) {
            return {};
        }
        idx = _pop_index.fetch_add(1);
        if (idx < _num_morsels) {
            return std::move(_morsels[idx]);
        } else {
            return {};
        }
    }

    bool empty() const { return _pop_index >= _num_morsels; }

    // Split the morsel queue into `split_size` morsel queues.
    // For example:
    // morsel queue is: [1, 2, 3, 4, 5, 6, 7]
    // split_size is: 3
    // return:
    //  [1, 4, 7]
    //  [2, 5]
    //  [3, 6]
    std::vector<MorselQueuePtr> split_by_size(size_t split_size) {
        // split_size is in (0, split_size].
        DCHECK_GT(split_size, 0);
        DCHECK(_num_morsels == 0 || split_size <= _num_morsels);

        std::vector<Morsels> split_morsels_list(split_size);
        for (int i = 0; i < _num_morsels; ++i) {
            auto maybe_morsel = try_get();
            DCHECK(maybe_morsel.has_value());
            split_morsels_list[i % split_size].emplace_back(std::move(maybe_morsel.value()));
        }

        std::vector<MorselQueuePtr> split_morsel_queues;
        split_morsel_queues.reserve(split_size);
        for (auto& split_morsels : split_morsels_list) {
            split_morsel_queues.emplace_back(std::make_unique<MorselQueue>(std::move(split_morsels)));
        }

        return split_morsel_queues;
    }

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
};

} // namespace pipeline
} // namespace starrocks