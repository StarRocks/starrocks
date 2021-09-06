// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

class OlapMorsel final : public Morsel {
public:
    OlapMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range) : Morsel(plan_node_id) {
        _scan_range = std::make_unique<TInternalScanRange>(scan_range.scan_range.internal_scan_range);
    }

    TInternalScanRange* get_scan_range() { return _scan_range.get(); }

private:
    std::unique_ptr<TInternalScanRange> _scan_range;
};

class MorselQueue {
public:
    MorselQueue(Morsels&& morsels) : _morsels(std::move(morsels)), _num_morsels(_morsels.size()), _pop_index(0) {}

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

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
};

} // namespace pipeline
} // namespace starrocks