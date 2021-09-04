// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "gen_cpp/InternalService_types.h"
#include "storage/olap_common.h"

namespace starrocks {
namespace pipeline {
class Morsel;
using MorselPtr = std::shared_ptr<Morsel>;
using Morsels = std::vector<MorselPtr>;

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

} // namespace pipeline
} // namespace starrocks