// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"

namespace starrocks {
class SlotDescriptor;

namespace vectorized {
class RuntimeFilterProbeDescriptor;
class PredicateParser;
class ColumnPredicate;
class SparseRange;

struct UnarrivedRuntimeFilterList {
    std::vector<const RuntimeFilterProbeDescriptor*> unarrived_runtime_filters;
    std::vector<const SlotDescriptor*> slot_descs;
    void add_unarrived_rf(const RuntimeFilterProbeDescriptor* desc, const SlotDescriptor* slot_desc) {
        unarrived_runtime_filters.push_back(desc);
        slot_descs.push_back(slot_desc);
    }
};

class OlapRuntimeScanRangePruner {
public:
    using PredicatesPtrs = std::vector<std::unique_ptr<ColumnPredicate>>;
    using PredicatesRawPtrs = std::vector<const ColumnPredicate*>;
    using RuntimeFilterArrivedCallBack = std::function<Status(int, const PredicatesRawPtrs&)>;

    OlapRuntimeScanRangePruner() = default;
    OlapRuntimeScanRangePruner(PredicateParser* parser, const UnarrivedRuntimeFilterList& params) {
        _parser = parser;
        _init(params);
    }

    void set_predicate_parser(PredicateParser* parser) { _parser = parser; }

    Status update_range_if_arrived(RuntimeFilterArrivedCallBack&& updater) {
        if (_arrived_runtime_filters_masks.empty()) return Status::OK();
        return _update(std::move(updater));
    }

private:
    std::vector<const RuntimeFilterProbeDescriptor*> _unarrived_runtime_filters;
    std::vector<const SlotDescriptor*> _slot_descs;
    std::vector<bool> _arrived_runtime_filters_masks;
    PredicateParser* _parser = nullptr;

    // get predicate
    StatusOr<PredicatesPtrs> _get_predicates(size_t idx);

    PredicatesRawPtrs _as_raw_predicates(const std::vector<std::unique_ptr<ColumnPredicate>>& predicates);

    Status _update(RuntimeFilterArrivedCallBack&& updater);

    void _init(const UnarrivedRuntimeFilterList& params);
};
} // namespace vectorized
} // namespace starrocks
