// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

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

struct RuntimeRangerParams {
    std::vector<const RuntimeFilterProbeDescriptor*> unreached_runtime_filters;
    std::vector<const SlotDescriptor*> slot_descs;
    void add_unreached_rf(const RuntimeFilterProbeDescriptor* desc, const SlotDescriptor* slot_desc) {
        unreached_runtime_filters.push_back(desc);
        slot_descs.push_back(slot_desc);
    }
};

class RuntimeRangerContext {
public:
    RuntimeRangerContext() = default;
    RuntimeRangerContext(const RuntimeRangerParams& params, PredicateParser* parser) {
        _parser = parser;
        DCHECK_EQ(_unreached_runtime_filters.size(), _slot_descs.size());
        _init(params);
    }

    void set_predicate_parser(PredicateParser* parser) { _parser = parser; }

    template <class Updater>
    Status update_range_if_arrived(Updater&& updater) {
        if (_arrived_runtime_filters.empty()) return Status::OK();
        return _update<Updater>(std::move(updater));
    }

private:
    using PredicatesPtrs = std::vector<std::unique_ptr<ColumnPredicate>>;
    using PredicatesRawPtrs = std::vector<const ColumnPredicate*>;

    std::vector<const RuntimeFilterProbeDescriptor*> _unreached_runtime_filters;
    std::vector<const SlotDescriptor*> _slot_descs;
    std::vector<bool> _arrived_runtime_filters;
    PredicateParser* _parser = nullptr;

    // get predicate
    StatusOr<PredicatesPtrs> _get_predicates(size_t idx);

    PredicatesRawPtrs _as_raw_predicates(const std::vector<std::unique_ptr<ColumnPredicate>>& predicates);

    template <class Updater>
    Status _update(Updater&& updater);

    void _init(const RuntimeRangerParams& params);
};
} // namespace vectorized
} // namespace starrocks