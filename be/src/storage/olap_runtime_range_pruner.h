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

#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/range.h"

namespace starrocks {
class SlotDescriptor;

class RuntimeFilterProbeDescriptor;
class PredicateParser;
class ColumnPredicate;

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
    static constexpr auto rf_update_threhold = 4096 * 10;

    OlapRuntimeScanRangePruner() = default;
    OlapRuntimeScanRangePruner(PredicateParser* parser, const UnarrivedRuntimeFilterList& params) {
        _parser = parser;
        _init(params);
    }

    void set_predicate_parser(PredicateParser* parser) { _parser = parser; }

    Status update_range_if_arrived(const ColumnIdToGlobalDictMap* global_dictmaps,
                                   RuntimeFilterArrivedCallBack&& updater, size_t raw_read_rows) {
        if (_arrived_runtime_filters_masks.empty()) return Status::OK();
        return _update(global_dictmaps, std::move(updater), raw_read_rows);
    }

private:
    std::vector<const RuntimeFilterProbeDescriptor*> _unarrived_runtime_filters;
    std::vector<const SlotDescriptor*> _slot_descs;
    std::vector<bool> _arrived_runtime_filters_masks;
    std::vector<size_t> _rf_versions;
    PredicateParser* _parser = nullptr;
    size_t _raw_read_rows = 0;

    // get predicate
    StatusOr<PredicatesPtrs> _get_predicates(const ColumnIdToGlobalDictMap* global_dictmaps, size_t idx);

    PredicatesRawPtrs _as_raw_predicates(const std::vector<std::unique_ptr<ColumnPredicate>>& predicates);

    Status _update(const ColumnIdToGlobalDictMap* global_dictmaps, RuntimeFilterArrivedCallBack&& updater,
                   size_t raw_read_rows);

    void _init(const UnarrivedRuntimeFilterList& params);
};
} // namespace starrocks
