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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/status.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"

namespace starrocks {
class RuntimeFilterProbeDescriptor;

class RuntimeFilterPredicate {
public:
    RuntimeFilterPredicate(RuntimeFilterProbeDescriptor* desc, ColumnId column_id)
            : _rf_desc(desc), _column_id(column_id) {}
    RuntimeFilterPredicate(ColumnId column_id, const RuntimeFilter* rf)
            : _rf_desc(nullptr), _rf(rf), _column_id(column_id) {}
    virtual ~RuntimeFilterPredicate() = default;

    virtual Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to);
    virtual StatusOr<uint16_t> evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size, uint16_t* target_sel);
    virtual Status evaluate(Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
        return Status::NotSupported("RuntimeFilterPredicate::evaluate(Column*) not supported");
    }

    ColumnId get_column_id() const { return _column_id; }
    RuntimeFilterProbeDescriptor* get_rf_desc() const { return _rf_desc; }
    bool has_rf() const { return _rf != nullptr; }
    bool init(int32_t driver_sequence);
    virtual Status init() { return Status::OK(); }
    virtual Status refresh_if_updated(int32_t driver_sequence = -1) { return Status::OK(); }
    virtual void set_static_dict_mask(const Filter& mask, bool is_nullable) {
        _static_dict_mask = mask;
        _has_static_dict_mask = true;
        _is_nullable = is_nullable;
    }
    bool has_static_dict_mask() const { return _has_static_dict_mask; }
    const Filter& static_dict_mask() const { return _static_dict_mask; }
    bool is_nullable() const { return _is_nullable; }
    virtual bool is_always_false() const { return false; }

protected:
    // Only the RunningContext overload of RuntimeFilter::compute_partition_index() knows
    // about layout.bucket_properties(); the selection-aware overloads do not.  See
    // RuntimeFilterPredicate::evaluate() for why that distinction is load-bearing.
    bool _needs_bucket_aware_partition_index() const {
        return _rf_desc != nullptr && !_rf_desc->layout().bucket_properties().empty();
    }

    RuntimeFilterProbeDescriptor* _rf_desc;
    const RuntimeFilter* _rf = nullptr;
    // whether _rf passes every row, see RuntimeFilter::is_pass_all; computed once, since it compares the bounds.
    bool _pass_all = false;
    ColumnId _column_id;
    // Holds hash_values plus the bucket-aware scratch vectors, reused across chunks.  Each
    // HdfsScanner/OlapChunkSource gets its own RuntimeFilterPredicate instances
    // (ScanConjunctsManager::get_runtime_filter_predicates news them per call), so this
    // mutable state is not shared between concurrent readers.
    RuntimeFilter::RunningContext _running_ctx;
    std::vector<uint8_t> _input_selection;
    Filter _static_dict_mask;
    bool _has_static_dict_mask = false;
    bool _is_nullable = true;
};

class DictColumnRuntimeFilterPredicate : public RuntimeFilterPredicate {
public:
    using RuntimeFilterPredicate::init;

    DictColumnRuntimeFilterPredicate(RuntimeFilterProbeDescriptor* rf_desc, ColumnId column_id,
                                     std::vector<std::string> dict_words, bool is_nullable = true)
            : RuntimeFilterPredicate(rf_desc, column_id), _dict_words(std::move(dict_words)) {
        _is_nullable = is_nullable;
    }
    DictColumnRuntimeFilterPredicate(ColumnId column_id, ColumnPtr dict_words, const RuntimeFilter* rf,
                                     bool is_nullable = true)
            : RuntimeFilterPredicate(column_id, rf), _dict_words_col(std::move(dict_words)) {
        _is_nullable = is_nullable;
    }
    ~DictColumnRuntimeFilterPredicate() override = default;

    Status init() override;
    Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) override;
    StatusOr<uint16_t> evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size, uint16_t* dst_sel) override;
    Status evaluate(Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override;
    Status refresh_if_updated(int32_t driver_sequence = -1) override;
    void set_static_dict_mask(const Filter& mask, bool is_nullable) override;
    bool is_always_false() const override { return _is_always_false; }

    mutable Filter _dict_codes_selection;

private:
    Status prepare();
    Status _recompute_selection();

    std::vector<std::string> _dict_words;
    ColumnPtr _dict_words_col;
    size_t _rf_version = 0;
    bool _rf_has_null = false;
    bool _initialized = false;
    bool _is_always_false = false;
};

// RuntimeFilterPredicates is used to evaluate runtime filter predicates in the storage layer.
// The general workflow is as follows:
// 1. INIT stage: collect all available runtime filters. If there is at least one runtime filter, enter the SAMPLE stage.
// 2. SAMPLE stage: evaluate each avaiable runtime filter on a small portion of data and record the selectivity of each run time filter,
//    at the end of SAMPLE, select and retain the runtime filters with the best selectivity.
// 3. NORMAL stage: execute all selected runtime filters on the input data. after processing enough data, it will return to the INIT stage and start the next stage of sampling.
class RuntimeFilterPredicates {
    friend class RuntimeFilterPredicatesRewriter;

public:
    enum State : uint8_t {
        INIT = 0,
        SAMPLE = 1,
        NORMAL = 2,
    };
    RuntimeFilterPredicates() = default;
    RuntimeFilterPredicates(int32_t driver_sequence) : _driver_sequence(driver_sequence) {}

    void add_predicate(RuntimeFilterPredicate* pred) { _rf_predicates.emplace_back(pred); }
    bool empty() const { return _rf_predicates.empty(); }

    Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to);
    std::vector<RuntimeFilterPredicate*> rf_predicates() const { return _rf_predicates; }
    std::vector<RuntimeFilterPredicate*> preds(ColumnId column_id) const {
        std::vector<RuntimeFilterPredicate*> res;
        for (auto* pred : _rf_predicates) {
            if (pred->get_column_id() == column_id) {
                res.push_back(pred);
            }
        }
        return res;
    }
    int32_t driver_sequence() const { return _driver_sequence; }

    // True once at least one runtime filter has arrived.  Cheap (an atomic load per
    // predicate), so callers may poll it per chunk to skip work that is only needed
    // when a filter can actually be applied -- e.g. materializing the probe column.
    // Note this is deliberately not cached: filters keep arriving during a scan.
    bool any_filter_ready() const;

    Status refresh_dict_filters();
    bool has_always_false_dict_filter() const;
    void set_static_dict_mask(ColumnId column_id, const Filter& mask, bool is_nullable);

private:
    template <bool is_sample>
    Status _evaluate_selection(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to);

    template <bool is_sample>
    Status _evaluate_branchless(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to);

    void _update_selectivity_map();

    State _state = INIT;
    std::vector<RuntimeFilterPredicate*> _rf_predicates;
    struct SamplingCtx {
        RuntimeFilterPredicate* pred = nullptr;
        size_t filter_rows = 0;
    };
    std::vector<SamplingCtx> _sampling_ctxs;
    // some variables that need to be reused for each evaluation
    std::vector<uint8_t> _input_selection;
    std::vector<uint8_t> _tmp_selection;
    std::vector<uint16_t> _input_sel;
    std::vector<uint16_t> _tmp_sel;
    std::vector<uint16_t> _hit_count;
    uint16_t _input_count = 0;

    size_t _sample_rows = 0;
    size_t _skip_rows = 0;
    size_t _target_skip_rows = 0;

    std::map<double, RuntimeFilterPredicate*, std::greater<double>> _selectivity_map;

    int32_t _driver_sequence = -1;
};

using RuntimeFilterPredicatesPtr = std::shared_ptr<RuntimeFilterPredicates>;

} // namespace starrocks
