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

#include "storage/runtime_filter_predicate.h"

#include <cstring>

#include "common/config.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_tracker.h"
#include "simd/simd.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

bool RuntimeFilterPredicate::init(int32_t driver_sequence) {
    if (_rf) {
        return true;
    }
    _rf = _rf_desc->runtime_filter(driver_sequence);
    return _rf != nullptr;
}

Status RuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    auto column = chunk->get_column_by_id(_column_id);
    if (_rf->num_hash_partitions() > 0) {
        hash_values.resize(column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, selection, from, to, hash_values);
    }
    _rf->evaluate(column.get(), hash_values, selection, from, to);
    return Status::OK();
}

StatusOr<uint16_t> RuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size,
                                                    uint16_t* target_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    auto column = chunk->get_column_by_id(_column_id);
    if (_rf->num_hash_partitions() > 0) {
        hash_values.resize(column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, sel, sel_size, hash_values);
    }
    uint16_t ret = _rf->evaluate(column.get(), hash_values, sel, sel_size, target_sel);
    return ret;
}

Status DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    RETURN_IF_ERROR(prepare());
    auto column = chunk->get_column_by_id(_column_id).get();
    if (column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(column);
        // dict code column must be int column
        const auto& data = GetContainer<TYPE_INT>::get_data(nullable_column->data_column());
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (uint16_t i = from; i < to; i++) {
                if (!selection[i]) continue;
                if (null_data[i]) {
                    selection[i] = _rf->has_null();
                } else {
                    selection[i] = _result[data[i]];
                }
            }
        } else {
            for (uint16_t i = from; i < to; i++) {
                if (!selection[i]) continue;
                selection[i] = _result[data[i]];
            }
        }
    } else {
        const auto& data = GetContainer<TYPE_INT>::get_data(column);
        for (uint16_t i = from; i < to; i++) {
            if (!selection[i]) continue;
            selection[i] = _result[data[i]];
        }
    }
    return Status::OK();
}

StatusOr<uint16_t> DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size,
                                                              uint16_t* target_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    RETURN_IF_ERROR(prepare());
    auto column = chunk->get_column_by_id(_column_id).get();
    uint16_t new_size = 0;
    if (column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(column);
        const auto& data = GetContainer<TYPE_INT>::get_data(nullable_column->data_column());
        if (nullable_column->has_null()) {
            const uint8_t* null_data = nullable_column->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; i++) {
                uint16_t idx = sel[i];
                target_sel[new_size] = idx;
                if (null_data[idx]) {
                    new_size += _rf->has_null();
                } else {
                    new_size += _result[data[idx]];
                }
            }
        } else {
            for (uint16_t i = 0; i < sel_size; i++) {
                uint16_t idx = sel[i];
                target_sel[new_size] = idx;
                new_size += _result[data[idx]];
            }
        }
    } else {
        const auto& data = GetContainer<TYPE_INT>::get_data(column);
        for (uint16_t i = 0; i < sel_size; i++) {
            uint16_t idx = sel[i];
            target_sel[new_size] = idx;
            new_size += _result[data[idx]];
        }
    }
    return new_size;
}

Status DictColumnRuntimeFilterPredicate::prepare() {
    DCHECK(_rf != nullptr);
    if (!_result.empty()) {
        return Status::OK();
    }
    auto binary_column = BinaryColumn::create();
    std::vector<Slice> data_slice;
    for (const auto& word : _dict_words) {
        data_slice.emplace_back(word.data(), word.size());
    }
    binary_column->append_strings(data_slice.data(), data_slice.size());

    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    ctx.compatibility = false;
    ctx.selection.assign(_dict_words.size(), 1);

    if (_rf->num_hash_partitions() > 0) {
        // compute hash
        ctx.hash_values.resize(binary_column->size());
        _rf->compute_partition_index(_rf_desc->layout(), {binary_column.get()}, &ctx);
    }
    _rf->evaluate(binary_column.get(), &ctx);
    _result.resize(_dict_words.size());

    for (size_t i = 0; i < _dict_words.size(); ++i) {
        _result[i] = ctx.selection[i];
    }

    return Status::OK();
}

void RuntimeFilterPredicates::_update_selectivity_map() {
    _selectivity_map.clear();
    std::sort(_sampling_ctxs.begin(), _sampling_ctxs.end(),
              [](const SamplingCtx& a, const SamplingCtx& b) { return a.filter_rows > b.filter_rows; });
    for (size_t i = 0; i < _sampling_ctxs.size() && _selectivity_map.size() < 3; i++) {
        auto pred = _sampling_ctxs[i].pred;
        double filter_ratio = _sampling_ctxs[i].filter_rows * 1.0 / _sample_rows;

        if (filter_ratio >= 0.95) {
            // very good, only keep one
            _selectivity_map.clear();
            _selectivity_map.emplace(filter_ratio, pred);
            break;
        }
        if (filter_ratio >= 0.5) {
            _selectivity_map.emplace(filter_ratio, pred);
        }
    }
}

Status RuntimeFilterPredicates::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    if (_rf_predicates.empty()) {
        return Status::OK();
    }
    _input_count = SIMD::count_nonzero(selection + from, to - from);
    if (_input_count == 0) {
        return Status::OK();
    }
    size_t num_rows = chunk->num_rows();
    // We will decide which execution mode to use based on the sparseness of the rows selected in the chunk.
    bool use_branchless_mode = (num_rows >= config::rf_branchless_ratio * _input_count);
    switch (_state) {
    case INIT: {
        CHECK(_sampling_ctxs.empty()) << "sampling ctxs must be empty";
        for (auto pred : _rf_predicates) {
            if (pred->init(_driver_sequence)) {
                _sampling_ctxs.emplace_back(SamplingCtx{pred, 0});
            }
        }
        if (!_sampling_ctxs.empty()) {
            _state = SAMPLE;
            _sample_rows = 0;
        } else {
            // no rf can be used, just return
            return Status::OK();
        }
    }
    case SAMPLE: {
        if (use_branchless_mode) {
            RETURN_IF_ERROR(_evaluate_branchless<true>(chunk, selection, from, to));
        } else {
            RETURN_IF_ERROR(_evaluate_selection<true>(chunk, selection, from, to));
        }
        _sample_rows += _input_count;
        if (_sample_rows >= config::rf_sample_rows) {
            _update_selectivity_map();
            _state = NORMAL;
            _skip_rows = 0;
            _target_skip_rows = _sample_rows * config::rf_sample_ratio;
        }
        return Status::OK();
    }
    case NORMAL: {
        if (use_branchless_mode) {
            RETURN_IF_ERROR(_evaluate_branchless<false>(chunk, selection, from, to));
        } else {
            RETURN_IF_ERROR(_evaluate_selection<false>(chunk, selection, from, to));
        }
        _skip_rows += _input_count;
        if (_skip_rows >= _target_skip_rows) {
            _state = INIT;
            _sampling_ctxs.clear();
            _sample_rows = 0;
        }
        return Status::OK();
    }
    default:
        break;
    }
    __builtin_unreachable();
}

template <bool is_sample>
Status RuntimeFilterPredicates::_evaluate_branchless(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    _input_sel.clear();
    for (uint16_t i = from; i < to; i++) {
        if (selection[i]) {
            _input_sel.emplace_back(i);
        }
    }
    CHECK(!_input_sel.empty()) << "input selection must not be empty";
    uint16_t output_count = _input_count;
    if constexpr (is_sample) {
        if (_sampling_ctxs.size() == 1) {
            auto pred = _sampling_ctxs[0].pred;
            auto& filter_rows = _sampling_ctxs[0].filter_rows;
            ASSIGN_OR_RETURN(output_count,
                             pred->evaluate(chunk, _input_sel.data(), _input_sel.size(), _input_sel.data()));
            memset(selection + from, 0, to - from);
            for (uint16_t i = 0; i < output_count; i++) {
                selection[_input_sel[i]] = 1;
            }
            filter_rows += _input_count - output_count;
        } else {
            // if there are multiple runtime filters, we need to record the number of times each row hits the RF.
            // only when all runtime filters do not filter out the data, the corresponding row is retained.
            _hit_count.resize(chunk->num_rows());
            memset(_hit_count.data() + from, 0, (to - from) * sizeof(uint16_t));
            _tmp_sel.resize(_input_sel.size());
            for (size_t i = 0; i < _sampling_ctxs.size(); i++) {
                auto pred = _sampling_ctxs[i].pred;
                auto& filter_rows = _sampling_ctxs[i].filter_rows;
                ASSIGN_OR_RETURN(output_count,
                                 pred->evaluate(chunk, _input_sel.data(), _input_sel.size(), _tmp_sel.data()));
                for (uint16_t j = 0; j < output_count; j++) {
                    _hit_count[_tmp_sel[j]]++;
                }
                filter_rows += _input_count - output_count;
            }
            uint16_t target_num = _sampling_ctxs.size();
            for (uint16_t i = from; i < to; i++) {
                selection[i] = (_hit_count[i] == target_num);
            }
        }
    } else {
        for (auto& [_, pred] : _selectivity_map) {
            ASSIGN_OR_RETURN(output_count, pred->evaluate(chunk, _input_sel.data(), output_count, _input_sel.data()));
        }
        memset(selection + from, 0, to - from);
        for (uint16_t i = 0; i < output_count; i++) {
            selection[_input_sel[i]] = 1;
        }
    }
    return Status::OK();
}

template <bool is_sample>
Status RuntimeFilterPredicates::_evaluate_selection(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    CHECK_GT(_input_count, 0) << "input count must be greater than 0";
    if constexpr (is_sample) {
        if (_sampling_ctxs.size() == 1) {
            // only one filter, no need to merge selection
            auto pred = _sampling_ctxs[0].pred;
            auto& filter_rows = _sampling_ctxs[0].filter_rows;
            RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
            uint16_t output_count = SIMD::count_nonzero(selection + from, to - from);
            filter_rows += _input_count - output_count;
        } else {
            _input_selection.resize(chunk->num_rows());
            _tmp_selection.resize(chunk->num_rows());
            strings::memcpy_inlined(_input_selection.data() + from, selection + from, to - from);
            for (size_t i = 0; i < _sampling_ctxs.size(); i++) {
                auto pred = _sampling_ctxs[i].pred;
                auto& filter_rows = _sampling_ctxs[i].filter_rows;
                strings::memcpy_inlined(_tmp_selection.data() + from, _input_selection.data() + from, to - from);
                RETURN_IF_ERROR(pred->evaluate(chunk, _tmp_selection.data(), from, to));
                uint16_t output_count = SIMD::count_nonzero(_tmp_selection.data() + from, to - from);
                filter_rows += _input_count - output_count;
                for (uint16_t j = from; j < to; j++) {
                    selection[j] = selection[j] & _tmp_selection[j];
                }
            }
        }
    } else {
        for (auto& [_, pred] : _selectivity_map) {
            RETURN_IF_ERROR(pred->evaluate(chunk, selection, from, to));
        }
    }
    return Status::OK();
}

Status RuntimeFilterPredicatesRewriter::rewrite(ObjectPool* obj_pool, RuntimeFilterPredicates& preds,
                                                const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators,
                                                const Schema& schema) {
    auto& predicates = preds._rf_predicates;
    for (size_t i = 0; i < predicates.size(); i++) {
        auto* pred = predicates[i];
        ColumnId column_id = pred->get_column_id();
        if (!column_iterators[column_id]->all_page_dict_encoded()) {
            continue;
        }
        auto* rf_desc = pred->get_rf_desc();
        // For dictionary-encoded columns, we can directly calculate the runtime filter based on the dictionary words
        // instead of decoding it first.
        std::vector<Slice> all_words;
        RETURN_IF_ERROR(column_iterators[column_id]->fetch_all_dict_words(&all_words));
        std::vector<std::string> dict_words;
        for (const auto& word : all_words) {
            dict_words.emplace_back(std::string(word.get_data(), word.get_size()));
        }
        predicates[i] = obj_pool->add(new DictColumnRuntimeFilterPredicate(rf_desc, column_id, std::move(dict_words)));
    }
    return Status::OK();
}
} // namespace starrocks