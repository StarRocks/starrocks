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

#include "storage_primitive/runtime_filter_predicate.h"

#include <cstring>

#include "base/simd/simd.h"
#include "common/config_rowset_fwd.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

bool RuntimeFilterPredicate::init(int32_t driver_sequence) {
    if (_rf) {
        return true;
    }
    _rf = _rf_desc->runtime_filter(driver_sequence);
    _pass_all = _rf != nullptr && _rf->always_true() && _rf->is_pass_all();
    return _rf != nullptr;
}

// A partitioned runtime filter is an array of sub-filters; a probe row must be tested
// against the very sub-filter its build-side counterpart went into, and _test_data_with_hash
// performs that lookup without validating it.  Picking a different index than the
// operator-level probe does not fail loudly -- it tests the row against a bloom filter that
// never saw it, and the row is silently dropped.
//
// So the index has to be computed by the same code the operator uses, not merely by
// equivalent-looking code.  The selection-aware compute_partition_index() overloads are NOT
// equivalent to the RunningContext one: they have no layout.bucket_properties() branch.
// Iceberg bucket-aware execution attaches bucket properties to LOCAL_HASH_BUCKET / COLOCATE
// filters, where the operator hashes with the murmur3 bucket transform and indexes
// bucketseq_to_instance[bucket_id] while the selection-aware overloads hash with CRC32 and
// index bucketseq_to_instance[hash % size] -- unrelated functions, not an off-by-one.
//
// Whenever bucket properties are present, delegate to the RunningContext overload verbatim
// rather than growing a second implementation of the transform here.
//
// compatibility is inert on that branch (it only gates the PARTITIONED /
// SHUFFLE_HASH_BUCKET ReduceOp arm, and those modes never carry bucket properties into it),
// but set it explicitly: RunningContext defaults it to true, and letting it default is what
// made an earlier attempt at this flip the shuffle modes from ReduceOp to ModuloOp in code
// shared with the OLAP scan path.
Status RuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    // A join filter that passes every row filters nothing, also no row its min/max can not hold, e.g. NaN.
    if (_pass_all) {
        return Status::OK();
    }
    auto column = chunk->get_column_by_id(_column_id);
    auto& hash_values = _running_ctx.hash_values;
    if (_rf->num_hash_partitions() > 0) {
        _running_ctx.exchange_hash_function_version = _rf_desc->exchange_hash_function_version();
        if (_needs_bucket_aware_partition_index()) {
            _running_ctx.compatibility = false;
            _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, &_running_ctx);
        } else {
            hash_values.resize(column->size());
            _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, selection, from, to, hash_values,
                                         &_running_ctx);
        }
    }
    if (!column->has_null()) {
        _rf->evaluate(column.get(), hash_values, selection, from, to);
        return Status::OK();
    }
    // A filter writes its has_null verdict for a NULL key without looking at selection[i], so a <=> join
    // filter would revive NULL-key rows an earlier predicate rejected unless its result is ANDed back.
    _input_selection.assign(selection + from, selection + to);
    _rf->evaluate(column.get(), hash_values, selection, from, to);
    for (uint16_t i = from; i < to; i++) {
        selection[i] &= _input_selection[i - from];
    }
    return Status::OK();
}

StatusOr<uint16_t> RuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size,
                                                    uint16_t* target_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    if (_pass_all) {
        if (target_sel != sel) {
            memcpy(target_sel, sel, sel_size * sizeof(uint16_t));
        }
        return sel_size;
    }
    auto column = chunk->get_column_by_id(_column_id);
    auto& hash_values = _running_ctx.hash_values;
    if (_rf->num_hash_partitions() > 0) {
        _running_ctx.exchange_hash_function_version = _rf_desc->exchange_hash_function_version();
        if (_needs_bucket_aware_partition_index()) {
            _running_ctx.compatibility = false;
            _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, &_running_ctx);
        } else {
            hash_values.resize(column->size());
            _rf->compute_partition_index(_rf_desc->layout(), {column.get()}, sel, sel_size, hash_values, &_running_ctx);
        }
    }
    uint16_t ret = _rf->evaluate(column.get(), hash_values, sel, sel_size, target_sel);
    return ret;
}

Status DictColumnRuntimeFilterPredicate::init() {
    if (_rf == nullptr) {
        return Status::OK();
    }
    return _recompute_selection();
}

void DictColumnRuntimeFilterPredicate::set_static_dict_mask(const Filter& mask, bool is_nullable) {
    _static_dict_mask = mask;
    _has_static_dict_mask = true;
    _is_nullable = is_nullable;
    _initialized = false;
}

Status DictColumnRuntimeFilterPredicate::refresh_if_updated(int32_t driver_sequence) {
    if (_rf == nullptr && _rf_desc != nullptr) {
        RuntimeFilterPredicate::init(driver_sequence);
    }
    if (_rf == nullptr) {
        return Status::OK();
    }
    if (!_initialized || _rf->rf_version() != _rf_version || _rf->has_null() != _rf_has_null) {
        return _recompute_selection();
    }
    return Status::OK();
}

Status DictColumnRuntimeFilterPredicate::_recompute_selection() {
    DCHECK(_rf != nullptr);
    if (_dict_words_col == nullptr) {
        auto binary_column = BinaryColumn::create();
        std::vector<Slice> data_slice;
        data_slice.reserve(_dict_words.size());
        for (const auto& word : _dict_words) {
            data_slice.emplace_back(word.data(), word.size());
        }
        binary_column->append_strings(data_slice.data(), data_slice.size());
        _dict_words_col = std::move(binary_column);
    }

    _rf_version = _rf->rf_version();
    _rf_has_null = _rf->has_null();
    _initialized = true;

    const size_t size = _dict_words_col->size();
    if (size == 0) {
        _dict_codes_selection.clear();
        _is_always_false = (!_is_nullable || !_rf->has_null());
        return Status::OK();
    }

    RuntimeFilter::RunningContext ctx;
    ctx.use_merged_selection = false;
    ctx.compatibility = false;
    ctx.selection.assign(size, 1);
    if (_rf_desc != nullptr) {
        ctx.exchange_hash_function_version = _rf_desc->exchange_hash_function_version();
        if (_rf->num_hash_partitions() > 0) {
            ctx.hash_values.resize(size);
            _rf->compute_partition_index(_rf_desc->layout(), {_dict_words_col.get()}, &ctx);
        }
    }
    _rf->evaluate(_dict_words_col.get(), &ctx);

    if (const RuntimeFilter* mm_base = _rf->get_min_max_filter(); mm_base == _rf) {
        auto apply_slice_minmax = [&](const auto* mm_rf) {
            ObjectPool tmp_pool;
            const Slice min_val = mm_rf->min_value(&tmp_pool);
            const Slice max_val = mm_rf->max_value(&tmp_pool);
            const bool left_close = mm_rf->left_close_interval();
            const bool right_close = mm_rf->right_close_interval();
            const auto* binary_col =
                    down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(_dict_words_col.get()));
            for (size_t i = 0; i < size; ++i) {
                if (!ctx.selection[i]) continue;
                const Slice val = binary_col->get_slice(i);
                const bool left_ok = left_close ? (val >= min_val) : (val > min_val);
                const bool right_ok = right_close ? (val <= max_val) : (val < max_val);
                ctx.selection[i] = static_cast<uint8_t>(left_ok && right_ok);
            }
        };
        if (const auto* varchar_mm = dynamic_cast<const MinMaxRuntimeFilter<TYPE_VARCHAR>*>(mm_base)) {
            apply_slice_minmax(varchar_mm);
        } else if (const auto* char_mm = dynamic_cast<const MinMaxRuntimeFilter<TYPE_CHAR>*>(mm_base)) {
            apply_slice_minmax(char_mm);
        }
    }

    _dict_codes_selection.swap(ctx.selection);
    if (_has_static_dict_mask && _static_dict_mask.size() == size) {
        for (size_t i = 0; i < size; ++i) {
            _dict_codes_selection[i] &= _static_dict_mask[i];
        }
    }

    const size_t active = SIMD::count_nonzero(_dict_codes_selection);
    _is_always_false = (active == 0) && (!_is_nullable || !_rf->has_null());
    return Status::OK();
}

Status DictColumnRuntimeFilterPredicate::evaluate(Column* column, uint8_t* selection, uint16_t from,
                                                  uint16_t to) const {
    if (_is_always_false) {
        memset(selection + from, 0, to - from);
        return Status::OK();
    }
    if (column->is_nullable()) {
        auto* nullable_col = down_cast<NullableColumn*>(column);
        const auto& dict_codes = down_cast<Int32Column*>(nullable_col->data_column_raw_ptr())->get_data();
        if (nullable_col->has_null()) {
            const auto& null_data = nullable_col->immutable_null_column_data();
            const uint8_t rf_has_null = static_cast<uint8_t>(_rf->has_null());
            for (uint16_t i = from; i < to; ++i) {
                if (!selection[i]) continue;
                selection[i] = null_data[i] ? rf_has_null : _dict_codes_selection[dict_codes[i]];
            }
        } else {
            for (uint16_t i = from; i < to; ++i) {
                if (!selection[i]) continue;
                selection[i] = _dict_codes_selection[dict_codes[i]];
            }
        }
    } else {
        const auto& dict_codes = down_cast<Int32Column*>(column)->get_data();
        for (uint16_t i = from; i < to; ++i) {
            if (!selection[i]) continue;
            selection[i] = _dict_codes_selection[dict_codes[i]];
        }
    }
    return Status::OK();
}

Status DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) {
    RETURN_IF_ERROR(prepare());
    auto* column = chunk->get_column_raw_ptr_by_id(_column_id);
    return evaluate(column, selection, from, to);
}

StatusOr<uint16_t> DictColumnRuntimeFilterPredicate::evaluate(Chunk* chunk, uint16_t* sel, uint16_t sel_size,
                                                              uint16_t* target_sel) {
    if (sel_size == 0) {
        return sel_size;
    }
    RETURN_IF_ERROR(prepare());
    if (_is_always_false) {
        return 0;
    }
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
                    new_size += _dict_codes_selection[data[idx]];
                }
            }
        } else {
            for (uint16_t i = 0; i < sel_size; i++) {
                uint16_t idx = sel[i];
                target_sel[new_size] = idx;
                new_size += _dict_codes_selection[data[idx]];
            }
        }
    } else {
        const auto& data = GetContainer<TYPE_INT>::get_data(column);
        for (uint16_t i = 0; i < sel_size; i++) {
            uint16_t idx = sel[i];
            target_sel[new_size] = idx;
            new_size += _dict_codes_selection[data[idx]];
        }
    }
    return new_size;
}

Status DictColumnRuntimeFilterPredicate::prepare() {
    return refresh_if_updated();
}

bool RuntimeFilterPredicates::any_filter_ready() const {
    for (const auto* pred : _rf_predicates) {
        if ((pred->get_rf_desc() != nullptr && pred->get_rf_desc()->runtime_filter(_driver_sequence) != nullptr) ||
            pred->has_rf()) {
            return true;
        }
    }
    return false;
}

Status RuntimeFilterPredicates::refresh_dict_filters() {
    for (auto* pred : _rf_predicates) {
        RETURN_IF_ERROR(pred->refresh_if_updated(_driver_sequence));
    }
    return Status::OK();
}

bool RuntimeFilterPredicates::has_always_false_dict_filter() const {
    for (const auto* pred : _rf_predicates) {
        if (pred->is_always_false()) {
            return true;
        }
    }
    return false;
}

void RuntimeFilterPredicates::set_static_dict_mask(ColumnId column_id, const Filter& mask, bool is_nullable) {
    for (auto* pred : _rf_predicates) {
        if (pred->get_column_id() == column_id) {
            pred->set_static_dict_mask(mask, is_nullable);
        }
    }
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
        if (filter_ratio >= 0.5 || pred->is_always_false() ||
            (pred->get_rf_desc() != nullptr && pred->get_rf_desc()->is_topn_filter())) {
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

} // namespace starrocks