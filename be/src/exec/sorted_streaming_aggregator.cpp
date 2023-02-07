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

#include "exec/sorted_streaming_aggregator.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "column/column_visitor_adapter.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exprs/expr_context.h"
#include "glog/logging.h"
#include "runtime/mem_pool.h"

namespace starrocks {

using NullMasks = NullColumn::Container;

// compare the value by column
//
// cmp_vector[i] == 0 means data[i - 1] equels to data[i]
// cmp_vector[0] = first_column.data[0].compare(data[0]) != 0
// cmp_vector[i] = data[i - 1].compare(data[i]) != 0
class ColumnSelfComparator : public ColumnVisitorAdapter<ColumnSelfComparator> {
public:
    ColumnSelfComparator(const ColumnPtr& first_column, std::vector<uint8_t>& cmp_vector, const NullMasks& null_masks)
            : ColumnVisitorAdapter(this),
              _first_column(first_column),
              _cmp_vector(cmp_vector),
              _null_masks(null_masks) {}

    Status do_visit(const NullableColumn& column) {
        ColumnPtr ptr = down_cast<NullableColumn*>(_first_column.get())->data_column();
        ColumnSelfComparator comparator(ptr, _cmp_vector, column.immutable_null_column_data());
        RETURN_IF_ERROR(column.data_column()->accept(&comparator));

        const auto& data_column = column.data_column();
        // NOTE
        if (!_first_column->empty()) {
            _cmp_vector[0] |= _first_column->compare_at(0, 0, *data_column, 1) != 0;
        } else {
            _cmp_vector[0] |= 1;
        }
        return Status::OK();
    }
    Status do_visit(const ConstColumn& column) {
        return Status::NotSupported("Unsupported const column in column wise comparator");
    }
    Status do_visit(const ArrayColumn& column) {
        return Status::NotSupported("Unsupported array column in column wise comparator");
    }
    Status do_visit(const LargeBinaryColumn& column) {
        return Status::NotSupported("Unsupported large binary column in column wise comparator");
    }

    Status do_visit(const BinaryColumn& column) {
        size_t num_rows = column.size();
        if (!_first_column->empty()) {
            _cmp_vector[0] |= _first_column->compare_at(0, 0, column, 1) != 0;
        } else {
            _cmp_vector[0] |= 1;
        }
        if (!_null_masks.empty()) {
            DCHECK_EQ(_null_masks.size(), num_rows);
            for (size_t i = 1; i < num_rows; ++i) {
                if (_null_masks[i - 1] == 0 && _null_masks[i] == 0) {
                    _cmp_vector[i] |= column.get_slice(i - 1).compare(column.get_slice(i)) != 0;
                } else {
                    _cmp_vector[i] |= _null_masks[i - 1] != _null_masks[i];
                }
            }
        } else {
            for (size_t i = 1; i < num_rows; ++i) {
                _cmp_vector[i] |= column.get_slice(i - 1).compare(column.get_slice(i)) != 0;
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        size_t num_rows = column.size();
        if (!_first_column->empty()) {
            _cmp_vector[0] |= _first_column->compare_at(0, 0, column, 1) != 0;
        } else {
            _cmp_vector[0] |= 1;
        }
        const auto& data_container = column.get_data();
        if (!_null_masks.empty()) {
            DCHECK_EQ(_null_masks.size(), num_rows);
            for (size_t i = 1; i < num_rows; ++i) {
                if (_null_masks[i - 1] == 0 && _null_masks[i] == 0) {
                    _cmp_vector[i] |= data_container[i - 1] != data_container[i];
                } else {
                    _cmp_vector[i] |= _null_masks[i - 1] != _null_masks[i];
                }
            }
        } else {
            for (size_t i = 1; i < num_rows; ++i) {
                _cmp_vector[i] |= data_container[i - 1] != data_container[i];
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        return Status::NotSupported("Unsupported object column in column wise comparator");
    }

    Status do_visit(const MapColumn& column) {
        return Status::NotSupported("Unsupported map column in column wise comparator");
    }

    Status do_visit(const StructColumn& column) {
        return Status::NotSupported("Unsupported struct column in column wise comparator");
    }

private:
    const ColumnPtr& _first_column;
    std::vector<uint8_t>& _cmp_vector;
    const NullColumn::Container& _null_masks;
};

// append the result by selector
// selector[i] == 0 means selected
//
class AppendWithMask : public ColumnVisitorMutableAdapter<AppendWithMask> {
public:
    using SelMask = std::vector<uint8_t>;
    AppendWithMask(Column* column, SelMask sel_mask, size_t selected_size)
            : ColumnVisitorMutableAdapter(this),
              _column(column),
              _sel_mask(std::move(sel_mask)),
              _selected_size(selected_size) {}

    Status do_visit(NullableColumn* column) {
        auto col = down_cast<NullableColumn*>(_column);
        AppendWithMask data_appender(col->data_column().get(), _sel_mask, _selected_size);
        RETURN_IF_ERROR(column->data_column()->accept_mutable(&data_appender));
        AppendWithMask null_appender(col->null_column().get(), _sel_mask, _selected_size);
        RETURN_IF_ERROR(column->null_column()->accept_mutable(&null_appender));
        column->update_has_null();
        return Status::OK();
    }

    Status do_visit(ConstColumn* column) {
        return Status::NotSupported("Unsupported const column in column wise comparator");
    }

    Status do_visit(ArrayColumn* column) {
        return Status::NotSupported("Unsupported array column in column wise comparator");
    }

    Status do_visit(LargeBinaryColumn* column) {
        return Status::NotSupported("Unsupported large binary column in column wise comparator");
    }

    Status do_visit(BinaryColumn* column) {
        auto col = down_cast<BinaryColumn*>(_column);
        auto& slices = col->get_proxy_data();
        std::vector<Slice> datas(_sel_mask.size());
        size_t offsets = 0;

        for (size_t i = 0; i < _sel_mask.size(); ++i) {
            datas[offsets] = slices[i];
            offsets += !_sel_mask[i];
        }
        DCHECK_EQ(_selected_size, offsets);
        datas.resize(_selected_size);
        column->append_strings(datas);
        return Status::OK();
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        auto col = down_cast<FixedLengthColumnBase<T>*>(_column);
        const auto& container = col->get_data();
        std::vector<T> datas(_sel_mask.size());
        size_t offsets = 0;

        for (size_t i = 0; i < _sel_mask.size(); ++i) {
            datas[offsets] = container[i];
            offsets += !_sel_mask[i];
        }

        DCHECK_EQ(_selected_size, offsets);
        datas.resize(_selected_size);
        column->append_numbers(datas.data(), sizeof(T) * _selected_size);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* column) {
        return Status::NotSupported("Unsupported object column in column wise comparator");
    }

    Status do_visit(MapColumn* column) {
        return Status::NotSupported("Unsupported map column in column wise comparator");
    }

    Status do_visit(StructColumn* column) {
        return Status::NotSupported("Unsupported struct column in column wise comparator");
    }

private:
    Column* _column;
    const SelMask _sel_mask;
    size_t _selected_size;
};

// batch allocate states
// For streaming aggregates, the maximum number of aggregate states we can hold is chunk_size + 1
// (the states for processing a batch of chunks and the last remaining state).
// So we can just allocate chunk size * 2 states and decide which states
// to return based on the remaining states
struct StateAllocator {
    static size_t constexpr aligned = 16;
    struct buffer_range {
        uint8_t* start;
        uint8_t* end;
    };
    StateAllocator(MemPool* pool_, size_t batch_size, size_t aggregate_state_size) : pool(pool_) {
        buffer[0].start = pool->allocate_aligned(batch_size * aggregate_state_size, aligned);
        buffer[0].end = buffer[0].start + batch_size * aggregate_state_size;
        buffer[1].start = pool->allocate_aligned(batch_size * aggregate_state_size, aligned);
        buffer[1].end = buffer[1].start + batch_size * aggregate_state_size;
    }

    uint8_t* allocated(uint8_t* input) {
        if (input == nullptr) {
            return buffer[0].start;
        }
        DCHECK((input >= buffer[0].start && input < buffer[0].end) ||
               (input >= buffer[1].start && input < buffer[1].end));
        if (input >= buffer[0].start && input < buffer[0].end) {
            return buffer[1].start;
        }
        return buffer[0].start;
    }

private:
    MemPool* pool;
    buffer_range buffer[2];
};

SortedStreamingAggregator::SortedStreamingAggregator(AggregatorParamsPtr&& params) : Aggregator(std::move(params)) {}

SortedStreamingAggregator::~SortedStreamingAggregator() {
    if (_state) {
        close(_state);
    }
}

Status SortedStreamingAggregator::prepare(RuntimeState* state, ObjectPool* pool, RuntimeProfile* runtime_profile,
                                          MemTracker* mem_tracker) {
    RETURN_IF_ERROR(Aggregator::prepare(state, pool, runtime_profile, mem_tracker));
    _streaming_state_allocator =
            std::make_shared<StateAllocator>(_mem_pool.get(), _state->chunk_size(), _agg_states_total_size);
    return Status::OK();
}

Status SortedStreamingAggregator::streaming_compute_agg_state(size_t chunk_size) {
    if (chunk_size == 0) {
        return Status::OK();
    }

    _tmp_agg_states.resize(chunk_size);
    _cmp_vector.resize(chunk_size);

    if (_last_columns.empty()) {
        _last_columns.resize(_group_by_columns.size());
        for (int i = 0; i < _last_columns.size(); ++i) {
            _last_columns[i] = _group_by_columns[i]->clone_empty();
        }
    }

    RETURN_IF_ERROR(_compute_group_by(chunk_size));

    RETURN_IF_ERROR(_update_states(chunk_size));

    // selector[i] == 0 means selected
    std::vector<uint8_t> selector(chunk_size);
    size_t selected_size = 0;
    {
        SCOPED_TIMER(_agg_stat->agg_compute_timer);
        for (size_t i = 1; i < _cmp_vector.size(); ++i) {
            selector[i - 1] = _cmp_vector[i] == 0;
            selected_size += !selector[i - 1];
        }
        // we will never select the last rows
        selector[chunk_size - 1] = 1;
    }

    // finalize state
    // group[i] != group[i - 1] means we have add a new state for group[i], then we need call finalize for group[i - 1]
    // get result from aggregate values. such as count(*), sum(col)
    bool use_intermediate = _use_intermediate_as_output();
    auto agg_result_columns = _create_agg_result_columns(chunk_size, use_intermediate);
    RETURN_IF_ERROR(_get_agg_result_columns(chunk_size, selector, agg_result_columns));

    DCHECK_LE(agg_result_columns[0]->size(), _state->chunk_size());

    _close_group_by(chunk_size, selector);

    // combine group by keys
    auto res_group_by_columns = _create_group_by_columns(chunk_size);
    RETURN_IF_ERROR(_build_group_by_columns(chunk_size, selected_size, selector, res_group_by_columns));
    auto result_chunk = _build_output_chunk(res_group_by_columns, agg_result_columns, use_intermediate);

    // TODO merge small chunk
    this->offer_chunk_to_buffer(result_chunk);

    // prepare for next
    for (size_t i = 0; i < _last_columns.size(); ++i) {
        // last column should never be the same column with new input column
        DCHECK_NE(_last_columns[i].get(), _group_by_columns[i].get());
        _last_columns[i]->reset_column();
        _last_columns[i]->append(*_group_by_columns[i], chunk_size - 1, 1);
    }
    _last_state = _tmp_agg_states[chunk_size - 1];
    DCHECK(!_group_by_columns[0]->empty());
    DCHECK(!_last_columns[0]->empty());
    return Status::OK();
}

Status SortedStreamingAggregator::_compute_group_by(size_t chunk_size) {
    // compare stage
    // _cmp_vector[i] = group[i - 1].equals(group[i])
    // _cmp_vector[i] == 0 means group[i - 1].equals(group[i])
    _cmp_vector.assign(chunk_size, 0);
    const std::vector<uint8_t> dummy;
    SCOPED_TIMER(_agg_stat->agg_compute_timer);
    for (size_t i = 0; i < _group_by_columns.size(); ++i) {
        ColumnSelfComparator cmp(_last_columns[i], _cmp_vector, dummy);
        RETURN_IF_ERROR(_group_by_columns[i]->accept(&cmp));
        // TODO short-circuit
    }
    return Status::OK();
}

Status SortedStreamingAggregator::_update_states(size_t chunk_size) {
    // TODO: split the states
    // allocate state stage
    {
        SCOPED_TIMER(_agg_stat->allocate_state_timer);
        auto batch_allocated_states = _streaming_state_allocator->allocated(_last_state);
        AggDataPtr last_state = _last_state;
        size_t cnt = 0;
        //
        for (size_t i = 0; i < _cmp_vector.size(); ++i) {
            if (_cmp_vector[i] == 0) {
                _tmp_agg_states[i] = last_state;
            } else {
                _tmp_agg_states[i] = batch_allocated_states + cnt++ * _agg_states_total_size;
            }
            last_state = _tmp_agg_states[i];
        }

        // only create the state when selector == 0
        std::vector<uint8_t> create_selector(chunk_size);
        for (size_t i = 0; i < _cmp_vector.size(); ++i) {
            create_selector[i] = _cmp_vector[i] == 0;
        }

        //
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            _agg_functions[i]->batch_create_with_selection(_agg_fn_ctxs[i], chunk_size, _tmp_agg_states,
                                                           _agg_states_offsets[i], create_selector);
        }
    }

    bool use_intermediate = _use_intermediate_as_input();
    // prepare output column
    // batch_update/merge stage
    {
        SCOPED_TIMER(_agg_stat->agg_compute_timer);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            if (!_is_merge_funcs[i] && !use_intermediate) {
                _agg_functions[i]->update_batch(_agg_fn_ctxs[i], chunk_size, _agg_states_offsets[i],
                                                _agg_input_raw_columns[i].data(), _tmp_agg_states.data());
            } else {
                DCHECK_GE(_agg_input_columns[i].size(), 1);
                _agg_functions[i]->merge_batch(_agg_fn_ctxs[i], _agg_input_columns[i][0]->size(),
                                               _agg_states_offsets[i], _agg_input_columns[i][0].get(),
                                               _tmp_agg_states.data());
            }
        }
    }
    return Status::OK();
}

Status SortedStreamingAggregator::_get_agg_result_columns(size_t chunk_size, const std::vector<uint8_t>& selector,
                                                          Columns& agg_result_columns) {
    SCOPED_TIMER(_agg_stat->get_results_timer);
    if (_cmp_vector[0] != 0 && _last_state) {
        TRY_CATCH_BAD_ALLOC(_finalize_to_chunk(_last_state, agg_result_columns));
    }

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        TRY_CATCH_BAD_ALLOC(_agg_functions[i]->batch_finalize_with_selection(_agg_fn_ctxs[i], chunk_size,
                                                                             _tmp_agg_states, _agg_states_offsets[i],
                                                                             agg_result_columns[i].get(), selector));
    }
    return Status::OK();
}

void SortedStreamingAggregator::_close_group_by(size_t chunk_size, const std::vector<uint8_t>& selector) {
    // close stage
    SCOPED_TIMER(_agg_stat->state_destroy_timer);
    if (_cmp_vector[0] != 0 && _last_state) {
        _destroy_state(_last_state);
    }

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->batch_destroy_with_selection(_agg_fn_ctxs[i], chunk_size, _tmp_agg_states,
                                                        _agg_states_offsets[i], selector);
    }
}

Status SortedStreamingAggregator::_build_group_by_columns(size_t chunk_size, size_t selected_size,
                                                          const std::vector<uint8_t>& selector,
                                                          Columns& agg_group_by_columns) {
    SCOPED_TIMER(_agg_stat->agg_append_timer);
    if (_cmp_vector[0] != 0 && _last_state) {
        for (size_t i = 0; i < agg_group_by_columns.size(); ++i) {
            agg_group_by_columns[i]->append(*_last_columns[i], 0, 1);
        }
    }

    for (size_t i = 0; i < agg_group_by_columns.size(); ++i) {
        AppendWithMask appender(_group_by_columns[i].get(), selector, selected_size);
        RETURN_IF_ERROR(agg_group_by_columns[i]->accept_mutable(&appender));
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SortedStreamingAggregator::pull_eos_chunk() {
    if (_last_state == nullptr) {
        return nullptr;
    }
    bool use_intermediate = _use_intermediate_as_output();
    auto agg_result_columns = _create_agg_result_columns(1, use_intermediate);
    auto group_by_columns = _last_columns;

    TRY_CATCH_BAD_ALLOC(_finalize_to_chunk(_last_state, agg_result_columns));
    _destroy_state(_last_state);
    _last_state = nullptr;
    _last_columns.clear();

    return _build_output_chunk(group_by_columns, agg_result_columns, use_intermediate);
}

} // namespace starrocks
