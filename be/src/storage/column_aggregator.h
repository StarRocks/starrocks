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

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"

namespace starrocks {

class ColumnAggregatorBase {
public:
    ColumnAggregatorBase() : _source_column(nullptr) {}

    virtual ~ColumnAggregatorBase() = default;

    // update input column. |src| is readonly.
    virtual void update_source(const ColumnPtr& src) { _source_column = src; }

    // update output aggregate column
    virtual void update_aggregate(Column* agg) { _aggregate_column = agg; }

    virtual void aggregate_keys(int start, int nums, const uint32* selective_index) {}

    virtual void aggregate_values(int start, int nums, const uint32* aggregate_loops, bool previous_neq) {}

    virtual void finalize() { _aggregate_column = nullptr; }

public:
    ColumnPtr _source_column;
    Column* _aggregate_column{nullptr};
};

template <typename ColumnType>
class KeyColumnAggregator final : public ColumnAggregatorBase {
    void aggregate_keys(int start, int nums, const uint32* selective_index) override {
        _aggregate_column->append_selective(*_source_column, selective_index, 0, nums);
    }
};

class ValueColumnAggregatorBase : public ColumnAggregatorBase {
public:
    ~ValueColumnAggregatorBase() override = default;

    virtual void reset() = 0;

    virtual void append_data(Column* agg) = 0;

    // |data| is readonly.
    virtual void aggregate_impl(int row, const ColumnPtr& data) = 0;

    // |data| is readonly.
    virtual void aggregate_batch_impl(int start, int end, const ColumnPtr& data) = 0;

    virtual bool need_deep_copy() const { return false; };
};

using ColumnAggregatorPtr = std::unique_ptr<ColumnAggregatorBase>;
using ValueColumnAggregatorPtr = std::unique_ptr<ValueColumnAggregatorBase>;

template <typename ColumnType, typename StateType>
class ValueColumnAggregator : public ValueColumnAggregatorBase {
public:
    void update_aggregate(Column* agg) override {
        down_cast<ColumnType*>(agg);
        _aggregate_column = agg;
        reset();
    }

    void aggregate_values(int start, int nums, const uint32* aggregate_loops, bool previous_neq) override {
        if (nums <= 0) {
            return;
        }

        // if different with last row in previous chunk
        if (previous_neq) {
            append_data(_aggregate_column);
            reset();
        }

        for (int i = 0; i < nums - 1; ++i) {
            aggregate_batch_impl(start, start + aggregate_loops[i], _source_column);
            append_data(_aggregate_column);

            start += aggregate_loops[i];
            reset();
        }

        CHECK(nums > 0);
        // last row just aggregate, not finalize
        int end = start + aggregate_loops[nums - 1];
        int size = end - start;
        if (need_deep_copy() && end == _source_column->size()) {
            // copy the last rows of same key to prevent to be overwritten or reset by get_next in aggregate iterator.
            ColumnPtr column = _source_column->clone_empty();
            column->append(*_source_column, start, size);
            aggregate_batch_impl(0, size, column);
        } else {
            aggregate_batch_impl(start, start + aggregate_loops[nums - 1], _source_column);
        }
    }

    void finalize() override {
        append_data(_aggregate_column);
        _aggregate_column = nullptr;
    }

    StateType& data() { return _data; }

    void reset() override { this->data() = StateType{}; }

private:
    StateType _data{};
};

class ValueNullableColumnAggregator final : public ValueColumnAggregatorBase {
public:
    explicit ValueNullableColumnAggregator(ValueColumnAggregatorPtr child) : _child(std::move(child)) {}

    void update_source(const ColumnPtr& src) override {
        _source_column = src;

        auto* nullable = down_cast<NullableColumn*>(src.get());
        _child->update_source(nullable->data_column());

        _source_nulls_data = nullable->null_column_data().data();
    }

    void update_aggregate(Column* agg) override {
        _aggregate_column = agg;

        auto* n = down_cast<NullableColumn*>(agg);
        _child->update_aggregate(n->data_column().get());

        _aggregate_nulls = down_cast<NullColumn*>(n->null_column().get());
        reset();
    }

    void aggregate_values(int start, int nums, const uint32* aggregate_loops, bool previous_neq) override {
        if (nums <= 0) {
            return;
        }

        if (previous_neq) {
            _append_data();
            reset();
        }

        size_t row_nums = 0;
        for (int i = 0; i < nums; ++i) {
            row_nums += aggregate_loops[i];
        }

        size_t zeros = SIMD::count_zero(_source_nulls_data + start, row_nums);

        if (zeros == 0) {
            // all null
            for (int i = 0; i < nums - 1; ++i) {
                _row_is_null &= 1u;
                _append_data();
                start += aggregate_loops[i];
                reset();
            }

            CHECK(nums >= 1);
            _row_is_null &= 1u;
        } else if (zeros == row_nums) {
            // all not null
            for (int i = 0; i < nums - 1; ++i) {
                _row_is_null &= 0u;
                _child->aggregate_batch_impl(start, start + implicit_cast<int>(aggregate_loops[i]),
                                             _child->_source_column);
                _append_data();
                start += aggregate_loops[i];
                reset();
            }

            CHECK(nums >= 1);

            _row_is_null &= 0u;
            int end = start + implicit_cast<int>(aggregate_loops[nums - 1]);
            int size = end - start;
            if (_child->need_deep_copy() && end == _child->_source_column->size()) {
                // copy the last rows of same key to prevent to be overwritten or reset by get_next in aggregate iterator.
                ColumnPtr column = _child->_source_column->clone_empty();
                column->append(*_child->_source_column, start, size);
                _child->aggregate_batch_impl(0, size, column);
            } else {
                _child->aggregate_batch_impl(start, end, _child->_source_column);
            }
        } else {
            for (int i = 0; i < nums - 1; ++i) {
                for (int j = start; j < start + aggregate_loops[i]; ++j) {
                    if (_source_nulls_data[j] != 1) {
                        _row_is_null &= 0u;
                        _child->aggregate_impl(j, _child->_source_column);
                    }
                }

                _append_data();
                start += aggregate_loops[i];
                reset();
            }

            CHECK(nums >= 1);
            int end = start + aggregate_loops[nums - 1];
            int size = end - start;

            if (_child->need_deep_copy() && end == _child->_source_column->size()) {
                // copy the last rows of same key to prevent to be overwritten or reset by get_next in aggregate iterator.
                ColumnPtr column = _child->_source_column->clone_empty();
                column->append(*_child->_source_column, start, size);
                for (int j = 0; j < size; ++j) {
                    if (_source_nulls_data[start + j] != 1) {
                        _row_is_null &= 0u;
                        _child->aggregate_impl(j, column);
                    }
                }
            } else {
                for (int j = start; j < end; ++j) {
                    if (_source_nulls_data[j] != 1) {
                        _row_is_null &= 0u;
                        _child->aggregate_impl(j, _child->_source_column);
                    }
                }
            }
        }
    }

    void finalize() override {
        _child->finalize();
        _aggregate_nulls->append(_row_is_null);
        down_cast<NullableColumn*>(_aggregate_column)->set_has_null(SIMD::count_nonzero(_aggregate_nulls->get_data()));

        _aggregate_nulls = nullptr;
        _aggregate_column = nullptr;
    }

    void reset() override {
        _row_is_null = 1;
        _child->reset();
    }

    void append_data(Column* agg) override {
        LOG(FATAL) << "append_data is not implemented in ValueNullableColumnAggregator";
    }

    void aggregate_impl(int row, const ColumnPtr& data) override {
        LOG(FATAL) << "aggregate_impl is not implemented in ValueNullableColumnAggregator";
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& data) override {
        LOG(FATAL) << "aggregate_batch_impl is not implemented in ValueNullableColumnAggregator";
    }

private:
    void _append_data() {
        _aggregate_nulls->append(_row_is_null);
        _child->append_data(_child->_aggregate_column);
    }

    ValueColumnAggregatorPtr _child;

    NullColumn* _aggregate_nulls{nullptr};

    uint8_t* _source_nulls_data{nullptr};

    uint8_t _row_is_null{0};
};

} // namespace starrocks
