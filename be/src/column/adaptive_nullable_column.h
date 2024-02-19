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

#include <fmt/format.h>

#include "column/column.h"
#include "column/nullable_column.h"
#include "simd/simd.h"

namespace starrocks {

// NullableColumn has two columns: data column and null column. Based on the data, we classify null column into four types:
// (1) All data is null: data column is all default value, and null column is all 1
// (2) All data has default value(not null): data column is all default value, and null column is all 0
// (3) All data has different value(not null): data column has different value, and null column is all 0
// (4) Some data is null, some data has different value: data column has different value, and null column has 0 and 1
// For type (1) (2) (3), we can compress the nullable column, no need to materialize the data and null column.
// We can set a flag to indicate that data column is all default value and null column is all 0/1, especially for
// type (1) and (2), both data column and null column can be compressed, for type (3), we can only compress null column.
//
// AdaptiveNullableColumn is designed to handle this issue. It has five state:  kUninitialized, kNull, kConstant
// kNotConstant and kMaterialized.
// (1) kUninitialized:         AdaptiveNullableColumn is in init stat.
// (2) kNull:                  AdaptiveNullableColumn is all null. (data column and null column is compressed)
// (3) kConstant:              AdaptiveNullableColumn is all default value (not null) (data column and null column is compressed)
// (4) kNotConstant:           AdaptiveNullableColumn has different value (not null) (null column is compressed)
// (5) kMaterialized:          AdaptiveNullableColumn has null and differnt value
// The state transfer is as following:
// kUninitialized ------------->  kNull --------------
//                     |                              ｜
//                     -------->  kConstant -----------
//                     |                              ｜
//                     -------->  kNotConstant --------
//                     |                              ｜
//                     ----------------------------------------------------->   kMaterialized
// In the beginning, AdaptiveNullableColumn is in kUninitialized stat, then it will transfer to kNull,
// kConstant, kNotConstant and kMaterialized after receiving some data. In kMaterialized state,
// both data column and null column are materialized and the behaviour is same as nullable column.
// AdaptiveNullableColumn can optimize both CPU and memory, for example, if the data is all null,
// when append null data to AdaptiveNullableColumn, we only need increase _size in AdaptiveNullableColumn,
// no need to append default to data column and 1 to null column.
// At the end of AdaptiveNullableColumn, you need to call materialized_nullable() if you want to use the data column and null column.
class AdaptiveNullableColumn final : public ColumnFactory<NullableColumn, AdaptiveNullableColumn, Column> {
public:
    using SuperClass = ColumnFactory<NullableColumn, AdaptiveNullableColumn, Column>;
    enum class State {
        kUninitialized,
        kNull,
        kConstant,
        kNotConstant,
        kMaterialized,
    };

    AdaptiveNullableColumn() = default;

    explicit AdaptiveNullableColumn(MutableColumnPtr&& data_column, MutableColumnPtr&& null_column)
            : SuperClass(std::move(data_column), std::move(null_column)) {
        DCHECK_EQ(_null_column->size(), _data_column->size());
        if (_data_column->size() == 0) {
            _state = State::kUninitialized;
            _size = 0;
        } else {
            _state = State::kMaterialized;
        }
    }

    explicit AdaptiveNullableColumn(ColumnPtr data_column, NullColumnPtr null_column)
            : SuperClass(data_column, null_column) {
        DCHECK_EQ(_null_column->size(), _data_column->size());
        if (_data_column->size() == 0) {
            _state = State::kUninitialized;
            _size = 0;
        } else {
            _state = State::kMaterialized;
        }
    }

    State state() { return _state; }

    AdaptiveNullableColumn(const AdaptiveNullableColumn& rhs) { CHECK(false) << "unimplemented"; }

    AdaptiveNullableColumn(AdaptiveNullableColumn&& rhs) { CHECK(false) << "unimplemented"; }

    AdaptiveNullableColumn& operator=(const AdaptiveNullableColumn& rhs) {
        AdaptiveNullableColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    AdaptiveNullableColumn& operator=(AdaptiveNullableColumn&& rhs) {
        AdaptiveNullableColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~AdaptiveNullableColumn() override = default;

    void swap_column(Column& rhs) override {
        auto& r = down_cast<AdaptiveNullableColumn&>(rhs);
        NullableColumn::swap_column(r);
        std::swap(_state, r._state);
        std::swap(_size, r._size);
    }

    void reset_column() override {
        materialized_nullable();
        NullableColumn::reset_column();
        _state = State::kUninitialized;
        _size = 0;
    }

    bool has_null() const override {
        switch (_state) {
        case State::kUninitialized: {
            return false;
        }
        case State::kNull: {
            return true;
        }
        case State::kConstant:
        case State::kNotConstant: {
            return false;
        }
        case State::kMaterialized: {
            return _has_null;
        }
        default: {
            __builtin_unreachable();
        }
        }
    }

    void set_has_null(bool has_null) {
        materialized_nullable();
        _has_null = _has_null | has_null;
    }

    // Update null element to default value
    void fill_null_with_default();

    void update_has_null();

    bool is_null(size_t index) const override {
        switch (_state) {
        case State::kUninitialized: {
            return false;
        }
        case State::kNull: {
            return true;
        }
        case State::kConstant:
        case State::kNotConstant: {
            return false;
        }
        case State::kMaterialized: {
            return _has_null && _null_column->get_data()[index];
        }
        default: {
            __builtin_unreachable();
        }
        }
        return true;
    }

    const uint8_t* raw_data() const override {
        materialized_nullable();
        return _data_column->raw_data();
    }

    uint8_t* mutable_raw_data() override {
        materialized_nullable();
        return reinterpret_cast<uint8_t*>(_data_column->mutable_raw_data());
    }

    size_t size() const override {
        if (_state == State::kMaterialized) {
            DCHECK_EQ(_data_column->size(), _null_column->size());
            return _data_column->size();
        }
        return _size;
    }

    size_t capacity() const override {
        materialized_nullable();
        return _data_column->capacity();
    }

    size_t type_size() const override { return _data_column->type_size() + _null_column->type_size(); }

    size_t byte_size() const override { return byte_size(0, size()); }

    size_t byte_size(size_t from, size_t size) const override {
        materialized_nullable();
        DCHECK_LE(from + size, this->size()) << "Range error";
        return _data_column->byte_size(from, size) + _null_column->byte_size(from, size);
    }

    size_t byte_size(size_t idx) const override {
        materialized_nullable();
        return _data_column->byte_size(idx) + sizeof(bool);
    }

    void reserve(size_t n) override {
        _data_column->reserve(n);
        _null_column->reserve(n);
    }

    void resize(size_t n) override {
        switch (_state) {
        case State::kUninitialized: {
            DCHECK(n == 0);
            break;
        }
        case State::kNull:
        case State::kConstant: {
            _size = n;
            break;
        }
        case State::kNotConstant: {
            _data_column->resize(n);
            _size = n;
            break;
        }
        case State::kMaterialized: {
            _data_column->resize(n);
            _null_column->resize(n);
            _has_null = SIMD::contain_nonzero(_null_column->get_data(), 0);
            break;
        }
        default: {
            throw std::runtime_error("unknown state");
        }
        }
    }

    void resize_uninitialized(size_t n) override {
        materialized_nullable();
        _data_column->resize_uninitialized(n);
        _null_column->resize_uninitialized(n);
    }

    void assign(size_t n, size_t idx) override {
        materialized_nullable();
        _data_column->assign(n, idx);
        _null_column->assign(n, idx);
    }

    void remove_first_n_values(size_t count) override {
        materialized_nullable();
        _data_column->remove_first_n_values(count);
        _null_column->remove_first_n_values(count);
    }

    void append_datum(const Datum& datum) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override;

    StatusOr<ColumnPtr> upgrade_if_overflow() override {
        materialized_nullable();
        if (_null_column->capacity_limit_reached()) {
            return Status::InternalError("Size of NullableColumn exceed the limit");
        }

        return upgrade_helper_func(&_data_column);
    }

    StatusOr<ColumnPtr> downgrade() override {
        materialized_nullable();
        return downgrade_helper_func(&_data_column);
    }

    bool has_large_column() const override {
        materialized_nullable();
        return _data_column->has_large_column();
    }

    bool append_strings(const Buffer<Slice>& strs) override;

    bool append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) override;

    bool append_continuous_strings(const Buffer<Slice>& strs) override;

    bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override;

    size_t append_numbers(const void* buff, size_t length) override;

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override { append_nulls(1); }

    void append_default_not_null_value() {
        switch (_state) {
        case State::kUninitialized: {
            _state = State::kConstant;
            _size = 1;
            break;
        }
        case State::kConstant: {
            _size++;
            break;
        }
        case State::kMaterialized: {
            _data_column->append_default();
            _null_column->append(0);
            break;
        }
        default: {
            materialized_nullable();
            _data_column->append_default();
            _null_column->append(0);
            break;
        }
        }
    }

    void append_default(size_t count) override { append_nulls(count); }

    Status update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t max_one_element_serialize_size() const override {
        materialized_nullable();
        return sizeof(bool) + _data_column->max_one_element_serialize_size();
    }

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override {
        materialized_nullable();
        if (_null_column->get_data()[idx]) {
            return sizeof(uint8_t);
        }
        return sizeof(uint8_t) + _data_column->serialize_size(idx);
    }

    MutableColumnPtr clone_empty() const override {
        return NullableColumn::create_mutable(_data_column->clone_empty(), _null_column->clone_empty());
    }

    size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                       size_t count) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    const ColumnPtr& begin_append_not_default_value() const {
        switch (_state) {
        case State::kUninitialized: {
            _state = State::kNotConstant;
            break;
        }
        case State::kNotConstant:
        case State::kMaterialized: {
            break;
        }
        default: {
            materialized_nullable();
            break;
        }
        }
        return _data_column;
    }

    Column* mutable_begin_append_not_default_value() const {
        switch (_state) {
        case State::kUninitialized: {
            _state = State::kNotConstant;
            break;
        }
        case State::kNotConstant:
        case State::kMaterialized: {
            break;
        }
        default: {
            materialized_nullable();
            break;
        }
        }
        return _data_column.get();
    }

    void finish_append_one_not_default_value() const {
        switch (_state) {
        case State::kNotConstant: {
            _size++;
            break;
        }
        case State::kMaterialized: {
            null_column_data().emplace_back(0);
            break;
        }
        default: {
            throw std::runtime_error(
                    "adaptive nullable column's state cann't be : " + std::to_string(static_cast<int>(_state)) +
                    " after it finishes appending one not default value");
        }
        }
    }

    ColumnPtr& materialized_raw_data_column() {
        materialized_nullable();
        return _data_column;
    }

    const NullColumnPtr& materialized_raw_null_column() const {
        materialized_nullable();
        return _null_column;
    }

    // User may have a nullable column's pointer which points to an adaptive nullable column,
    // then access the adaptive nullable column through the nullable column's api (polymorphic in c++),
    // this is very dangerous for adaptive nullable column, as its nullable column and data column may
    // not materialized. To avoid this, we need to materialized_nullable() it in the immutable_null_column_data() here,
    // however, this may is not user want because once adaptive nullable column materialized,
    // its performance will be degraded to nullable column. Due to the following reason, we add
    // DCHECK(false) here and disable the behaviour.
    const NullData& immutable_null_column_data() const {
        DCHECK(false);
        materialized_nullable();
        return _null_column->get_data();
    }

    Column* mutable_data_column() {
        DCHECK(false);
        materialized_nullable();
        return _data_column.get();
    }

    NullColumn* mutable_null_column() {
        DCHECK(false);
        materialized_nullable();
        return _null_column.get();
    }

    const Column& data_column_ref() const {
        DCHECK(false);
        materialized_nullable();
        return *_data_column;
    }

    const ColumnPtr& data_column() const {
        DCHECK(false);
        materialized_nullable();
        return _data_column;
    }

    ColumnPtr& data_column() {
        DCHECK(false);
        materialized_nullable();
        return _data_column;
    }

    const NullColumn& null_column_ref() const {
        DCHECK(false);
        materialized_nullable();
        return *_null_column;
    }

    const NullColumnPtr& null_column() const {
        DCHECK(false);
        materialized_nullable();
        return _null_column;
    }

    size_t null_count() const;
    size_t null_count(size_t offset, size_t count) const;

    Datum get(size_t n) const override {
        switch (_state) {
        case State::kUninitialized:
        case State::kNull: {
            return {};
        }
        default: {
            materialized_nullable();
            return NullableColumn::get(n);
        }
        }
    }

    bool set_null(size_t idx) override {
        switch (_state) {
        case State::kUninitialized:
        case State::kNull: {
            return true;
        }
        default: {
            materialized_nullable();
            return NullableColumn::set_null(idx);
        }
        }
    }

    ColumnPtr replicate(const std::vector<uint32_t>& offsets) override {
        materialized_nullable();
        return NullableColumn::replicate(offsets);
    }

    size_t memory_usage() const override {
        materialized_nullable();
        return _data_column->memory_usage() + _null_column->memory_usage() + sizeof(bool) + sizeof(State) +
               sizeof(size_t);
    }

    size_t container_memory_usage() const override {
        materialized_nullable();
        return _data_column->container_memory_usage() + _null_column->container_memory_usage();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override {
        materialized_nullable();
        return NullableColumn::reference_memory_usage(from, size);
    }

    std::string debug_item(size_t idx) const override {
        materialized_nullable();
        return NullableColumn::debug_item(idx);
    }

    std::string debug_string() const override {
        materialized_nullable();
        return NullableColumn::debug_string();
    }

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        materialized_nullable();
        return NullableColumn::capacity_limit_reached(msg);
    }

    void check_or_die() const override {
        if (_state == State::kMaterialized) {
            NullableColumn::check_or_die();
        }
    }

    void materialized_nullable() const override {
        if (_state == State::kMaterialized) {
            return;
        }
        if (LIKELY(_size > 0)) {
            switch (_state) {
            case State::kNull: {
                _data_column->append_default(_size);
                null_column_data().insert(null_column_data().end(), _size, 1);
                _has_null = true;
                break;
            }
            case State::kConstant: {
                _data_column->append_default(_size);
                null_column_data().insert(null_column_data().end(), _size, 0);
                break;
            }
            case State::kNotConstant: {
                null_column_data().insert(null_column_data().end(), _size, 0);
                break;
            }
            default: {
                break;
            }
            }
        }
        DCHECK_EQ(_null_column->size(), _data_column->size());
        _state = State::kMaterialized;
    }

private:
    NullData& null_column_data() const { return _null_column->get_data(); }

    mutable State _state;
    mutable size_t _size;
};

} // namespace starrocks
