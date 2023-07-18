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

#include "column/column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"

namespace starrocks {

class ConstColumn final : public ColumnFactory<Column, ConstColumn> {
    friend class ColumnFactory<Column, ConstColumn>;

public:
    explicit ConstColumn(ColumnPtr data_column);
    ConstColumn(ColumnPtr data_column, size_t size);

    ConstColumn(const ConstColumn& rhs) : _data(rhs._data->clone_shared()), _size(rhs._size) {}

    ConstColumn(ConstColumn&& rhs) noexcept : _data(std::move(rhs._data)), _size(rhs._size) {}

    ConstColumn& operator=(const ConstColumn& rhs) {
        ConstColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    ConstColumn& operator=(ConstColumn&& rhs) noexcept {
        ConstColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~ConstColumn() override = default;

    bool is_nullable() const override { return _data->is_nullable(); }

    bool is_null(size_t index) const override { return _data->is_null(0); }

    bool only_null() const override { return _data->is_null(0); }

    bool has_null() const override { return _data->has_null(); }

    bool is_constant() const override { return true; }

    const uint8_t* raw_data() const override { return _data->raw_data(); }

    uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data->mutable_raw_data()); }

    size_t size() const override { return _size; }

    size_t capacity() const override { return UINT32_MAX; }

    size_t type_size() const override { return _data->type_size(); }

    size_t byte_size() const override { return _data->byte_size() + sizeof(_size); }

    // const column has only one element
    size_t byte_size(size_t from, size_t size) const override { return byte_size(); }

    size_t byte_size(size_t idx) const override { return _data->byte_size(0); }

    void reserve(size_t n) override {}

    void resize(size_t n) override { _size = n; }

    // This method resize the underlying data column,
    // Because when sometimes(agg functions), we want to handle const column as normal data column
    void assign(size_t n, size_t idx) override {
        _size = n;
        _data->assign(n, 0);
    }

    void remove_first_n_values(size_t count) override { _size = std::max<size_t>(1, _size - count); }

    void append_datum(const Datum& datum) override {
        if (_size == 0) {
            _data->resize(0);
            _data->append_datum(datum);
        }
        _size++;
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) override;

    ColumnPtr replicate(const std::vector<uint32_t>& offsets) override;

    bool append_nulls(size_t count) override {
        if (_data->is_nullable()) {
            bool ok = true;
            if (_size == 0) {
                ok = _data->append_nulls(1);
            }
            _size += ok;
            return ok;
        } else {
            return false;
        }
    }

    bool append_strings(const Buffer<Slice>& strs) override { return false; }

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override {
        if (_size == 0 && count > 0) {
            _data->append_value_multiple_times(value, 1);
        }
    }

    void append_default() override { _size++; }

    void append_default(size_t count) override { _size += count; }

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t serialize(size_t idx, uint8_t* pos) override { return _data->serialize(0, pos); }

    uint32_t serialize_default(uint8_t* pos) override { return _data->serialize_default(pos); }

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override {
        for (size_t i = 0; i < chunk_size; ++i) {
            slice_sizes[i] += _data->serialize(0, dst + i * max_one_row_size + slice_sizes[i]);
        }
    }

    const uint8_t* deserialize_and_append(const uint8_t* pos) override {
        ++_size;
        if (_data->empty()) {
            return _data->deserialize_and_append(pos);
        }
        // Note: we must update the pos
        return pos + _data->serialize_size(0);
    }

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override {
        _size += chunk_size;
        if (_data->empty()) {
            _data->deserialize_and_append((uint8_t*)srcs[0].data);
        }
        uint32_t serialize_size = _data->serialize_size(0);
        // Note: we must update the pos
        for (size_t i = 0; i < chunk_size; i++) {
            srcs[0].data = srcs[0].data + serialize_size;
        }
    }

    uint32_t max_one_element_serialize_size() const override { return _data->max_one_element_serialize_size(); }

    uint32_t serialize_size(size_t idx) const override { return _data->serialize_size(0); }

    MutableColumnPtr clone_empty() const override { return create_mutable(_data->clone_empty(), 0); }

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;

    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override { _data->put_mysql_row_buffer(buf, 0); }

    std::string get_name() const override { return "const-" + _data->get_name(); }

    ColumnPtr* mutable_data_column() { return &_data; }

    const ColumnPtr& data_column() const { return _data; }

    Datum get(size_t n __attribute__((unused))) const override { return _data->get(0); }

    size_t memory_usage() const override { return _data->memory_usage() + sizeof(size_t); }

    size_t container_memory_usage() const override { return _data->container_memory_usage(); }

    size_t reference_memory_usage() const override { return _data->reference_memory_usage(); }

    size_t reference_memory_usage(size_t from, size_t size) const override {
        // const column has only one element
        return size == 0 ? 0 : reference_memory_usage();
    }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<ConstColumn&>(rhs);
        _data->swap_column(*r._data);
        std::swap(_delete_state, r._delete_state);
        std::swap(_size, r._size);
    }

    void reset_column() override {
        Column::reset_column();
        _data->reset_column();
        _size = 0;
    }

    std::string debug_item(size_t idx) const override {
        std::stringstream ss;
        ss << "CONST: " << _data->debug_item(0);
        return ss.str();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "CONST: " << _data->debug_item(0) << " Size : " << _size;
        return ss.str();
    }

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        RETURN_IF_UNLIKELY(_data->capacity_limit_reached(msg), true);
        if (_size > Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("Row count of const column reach limit: " + std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        }
        return false;
    }

    void check_or_die() const override;

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override { return _data->has_large_column(); }

private:
    ColumnPtr _data;
    uint64_t _size;
};

} // namespace starrocks
