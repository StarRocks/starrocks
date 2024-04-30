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

#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"

namespace starrocks {

    using IndexData = FixedLengthColumn<int32_t>::Container;
    using IndexColumn = FixedLengthColumn<int32_t>;
    using IndexColumnPtr = FixedLengthColumn<int32_t>::Ptr;

    class DictionaryColumn : public ColumnFactory<Column, DictionaryColumn> {
        friend class ColumnFactory<Column, DictionaryColumn>;

    public:
        DictionaryColumn() = default;

        DictionaryColumn(MutableColumnPtr&& data_column, MutableColumnPtr&& index_column);
        DictionaryColumn(ColumnPtr data_column, IndexColumnPtr index_column);

        DictionaryColumn(const DictionaryColumn& rhs)
                : _data_column(rhs._data_column->clone_shared()),
                  _index_column(std::static_pointer_cast<IndexColumn>(rhs._index_column->clone_shared())) {}

        DictionaryColumn(DictionaryColumn&& rhs) noexcept
                : _data_column(std::move(rhs._data_column)),
                _index_column(std::move(rhs._index_column)) {}

        DictionaryColumn& operator=(const DictionaryColumn& rhs) {
            DictionaryColumn tmp(rhs);
            this->swap_column(tmp);
            return *this;
        }

        DictionaryColumn& operator=(DictionaryColumn&& rhs) noexcept {
            DictionaryColumn tmp(std::move(rhs));
            this->swap_column(tmp);
            return *this;
        }

        ~DictionaryColumn() override = default;

        void fill_default(const Filter& filter) override {}

        bool is_dictionary() const override { return true; }

        const uint8_t* raw_data() const override { return _data_column->raw_data(); }

        uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data_column->mutable_raw_data()); }

        const ColumnPtr& data_column() const { return _data_column; }
        const IndexColumnPtr& index_column() const { return _index_column; }

        size_t size() const override {
            return _index_column->size();
        }

        size_t capacity() const override { return _index_column->capacity(); }

        size_t type_size() const override { return _data_column->type_size(); }

        size_t byte_size() const override { return byte_size(0, size()); }

        size_t byte_size(size_t from, size_t size) const override {
            DCHECK_LE(from + size, this->size()) << "Range error";
            return _data_column->byte_size() + _index_column->byte_size(from, size);
        }

        size_t byte_size(size_t idx) const override { return _data_column->byte_size() + sizeof(uint32_t); }

        void reserve(size_t n) override {
            _index_column->reserve(n);
        }

        void resize(size_t n) override {
            _index_column->resize(n);
        }

        void resize_uninitialized(size_t n) override {
            _index_column->resize_uninitialized(n);
        }

        void assign(size_t n, size_t idx) override {
            _index_column->assign(n, idx);
        }

        void remove_first_n_values(size_t count) override;

        void append_datum(const Datum& datum) override;

        void append(const Column& src, size_t offset, size_t count) override;

        void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

        void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

        [[nodiscard]] bool append_nulls(size_t count __attribute__((unused))) override { return false; }

        StatusOr<ColumnPtr> upgrade_if_overflow() override;

        StatusOr<ColumnPtr> downgrade() override;

        bool has_large_column() const override { return _data_column->has_large_column(); }

        [[nodiscard]] bool append_strings(const Buffer<Slice>& strs __attribute__((unused))) override { return false; }

        [[nodiscard]] bool append_strings_overflow(const Buffer<Slice>& strs __attribute__((unused)),
                                                   size_t max_length __attribute__((unused))) override { return false; }

        [[nodiscard]] bool append_continuous_strings(const Buffer<Slice>& strs __attribute__((unused))) override {
            return false;
        }

        bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override {
            return false;
        }

        size_t append_numbers(const void* buff, size_t length) override { return -1; }

        void append_value_multiple_times(const void* value, size_t count) override {}

        void append_default() override {}

        void append_default(size_t count) override {}

        void update_rows(const Column& src, const uint32_t* indexes) override {}

        uint32_t max_one_element_serialize_size() const override {
            return _data_column->max_one_element_serialize_size();
        }

        uint32_t serialize(size_t idx, uint8_t* pos) override;

        uint32_t serialize_default(uint8_t* pos) override;

        void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                             uint32_t max_one_row_size) override;

        const uint8_t* deserialize_and_append(const uint8_t* pos) override;

        void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

        uint32_t serialize_size(size_t idx) const override {
            DCHECK(_index_column->get_data()[idx] < _data_column->size());
            return _data_column->serialize_size(_index_column->get_data()[idx]);
        }

        MutableColumnPtr clone_empty() const override {
            return _data_column->clone_empty();
        }

        size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                           size_t count) override;

        size_t filter_range(const Filter& filter, size_t from, size_t to) override;

        int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

        int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;

        void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

        void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

        int64_t xor_checksum(uint32_t from, uint32_t to) const override;

        void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

        std::string get_name() const override { return "dictionary-" + _data_column->get_name(); }

        Datum get(size_t n) const override {
            DCHECK(n < _index_column->size());
            DCHECK(_index_column->get_data()[n] < _data_column->size());
            return _data_column->get(_index_column->get_data()[n]);
        }

        bool set_null(size_t idx) override {
            return false;
        }

        ColumnPtr replicate(const std::vector<uint32_t>& offsets) override;

        size_t memory_usage() const override {
            return _data_column->memory_usage() + _index_column->memory_usage();
        }

        size_t container_memory_usage() const override {
            return _data_column->container_memory_usage() + _index_column->container_memory_usage();
        }

        size_t reference_memory_usage(size_t from, size_t size) const override {
            DCHECK(false) << "Dictionary column not support reference_memory_usage";
            return _data_column->reference_memory_usage() + _index_column->reference_memory_usage(from, size);
        }

        void swap_column(Column& rhs) override {
            auto& r = down_cast<DictionaryColumn&>(rhs);
            _data_column->swap_column(*r._data_column);
            _index_column->swap_column(*r._index_column);
            std::swap(_delete_state, r._delete_state);
        }

        void reset_column() override {
            Column::reset_column();
            _data_column->reset_column();
            _index_column->reset_column();
        }

        std::string debug_item(size_t idx) const override {
            std::stringstream ss;
            if (_index_column->get_data()[idx]) {
                ss << "NULL";
            } else {
                ss << _data_column->debug_item(_index_column->get_data()[idx]);
            }
            return ss.str();
        }

        std::string debug_string() const override {
            std::stringstream ss;
            ss << "[";
            size_t size = _index_column->size();
            for (size_t i = 0; i + 1 < size; ++i) {
                ss << debug_item(i) << ", ";
            }
            if (size > 0) {
                ss << debug_item(size - 1);
            }
            ss << "]";
            return ss.str();
        }

        bool capacity_limit_reached(std::string* msg = nullptr) const override {
            return _data_column->capacity_limit_reached(msg) || _index_column->capacity_limit_reached(msg);
        }

        void check_or_die() const override;

    protected:
        ColumnPtr _data_column;
        IndexColumnPtr _index_column;
    };

} // namespace starrocks
