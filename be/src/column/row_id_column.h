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
#include <stdexcept>

#include "column/column.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {
class RowIdColumn final : public CowFactory<ColumnFactory<Column, RowIdColumn>, RowIdColumn> {
    friend class CowFactory<ColumnFactory<Column, RowIdColumn>, RowIdColumn>;

public:
    using ValueType = DatumRowId;
    RowIdColumn();
    RowIdColumn(size_t n);

    RowIdColumn(UInt32Column::Ptr be_ids, UInt32Column::Ptr seg_ids, UInt32Column::Ptr ord_ids);

    RowIdColumn(const RowIdColumn& rhs);

    RowIdColumn(RowIdColumn&& rhs) noexcept;

    RowIdColumn& operator=(const RowIdColumn& rhs) {
        RowIdColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    RowIdColumn& operator=(RowIdColumn&& rhs) noexcept {
        RowIdColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }
    ~RowIdColumn() override = default;
    bool is_global_row_id() const override { return true; }

    const uint8_t* raw_data() const override {
        DCHECK(false) << "RowIdColumn::raw_data() is not supported";
        throw std::runtime_error("RowIdColumn::raw_data() is not supported");
    }

    uint8_t* mutable_raw_data() override {
        DCHECK(false) << "RowIdColumn::mutable_raw_data() is not supported";
        throw std::runtime_error("RowIdColumn::mutable_raw_data() is not supported");
    }

    size_t size() const override { return _be_ids->size(); }
    size_t capacity() const override { return _be_ids->capacity(); }
    size_t type_size() const override { return _be_ids->type_size() + _seg_ids->type_size() + _ord_ids->type_size(); }
    size_t byte_size() const override { return _be_ids->byte_size() + _seg_ids->byte_size() + _ord_ids->byte_size(); }
    size_t byte_size(size_t from, size_t count) const override {
        return _be_ids->byte_size(from, count) + _seg_ids->byte_size(from, count) + _ord_ids->byte_size(from, count);
    }
    size_t byte_size(size_t idx) const override {
        return _be_ids->byte_size(idx) + _seg_ids->byte_size(idx) + _ord_ids->byte_size(idx);
    }

    void reserve(size_t n) override {
        _be_ids->reserve(n);
        _seg_ids->reserve(n);
        _ord_ids->reserve(n);
    }
    void resize(size_t n) override {
        _be_ids->resize(n);
        _seg_ids->resize(n);
        _ord_ids->resize(n);
    }
    void assign(size_t n, size_t idx) override;

    void append_datum(const Datum& datum) override;
    void append(const Column& src, size_t offset, size_t count) override;
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;
    void append_value_multiple_times(const Column& src, uint32_t idx, uint32_t count) override;
    bool append_nulls(size_t count) override { return false; }

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;
    void append_default() override {
        _be_ids->append_default();
        _seg_ids->append_default();
        _ord_ids->append_default();
    }
    void append_default(size_t count) override {
        _be_ids->append_default(count);
        _seg_ids->append_default(count);
        _ord_ids->append_default(count);
    }
    void fill_default(const Filter& filter) override {
        DCHECK(false) << "RowIdColumn::fill_default() is not supported";
    }
    void update_rows(const Column& src, const uint32_t* indexes) override {
        DCHECK(false) << "RowIdColumn::update_rows() is not supported";
    }
    void remove_first_n_values(size_t count) override;
    uint32_t max_one_element_serialize_size() const override;
    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_default(uint8_t* pos) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sized, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    uint32_t serialize_size(size_t idx) const override {
        return _be_ids->serialize_size(idx) + _seg_ids->serialize_size(idx) + _ord_ids->serialize_size(idx);
    }
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const override;

    void fnv_hash_at(uint32_t* seed, uint32_t idx) const override {
        DCHECK(false) << "RowIdColumn::fnv_hash_at() is not supported";
        throw std::runtime_error("RowIdColumn::fnv_hash_at() is not supported");
    }
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {
        DCHECK(false) << "RowIdColumn::fnv_hash() is not supported";
        throw std::runtime_error("RowIdColumn::fnv_hash() is not supported");
    }

    void crc32_hash_at(uint32_t* seed, uint32_t idx) const override {
        DCHECK(false) << "RowIdColumn::crc32_hash_at() is not supported";
        throw std::runtime_error("RowIdColumn::crc32_hash_at() is not supported");
    }
    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {
        DCHECK(false) << "RowIdColumn::crc32_hash() is not supported";
        throw std::runtime_error("RowIdColumn::crc32_hash() is not supported");
    }

    int64_t xor_checksum(uint32_t from, uint32_t to) const override {
        DCHECK(false) << "RowIdColumn::xor_checksum() is not supported";
        throw std::runtime_error("RowIdColumn::xor_checksum() is not supported");
    }

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override {
        DCHECK(false) << "RowIdColumn::put_mysql_row_buffer() is not supported";
        throw std::runtime_error("RowIdColumn::put_mysql_row_buffer() is not supported");
    }

    StatusOr<ColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    std::string get_name() const override { return "row-id"; }

    Datum get(size_t idx) const override;

    bool set_null(size_t idx) override { return false; }

    size_t memory_usage() const override {
        return _be_ids->memory_usage() + _seg_ids->memory_usage() + _ord_ids->memory_usage();
    }

    size_t container_memory_usage() const override {
        return _be_ids->container_memory_usage() + _seg_ids->container_memory_usage() +
               _ord_ids->container_memory_usage();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override;

    void reset_column() override;

    const Columns columns() const { return Columns{_be_ids, _seg_ids, _ord_ids}; }
    Columns columns() { return Columns{_be_ids, _seg_ids, _ord_ids}; }

    const UInt32Column::Ptr& be_ids_column() const { return _be_ids; }
    UInt32Column::Ptr& be_ids_column() { return _be_ids; }
    const UInt32Column::Ptr& seg_ids_column() const { return _seg_ids; }
    UInt32Column::Ptr& seg_ids_column() { return _seg_ids; }
    const UInt32Column::Ptr& ord_ids_column() const { return _ord_ids; }
    UInt32Column::Ptr& ord_ids_column() { return _ord_ids; }

    bool is_nullable() const override { return false; }

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

    Status capacity_limit_reached() const override {
        RETURN_IF_ERROR(_be_ids->capacity_limit_reached());
        RETURN_IF_ERROR(_seg_ids->capacity_limit_reached());
        return _ord_ids->capacity_limit_reached();
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override { return nullptr; }

    StatusOr<ColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return false; }

    void check_or_die() const override;

    Status unfold_const_children(const starrocks::TypeDescriptor& type) override {
        return Status::NotSupported("RowIdColumn::unfold_const_children() is not supported");
    }
    void mutate_each_subcolumn() override {
        _be_ids = UInt32Column::static_pointer_cast((std::move(*_be_ids)).mutate());
        _seg_ids = UInt32Column::static_pointer_cast((std::move(*_seg_ids)).mutate());
        _ord_ids = UInt32Column::static_pointer_cast((std::move(*_ord_ids)).mutate());
    }

private:
    // @TODO(silverbullet233): if this column is generated by local be, we don't need be_ids
    UInt32Column::Ptr _be_ids;
    UInt32Column::Ptr _seg_ids;
    UInt32Column::Ptr _ord_ids;
};
} // namespace starrocks