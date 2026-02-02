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
#include <column/column.h>
#include <column/datum.h>

#include "util/cow.h"

#if defined(__GNUC__) || defined(__clang__)
#define NOT_SUPPORT()                                                                                         \
    do {                                                                                                      \
        throw std::runtime_error(std::string("ColumnView not support method '") + __PRETTY_FUNCTION__ + "'"); \
    } while (0);
#elif defined(_MSC_VER)
#define NOT_SUPPORT()                                                                                 \
    do {                                                                                              \
        throw std::runtime_error(std::string("ColumnView not support method '") + __FUNCSIG__ + "'"); \
    } while (0);
#else
#define NOT_SUPPORT()                                                                              \
    do {                                                                                           \
        throw std::runtime_error(std::string("ColumnView not support method '") + __func__ + "'"); \
    } while (0);
#endif
namespace starrocks {
class ColumnViewBase : public Column {
public:
    using LocationType = uint32_t;
    using Locations = std::vector<LocationType>;

    StatusOr<MutableColumnPtr> upgrade_if_overflow() override { return nullptr; };

    StatusOr<MutableColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return false; }

    size_t size() const override { return _num_rows; }

    size_t container_memory_usage() const override;

    size_t reference_memory_usage(size_t from, size_t size) const override;

    size_t memory_usage() const override;

    void append(const Column& src) override { append(src, 0, src.size()); }
    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    ColumnViewBase(ColumnPtr&& default_column, long concat_rows_limit, long concat_bytes_limit)
            : _default_column(std::move(default_column)),
              _concat_rows_limit(concat_rows_limit),
              _concat_bytes_limit(concat_bytes_limit) {}

    ColumnViewBase(const ColumnViewBase& that)
            : _default_column(that._default_column->clone()),
              _concat_rows_limit(that._concat_rows_limit),
              _concat_bytes_limit(that._concat_bytes_limit),
              _habitats(that._habitats),
              _num_rows(that._num_rows),
              _tasks(that._tasks),
              _habitat_idx(that._habitat_idx),
              _row_idx(that._row_idx),
              _concat_column(that._concat_column) {}

    ColumnViewBase(ColumnViewBase&&) = delete;
    void append_default() override;

    MutableColumnPtr clone_empty() const override { return _default_column->clone_empty(); }

    virtual void append_to(Column& dest_column, const uint32_t* indexes, uint32_t from, uint32_t count) const;

    const uint8_t* raw_data() const override { NOT_SUPPORT(); }

    uint8_t* mutable_raw_data() override { NOT_SUPPORT(); }
    size_t capacity() const override { NOT_SUPPORT(); }
    size_t byte_size() const override { NOT_SUPPORT(); }
    size_t type_size() const override { NOT_SUPPORT(); }
    size_t byte_size(size_t from, size_t size) const override { NOT_SUPPORT(); }
    size_t byte_size(size_t idx) const override { NOT_SUPPORT(); };
    void reserve(size_t n) override { NOT_SUPPORT(); }
    void resize(size_t n) override { NOT_SUPPORT(); }
    void assign(size_t n, size_t idx) override { NOT_SUPPORT(); }
    void append_datum(const Datum& datum) override { NOT_SUPPORT(); }
    void remove_first_n_values(size_t count) override { NOT_SUPPORT(); }
    void fill_default(const Filter& filter) override { NOT_SUPPORT(); }
    void update_rows(const Column& src, const uint32_t* indexes) override { NOT_SUPPORT(); }
    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override { NOT_SUPPORT(); }
    bool append_nulls(size_t count) override { NOT_SUPPORT(); }
    [[nodiscard]] size_t append_numbers(const void* buff, size_t length) override { NOT_SUPPORT(); }
    void append_value_multiple_times(const void* value, size_t count) override { NOT_SUPPORT(); }
    void append_default(size_t count) override { NOT_SUPPORT(); }
    uint32_t max_one_element_serialize_size() const override { NOT_SUPPORT(); }
    uint32_t serialize(size_t idx, uint8_t* pos) const override { NOT_SUPPORT(); }
    uint32_t serialize_default(uint8_t* pos) const override { NOT_SUPPORT(); }
    void serialize_batch(uint8_t*, starrocks::Buffer<unsigned int>&, size_t, uint32_t) const override { NOT_SUPPORT(); }
    const uint8_t* deserialize_and_append(const uint8_t* pos) override { NOT_SUPPORT(); }
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override { NOT_SUPPORT(); }
    void deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size, Buffer<uint8_t>& is_nulls,
                                               bool& has_null) override {
        NOT_SUPPORT();
    }
    MutablePtr clone() const override { NOT_SUPPORT(); }
    uint32_t serialize_size(size_t idx) const override { NOT_SUPPORT(); }
    size_t filter_range(const Filter& filter, size_t from, size_t to) override { NOT_SUPPORT(); }
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override {
        NOT_SUPPORT();
    }
    int64_t xor_checksum(uint32_t from, uint32_t to) const override { NOT_SUPPORT(); }
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override {
        NOT_SUPPORT();
    }
    std::string get_name() const override { NOT_SUPPORT(); }
    Datum get(size_t n) const override { NOT_SUPPORT(); }
    void swap_column(Column& rhs) override { NOT_SUPPORT(); }
    Status capacity_limit_reached() const override { NOT_SUPPORT(); }
    void check_or_die() const override {}
    Status accept(ColumnVisitor* visitor) const override { NOT_SUPPORT(); }
    Status accept_mutable(ColumnVisitorMutable* visitor) override { NOT_SUPPORT(); }
    bool is_nullable_view() const override { return _default_column->is_nullable(); }

protected:
    void _append_selective(const Column& src, std::vector<uint32_t>&& indexes);

    virtual void _append(int habitat_idx, const ColumnPtr& src, size_t offset, size_t count);

    virtual void _append_selective(int habitat_idx, const ColumnPtr& src, const std::vector<uint32_t>& index_container);

    virtual void _to_view() const;
    void _concat_if_need() const;

    ColumnPtr _default_column;
    const long _concat_rows_limit;
    const long _concat_bytes_limit;
    mutable Columns _habitats;
    size_t _num_rows{0};
    mutable std::once_flag _to_view_flag;
    mutable std::vector<std::function<void()> > _tasks;
    mutable Locations _habitat_idx;
    mutable Locations _row_idx;
    mutable ColumnPtr _concat_column;
};
} // namespace starrocks
