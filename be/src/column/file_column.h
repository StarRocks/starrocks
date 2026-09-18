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

#include <array>
#include <cstdint>
#include <optional>
#include <string>

#include "base/string/slice.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class NullableColumn;

// FileColumn is the physical column of TYPE_FILE: an indivisible reference to a file, shaped after
// the Parquet FILE logical type. It always carries the same fixed set of nullable sub-columns:
//   uri VARCHAR | offset BIGINT | size BIGINT | content_type VARCHAR | checksum VARCHAR | inline VARBINARY
// A row is either a reference (uri/offset/size set) or inline bytes (inline set); every field may
// be NULL. Serialization simply concatenates the fields in this fixed order, so no schema is
// written to the wire. Rows print as {uri:"...",offset:1,size:2,content_type:null,checksum:null,inline:"<hex>"}.
class FileColumn final : public CowFactory<ColumnFactory<Column, FileColumn>, FileColumn> {
    friend class CowFactory<ColumnFactory<Column, FileColumn>, FileColumn>;
    using Base = CowFactory<ColumnFactory<Column, FileColumn>, FileColumn>;

public:
    using ValueType = void;
    using Container = Buffer<std::string>;

    enum FileField : size_t {
        URI = 0,
        OFFSET = 1,
        SIZE = 2,
        CONTENT_TYPE = 3,
        CHECKSUM = 4,
        INLINE = 5,
        NUM_FIELDS = 6
    };
    static constexpr std::array<const char*, NUM_FIELDS> kFieldNames = {"uri",          "offset",   "size",
                                                                        "content_type", "checksum", "inline"};

    // Empty nullable sub-columns.
    FileColumn();
    // Sub-columns holding `size` NULL rows.
    explicit FileColumn(size_t size);
    DISALLOW_COPY(FileColumn);
    FileColumn(FileColumn&& rhs) noexcept;

    ~FileColumn() override = default;

    bool is_file() const override { return true; }

    // ---- Business append entry points (used by the Paimon BLOB converter) ----
    void append_null_row();
    void append_reference(const Slice& uri, int64_t offset, std::optional<int64_t> size);
    void append_inline(const Slice& bytes);

    // ---- Typed sub-column access ----
    NullableColumn* uri_column();
    NullableColumn* offset_column();
    NullableColumn* size_column();
    NullableColumn* content_type_column();
    NullableColumn* checksum_column();
    NullableColumn* inline_column();
    const NullableColumn* uri_column() const;
    const NullableColumn* offset_column() const;
    const NullableColumn* size_column() const;
    const NullableColumn* content_type_column() const;
    const NullableColumn* checksum_column() const;
    const NullableColumn* inline_column() const;
    // All sub-columns in FileField order, for visitors that recurse field by field.
    std::array<Column*, NUM_FIELDS> field_columns();
    std::array<const Column*, NUM_FIELDS> field_columns() const;

    // ---- Column interface ----
    size_t size() const override;
    size_t capacity() const override;
    size_t type_size() const override;
    size_t byte_size() const override;
    size_t byte_size(size_t idx) const override;
    size_t byte_size(size_t from, size_t size) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    StatusOr<MutableColumnPtr> upgrade_if_overflow() override;
    StatusOr<MutableColumnPtr> downgrade() override;
    bool has_large_column() const override;
    void assign(size_t n, size_t idx) override;
    void append_datum(const Datum& datum) override;
    void remove_first_n_values(size_t count) override;
    void append(const Column& src, size_t offset, size_t count) override;
    void fill_default(const Filter& filter) override;
    void update_rows(const Column& src, const uint32_t* indexes) override;
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;
    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;
    [[nodiscard]] bool append_nulls(size_t count) override;
    [[nodiscard]] size_t append_numbers(const void* buff, size_t length) override;
    void append_value_multiple_times(const void* value, size_t count) override;
    void append_default() override;
    void append_default(size_t count) override;
    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_default(uint8_t* pos) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;
    uint32_t max_one_element_serialize_size() const override;
    uint32_t serialize_size(size_t idx) const override;
    MutableColumnPtr clone_empty() const override;
    MutableColumnPtr clone() const override {
        auto p = clone_empty();
        p->append(*this, 0, size());
        return p;
    }
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;
    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;
    int64_t xor_checksum(uint32_t from, uint32_t to) const override;
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const override;
    std::string debug_item(size_t idx) const override;
    std::string debug_string() const override;
    std::string get_name() const override;
    Datum get(size_t n) const override;
    size_t memory_usage() const override;
    size_t container_memory_usage() const override;
    size_t reference_memory_usage(size_t from, size_t size) const override;
    void swap_column(Column& rhs) override;
    void reset_column() override;
    Status capacity_limit_reached() const override;
    void check_or_die() const override;
    void mutate_each_subcolumn() override;

private:
    // Applies `f` to each sub-column in FileField order.
    template <typename F>
    void for_each_field(F&& f) {
        f(_uri);
        f(_offset);
        f(_size);
        f(_content_type);
        f(_checksum);
        f(_inline);
    }
    template <typename F>
    void for_each_field(F&& f) const {
        f(_uri);
        f(_offset);
        f(_size);
        f(_content_type);
        f(_checksum);
        f(_inline);
    }

    Column::WrappedPtr _uri;          // NullableColumn<BinaryColumn>
    Column::WrappedPtr _offset;       // NullableColumn<Int64Column>
    Column::WrappedPtr _size;         // NullableColumn<Int64Column>
    Column::WrappedPtr _content_type; // NullableColumn<BinaryColumn>
    Column::WrappedPtr _checksum;     // NullableColumn<BinaryColumn>
    Column::WrappedPtr _inline;       // NullableColumn<BinaryColumn>, VARBINARY payload
};

} // namespace starrocks
