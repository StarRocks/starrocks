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
#include <optional>

#include "column/binary_column.h"
#include "types/geo_type_descriptor.h"
#include "types/geo_wkb.h"

namespace starrocks {

// Standalone physical column; not a VARBINARY SQL type or a TypeDescriptor attachment.
// The descriptor is immutable. Column-to-column copies require identical descriptors;
// SQL assignment/coercion belongs to FE, not to these physical copy operations.
// NULL is represented by NullableColumn. Empty bytes are only a default/null placeholder,
// not an OGC EMPTY geometry. Payload ingestion preserves bytes without eager parsing.
class GeoColumn final : public CowFactory<Column, GeoColumn> {
public:
    explicit GeoColumn(GeoColumnDescriptor descriptor, GeoWkbLimits limits = {});
    DISALLOW_COPY(GeoColumn);

    const GeoColumnDescriptor& descriptor() const { return _descriptor; }
    Slice get_wkb(size_t row) const { return _data->get_slice(row); }
    void append_wkb(Slice wkb);
    // Batch ingestion from external buffers (must not alias this column).
    void append_wkb_batch(const Slice* values, size_t count);
    // Borrowed slices are read-only and follow Column lifetime rules: mutation may invalidate them.
    // Inspection returns a pointer-free value. Only a mutable column can fill the cache:
    // immutable shared columns never acquire mutable execution state through const access.
    StatusOr<GeoWkbInfo> inspect_wkb(size_t row);
    bool has_wkb_cache() const { return _cache.has_value(); }
    void clear_wkb_cache() { _cache.reset(); }

    size_t size() const override { return _data->size(); }
    size_t capacity() const override { return _data->capacity(); }
    size_t type_size() const override { return sizeof(Slice); }
    size_t byte_size() const override { return _data->byte_size(); }
    size_t byte_size(size_t from, size_t count) const override { return _data->byte_size(from, count); }
    size_t byte_size(size_t row) const override { return _data->byte_size(row); }
    void reserve(size_t count) override { _data->reserve(count); }
    void reserve(size_t count, size_t bytes) { _data->reserve(count, bytes); }
    void resize(size_t count) override;
    void assign(size_t count, size_t row) override;
    void remove_first_n_values(size_t count) override;
    void append_datum(const Datum& datum) override { append_wkb(datum.get_slice()); }
    using Column::append;
    void append(const Column& src, size_t offset, size_t count) override;
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t count) override;
    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) override;
    void append_value_multiple_times(const void* value, size_t count) override;
    bool append_nulls(size_t count) override { return false; }
    size_t append_numbers(const void* data, size_t length) override { return size_t(-1); }
    void append_default() override { append_default(1); }
    void append_default(size_t count) override;
    void fill_default(const Filter& filter) override;
    void update_rows(const Column& src, const uint32_t* indexes) override;
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    MutableColumnPtr clone_empty() const override;
    MutableColumnPtr clone() const override;
    void swap_column(Column& rhs) override;
    void reset_column() override;

    Datum get(size_t row) const override { return Datum(get_wkb(row)); }
    std::string get_name() const override { return "geo"; }
    std::string debug_item(size_t row) const override;
    size_t container_memory_usage() const override;
    size_t reference_memory_usage(size_t from, size_t count) const override { return 0; }
    Status capacity_limit_reached() const override { return _data->capacity_limit_reached(); }
    void check_or_die() const override { _data->check_or_die(); }
    bool has_large_column() const override { return _data->has_large_column(); }
    StatusOr<MutableColumnPtr> upgrade_if_overflow() override;
    StatusOr<MutableColumnPtr> downgrade() override { return MutableColumnPtr{}; }

    // No binary visitor fallback. Serde/public rendering are Contract 2.3;
    // equality, ordering and geo keys remain unsupported.
    Status accept(ColumnVisitor* visitor) const override { return Status::NotSupported("GeoColumn visitor"); }
    Status accept_mutable(ColumnVisitorMutable* visitor) override { return Status::NotSupported("GeoColumn visitor"); }
    int compare_at(size_t left, size_t right, const Column& rhs, int hint) const override;
    int64_t xor_checksum(uint32_t from, uint32_t to) const override;
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t row, bool binary = false) const override;
    uint32_t max_one_element_serialize_size() const override;
    uint32_t serialize(size_t row, uint8_t* pos) const override;
    uint32_t serialize_default(uint8_t* pos) const override;
    uint32_t serialize_size(size_t row) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& sizes, size_t count, uint32_t max_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;
    void deserialize_and_append_batch(Buffer<Slice>& src, size_t count) override;
    void deserialize_and_append_batch_nullable(Buffer<Slice>& src, size_t count, Buffer<uint8_t>& nulls,
                                               bool& has_null) override;

private:
    const GeoColumn& _source(const Column& src) const;
    struct CachedWkb {
        size_t row;
        GeoWkbInfo info;
    };
    GeoColumnDescriptor _descriptor;
    GeoWkbLimits _limits;
    // Exclusive ownership: no mutable BinaryColumn escape hatch or shared backing buffers.
    std::unique_ptr<BinaryColumn> _data = std::make_unique<BinaryColumn>();
    // One fixed-size entry; no heap allocation, geometry objects or source-buffer references.
    std::optional<CachedWkb> _cache;
};

} // namespace starrocks
