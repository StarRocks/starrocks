// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "util/json.h"

namespace starrocks::vectorized {

// JsonColumn column for JSON type
// format_version 1: store each JSON in binary encoding individually
// format_version 2: TODO columnar encoding for JSON
class JsonColumn final : public ColumnFactory<ObjectColumn<JsonValue>, JsonColumn, Column> {
public:
    using ValueType = JsonValue;
    using SuperClass = ColumnFactory<ObjectColumn<JsonValue>, JsonColumn, Column>;
    using BaseClass = JsonColumnBase;

    JsonColumn() = default;
    explicit JsonColumn(size_t size) : SuperClass(size) {}
    JsonColumn(const JsonColumn& rhs) : SuperClass(rhs) {}

    MutableColumnPtr clone() const override;
    MutableColumnPtr clone_empty() const override;
    ColumnPtr clone_shared() const override;

    void append_datum(const Datum& datum) override;
    int compare_at(size_t left, size_t right, const starrocks::vectorized::Column& rhs,
                   int nan_direction_hint) const override;
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;
    void put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx) const override;
    std::string debug_item(uint32_t idx) const override;
    std::string get_name() const override;

<<<<<<< HEAD
private:
=======
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;
    uint32_t serialize_size(size_t idx) const override;
    uint32_t serialize(size_t idx, uint8_t* pos) override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;
>>>>>>> ff04bda779 ([Enhancement] Fix StructColumn::byte_size memory estimation error (#28283))
};

} // namespace starrocks::vectorized
