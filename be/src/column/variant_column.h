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

#include <utility>

#include "column/column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "types/variant_value.h"

namespace starrocks {

class VariantColumn final
        : public CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column> {
public:
    using ValueType = VariantRowValue;
    using SuperClass = CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column>;
    using BaseClass = VariantColumnBase;
    using ImmContainer = ObjectDataProxyContainer;

    VariantColumn() = default;
    explicit VariantColumn(size_t size) : SuperClass(size) {}
    VariantColumn(const VariantColumn& rhs) : SuperClass(rhs) {}

    VariantColumn(VariantColumn&& rhs) noexcept : SuperClass(std::move(rhs)) {}

    MutableColumnPtr clone() const override { return BaseClass::clone(); }
    MutableColumnPtr clone_empty() const override { return this->create(); }

    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_size(size_t idx) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void append_datum(const Datum& datum) override;
    void append(const Column& src, size_t offset, size_t count) override;

    // Add a forwarding function to expose the base class append function
    void append(const Column& src) { append(src, 0, src.size()); }
    void append(const VariantRowValue* object);
    void append(VariantRowValue&& object);
    void append(const VariantRowValue& object);
    bool append_nulls(size_t count) override;
<<<<<<< HEAD
=======
    void append_default() override;
    void append_default(size_t count) override;

    size_t size() const override;
    size_t capacity() const override;
    size_t byte_size(size_t from, size_t size) const override;
    void resize(size_t n) override;
    void assign(size_t n, size_t idx) override;
    void remove_first_n_values(size_t count) override;
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;
    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;
    void swap_column(Column& rhs) override;
    void reset_column() override;
    void check_or_die() const override;
>>>>>>> 45fdd3c ([BugFix] Fix shredded Variant compatibility in generic operations (#78296))

    bool is_variant() const override { return true; }

    std::string get_name() const override { return "variant"; }

<<<<<<< HEAD
=======
    void set_shredded_columns(std::vector<std::string> paths, std::vector<TypeDescriptor> type_descs,
                              MutableColumns columns, BinaryColumn::MutablePtr metadata_column,
                              BinaryColumn::MutablePtr remain_value_column);
    static Status validate_shredded_schema(const std::vector<std::string>& paths,
                                           const std::vector<TypeDescriptor>& type_descs, const MutableColumns& columns,
                                           const BinaryColumn::MutablePtr& metadata_column,
                                           const BinaryColumn::MutablePtr& remain_value_column);

    void clear_shredded_columns();

    const std::vector<std::string>& shredded_paths() const { return _shredded_paths; }

    // Cached parsed form of _shredded_paths. Populated by set_shredded_columns(),
    // kept in sync with _shredded_paths. Avoids per-row re-parsing in hot paths.
    const std::vector<VariantPath>& parsed_shredded_paths() const { return _parsed_shredded_paths; }

    const std::vector<TypeDescriptor>& shredded_types() const { return _shredded_types; }

    std::vector<TypeDescriptor>& mutable_shredded_types() { return _shredded_types; }

    const MutableColumns& typed_columns() const { return _typed_columns; }

    MutableColumns& mutable_typed_columns() { return _typed_columns; }
    int find_shredded_path(std::string_view path) const;
    const Column* typed_column_by_index(size_t idx) const;

    const BinaryColumn::MutablePtr& metadata_column() const { return _metadata_column; }

    const BinaryColumn::MutablePtr& remain_value_column() const { return _remain_value_column; }

    bool has_metadata_column() const { return _metadata_column != nullptr; }

    bool has_remain_value() const { return _remain_value_column != nullptr; }

    bool is_equal_schema(const VariantColumn* other) const;

    void mutate_each_subcolumn() override {
        for (auto& column : _typed_columns) {
            column = (std::move(*column)).mutate();
        }
        if (_metadata_column != nullptr) {
            _metadata_column = BinaryColumn::static_pointer_cast((std::move(*_metadata_column)).mutate());
        }
        if (_remain_value_column != nullptr) {
            _remain_value_column = BinaryColumn::static_pointer_cast((std::move(*_remain_value_column)).mutate());
        }
    }

    // Encode one typed cell into a row-level Variant value. Complex values containing
    // nested Variant children are traversed column-wise to avoid the legacy Datum path.
    static StatusOr<EncodedVariantResult> encode_typed_row_as_variant(const Column* typed_column, size_t typed_row,
                                                                      const TypeDescriptor& type_desc);

    // Deep-copy a shredded VariantColumn, duplicating typed columns, metadata, and remain.
    // Caller must ensure src.is_shredded_variant().
    static MutableColumnPtr deep_copy_shredded(const VariantColumn& src);

    // Ensure base metadata/remain columns exist.
    // For typed-only schema, this attaches null base payload rows while preserving typed ownership.
    Status ensure_base_variant_column();

    // Align destination schema from `src` before append/merge.
    // Empty destination is initialized directly from `src`; otherwise schema
    // is aligned via path union and compatibility checks.
    bool align_schema_from(const VariantColumn& src);

>>>>>>> 45fdd3c ([BugFix] Fix shredded Variant compatibility in generic operations (#78296))
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;
};

} // namespace starrocks
