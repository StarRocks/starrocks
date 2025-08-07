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
        : public CowFactory<ColumnFactory<ObjectColumn<VariantValue>, VariantColumn>, VariantColumn, Column> {
public:
    using ValueType = VariantValue;
    using SuperClass = CowFactory<ColumnFactory<ObjectColumn<VariantValue>, VariantColumn>, VariantColumn, Column>;
    using BaseClass = VariantColumnBase;

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
    void append(const VariantValue* object);
    void append(VariantValue&& object);
    void append(const VariantValue& object);

    bool is_variant() const override { return true; }

    std::string get_name() const override { return "variant"; }

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;
};

} // namespace starrocks
