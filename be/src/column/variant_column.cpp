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

#include "variant_column.h"

#include "types/variant_value.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

uint32_t VariantColumn::serialize(size_t idx, uint8_t* pos) const {
    return static_cast<uint32_t>(get_object(idx)->serialize(pos));
}

void VariantColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                    uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

uint32_t VariantColumn::serialize_size(size_t idx) const {
    return static_cast<uint32_t>(get_object(idx)->serialize_size());
}

const uint8_t* VariantColumn::deserialize_and_append(const uint8_t* pos) {
    // Read the first 4 bytes to get the size of the variant
    uint32_t variant_length = *reinterpret_cast<const uint32_t*>(pos);
    auto variant_result = VariantValue::create(Slice(pos, variant_length + sizeof(uint32_t)));

    if (!variant_result.ok()) {
        throw std::runtime_error("Failed to deserialize variant: " + variant_result.status().to_string());
    }

    uint32_t serialize_size = variant_result->serialize_size();
    append(std::move(*variant_result));

    return pos + serialize_size;
}

void VariantColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    VariantValue* variant = get_object(idx);
    DCHECK(variant != nullptr) << "Variant value is null at index " << idx;

    auto json = variant->to_json();
    if (!json.ok()) {
        buf->push_null();
    } else {
        buf->push_string(json->data(), json->size(), '\'');
    }
}

void VariantColumn::append_datum(const Datum& datum) {
    BaseClass::append(datum.get<VariantValue*>());
}

void VariantColumn::append(const Column& src, size_t offset, size_t count) {
    BaseClass::append(src, offset, count);
}

void VariantColumn::append(const VariantValue& object) {
    BaseClass::append(object);
}

void VariantColumn::append(const VariantValue* object) {
    BaseClass::append(object);
}

void VariantColumn::append(VariantValue&& object) {
    BaseClass::append(std::move(object));
}

} // namespace starrocks