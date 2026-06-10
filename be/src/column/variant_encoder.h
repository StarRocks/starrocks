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

#include <velocypack/Slice.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base/status.h"
#include "base/statusor.h"
#include "column/column.h"
#include "column/column_builder.h"
#include "types/datum.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"
#include "types/variant.h"

namespace starrocks {

// VariantEncoder converts StarRocks typed values into the Variant binary format
// (Apache Parquet Variant spec: metadata dictionary + value bytes).
//
// Why this class exists:
//   Multiple call sites need to produce VariantRowValue from different input forms
//   (typed Column, Datum, JsonValue, VPack slice, raw JSON text). Centralizing the
//   encoding logic here prevents duplicated format knowledge and ensures a single
//   implementation of the metadata/value binary layout.
//
// Encoding model:
//   Each VariantRowValue = metadata (sorted key-name dictionary) + value (binary payload).
//   Object fields are addressed by integer IDs that index into the metadata dictionary,
//   so metadata must be built before encoding object/map/struct values.
class VariantEncoder {
public:
    inline static uint8_t minimal_uint_size(uint32_t value) {
        if (value <= 0xFF) return 1;
        if (value <= 0xFFFF) return 2;
        if (value <= 0xFFFFFF) return 3;
        return 4;
    }

    inline static void append_uint_le(std::string* out, uint32_t value, uint8_t size) {
        for (uint8_t i = 0; i < size; ++i) {
            out->push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
        }
    }

    inline static void append_null_value(std::string* out) {
        out->append(VariantValue::kEmptyValue.data(), VariantValue::kEmptyValue.size());
    }

    static void append_array_container(std::string* out, const std::vector<uint32_t>& end_offsets,
                                       std::string_view payload);

    static void append_object_container(std::string* out, const std::vector<uint32_t>& field_ids,
                                        const std::vector<uint32_t>& end_offsets, std::string_view payload);

    // Encode an entire column of the given type into a VariantColumn builder.
    //
    // Why: CAST(col AS VARIANT) and similar bulk-conversion operators need to process
    // a whole column at once. This avoids per-row virtual-dispatch overhead by using
    // typed column viewers (encode_column_with_viewer) that read raw typed arrays.
    //
    // allow_throw_exception: if true, returns error on first failure; if false,
    // appends SQL NULL for failed rows and continues.
    static Status encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw_exception);

    // Encode a single Datum of the given type into a VariantRowValue.
    //
    // Why: Row-by-row materialization paths (e.g., typed-column -> remain fallback,
    // root-only conflict resolution) need to convert individual values. This function
    // handles the two-phase encoding: collect all object keys first, build metadata,
    // then encode the value referencing that metadata.
    static StatusOr<VariantRowValue> encode_datum(const Datum& datum, const TypeDescriptor& type);

    // Encode a VelocyPack (VPack) slice into a VariantRowValue.
    //
    // Why: VPack is StarRocks' internal binary JSON representation. JSON values
    // frequently flow through the system as vpack::Slice (e.g., from storage reads,
    // expression evaluation). This function avoids an intermediate JsonValue wrapper
    // and works directly on the VPack binary, collecting object keys in one pass and
    // then encoding the value.
    static StatusOr<VariantRowValue> encode_vslice_to_variant(const vpack::Slice& slice);

    // Encode a JsonValue into a VariantRowValue.
    //
    // Why: JsonValue is the public JSON type used in StarRocks expressions and UDFs.
    // This is a thin wrapper around encode_vslice_to_variant for callers that already
    // hold a JsonValue.
    static StatusOr<VariantRowValue> encode_json_to_variant(const JsonValue& json);

    // Parse a raw JSON text string and encode it into a VariantRowValue.
    //
    // Why: Ingestion paths and user-provided inputs often carry JSON as plain text.
    // This function parses the text via VPack and then delegates to encode_json_to_variant.
    // It also provides a backward-compatibility fallback: if the text is not valid JSON
    // (e.g., a bare scalar string like "abc"), it is treated as a Variant string value.
    static StatusOr<VariantRowValue> encode_json_text_to_variant(std::string_view json_text);

    // Build the binary encoding of a Variant ARRAY value from pre-encoded element bytes.
    //
    // Why: The recursive encoding path and VariantBuilder both need to assemble array
    // values from individually encoded elements. Extracting this as a shared primitive
    // avoids duplicating the Variant array binary layout (header, element count,
    // offset table, data region).
    static std::string encode_array_from_elements(const std::vector<std::string>& elements);

    // Build the binary encoding of a Variant OBJECT value from a field-id -> value map.
    //
    // Why: The Variant object format addresses fields by integer IDs that index into
    // the metadata dictionary rather than by name strings. Both the recursive datum
    // encoder and VariantBuilder use this primitive to construct object payloads once
    // keys have been resolved to IDs via build_variant_metadata.
    static std::string encode_object_from_fields(const std::map<uint32_t, std::string>& fields);

    // Build the Variant metadata binary (sorted key-name dictionary) from a set of
    // string keys, and optionally populate a key->id mapping for subsequent value encoding.
    //
    // Why: The Variant spec separates the metadata (key dictionary) from the value
    // binary. All object/map/struct encodings must first resolve key strings to integer
    // IDs via this dictionary. Centralizing metadata construction here ensures the
    // sorted-flag and offset-size fields are set correctly per the spec, and lets callers
    // share the resulting key_to_id map across a single encoding session.
    //
    // Returns empty-metadata sentinel if keys is empty.
    static StatusOr<std::string> build_variant_metadata(const std::unordered_set<std::string>& keys,
                                                        std::unordered_map<std::string, uint32_t>* key_to_id);
};

} // namespace starrocks
