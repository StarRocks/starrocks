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

#include <cctz/time_zone.h>

#include <string_view>

#include "common/statusor.h"
#include "fmt/format.h"
#include "util/slice.h"
#include "util/variant.h"
#include "util/raw_container.h"

namespace starrocks {

class VariantRowValue {
public:
    VariantRowValue(const std::string_view metadata, const std::string_view value)
            : _raw(), _metadata_size(metadata.size()), _metadata(), _value() {
        _raw.reserve(metadata.size() + value.size());
        _raw.assign(metadata.data(), metadata.size());
        _raw.append(value.data(), value.size());
        _rebind_views();
    }

    /**
     * Default constructor creates an empty VariantRowValue representing a NULL variant.
     * Uses predefined constants for empty metadata and a null variant value.
     * This ensures moved-from objects and default-constructed objects are in a valid state.
     */
    VariantRowValue() : VariantRowValue(VariantMetadata::kEmptyMetadata, VariantValue::kEmptyValue) {}
    /**
     * Static factory method to create a VariantRowValue from a Slice.
     * @param slice The Slice must contain the full variant binary including size header.
     * The first 4 bytes of the Slice are expected to be the size of the variant.
     * The memory layout is: [total size (4 bytes)][metadata][value].
     * @return The created VariantRowValue or an error status.
     */
    static StatusOr<VariantRowValue> create(const Slice& slice);

    /**
     * Static factory method to create a VariantRowValue from metadata and value Slices.
     * In this method, the metadata will be validated.
     * @param metadata The metadata Slice.
     * @param value The value Slice.
     * @return The created VariantRowValue or an error status.
     */
    static StatusOr<VariantRowValue> create(const std::string_view metadata, const std::string_view value);

    /**
     * Create a VariantRowValue from an existing Variant and its metadata.
     * This is the standard way to wrap a Variant into a VariantRowValue.
     */
    static VariantRowValue from_variant(const VariantMetadata& metadata, const VariantValue& variant);

    /**
     * Copy constructor. Creates a deep copy of the VariantRowValue.
     * After copying the underlying string data, _metadata and _value are reconstructed
     * to point to the new object's _raw string.
     */
    VariantRowValue(const VariantRowValue& rhs)
            : _raw(rhs._raw), _metadata_size(rhs._metadata_size), _metadata(), _value() {
        _rebind_views();
    }

    /**
     * Move constructor. Transfers ownership of the underlying string data.
     * After moving, _metadata and _value are bound to the new object's storage,
     * and the source object is reset to a valid empty state to prevent dangling references.
     */
    VariantRowValue(VariantRowValue&& rhs) noexcept
            : _raw(std::move(rhs._raw)), _metadata_size(rhs._metadata_size), _metadata(), _value() {
        _rebind_views();
        rhs._reset_to_empty();
    }

    static Status validate_metadata(const std::string_view metadata);

    /**
     * Create a VariantRowValue representing a NULL value.
     * Follows the codebase convention of using from_null() for null factory methods.
     */
    static VariantRowValue from_null();

    VariantRowValue& operator=(const VariantRowValue& rhs) {
        if (this != &rhs) {
            _raw = rhs._raw;
            _metadata_size = rhs._metadata_size;
            _rebind_views();
        }
        return *this;
    }

    VariantRowValue& operator=(VariantRowValue&& rhs) noexcept {
        if (this != &rhs) {
            _raw = std::move(rhs._raw);
            _metadata_size = rhs._metadata_size;
            _rebind_views();
            rhs._reset_to_empty();
        }
        return *this;
    }

    // Serialize the VariantRowValue to a byte array.
    // return the number of bytes written
    size_t serialize(uint8_t* dst) const;

    // Calculate the size of the serialized VariantRowValue.
    // 4 bytes for value size + metadata size + value size
    uint32_t serialize_size() const;

    uint64_t mem_usage() const { return serialize_size(); }

    // Convert to a JSON string
    StatusOr<std::string> to_json(cctz::time_zone timezone = cctz::local_time_zone()) const;
    std::string to_string() const;

    const VariantMetadata& get_metadata() const { return _metadata; }
    const VariantValue& get_value() const { return _value; }

    // Variant value has a maximum size limit of 16MB to prevent excessive memory usage.
    static constexpr uint32_t kMaxVariantSize = 16 * 1024 * 1024;

private:
    static constexpr uint8_t kVersionMask = 0b1111;
    static constexpr uint8_t kSortedStrings = 0b10000;
    static constexpr uint8_t kOffsetSizeMask = 0b11000000;
    static constexpr uint8_t kOffsetSizeShift = 6;
    static constexpr uint8_t kHeaderSize = 1;
    static constexpr size_t kMinMetadataSize = 3;

    /**
     * Rebinds the metadata/value views after _raw changes.
     *
     * CRITICAL: VariantMetadata and VariantValue store string_views pointing to the
     * underlying _raw string. After copy/move operations that change _raw, we must
     * reconstruct _metadata and _value to point to the new storage, otherwise we'd
     * have dangling references.
     *
     * This is called after:
     * - Copy assignment (after copying string)
     * - Move assignment (after moving string)
     * - Reset to empty (after assigning empty constants)
     */
    void _rebind_views() {
        std::string_view raw_view(_raw);
        _metadata = VariantMetadata(raw_view.substr(0, _metadata_size));
        _value = VariantValue(raw_view.substr(_metadata_size));
    }

    /**
     * Puts the object back to a known empty state; keeps moved-from objects valid.
     * This ensures that moved-from objects remain in a valid state that can be
     * safely destroyed or reassigned, as required by C++ move semantics.
     */
    void _reset_to_empty() {
        _raw.clear();
        _raw.reserve(VariantMetadata::kEmptyMetadata.size() + VariantValue::kEmptyValue.size());
        _raw.assign(VariantMetadata::kEmptyMetadata.data(), VariantMetadata::kEmptyMetadata.size());
        _raw.append(VariantValue::kEmptyValue.data(), VariantValue::kEmptyValue.size());
        _metadata_size = VariantMetadata::kEmptyMetadata.size();
        _rebind_views();
    }

    // Load metadata from the variant binary.
    // will slice the variant binary to extract metadata
    static StatusOr<std::string_view> load_metadata(std::string_view variant_binary);

    /**
     * Data layout:
     * - _raw: Owns a single contiguous buffer containing [metadata][value]
     * - _metadata_size: The size of the metadata portion in bytes
     * - _metadata: Wrapper holding a string_view into _raw[0.._metadata_size)
     * - _value: Wrapper holding a string_view into _raw[_metadata_size.._raw.size())
     *
     * The wrappers (_metadata, _value) must be kept in sync with their
     * underlying storage (_raw) via _rebind_views() whenever _raw is modified.
     */
    raw::RawString _raw;
    size_t _metadata_size;
    VariantMetadata _metadata;
    VariantValue _value;
};

// append json string to the stream
std::ostream& operator<<(std::ostream& os, const VariantRowValue& json);

} // namespace starrocks

// fmt::format
template <>
struct fmt::formatter<starrocks::VariantRowValue> : formatter<std::string> {
    template <typename FormatContext>
    auto format(const starrocks::VariantRowValue& p, FormatContext& ctx) -> decltype(ctx.out()) {
        return formatter<std::string>::format(p.to_string(), ctx);
    }
}; // namespace fmt
