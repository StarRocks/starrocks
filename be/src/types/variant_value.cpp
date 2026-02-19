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

#include "types/variant_value.h"

#include <cstring>
#include <sstream>
#include <string>
#include <string_view>

namespace starrocks {

static const std::string kDefultVariantRowValueBinary =
        std::string(VariantMetadata::kEmptyMetadata) + std::string(VariantValue::kEmptyValue);

StatusOr<VariantRowValue> VariantRowValue::create(const Slice& slice) {
    // Validate slice first
    if (slice.get_data() == nullptr) {
        return Status::InvalidArgument("Invalid variant slice: null data pointer");
    }

    if (slice.get_size() < sizeof(uint32_t)) {
        return Status::InvalidArgument("Invalid variant slice: too small to contain size header");
    }

    const char* variant_raw = slice.get_data();
    // The first 4 bytes are the size of the variant
    uint32_t variant_size;
    std::memcpy(&variant_size, variant_raw, sizeof(uint32_t));
    // Check variant size limit (16MB)
    if (variant_size > kMaxVariantSize) {
        return Status::InvalidArgument("Variant size exceeds maximum limit: " + std::to_string(variant_size) + " > " +
                                       std::to_string(kMaxVariantSize));
    }

    if (variant_size > slice.get_size() - sizeof(uint32_t)) {
        return Status::InvalidArgument(
                "Invalid variant size: " + std::to_string(variant_size) +
                " exceeds available data: " + std::to_string(slice.get_size() - sizeof(uint32_t)));
    }

    const auto variant = std::string_view(variant_raw + sizeof(uint32_t), variant_size);
    ASSIGN_OR_RETURN(const auto metadata_view, load_metadata(variant));
    if (metadata_view.size() > variant_size) {
        return Status::InvalidArgument("Metadata size exceeds variant size");
    }

    RETURN_IF_ERROR(validate_metadata(metadata_view));
    std::string_view value_view(variant_raw + sizeof(uint32_t) + metadata_view.size(),
                                variant_size - metadata_view.size());

    return VariantRowValue(metadata_view, value_view);
}

StatusOr<VariantRowValue> VariantRowValue::create(const std::string_view metadata, const std::string_view value) {
    if (metadata.empty()) {
        return from_null();
    }

    RETURN_IF_ERROR(validate_metadata(metadata));
    // validate value size limit (16MB)
    if (metadata.size() + value.size() > kMaxVariantSize) {
        return Status::InvalidArgument("Variant value size exceeds maximum limit: " + std::to_string(value.size()) +
                                       " > " + std::to_string(kMaxVariantSize));
    }

    return VariantRowValue(metadata, value);
}

// Create a VariantRowValue from a Parquet Variant.
VariantRowValue VariantRowValue::from_variant(const VariantMetadata& metadata, const VariantValue& variant) {
    return VariantRowValue(metadata.raw(), variant.raw());
}

Status VariantRowValue::validate_metadata(const std::string_view metadata) {
    // metadata at least 3 bytes: version, dictionarySize and at least one offset.
    if (metadata.size() < kMinMetadataSize) {
        return Status::InternalError("Variant metadata is too short");
    }

    const uint8_t header = static_cast<uint8_t>(metadata[0]);
    if (const uint8_t version = header & kVersionMask; version != 1) {
        return Status::NotSupported("Unsupported variant version: " + std::to_string(version));
    }

    return Status::OK();
}

VariantRowValue VariantRowValue::from_null() {
    return VariantRowValue();
}

StatusOr<std::string_view> VariantRowValue::load_metadata(const std::string_view variant_binary) {
    if (variant_binary.empty()) {
        return Status::InvalidArgument("Variant is empty");
    }

    // Check variant size limit (16MB)
    if (variant_binary.size() > kMaxVariantSize) {
        return Status::InvalidArgument("Variant size exceeds maximum limit: " + std::to_string(variant_binary.size()) +
                                       " > " + std::to_string(kMaxVariantSize));
    }

    const uint8_t header = static_cast<uint8_t>(variant_binary[0]);
    if (const uint8_t version = header & kVersionMask; version != 1) {
        return Status::NotSupported("Unsupported variant version: " + std::to_string(version));
    }

    const uint8_t offset_size = 1 + ((header & kOffsetSizeMask) >> kOffsetSizeShift);
    if (offset_size < 1 || offset_size > 4) {
        return Status::InvalidArgument("Invalid offset size in variant metadata: " + std::to_string(offset_size) +
                                       ", expected 1, 2, 3 or 4 bytes");
    }

    if (variant_binary.size() < kHeaderSize + offset_size) {
        return Status::InvalidArgument("Variant too short to contain dict_size");
    }

    uint32_t dict_size = VariantUtil::read_little_endian_unsigned32(variant_binary.data() + 1, offset_size);
    uint32_t offset_list_offset = kHeaderSize + offset_size;

    // Check for potential overflow in offset list size calculation
    if (dict_size > (kMaxVariantSize - offset_list_offset) / offset_size - 1) {
        return Status::InvalidArgument("Dict size too large: " + std::to_string(dict_size));
    }

    uint32_t required_offset_list_size = (1 + dict_size) * offset_size;
    uint32_t data_offset = offset_list_offset + required_offset_list_size;
    uint32_t last_offset_pos = offset_list_offset + dict_size * offset_size;
    if (last_offset_pos + offset_size > variant_binary.size()) {
        return Status::InvalidArgument("Variant too short to contain all offsets");
    }

    uint32_t last_data_size =
            VariantUtil::read_little_endian_unsigned32(variant_binary.data() + last_offset_pos, offset_size);
    uint32_t end_offset = data_offset + last_data_size;

    if (end_offset > variant_binary.size()) {
        return Status::CapacityLimitExceed("Variant metadata end offset exceeds variant size: " +
                                           std::to_string(end_offset) + " > " + std::to_string(variant_binary.size()));
    }

    return std::string_view(variant_binary.data(), end_offset);
}

size_t VariantRowValue::serialize(uint8_t* dst) const {
    size_t offset = 0;

    // The first 4 bytes are the total size of the variant
    const char* raw_data = _raw.data();
    uint32_t total_size = static_cast<uint32_t>(_raw.size());
    if (total_size == 0) {
        // For null variant, use predefined empty variant size
        total_size = static_cast<uint32_t>(kDefultVariantRowValueBinary.size());
        raw_data = kDefultVariantRowValueBinary.data();
    }

    memcpy(dst + offset, &total_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // Copy the entire contiguous buffer [metadata][value]
    memcpy(dst + offset, raw_data, total_size);
    offset += total_size;

    return offset;
}

uint32_t VariantRowValue::serialize_size() const {
    uint32_t total_size = static_cast<uint32_t>(_raw.size());
    if (total_size == 0) {
        // For null variant, use predefined empty variant size
        total_size = static_cast<uint32_t>(kDefultVariantRowValueBinary.size());
    }
    return sizeof(uint32_t) + total_size;
}

StatusOr<std::string> VariantRowValue::to_json(cctz::time_zone timezone) const {
    std::stringstream json_str;
    auto status = VariantUtil::variant_to_json(_metadata, _value, json_str, timezone);
    if (!status.ok()) {
        return status;
    }

    return json_str.str();
}

std::string VariantRowValue::to_string() const {
    auto json_result = to_json();
    if (!json_result.ok()) {
        return "";
    }

    return json_result.value();
}

std::ostream& operator<<(std::ostream& os, const VariantRowValue& value) {
    return os << value.to_string();
}

} // namespace starrocks
