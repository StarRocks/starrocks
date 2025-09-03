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

#include "variant_value.h"

#include <arrow/util/endian.h>

#include <boost/uuid/uuid_io.hpp>
#include <cstring>

#include "util/url_coding.h"
#include "util/variant_util.h"

namespace starrocks {

StatusOr<VariantValue> VariantValue::create(const Slice& slice) {
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

    auto metadata_status = load_metadata(variant);
    if (!metadata_status.ok()) {
        return metadata_status.status();
    }

    const auto& metadata_view = metadata_status.value();
    if (metadata_view.size() > variant_size) {
        return Status::InvalidArgument("Metadata size exceeds variant size");
    }

    std::string metadata(metadata_view);
    RETURN_IF_ERROR(validate_metadata(metadata));
    std::string value(variant_raw + sizeof(uint32_t) + metadata_view.size(), variant_size - metadata_view.size());

    return VariantValue(std::move(metadata), std::move(value));
}

Status VariantValue::validate_metadata(const std::string_view metadata) {
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

VariantValue VariantValue::of_null() {
    static constexpr uint8_t header = static_cast<uint8_t>(VariantPrimitiveType::NULL_TYPE) << 2;
    static constexpr uint8_t null_chars[] = {header};
    return VariantValue(VariantMetadata::kEmptyMetadata,
                        std::string_view{reinterpret_cast<const char*>(null_chars), 1});
}

StatusOr<std::string_view> VariantValue::load_metadata(const std::string_view variant) {
    if (variant.empty()) {
        return Status::InvalidArgument("Variant is empty");
    }

    // Check variant size limit (16MB)
    if (variant.size() > kMaxVariantSize) {
        return Status::InvalidArgument("Variant size exceeds maximum limit: " + std::to_string(variant.size()) + " > " +
                                       std::to_string(kMaxVariantSize));
    }

    const uint8_t header = static_cast<uint8_t>(variant[0]);
    if (const uint8_t version = header & kVersionMask; version != 1) {
        return Status::NotSupported("Unsupported variant version: " + std::to_string(version));
    }

    const uint8_t offset_size = 1 + ((header & kOffsetSizeMask) >> kOffsetSizeShift);
    if (offset_size < 1 || offset_size > 4) {
        return Status::InvalidArgument("Invalid offset size in variant metadata: " + std::to_string(offset_size) +
                                       ", expected 1, 2, 3 or 4 bytes");
    }

    if (variant.size() < kHeaderSize + offset_size) {
        return Status::InvalidArgument("Variant too short to contain dict_size");
    }

    uint32_t dict_size = VariantUtil::readLittleEndianUnsigned(variant.data() + 1, offset_size);
    uint32_t offset_list_offset = kHeaderSize + offset_size;

    // Check for potential overflow in offset list size calculation
    if (dict_size > (kMaxVariantSize - offset_list_offset) / offset_size - 1) {
        return Status::InvalidArgument("Dict size too large: " + std::to_string(dict_size));
    }

    uint32_t required_offset_list_size = (1 + dict_size) * offset_size;
    uint32_t data_offset = offset_list_offset + required_offset_list_size;
    uint32_t last_offset_pos = offset_list_offset + dict_size * offset_size;
    if (last_offset_pos + offset_size > variant.size()) {
        return Status::InvalidArgument("Variant too short to contain all offsets");
    }

    uint32_t last_data_size = VariantUtil::readLittleEndianUnsigned(variant.data() + last_offset_pos, offset_size);
    uint32_t end_offset = data_offset + last_data_size;

    if (end_offset > variant.size()) {
        return Status::CapacityLimitExceed("Variant metadata end offset exceeds variant size: " +
                                           std::to_string(end_offset) + " > " + std::to_string(variant.size()));
    }

    return std::string_view(variant.data(), end_offset);
}

size_t VariantValue::serialize(uint8_t* dst) const {
    size_t offset = 0;

    // The first 4 bytes are the total size of the variant
    uint32_t total_size = static_cast<uint32_t>(_metadata.size() + _value.size());
    memcpy(dst + offset, &total_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // metadata
    memcpy(dst + offset, _metadata.data(), _metadata.size());
    offset += _metadata.size();

    // value
    memcpy(dst + offset, _value.data(), _value.size());
    offset += _value.size();

    return offset;
}

uint32_t VariantValue::serialize_size() const {
    return sizeof(uint32_t) + _metadata.size() + _value.size();
}

StatusOr<std::string> VariantValue::to_json(cctz::time_zone timezone) const {
    std::stringstream json_str;
    auto status = VariantUtil::variant_to_json(_metadata, _value, json_str, timezone);
    if (!status.ok()) {
        return status;
    }

    return json_str.str();
}

std::string VariantValue::to_string() const {
    auto json_result = to_json();
    if (!json_result.ok()) {
        return "";
    }

    return json_result.value();
}

std::ostream& operator<<(std::ostream& os, const VariantValue& value) {
    return os << value.to_string();
}

} // namespace starrocks
