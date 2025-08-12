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

VariantValue::VariantValue(const Slice& slice) {
    const char* variant_raw = slice.get_data();
    // convert variant_raw to a string_view
    // The first 4 bytes are the size of the value
    uint32_t variant_size;
    std::memcpy(&variant_size, variant_raw, sizeof(uint32_t));
    if (variant_size > slice.get_size() - sizeof(uint32_t)) {
        throw std::runtime_error("Invalid variant size");
    }

    const auto variant = std::string_view(variant_raw + sizeof(uint32_t), variant_size);
    auto metadata_status = load_metadata(variant);
    if (!metadata_status.ok()) {
        throw std::runtime_error("Failed to load metadata: " + metadata_status.status().to_string());
    }

    _metadata = std::string(metadata_status.value());
    _value = std::string(variant_raw + sizeof(uint32_t) + metadata_status.value().size(),
                         variant_size - metadata_status.value().size());
}

Status VariantValue::validate_metadata(const std::string_view metadata) {
    // metadata at least 3 bytes: version, dictionarySize and at least one offset.
    if (metadata.size() < 3) {
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

StatusOr<std::string_view> VariantValue::load_metadata(std::string_view variant) const {
    RETURN_IF_ERROR(validate_metadata(variant));

    const uint8_t header = static_cast<uint8_t>(variant[0]);
    const uint8_t offset_size = 1 + ((header & kOffsetSizeMask) >> kOffsetSizeShift);
    if (offset_size < 1 || offset_size > 4) {
        return Status::InvalidArgument("Invalid offset size in variant metadata: " + std::to_string(offset_size) +
                                       ", expected 1, 2, 3 or 4 bytes");
    }
    uint8_t dict_size = VariantUtil::readLittleEndianUnsigned(variant.data() + 1, offset_size);
    uint8_t offset_list_offset = kHeaderSize + offset_size;
    uint8_t data_offset = offset_list_offset + (1 + dict_size) * offset_size;
    uint8_t end_offset =
            data_offset + VariantUtil::readLittleEndianUnsigned(
                                  variant.data() + offset_list_offset + dict_size * offset_size, offset_size);

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

uint64_t VariantValue::serialize_size() const {
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