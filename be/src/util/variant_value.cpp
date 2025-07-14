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

#include <cstring>
#include <boost/uuid/uuid_io.hpp>

#include "url_coding.h"
#include "variant_util.h"
#include "formats/parquet/variant.h"

namespace starrocks {

StatusOr<std::string_view> VariantValue::load_metadata(std::string_view variant) const{
    if (variant.size() < 3) {
        return Status::NotSupported("Variant metadata is too short");
    }

    const uint8_t header = static_cast<uint8_t>(variant[0]);
    if (const uint8_t version = header & kVersionMask ;version != 1) {
        return Status::NotSupported("Unsupported variant version: " + std::to_string(version));
    }

    const uint8_t offset_size = 1 + ((header & kOffsetSizeMask) >> kOffsetSizeShift);
    if (offset_size < 1 || offset_size > 4) {
        return Status::InvalidArgument(
            "Invalid offset size in variant metadata: " + std::to_string(offset_size) +
            ", expected 1, 2, 3 or 4 bytes");
    }

    uint8_t dict_size = VariantUtil::readLittleEndianUnsigned(variant.data() + 1, offset_size);
    uint8_t offset_list_offset = kHeaderSize + offset_size;
    uint8_t data_offset = offset_list_offset + (1 + dict_size) * offset_size;
    uint8_t end_offset = data_offset + VariantUtil::readLittleEndianUnsigned(variant.data() + offset_list_offset + dict_size * offset_size, offset_size);

    if (end_offset > variant.size()) {
            return Status::CapacityLimitExceed(
                "Variant metadata end offset exceeds variant size: " + std::to_string(end_offset) +
                " > " + std::to_string(variant.size()));
    }

    return std::string_view(variant.data(), end_offset);
}


size_t VariantValue::serialize(uint8_t* dst) const{
    size_t offset = 0;

    // The first 4 bytes are total size of the variant
    uint32_t total_size = static_cast<uint32_t>(_metadata.size() + _value.size());
    memcpy(dst, &total_size, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // metadata
    memcpy(dst, _metadata.data(), _metadata.size());
    offset += _metadata.size();
    // value
    memcpy(dst + offset, _value.data(), _value.size());
    offset += _value.size();

    return offset;
}

uint64_t VariantValue::serialize_size() const {
    return sizeof(uint32_t) + _metadata.size() + _value.size();
}

StatusOr<std::string> VariantValue::to_json(cctz::time_zone timezone) const{
    std::stringstream json_str;
    auto status = VariantUtil::variant_to_json(_metadata, _value, json_str, timezone);
    if (!status.ok()) {
        return status;
    }

    return json_str.str();
}

std::string VariantValue::to_string() const{
    auto json_result = to_json();
    if (!json_result.ok()) {
        return "";
    }

    return json_result.value();
}

std::ostream& operator<<(std::ostream& os, const VariantValue& value){
    return os << value.to_string();
}

}