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

namespace starrocks {

class VariantValue {
public:
    VariantValue(const std::string_view metadata, const std::string_view value) : _metadata(metadata), _value(value) {
        if (auto status = validate_metadata(_metadata); !status.ok()) {
            throw std::runtime_error("Invalid metadata: " + status.to_string());
        }

        if (_value.empty()) {
            throw std::runtime_error("Value cannot be empty");
        }
    }

    VariantValue(std::string metadata, std::string value) : _metadata(std::move(metadata)), _value(std::move(value)) {
        if (auto status = validate_metadata(_metadata); !status.ok()) {
            throw std::runtime_error("Invalid metadata: " + status.to_string());
        }

        if (_value.empty()) {
            throw std::runtime_error("Value cannot be empty");
        }
    }

    explicit VariantValue(const Slice& slice);

    VariantValue() = default;

    VariantValue(const VariantValue& rhs) = default;

    VariantValue(VariantValue&& rhs) noexcept = default;

    static Status validate_metadata(const std::string_view metadata);

    VariantValue& operator=(const VariantValue& rhs) = default;

    VariantValue& operator=(VariantValue&& rhs) noexcept = default;

    static VariantValue of_null();

    // Load metadata from the variant binary.
    // will slice the variant binary to extract metadata
    StatusOr<std::string_view> load_metadata(std::string_view variant) const;

    // Serialize the VariantValue to a byte array.
    // return the number of bytes written
    size_t serialize(uint8_t* dst) const;

    // Calculate the size of the serialized VariantValue.
    // 4 bytes for value size + metadata size + value size
    uint64_t serialize_size() const;

    uint64_t mem_usage() const { return serialize_size(); }

    // Convert to a JSON string
    StatusOr<std::string> to_json(cctz::time_zone timezone = cctz::local_time_zone()) const;
    std::string to_string() const;

    std::string get_metadata() const { return _metadata; }
    std::string get_value() const { return _value; }

private:
    static constexpr uint8_t kVersionMask = 0b1111;
    static constexpr uint8_t kSortedStrings = 0b10000;
    static constexpr uint8_t kOffsetSizeMask = 0b11000000;
    static constexpr uint8_t kOffsetSizeShift = 6;
    static constexpr uint8_t kHeaderSize = 1;

    // Now directly store strings instead of string_views
    std::string _metadata;
    std::string _value;
};

// append json string to the stream
std::ostream& operator<<(std::ostream& os, const VariantValue& json);

} // namespace starrocks

// fmt::format
template <>
struct fmt::formatter<starrocks::VariantValue> : formatter<std::string> {
    template <typename FormatContext>
    auto format(const starrocks::VariantValue& p, FormatContext& ctx) -> decltype(ctx.out()) {
        return formatter<std::string>::format(p.to_string(), ctx);
    }
}; // namespace fmt