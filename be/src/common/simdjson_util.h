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

// for DCHECK
#include "base/string/faststring.h"
#include "common/logging.h"
#include "simdjson.h"

namespace starrocks {

namespace {

// A singleton wrapper of the simdjson::ondemand::parser for `unescape()` interface only.
class SimdjsonParser {
public:
    static simdjson::simdjson_result<std::string_view> unescape(simdjson::ondemand::raw_json_string in, uint8_t*& dst,
                                                                bool allow_replacement = false) noexcept {
        static SimdjsonParser s_wrapper;
        return s_wrapper._parser.unescape(in, dst, allow_replacement);
    }

private:
    SimdjsonParser() {
        // The `_parser` is only used for `unescape`ing, which touches nothing of the parser's internal structure,
        // so the capacity doesn't matter.
        [[maybe_unused]] auto err = _parser.allocate(0);
    }
    ~SimdjsonParser() = default;

private:
    simdjson::ondemand::parser _parser;
};
} // namespace

/**
 * Safely retrieves an unescaped JSON key while preventing buffer overflow risks
 *
 * This function provides a safe alternative to simdjson_result<field>::unescaped_key() by:
 * 1. Directly returning the original key from JSON token buffer when no unescaping is needed
 * 2. Using caller-provided buffer for unescaping when necessary, avoiding reliance on parser's internal buffers
 *
 * Background: The native unescaped_key() may eventually overflow its internal buffer with repeated use.
 * Our solution prioritizes safety by:
 * - Checking if the key contains escaped characters using SIMDJSON's internal validation
 * - Using stack-allocated buffers through the caller-managed faststring when unescaping is required
 *
 * @tparam T        simdjson result type (automatically deduced)
 * @param result    simdjson result object containing the field to examine
 * @param buffer    Pre-allocated memory buffer for unescaping operations. Caller must maintain
 *                  the buffer's lifetime until the returned string_view is no longer needed.
 * @return simdjson_result<std::string_view> View of the unescaped key, either pointing to
 *         the original JSON buffer or the caller-provided buffer
 * @throws simdjson_error If key retrieval or unescaping fails
 */
template <typename T>
inline simdjson::simdjson_result<std::string_view> field_unescaped_key_safe(simdjson::simdjson_result<T> field,
                                                                            faststring* buffer) {
    std::string_view escaped_key = field.escaped_key();
    if (escaped_key.find('\\') == std::string_view::npos) {
        // fast path, `escaped_key` is the same as `unescaped_key` on the token buff, safe to return
        return escaped_key;
    } else {
        auto padded_size = escaped_key.size() + simdjson::SIMDJSON_PADDING;
        // reserve() is good enough, but faststring does additional ASAN POISON on unused bytes
        buffer->resize(padded_size);
        uint8_t* pos = buffer->data();
        return SimdjsonParser::unescape(field.key().value(), pos);
    }
}

/**
 * Safely extracts and unescapes a string value from a simdjson::ondemand::value object.
 *
 * This function addresses a safety issue with simdjson's native get_string() method. The original
 * implementation copies strings into an internal buffer that isn't rewound until document processing
 * completes. While the buffer has some additional capacity beyond the input size, repeated string
 * operations can eventually exceed buffer boundaries, potentially causing heap-buffer-overflow.
 *
 * This implementation performs unescaping directly into the caller-provided buffer, ensuring safe
 * memory access patterns.
 *
 * @param value   The simdjson::ondemand::value containing the string to extract. Must be of type
 *                simdjson::ondemand::json_type::string (enforced by DCHECK).
 * @param buffer  Pre-allocated buffer where the unescaped string will be stored. The caller must
 *                maintain buffer validity for the lifetime of any returned string_view.
 * @return        simdjson_result containing a string_view referencing the unescaped data in the
 *                caller's buffer, or an error code if parsing fails.
 */
inline simdjson::simdjson_result<std::string_view> value_get_string_safe(simdjson::ondemand::value* value,
                                                                         faststring* buffer) {
    DCHECK(value->type() == simdjson::ondemand::json_type::string);

    // Reserve sufficient space including SIMDJSON's required padding
    const size_t padded_size = value->raw_json_token().size() + simdjson::SIMDJSON_PADDING;
    // reserve() is good enough, but faststring does additional ASAN POISON on unused bytes
    buffer->resize(padded_size);

    // Perform unescaping directly into the caller's buffer
    uint8_t* output_pos = buffer->data();
    return SimdjsonParser::unescape(value->get_raw_json_string(), output_pos);
}

} // namespace starrocks
