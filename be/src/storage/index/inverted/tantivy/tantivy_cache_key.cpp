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

#include "storage/index/inverted/tantivy/tantivy_cache_key.h"

#include <cstring>
#include <type_traits>

namespace starrocks {
namespace {

template <typename T, bool IsEnum = std::is_enum_v<T>>
struct IntegralValueType {
    using type = T;
};

template <typename T>
struct IntegralValueType<T, true> {
    using type = std::underlying_type_t<T>;
};

template <typename T>
void append_fixed(std::string* out, T value) {
    static_assert(std::is_integral_v<T> || std::is_enum_v<T>);
    using ValueType = typename IntegralValueType<T>::type;
    using Unsigned = std::make_unsigned_t<ValueType>;
    Unsigned bits = static_cast<Unsigned>(value);
    for (size_t i = 0; i < sizeof(Unsigned); ++i) {
        out->push_back(static_cast<char>((bits >> (i * 8)) & 0xff));
    }
}

void append_string(std::string* out, const std::string& value) {
    append_fixed<uint32_t>(out, static_cast<uint32_t>(value.size()));
    out->append(value);
}

} // namespace

std::string TantivyIndexIdentity::encode() const {
    std::string key;
    key.reserve(64 + canonical_path.size() + object_version.size() + index_suffix.size() + field_name.size() +
                tokenizer_name.size() + analyzer_digest.size());
    append_fixed<uint8_t>(&key, KEY_VERSION);
    append_fixed<uint8_t>(&key, static_cast<uint8_t>(storage_mode));
    append_string(&key, canonical_path);
    append_string(&key, object_version);
    append_fixed<uint64_t>(&key, file_size);
    append_fixed<uint32_t>(&key, compound_format_version);
    append_fixed<int64_t>(&key, index_id);
    append_string(&key, index_suffix);
    append_string(&key, field_name);
    append_string(&key, tokenizer_name);
    append_string(&key, analyzer_digest);
    append_fixed<uint64_t>(&key, encryption_meta_hash);
    return key;
}

std::string TantivyCanonicalQuery::encode_with(const TantivyIndexIdentity& identity) const {
    std::string key = identity.encode();
    append_fixed<uint8_t>(&key, KEY_VERSION);
    append_fixed<uint8_t>(&key, static_cast<uint8_t>(type));
    append_fixed<uint32_t>(&key, slop);
    append_string(&key, raw_value);
    append_fixed<uint32_t>(&key, static_cast<uint32_t>(terms.size()));
    for (const auto& term : terms) {
        append_string(&key, term);
    }
    return key;
}

} // namespace starrocks
