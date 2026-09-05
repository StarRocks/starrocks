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

#include <cstdint>
#include <string>
#include <vector>

namespace starrocks {

enum class TantivyStorageMode : uint8_t {
    LOCAL_DIR = 0,
    COMPOUND = 1,
};

struct TantivyIndexIdentity {
    static constexpr uint8_t KEY_VERSION = 2;

    TantivyStorageMode storage_mode = TantivyStorageMode::LOCAL_DIR;
    std::string canonical_path;
    std::string object_version;
    uint64_t file_size = 0;
    uint32_t compound_format_version = 0;
    int64_t index_id = 0;
    std::string index_suffix;
    std::string field_name;
    std::string tokenizer_name;
    std::string analyzer_digest;
    uint64_t encryption_meta_hash = 0;

    std::string encode() const;
};

enum class TantivyCanonicalQueryType : uint8_t {
    EQUAL = 0,
    MATCH_ANY = 1,
    MATCH_ALL = 2,
    MATCH_PHRASE = 3,
    MATCH_WILDCARD = 4,
};

struct TantivyCanonicalQuery {
    static constexpr uint8_t KEY_VERSION = 1;

    TantivyCanonicalQueryType type = TantivyCanonicalQueryType::EQUAL;
    std::vector<std::string> terms;
    std::string raw_value;
    uint32_t slop = 0;

    std::string encode_with(const TantivyIndexIdentity& identity) const;
};

} // namespace starrocks
