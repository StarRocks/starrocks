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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "base/hash/hash_std.hpp"

namespace starrocks {

enum CompactionType { BASE_COMPACTION = 1, CUMULATIVE_COMPACTION = 2, UPDATE_COMPACTION = 3, INVALID_COMPACTION };

inline std::string to_string(CompactionType type) {
    switch (type) {
    case BASE_COMPACTION:
        return "base";
    case CUMULATIVE_COMPACTION:
        return "cumulative";
    case UPDATE_COMPACTION:
        return "update";
    case INVALID_COMPACTION:
        return "invalid";
    default:
        return "unknown";
    }
}

// <start_version_id, end_version_id>, such as <100, 110>
//using Version = std::pair<TupleVersion, TupleVersion>;

struct Version {
    int64_t first{0};
    int64_t second{0};

    Version(int64_t first_, int64_t second_) : first(first_), second(second_) {}
    Version() = default;

    Version& operator=(const Version& version) = default;

    friend std::ostream& operator<<(std::ostream& os, const Version& version);

    bool operator!=(const Version& rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator==(const Version& rhs) const { return first == rhs.first && second == rhs.second; }

    bool contains(const Version& other) const { return first <= other.first && second >= other.second; }

    bool operator<(const Version& rhs) const {
        if (second < rhs.second) {
            return true;
        } else if (second > rhs.second) {
            return false;
        } else {
            // version with bigger first will be smaller.
            // design this for fast search in _contains_version() in tablet.cpp.
            return first > rhs.first;
        }
    }
};

typedef std::vector<Version> Versions;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << "[" << version.first << "-" << version.second << "]";
}

// used for hash-struct of hash_map<Version, Rowset*>.
struct HashOfVersion {
    size_t operator()(const Version& version) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
        seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
        return seed;
    }
};

} // namespace starrocks
