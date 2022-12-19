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

#include <iostream>

#include "gen_cpp/olap_common.pb.h"

namespace starrocks {

struct EditVersion {
    unsigned __int128 value = 0;
    EditVersion() = default;
    EditVersion(const EditVersionPB& pb) : EditVersion(pb.major(), pb.minor()) {}
    EditVersion(int64_t major, int64_t minor) { value = (((unsigned __int128)major) << 64) | minor; }
    int64_t major() const { return (int64_t)(value >> 64); }
    int64_t minor() const { return (int64_t)(value & 0xffffffffUL); }
    std::string to_string() const;
    bool operator<(const EditVersion& rhs) const { return value < rhs.value; }
    bool operator==(const EditVersion& rhs) const { return value == rhs.value; }

    void to_pb(EditVersionPB* pb) const {
        pb->set_major(major());
        pb->set_minor(minor());
    }
};

inline std::ostream& operator<<(std::ostream& os, const EditVersion& v) {
    return os << v.to_string();
}

} // namespace starrocks
