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

#include "util/int96.h"

#include "types/large_int_value.h"

namespace starrocks {

std::string int96_t::to_string() const {
    std::stringstream os;
    __int128 val128 = ((__int128)hi << 64) + lo;
    starrocks::operator<<(os, val128);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, const int96_t& val) {
    __int128 val128 = ((__int128)val.hi << 64) + val.lo;
    starrocks::operator<<(os, val128);
    return os;
}

} // namespace starrocks
