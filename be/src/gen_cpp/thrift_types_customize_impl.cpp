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

#include "gen_cpp/Types_types.h"

namespace starrocks {

// thrift generates `TUniqueId::operator<()` declaration but leaves unimplemented, it is required in order
// to put TUniqueId into std::set/std::map.
bool TUniqueId::operator<(const TUniqueId& rhs) const {
    return hi < rhs.hi || (hi == rhs.hi && lo < rhs.lo);
}

} // namespace starrocks
