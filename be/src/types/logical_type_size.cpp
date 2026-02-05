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

#include "column/type_traits.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

struct FixedLengthTypeGetter {
    template <LogicalType ltype>
    size_t operator()() {
        return RunTimeFixedTypeLength<ltype>::value;
    }
};

size_t get_size_of_fixed_length_type(LogicalType ltype) {
    return type_dispatch_all(ltype, FixedLengthTypeGetter());
}

} // namespace starrocks
