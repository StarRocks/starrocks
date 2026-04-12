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

#include "column/adaptive_offsets.h"

namespace starrocks {

void AdaptiveOffsets::_promote_to_large() {
    DCHECK(!_large);
    size_t n = _u32.size();
    _u64.resize(n);
    for (size_t i = 0; i < n; ++i) {
        _u64[i] = _u32[i];
    }
    _u32.clear();
    _u32.shrink_to_fit();
    _large = true;
}

} // namespace starrocks
