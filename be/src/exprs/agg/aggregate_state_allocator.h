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

#include "column/hash_set.h"
#include "runtime/memory/scope_counting_allocator.h"

namespace starrocks {

template <typename T>
using HashSetWithAggStateAllocator =
        phmap::flat_hash_set<T, StdHash<T>, phmap::priv::hash_default_eq<T>, ScopeCountingAllocator<T>>;

using SliceHashSetWithAggStateAllocator = phmap::flat_hash_set<SliceWithHash, HashOnSliceWithHash, EqualOnSliceWithHash,
                                                               ScopeCountingAllocator<SliceWithHash>>;

template <typename T>
using VectorWithAggStateAllocator = std::vector<T, ScopeCountingAllocator<T>>;

} // namespace starrocks
