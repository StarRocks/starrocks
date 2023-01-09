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

#include <vector>

#include "storage/chunk_iterator.h"

namespace starrocks {

// new_union_iterator create an iterator that will union the results of |children|.
// |children| must not empty and iterators in it must have the same schema.
ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children);

} // namespace starrocks
