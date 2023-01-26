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

#include "storage/chunk_iterator.h"

namespace starrocks {

// A projection iterator will fetch chunk from its |child| iterator and choosing the columns
// specified in |schema|.
// it is undefined if a column specified in |schema| not exists in the |child|'s schema.
// the relative order of columns in |schema| can be different from that in |child|'s schema, e.g,
// |child| may return columns in the order `c1`, `c2`, `c3`, the projection iterator may return
// columns in the order `c3`, `c1`.
ChunkIteratorPtr new_projection_iterator(const Schema& schema, const ChunkIteratorPtr& child);

} //  namespace starrocks
