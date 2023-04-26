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

#include <memory>
#include <vector>

#include "storage/chunk_iterator.h"

namespace starrocks {
class Segment;

class ColumnPredicate;
class Schema;
class SegmentReadOptions;

ChunkIteratorPtr new_segment_iterator(const std::shared_ptr<Segment>& segment, const Schema& schema,
                                      const SegmentReadOptions& options);

} // namespace starrocks
