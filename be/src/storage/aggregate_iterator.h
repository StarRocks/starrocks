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
#include "storage/row_source_mask.h"

namespace starrocks {

// new_aggregate_iterator create an aggregate iterator based on chunk aggregator.
// the rows from child iterator is sorted by heap merge.
//
// |factor| aggregate factor.
ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, int factor = 0);

// new_aggregate_iterator create an aggregate iterator based on chunk aggregator for vertical compaction.
// the rows from child iterator is sorted by heap merge (key columns) or mask merge (value columns).
//
// |is_key| chunk schema is key columns or value columns.
ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, bool is_key);

} // namespace starrocks
