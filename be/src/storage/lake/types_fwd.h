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

namespace starrocks {
class RowsetReadOptions;
class Segment;
class TabletSchema;
class DelVector;

class ChunkIterator;
class Schema;
class Column;
class RowsetMetadataPB;

using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;
using RowsetMetadata = RowsetMetadataPB;
using RowsetMetadataPtr = std::shared_ptr<const RowsetMetadata>;
using RowsetMetadataUniquePtr = std::unique_ptr<const RowsetMetadata>;

namespace lake {

class Rowset;
class Tablet;
class CompactionTask;
class LocationProvider;

using RowsetPtr = std::shared_ptr<starrocks::lake::Rowset>;
using SegmentPtr = std::shared_ptr<starrocks::Segment>;
using TabletSchemaPtr = std::shared_ptr<const starrocks::TabletSchema>;
using CompactionTaskPtr = std::shared_ptr<CompactionTask>;
using segment_rowid_t = uint32_t;
using DeletesMap = std::unordered_map<uint32_t, std::vector<segment_rowid_t>>;
using DelVectorPtr = std::shared_ptr<DelVector>;

} // namespace lake

} // namespace starrocks
