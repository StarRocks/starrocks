// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

namespace starrocks {
class RowsetReadOptions;
class Segment;
class TabletSchema;

namespace vectorized {
class ChunkIterator;
class VectorizedSchema;
} // namespace vectorized

namespace lake {

class Rowset;
class Tablet;
class CompactionTask;
class LocationProvider;

using ChunkIteratorPtr = std::shared_ptr<vectorized::ChunkIterator>;
using RowsetMetadata = RowsetMetadataPB;
using RowsetMetadataPtr = std::shared_ptr<const starrocks::lake::RowsetMetadata>;
using RowsetPtr = std::shared_ptr<starrocks::lake::Rowset>;
using SegmentPtr = std::shared_ptr<starrocks::Segment>;
using TabletSchemaPtr = std::shared_ptr<const starrocks::TabletSchema>;
using CompactionTaskPtr = std::shared_ptr<CompactionTask>;

} // namespace lake

} // namespace starrocks
