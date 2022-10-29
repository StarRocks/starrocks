// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

namespace starrocks {
class RowsetReadOptions;
class Segment;
class TabletSchema;

namespace vectorized {
class ChunkIterator;
class Schema;
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
