// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"

namespace starrocks {
class Segment;
class TabletSchema;

namespace vectorized {
class ChunkIterator;
class RowsetReadOptions;
class Schema;
} // namespace vectorized

namespace lake {

class Rowset;
class Tablet;

using ChunkIteratorPtr = std::shared_ptr<vectorized::ChunkIterator>;
using RowsetMetadata = RowsetMetadataPB;
using RowsetMetadataPtr = std::shared_ptr<const RowsetMetadata>;
using RowsetPtr = std::shared_ptr<Rowset>;
using SegmentPtr = std::shared_ptr<Segment>;
using TabletSchemaPtr = std::shared_ptr<const TabletSchema>;

class Rowset {
public:
    explicit Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata);
    ~Rowset();

    Status init();

    StatusOr<std::vector<ChunkIteratorPtr>> get_segment_iterators(const vectorized::Schema& schema,
                                                                  const vectorized::RowsetReadOptions& options);

private:
    bool is_overlapped() const { return _rowset_metadata->overlapped(); }

    int64_t num_segments() const { return _rowset_metadata->segments_size(); }

    Status load_segments();

private:
    // _tablet is owned by TabletReader
    Tablet* _tablet;
    std::string _group;
    TabletSchemaPtr _tablet_schema;
    RowsetMetadataPtr _rowset_metadata;

    std::vector<SegmentPtr> _segments;
};

} // namespace lake
} // namespace starrocks