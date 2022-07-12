// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"

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

    [[nodiscard]] StatusOr<ChunkIteratorPtr> read(const vectorized::Schema& schema, const RowsetReadOptions& options);

    [[nodiscard]] bool is_overlapped() const { return metadata().overlapped(); }

    [[nodiscard]] int64_t num_segments() const { return metadata().segments_size(); }

    [[nodiscard]] int64_t num_rows() const { return metadata().num_rows(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] const RowsetMetadata& metadata() const { return *_rowset_metadata; }

private:
    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments);

    // _tablet is owned by TabletReader
    Tablet* _tablet;
    TabletSchemaPtr _tablet_schema;
    RowsetMetadataPtr _rowset_metadata;
};

} // namespace lake
} // namespace starrocks
