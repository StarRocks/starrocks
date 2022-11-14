// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/types_fwd.h"

namespace starrocks::lake {
class Rowset {
public:
    explicit Rowset(Tablet* tablet, RowsetMetadataPtr rowset_metadata, int index);

    ~Rowset();

    [[nodiscard]] StatusOr<ChunkIteratorPtr> read(const vectorized::Schema& schema, const RowsetReadOptions& options);

    [[nodiscard]] bool is_overlapped() const { return metadata().overlapped(); }

    [[nodiscard]] int64_t num_segments() const { return metadata().segments_size(); }

    [[nodiscard]] int64_t num_rows() const { return metadata().num_rows(); }

    [[nodiscard]] int64_t data_size() const { return metadata().data_size(); }

    [[nodiscard]] uint32_t id() const { return metadata().id(); }

    [[nodiscard]] int index() const { return _index; }

    [[nodiscard]] const RowsetMetadata& metadata() const { return *_rowset_metadata; }

private:
    [[nodiscard]] Status load_segments(std::vector<SegmentPtr>* segments, bool fill_cache);

    // _tablet is owned by TabletReader
    Tablet* _tablet;
    RowsetMetadataPtr _rowset_metadata;
    int _index;
};

} // namespace starrocks::lake
