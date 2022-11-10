// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/lake/tablet.h"
#include "storage/meta_reader.h"


namespace starrocks::vectorized {
// Params for MetaReader
// mainly include tablet
struct LakeMetaReaderParams : MetaReaderParams {
    LakeMetaReaderParams(){};
    StatusOr<lake::Tablet> tablet;
    std::shared_ptr<const TabletSchema> tablet_schema;
};

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class LakeMetaReader final : public MetaReader {
public:
    LakeMetaReader();
    ~LakeMetaReader();

    Status init(const LakeMetaReaderParams& read_params);

    lake::Tablet tablet() { return _tablet.value(); }

    Status do_get_next(ChunkPtr* chunk) override;

private:
    StatusOr<lake::Tablet> _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    std::vector<lake::Rowset> _rowsets;

    LakeMetaReaderParams _params;

    Status _init_params(const LakeMetaReaderParams& read_params);

    Status _build_collect_context(const LakeMetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const LakeMetaReaderParams& read_params);

    Status _get_segments(lake::Tablet tablet, const Version& version,
                         std::vector<SegmentSharedPtr>* segments);

    Status _fill_result_chunk(Chunk* chunk) override;
};

} // namespace starrocks::vectorized
