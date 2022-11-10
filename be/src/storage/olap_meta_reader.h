// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet.h"
#include "storage/meta_reader.h"


namespace starrocks::vectorized {
class Tablet;
// Params for MetaReader
// mainly include tablet
struct OlapMetaReaderParams : MetaReaderParams {
    OlapMetaReaderParams(){};
    TabletSharedPtr tablet;
    void check_validation() const {
        if (UNLIKELY(version.first == -1)) {
            LOG(FATAL) << "version is not set. tablet=" << tablet->full_name();
        }
    }
};

class OlapMetaReader final : public MetaReader {
public:
    OlapMetaReader();
    ~OlapMetaReader();

    Status init(const OlapMetaReaderParams& read_params);

    TabletSharedPtr tablet() { return _tablet; }

    Status do_get_next(ChunkPtr* chunk) override;

private:
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _rowsets;

    OlapMetaReaderParams _params;

    Status _init_params(const OlapMetaReaderParams& read_params);

    Status _build_collect_context(const OlapMetaReaderParams& read_params);

    Status _fill_result_chunk(Chunk* chunk) override;

    Status _init_seg_meta_collecters(const OlapMetaReaderParams& read_params);

    Status _get_segments(const TabletSharedPtr& tablet, const Version& version,
                         std::vector<SegmentSharedPtr>* segments);
};

} // namespace starrocks::vectorized


