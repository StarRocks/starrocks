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

namespace starrocks {

class RuntimeState;

} // namespace starrocks

namespace starrocks::vectorized {

class SegmentMetaCollecter;
struct SegmentMetaCollecterParams;

// Params for MetaReader
// mainly include tablet
struct LakeMetaReaderParams {
    LakeMetaReaderParams(){};
    StatusOr<lake::Tablet> tablet;
    std::shared_ptr<const TabletSchema> tablet_schema;
    Version version = Version(-1, 0);
    const std::vector<SlotDescriptor*>* slots = nullptr;
    RuntimeState* runtime_state = nullptr;
    void check_validation() const {
        if (UNLIKELY(version.first == -1)) {
            LOG(FATAL) << "version is not set. tablet=" << tablet->id();
        }
    }

    const std::map<int32_t, std::string>* id_to_names = nullptr;
    const DescriptorTbl* desc_tbl = nullptr;

    int chunk_size = config::vector_chunk_size;
};

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class LakeMetaReader {
public:
    LakeMetaReader();
    ~LakeMetaReader();

    Status init(const LakeMetaReaderParams& read_params);

    lake::Tablet tablet() { return _tablet.value(); }

    Status open();

    Status do_get_next(ChunkPtr* chunk);

    bool has_more();

    struct CollectContext {
        SegmentMetaCollecterParams seg_collecter_params;
        std::vector<std::unique_ptr<SegmentMetaCollecter>> seg_collecters;
        size_t cursor_idx = 0;

        std::vector<int32_t> result_slot_ids;
    };

private:
    StatusOr<lake::Tablet> _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;
    Version _version;
    std::vector<lake::Rowset> _rowsets;

    bool _is_init;
    bool _has_more;
    int _chunk_size;
    LakeMetaReaderParams _params;

    CollectContext _collect_context;

    Status _init_params(const LakeMetaReaderParams& read_params);

    Status _build_collect_context(const LakeMetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const LakeMetaReaderParams& read_params);

    Status _fill_result_chunk(Chunk* chunk);

    Status _get_segments(lake::Tablet tablet, const Version& version,
                         std::vector<SegmentSharedPtr>* segments);

    Status _read(Chunk* chunk, size_t n);
};

} // namespace starrocks::vectorized
