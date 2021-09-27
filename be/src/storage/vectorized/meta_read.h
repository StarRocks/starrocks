// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#pragma once

#include <vector>
#include <string>

#include "storage/tablet.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/segment.h"

#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;

} // namespace starrocks

namespace starrocks::vectorized {

class Tablet;
class SegmentMetaCollecter;

// Params for MetaReader
// mainly include tablet
struct MetaReaderParams {
    MetaReaderParams() {};
    TabletSharedPtr tablet;
    Version version = Version(-1, 0);
    const std::vector<SlotDescriptor*>* slots = nullptr;
    RuntimeState* runtime_state = nullptr;
    void check_validation() const {
        if (UNLIKELY(version.first == -1)) {
            LOG(FATAL) << "version is not set. tablet=" << tablet->full_name();
        }
    }

    const std::map<int32_t, std::string>* id_to_names = nullptr;
    const DescriptorTbl*  desc_tbl = nullptr;

    int chunk_size = 1024;
};

struct SegmentMetaCollecterParams {
    std::vector<std::string> fields;
    std::vector<ColumnId> cids;
    std::vector<bool> read_page;
    std::vector<FieldType> field_type;
    int32_t max_cid;
};

class MetaReader {
public:
    MetaReader();
    ~MetaReader();

    Status init(const MetaReaderParams& read_params);

    TabletSharedPtr tablet() { return _tablet; }

    Status do_get_next(ChunkPtr* chunk);

    bool has_more();
    
private:
    TabletSharedPtr _tablet;
    SegmentMetaCollecterParams _seg_collecter_params;
    std::vector<SegmentMetaCollecter*> _seg_collecters;
    SegmentMetaCollecter* _collecter_cursor = nullptr;
    size_t _cursor_idx = 0;
    Version _version;
    ObjectPool _obj_pool;

    bool _is_init;
    bool _has_more;
    int _chunk_size;
    MetaReaderParams _params;

    std::vector<std::string> _collect_names;
    std::vector<ColumnId> _collect_col_ids;
    std::vector<int32_t>  _return_slot_ids;

    Status _init_params(const MetaReaderParams& read_params);
    Status _init_seg_meta_collecters(const MetaReaderParams& read_params);

    Status _get_segments(const TabletSharedPtr& tablet, const Version& version,
                         std::vector<segment_v2::SegmentSharedPtr>* segments);

    void _next_cursor();

    Status _read(Chunk* chunk, size_t n);
};

class SegmentMetaCollecter {
public:
    SegmentMetaCollecter(segment_v2::SegmentSharedPtr segment);
    ~SegmentMetaCollecter();
    Status init(const SegmentMetaCollecterParams* params);
    Status collect(std::vector<vectorized::Column*>* dsts);

public:

static std::vector<std::string> support_collect_fields;
static Status trait_field_and_col_name(const std::string& item, std::string* field, std::string* col_name);

private:
    Status _init_return_column_iterators();
    Status _collect(const std::string& name, ColumnId cid, vectorized::Column*column, FieldType type);
    Status _collect_dict(ColumnId cid, vectorized::Column* column, FieldType type);
    Status _collect_max(ColumnId cid, vectorized::Column* column, FieldType type);
    Status _collect_min(ColumnId cid, vectorized::Column* column, FieldType type);
    template<bool flag>
    Status __collect_max_or_min(ColumnId cid, vectorized::Column* column, FieldType type);
    segment_v2::SegmentSharedPtr _segment;
    std::vector<ColumnIterator*> _column_iterators;
    const SegmentMetaCollecterParams* _params = nullptr;
    std::unique_ptr<fs::ReadableBlock> _rblock;
    OlapReaderStatistics _stats;
    ObjectPool _obj_pool;

    
};

} // namespace starrocks::vectorized