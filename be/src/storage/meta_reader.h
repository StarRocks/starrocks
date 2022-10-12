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

namespace starrocks {

class RuntimeState;

} // namespace starrocks

namespace starrocks::vectorized {

class Tablet;
class SegmentMetaCollecter;

// Params for MetaReader
// mainly include tablet
struct MetaReaderParams {
    MetaReaderParams(){};
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
    const DescriptorTbl* desc_tbl = nullptr;

    int chunk_size = config::vector_chunk_size;
};

struct SegmentMetaCollecterParams {
    std::vector<std::string> fields;
    std::vector<ColumnId> cids;
    std::vector<bool> read_page;
    std::vector<FieldType> field_type;
    int32_t max_cid;
};

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class MetaReader {
public:
    MetaReader();
    ~MetaReader();

    Status init(const MetaReaderParams& read_params);

    TabletSharedPtr tablet() { return _tablet; }

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
    TabletSharedPtr _tablet;
    Version _version;
    std::vector<RowsetSharedPtr> _rowsets;

    bool _is_init;
    bool _has_more;
    int _chunk_size;
    MetaReaderParams _params;

    CollectContext _collect_context;

    Status _init_params(const MetaReaderParams& read_params);

    Status _build_collect_context(const MetaReaderParams& read_params);

    Status _init_seg_meta_collecters(const MetaReaderParams& read_params);

    Status _fill_result_chunk(Chunk* chunk);

    Status _get_segments(const TabletSharedPtr& tablet, const Version& version,
                         std::vector<SegmentSharedPtr>* segments);

    Status _read(Chunk* chunk, size_t n);
};

class SegmentMetaCollecter {
public:
    SegmentMetaCollecter(SegmentSharedPtr segment);
    ~SegmentMetaCollecter();
    Status init(const SegmentMetaCollecterParams* params);
    Status open();
    Status collect(std::vector<vectorized::Column*>* dsts);

public:
    static std::vector<std::string> support_collect_fields;
    static Status parse_field_and_colname(const std::string& item, std::string* field, std::string* col_name);

    using CollectFunc = std::function<Status(ColumnId, vectorized::Column*, FieldType)>;
    std::unordered_map<std::string, CollectFunc> support_collect_func;

private:
    Status _init_return_column_iterators();
    Status _collect(const std::string& name, ColumnId cid, vectorized::Column* column, FieldType type);
    Status _collect_dict(ColumnId cid, vectorized::Column* column, FieldType type);
    Status _collect_max(ColumnId cid, vectorized::Column* column, FieldType type);
    Status _collect_min(ColumnId cid, vectorized::Column* column, FieldType type);
    template <bool is_max>
    Status __collect_max_or_min(ColumnId cid, vectorized::Column* column, FieldType type);
    SegmentSharedPtr _segment;
    std::vector<ColumnIterator*> _column_iterators;
    const SegmentMetaCollecterParams* _params = nullptr;
    std::unique_ptr<RandomAccessFile> _read_file;
    OlapReaderStatistics _stats;
    ObjectPool _obj_pool;
};

} // namespace starrocks::vectorized
