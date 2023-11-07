// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment.h"

namespace starrocks {

class RuntimeState;

} // namespace starrocks

namespace starrocks {

class ColumnIterator;
class SegmentMetaCollecter;

// Params for MetaReader
// mainly include tablet
struct MetaReaderParams {
    MetaReaderParams() = default;

    int64_t tablet_id;
    Version version = Version(-1, 0);
    const std::vector<SlotDescriptor*>* slots = nullptr;
    RuntimeState* runtime_state = nullptr;

    const std::map<int32_t, std::string>* id_to_names = nullptr;
    const DescriptorTbl* desc_tbl = nullptr;

    int chunk_size = config::vector_chunk_size;

    void check_validation() const { LOG_IF(FATAL, version.first == -1) << "version is not set. tablet=" << tablet_id; }
};

struct SegmentMetaCollecterParams {
    std::vector<std::string> fields;
    std::vector<ColumnId> cids;
    std::vector<bool> read_page;
    std::vector<LogicalType> field_type;
    int32_t max_cid;
    bool use_page_cache;
    TabletSchemaCSPtr tablet_schema;
};

// MetaReader will implements
// 1. read meta info from segment footer
// 2. read dict info from dict page if column is dict encoding type
class MetaReader {
public:
    MetaReader();
    virtual ~MetaReader() = default;

    Status open();

    virtual Status do_get_next(ChunkPtr* chunk) = 0;

    bool has_more();

    struct CollectContext {
        SegmentMetaCollecterParams seg_collecter_params;
        std::vector<std::unique_ptr<SegmentMetaCollecter>> seg_collecters;
        size_t cursor_idx = 0;

        std::vector<int32_t> result_slot_ids;
    };

protected:
    CollectContext _collect_context;
    bool _is_init;
    bool _has_more;
    // this variable is introduced to solve compatibility issues,
    // see more details in the description of https://github.com/StarRocks/starrocks/pull/17619
    bool _has_count_agg = false;

    MetaReaderParams _params;

    virtual Status _fill_result_chunk(Chunk* chunk);
    void _fill_empty_result(Chunk* chunk);
    Status _read(Chunk* chunk, size_t n);
};

class SegmentMetaCollecter {
public:
    SegmentMetaCollecter(SegmentSharedPtr segment);
    ~SegmentMetaCollecter();
    Status init(const SegmentMetaCollecterParams* params);
    Status open();
    Status collect(std::vector<Column*>* dsts);

public:
    static std::vector<std::string> support_collect_fields;
    static Status parse_field_and_colname(const std::string& item, std::string* field, std::string* col_name);

    using CollectFunc = std::function<Status(ColumnId, Column*, LogicalType)>;
    std::unordered_map<std::string, CollectFunc> support_collect_func;

private:
    Status _init_return_column_iterators();
    Status _collect(const std::string& name, ColumnId cid, Column* column, LogicalType type);
    Status _collect_dict(ColumnId cid, Column* column, LogicalType type);
    Status _collect_max(ColumnId cid, Column* column, LogicalType type);
    Status _collect_min(ColumnId cid, Column* column, LogicalType type);
    Status _collect_count(Column* column, LogicalType type);
    template <bool is_max>
    Status __collect_max_or_min(ColumnId cid, Column* column, LogicalType type);
    SegmentSharedPtr _segment;
    std::vector<std::unique_ptr<ColumnIterator>> _column_iterators;
    const SegmentMetaCollecterParams* _params = nullptr;
    std::unique_ptr<RandomAccessFile> _read_file;
    OlapReaderStatistics _stats;
};

} // namespace starrocks
