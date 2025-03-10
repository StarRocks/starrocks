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

#include <parquet/types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "io/shared_buffered_input_stream.h"
#include "storage/column_predicate.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"
#include "storage/range.h"
#include "types/logical_type.h"

namespace tparquet {
class ColumnChunk;
class OffsetIndex;
class RowGroup;
} // namespace tparquet

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
class ColumnPredicate;
class ExprContext;
class NullableColumn;
class TIcebergSchemaField;
struct TypeDescriptor;

namespace parquet {
struct ParquetField;
} // namespace parquet
} // namespace starrocks

namespace starrocks::parquet {

struct ColumnOffsetIndexCtx {
    tparquet::OffsetIndex offset_index;
    std::vector<bool> page_selected;
    uint64_t rg_first_row;

    void collect_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                          bool active);

    // be compatible with PARQUET-1850
    bool check_dictionary_page(int64_t data_page_offset) {
        return offset_index.page_locations.size() > 0 && offset_index.page_locations[0].offset > data_page_offset;
    }
};

struct ColumnReaderOptions {
    std::string timezone;
    bool case_sensitive = false;
    int chunk_size = 0;
    HdfsScanStats* stats = nullptr;
    RandomAccessFile* file = nullptr;
    const tparquet::RowGroup* row_group_meta = nullptr;
    uint64_t first_row_index = 0;
    const FileMetaData* file_meta_data = nullptr;
};

class StoredColumnReader;

struct ColumnDictFilterContext {
    constexpr static const LogicalType kDictCodePrimitiveType = TYPE_INT;
    constexpr static const LogicalType kDictCodeFieldType = TYPE_INT;
    // conjunct ctxs for each dict filter column
    std::vector<ExprContext*> conjunct_ctxs;
    // preds transformed from `_conjunct_ctxs` for each dict filter column
    ColumnPredicate* predicate;
    // is output column ? if just used for filter, decode is no need
    bool is_decode_needed;
    SlotId slot_id;
    std::vector<std::string> sub_field_path;
    ObjectPool obj_pool;

public:
    Status rewrite_conjunct_ctxs_to_predicate(StoredColumnReader* reader, bool* is_group_filtered);
};

class ColumnReader {
public:
    explicit ColumnReader(const ParquetField* parquet_field) : _parquet_field(parquet_field) {}
    virtual ~ColumnReader() = default;
    virtual Status prepare() = 0;

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) = 0;

    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual void set_need_parse_levels(bool need_parse_levels) = 0;

    virtual bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                        const std::vector<std::string>& sub_field_path, const size_t& layer) {
        return false;
    }

    virtual Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered,
                                                      const std::vector<std::string>& sub_field_path,
                                                      const size_t& layer) {
        return Status::OK();
    }

    virtual void set_can_lazy_decode(bool can_lazy_decode) {}

    virtual Status filter_dict_column(ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                                      const size_t& layer) {
        return Status::OK();
    }

    virtual Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
        dst->swap_column(*src);
        return Status::OK();
    }

    virtual void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                         int64_t* end_offset, ColumnIOTypeFlags types, bool active) = 0;

    // For field which type is complex, the filed physical_column_index in file meta is not same with the column index
    // in row_group's column metas
    // For example:
    // table schema :
    //  -- col_tinyint tinyint
    //  -- col_struct  struct
    //  ----- name     string
    //  ----- age      int
    // file metadata schema :
    //  -- ParquetField(name=col_tinyint, physical_column_index=0)
    //  -- ParquetField(name=col_struct,physical_column_index=0,
    //                  children=[ParquetField(name=name, physical_column_index=1),
    //                            ParquetField(name=age, physical_column_index=2)])
    // row group column metas:
    //  -- ColumnMetaData(path_in_schema=[col_tinyint])
    //  -- ColumnMetaData(path_in_schema=[col_struct, name])
    //  -- ColumnMetaData(path_in_schema=[col_struct, age])
    // So for complex type, there isn't exist ColumnChunkMetaData
    virtual const tparquet::ColumnChunk* get_chunk_metadata() const { return nullptr; }

    const ParquetField* get_column_parquet_field() const { return _parquet_field; }

    virtual StatusOr<tparquet::OffsetIndex*> get_offset_index(const uint64_t rg_first_row) {
        return Status::NotSupported("get_offset_index is not supported");
    }

    virtual void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) = 0;

    // Return true means filtered, return false means don't filter
    virtual StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                     CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                                     const uint64_t rg_num_rows) const {
        // not implemented, don't filter
        return false;
    }

    // return true means page index filter happened
    // return false means no page index filter happened
    virtual StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                      SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                                      const uint64_t rg_first_row, const uint64_t rg_num_rows) {
        DCHECK(row_ranges->empty());
        return false;
    }

    virtual StatusOr<bool> row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                  CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                                  const uint64_t rg_num_rows) const {
        return false;
    }

    bool check_type_can_apply_bloom_filter(const TypeDescriptor& col_type, const ParquetField& field) const;
    StatusOr<bool> adaptive_judge_if_apply_bloom_filter(int64_t span_size) const;

private:
    // _parquet_field is generated by parquet format, so ParquetField's children order may different from ColumnReader's children.
    const ParquetField* _parquet_field = nullptr;
};

using ColumnReaderPtr = std::unique_ptr<ColumnReader>;

} // namespace starrocks::parquet
