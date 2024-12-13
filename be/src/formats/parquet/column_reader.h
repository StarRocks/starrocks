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

<<<<<<< HEAD
#include "formats/parquet/column_converter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/shared_buffered_input_stream.h"
#include "storage/column_predicate.h"
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
<<<<<<< HEAD
} // namespace starrocks

namespace starrocks::parquet {
struct ColumnReaderContext {
    Buffer<uint8_t>* filter = nullptr;
    size_t next_row = 0;
    size_t rows_to_skip = 0;

    void advance(size_t num_rows) { next_row += num_rows; }
};
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

struct ColumnReaderOptions {
    std::string timezone;
    bool case_sensitive = false;
    int chunk_size = 0;
    HdfsScanStats* stats = nullptr;
    RandomAccessFile* file = nullptr;
    const tparquet::RowGroup* row_group_meta = nullptr;
<<<<<<< HEAD
    ColumnReaderContext* context = nullptr;
=======
    uint64_t first_row_index = 0;
    const FileMetaData* file_meta_data = nullptr;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
    // TODO(zc): review this,
    // create a column reader
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         std::unique_ptr<ColumnReader>* output);

    // Create with iceberg schema
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         const TIcebergSchemaField* iceberg_schema_field, std::unique_ptr<ColumnReader>* output);

    // for struct type without schema change
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, std::vector<int32_t>& pos);

    // for schema changed
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, const TIcebergSchemaField* iceberg_schema_field,
                                                  std::vector<int32_t>& pos,
                                                  std::vector<const TIcebergSchemaField*>& iceberg_schema_subfield);

    virtual ~ColumnReader() = default;

    virtual Status prepare_batch(size_t* num_records, Column* column) = 0;
    virtual Status finish_batch() = 0;

    Status next_batch(size_t* num_records, Column* column) {
        RETURN_IF_ERROR(prepare_batch(num_records, column));
        return finish_batch();
    }
=======
    explicit ColumnReader(const ParquetField* parquet_field) : _parquet_field(parquet_field) {}
    virtual ~ColumnReader() = default;
    virtual Status prepare() = 0;

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) = 0;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual void set_need_parse_levels(bool need_parse_levels) = 0;

<<<<<<< HEAD
    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                                   Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    virtual bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                        const std::vector<std::string>& sub_field_path, const size_t& layer) {
        return false;
    }

    virtual Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered,
                                                      const std::vector<std::string>& sub_field_path,
                                                      const size_t& layer) {
        return Status::OK();
    }

<<<<<<< HEAD
    virtual void init_dict_column(ColumnPtr& column, const std::vector<std::string>& sub_field_path,
                                  const size_t& layer) {}
=======
    virtual void set_can_lazy_decode(bool can_lazy_decode) {}
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    virtual Status filter_dict_column(const ColumnPtr& column, Filter* filter,
                                      const std::vector<std::string>& sub_field_path, const size_t& layer) {
        return Status::OK();
    }

<<<<<<< HEAD
    virtual Status fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) {
=======
    virtual Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        dst->swap_column(*src);
        return Status::OK();
    }

<<<<<<< HEAD
    std::unique_ptr<ColumnConverter> converter;
};

=======
    virtual void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                         int64_t* end_offset, ColumnIOType type, bool active) = 0;

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

    // Return true means selected, return false means not selected
    virtual StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                     CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                                     const uint64_t rg_num_rows) const {
        // not implemented, select the whole row group by default
        return true;
    }

    // return true means page index filter happened
    // return false means no page index filter happened
    virtual StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                                      SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                                      const uint64_t rg_first_row, const uint64_t rg_num_rows) {
        DCHECK(row_ranges->empty());
        return false;
    }

private:
    // _parquet_field is generated by parquet format, so ParquetField's children order may different from ColumnReader's children.
    const ParquetField* _parquet_field = nullptr;
};

using ColumnReaderPtr = std::unique_ptr<ColumnReader>;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
} // namespace starrocks::parquet
