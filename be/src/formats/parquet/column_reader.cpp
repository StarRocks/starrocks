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

#include "formats/parquet/column_reader.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/utils.h"
#include "gen_cpp/parquet_types.h"
#include "simd/batch_run_counter.h"
#include "storage/column_or_predicate.h"
#include "storage/column_predicate.h"

namespace starrocks::parquet {

void ColumnOffsetIndexCtx::collect_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges,
                                            int64_t* end_offset, bool active) {
    for (size_t i = 0; i < page_selected.size(); i++) {
        if (page_selected[i]) {
            auto r = io::SharedBufferedInputStream::IORange(
                    offset_index.page_locations[i].offset, offset_index.page_locations[i].compressed_page_size, active);
            ranges->emplace_back(r);
            *end_offset = std::max(*end_offset, r.offset + r.size);
        }
    }
}

Status ColumnDictFilterContext::rewrite_conjunct_ctxs_to_predicate(StoredColumnReader* reader,
                                                                   bool* is_group_filtered) {
    // create dict value chunk for evaluation.
    MutableColumnPtr dict_value_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    RETURN_IF_ERROR(reader->get_dict_values(dict_value_column.get()));
    // append a null value to check if null is ok or not.
    dict_value_column->append_default();
    size_t dict_size = dict_value_column->size();
    ColumnPtr result_column = std::move(dict_value_column);
    for (int32_t i = sub_field_path.size() - 1; i >= 0; i--) {
        if (!result_column->is_nullable()) {
            result_column =
                    NullableColumn::create(std::move(result_column), NullColumn::create(result_column->size(), 0));
        }
        Columns columns;
        columns.emplace_back(result_column);
        std::vector<std::string> field_names;
        field_names.emplace_back(sub_field_path[i]);
        result_column = StructColumn::create(std::move(columns), std::move(field_names));
    }

    ChunkPtr dict_value_chunk = std::make_shared<Chunk>();
    dict_value_chunk->append_column(result_column, slot_id);
    Filter filter(dict_size, 1);
    int dict_values_after_filter = 0;
    ASSIGN_OR_RETURN(dict_values_after_filter,
                     ExecNode::eval_conjuncts_into_filter(conjunct_ctxs, dict_value_chunk.get(), &filter));

    // dict column is empty after conjunct eval, file group can be skipped
    if (dict_values_after_filter == 0) {
        *is_group_filtered = true;
        return Status::OK();
    }

    // ---------
    // get dict codes according to dict values pos.
    std::vector<int32_t> dict_codes;
    BatchRunCounter<32> batch_run(filter.data(), 0, filter.size() - 1);
    BatchCount batch = batch_run.next_batch();
    int index = 0;
    while (batch.length > 0) {
        if (batch.AllSet()) {
            for (int32_t i = 0; i < batch.length; i++) {
                dict_codes.emplace_back(index + i);
            }
        } else if (batch.NoneSet()) {
            // do nothing
        } else {
            for (int32_t i = 0; i < batch.length; i++) {
                if (filter[index + i]) {
                    dict_codes.emplace_back(index + i);
                }
            }
        }
        index += batch.length;
        batch = batch_run.next_batch();
    }

    bool null_is_ok = filter[filter.size() - 1] == 1;

    // eq predicate is faster than in predicate
    // TODO: improve not eq and not in
    if (dict_codes.size() == 0) {
        predicate = nullptr;
    } else if (dict_codes.size() == 1) {
        predicate = obj_pool.add(
                new_column_eq_predicate(get_type_info(kDictCodeFieldType), slot_id, std::to_string(dict_codes[0])));
    } else {
        predicate = obj_pool.add(
                new_dictionary_code_in_predicate(get_type_info(kDictCodeFieldType), slot_id, dict_codes, dict_size));
    }

    // deal with if NULL works or not.
    if (null_is_ok) {
        ColumnPredicate* result = nullptr;
        ColumnPredicate* is_null_pred =
                obj_pool.add(new_column_null_predicate(get_type_info(kDictCodeFieldType), slot_id, true));

        if (predicate != nullptr) {
            ColumnOrPredicate* or_pred =
                    obj_pool.add(new ColumnOrPredicate(get_type_info(kDictCodeFieldType), slot_id));
            or_pred->add_child(predicate);
            or_pred->add_child(is_null_pred);
            result = or_pred;
        } else {
            result = is_null_pred;
        }
        predicate = result;
    }

    return Status::OK();
}

bool ColumnReader::check_type_can_apply_bloom_filter(const TypeDescriptor& col_type, const ParquetField& field) const {
    bool appliable = false;
    auto type = col_type.type;
    auto parquet_type = field.physical_type;
    if (type == LogicalType::TYPE_BOOLEAN) {
        if (parquet_type == tparquet::Type::type::BOOLEAN) {
            appliable = true;
        }
    } else if (type == LogicalType::TYPE_DATE) {
        if (parquet_type == tparquet::Type::type::INT32) {
            // appliable = true;
            // TODO:
            // sr._julian - date::UNIX_EPOCH_JULIAN == parquet INT32;
        }
    } else if (type == LogicalType::TYPE_TINYINT || type == LogicalType::TYPE_SMALLINT ||
               type == LogicalType::TYPE_INT) {
        if (parquet_type == tparquet::Type::type::INT32) {
            appliable = true;
        }
        //TODO: if parquet type is int64, convert the val;
    } else if (type == LogicalType::TYPE_BIGINT) {
        if (parquet_type == tparquet::Type::type::INT64) {
            appliable = true;
        }
    } else if (type == LogicalType::TYPE_DOUBLE) {
        if (parquet_type == tparquet::Type::type::DOUBLE) {
            appliable = true;
        }
    } else if (type == LogicalType::TYPE_FLOAT) {
        if (parquet_type == tparquet::Type::type::FLOAT) {
            appliable = true;
        }
    } else if (type == LogicalType::TYPE_VARCHAR || type == LogicalType::TYPE_VARBINARY) {
        if (parquet_type == tparquet::Type::type::BYTE_ARRAY) {
            appliable = true;
        }
        //TODO: FLBA type should check the length and pad space.
    } else if (type == LogicalType::TYPE_CHAR || type == LogicalType::TYPE_BINARY) {
        //TODO: The char type will be padded with space
        //      And we should care about if 'a' == 'a  ' is ture, in SR it is false.
        //      Thus only the flba type can match the SR type char.
        //      And the length of the flba should be the same as the type char.
        //      And any convert should be disabled, because the length cannot change.
    } else if (type == LogicalType::TYPE_DECIMAL32 || type == LogicalType::TYPE_DECIMAL64 ||
               type == LogicalType::TYPE_DECIMAL128 || type == LogicalType::TYPE_DECIMALV2) {
        //TODO: Decimal can be stored as INT32, INT64, BYTE_ARRAY, FLBA in parquet
        //      SR stores the decimalxx as intxx with precision and scale,
        //      First the int type should match the parquet's physical type
        //      And the logical type of sr and parquet's scale and precision should also be the same
        //      Ohterwise, we need to convert the value.
        //      But we should notice that if the convert will cause precision loss, otherwise it should be disabled.
    } else {
        //TODO: Other types like TYPE_TIME, TYPE_DATE_V1, TYPE_DATETIME, TYPE_DATETIME_V1 is stored different with int type in sr
        //    should be converted as int32_t or int64_t.
    }
    //TODO: be/src/formats/parquet/level_builder.cpp have methods of the above data changing.
    return appliable;
}

StatusOr<bool> ColumnReader::adaptive_judge_if_apply_bloom_filter(int64_t span_size) const {
    bool apply_bloom_filter = true;
    if (!config::parquet_reader_enable_adpative_bloom_filter) {
        return apply_bloom_filter = false;
    } else if (get_chunk_metadata() == nullptr) {
        return apply_bloom_filter = true;
    }

    auto& column_metadata = get_chunk_metadata()->meta_data;

    if (column_metadata.__isset.bloom_filter_length) {
        if (1.0 * span_size / column_metadata.num_values * column_metadata.total_compressed_size <=
            column_metadata.bloom_filter_length) {
            return apply_bloom_filter = false;
        }
    }
    return apply_bloom_filter;
}

} // namespace starrocks::parquet
