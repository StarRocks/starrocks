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

} // namespace starrocks::parquet
