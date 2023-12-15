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

#include "exprs/table_function/list_rowsets.h"

#include <algorithm>

#include "column/binary_column.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "common/config.h"
#include "gutil/casts.h"
#include "json2pb/pb_to_json.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "types/logical_type.h"

namespace starrocks {

using TabletMetadata = lake::TabletMetadata;
using TabletMetadataPtr = lake::TabletMetadataPtr;
using RowsetMetadataPB = lake::RowsetMetadataPB;

static void append_bigint(ColumnPtr& col, int64_t value) {
    [[maybe_unused]] auto n = col->append_numbers(&value, sizeof(value));
    DCHECK_EQ(1, n);
};

static void fill_rowset_row(Columns& columns, const RowsetMetadataPB& rowset) {
    DCHECK_EQ(6, columns.size());
    if (UNLIKELY(!rowset.has_id())) {
        columns[0] = NullableColumn::wrap_if_necessary(columns[0]);
        columns[0]->append_nulls(1);
    } else {
        append_bigint(columns[0], rowset.id());
    }

    append_bigint(columns[1], rowset.segments_size());

    if (UNLIKELY(!rowset.has_num_rows())) {
        columns[2] = NullableColumn::wrap_if_necessary(columns[2]);
        columns[2]->append_nulls(1);
    } else {
        append_bigint(columns[2], rowset.num_rows());
    }

    if (UNLIKELY(!rowset.has_data_size())) {
        columns[3] = NullableColumn::wrap_if_necessary(columns[3]);
        columns[3]->append_nulls(1);
    } else {
        append_bigint(columns[3], rowset.data_size());
    }

    if (UNLIKELY(!rowset.has_overlapped())) {
        columns[4] = NullableColumn::wrap_if_necessary(columns[4]);
        columns[4]->append_nulls(1);
    } else {
        columns[4]->append_datum(Datum(rowset.overlapped()));
    }

    if (LIKELY(!rowset.has_delete_predicate())) {
        columns[5]->append_nulls(1);
    } else {
        // todo: convert DeletePredicatePB to SQL string.
        json2pb::Pb2JsonOptions opts;
        opts.pretty_json = false;
        std::string json;
        (void)json2pb::ProtoMessageToJson(rowset.delete_predicate(), &json, opts);
        (void)columns[5]->append_strings({json});
    }
}

std::pair<Columns, UInt32Column::Ptr> ListRowsets::process(TableFunctionState* base_state) const {
    auto state = down_cast<MyState*>(base_state);

    if (UNLIKELY(state->get_columns().size() != 2)) {
        // This should never happen in practice, but still check this for safety.
        state->set_status(Status::InternalError("unexpected argument count of function \"list_rowsets\""));
        return {};
    }

    auto tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    auto max_column_size = config::vector_chunk_size;
    auto arg_tablet_id = ColumnViewer<TYPE_BIGINT>(state->get_columns()[0]);
    auto arg_tablet_version = ColumnViewer<TYPE_BIGINT>(state->get_columns()[1]);
    auto curr_row = state->processed_rows();
    auto num_rows = state->input_rows();
    auto row_offset = state->get_offset();
    auto offsets = UInt32Column::create();
    auto result = Columns{
            Int64Column::create(),                                    // id
            Int64Column::create(),                                    // segments
            Int64Column::create(),                                    // rows
            Int64Column::create(),                                    // size
            BooleanColumn::create(),                                  // overlapped
            NullableColumn::wrap_if_necessary(BinaryColumn::create()) // delete_predicate
    };

    while (result[0]->size() < max_column_size && curr_row < num_rows) {
        offsets->append_datum(Datum((uint32_t)result[0]->size()));
        if (LIKELY(row_offset == 0)) {
            if (UNLIKELY(arg_tablet_id.is_null(curr_row))) {
                state->set_status(Status::InvalidArgument("list_rowsets: tablet id cannot be NULL"));
                break;
            }
            if (UNLIKELY(arg_tablet_version.is_null(curr_row))) {
                state->set_status(Status::InvalidArgument("list_rowsets: tablet version cannot be NULL"));
                break;
            }
            auto tablet_id = arg_tablet_id.value(curr_row);
            auto tablet_version = arg_tablet_version.value(curr_row);
            auto metadata_or = tablet_mgr->get_tablet_metadata(tablet_id, tablet_version);
            if (!metadata_or.ok()) {
                state->set_status(std::move(metadata_or).status());
                break;
            }
            state->metadata = std::move(metadata_or).value();
        } else {
            DCHECK(state->metadata != nullptr);
        }

        auto& metadata = state->metadata;

        auto count = std::min<int64_t>(max_column_size - result[0]->size(), metadata->rowsets_size() - row_offset);

        if (result[0]->size() == 0) {
            for (auto& col : result) {
                col->reserve(metadata->rowsets_size());
            }
        }

        for (int64_t i = 0; i < count; i++) {
            const auto& rowset = metadata->rowsets(row_offset + i);
            fill_rowset_row(result, rowset);
        }

        if (row_offset + count < metadata->rowsets_size()) {
            row_offset += count;
        } else {
            curr_row++;
            row_offset = 0;
            state->metadata = nullptr;
        }
        state->set_processed_rows(curr_row);
        state->set_offset(row_offset);
    }
    offsets->append_datum(Datum((uint32_t)result[0]->size()));

    return std::make_pair(std::move(result), std::move(offsets));
}

} // namespace starrocks