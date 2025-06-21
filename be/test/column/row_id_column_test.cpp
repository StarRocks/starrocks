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

#include "column/row_id_column.h"

#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/datum.h"
#include "serde/column_array_serde.h"
#include "storage/range.h"
#include "storage/rowset/global_rowid_column_iterator.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(RowIdColumnTest, test_create) {
    auto row_id_column = RowIdColumn::create();
    ASSERT_TRUE(row_id_column->is_global_row_id());
    ASSERT_EQ(row_id_column->size(), 0);

    auto be_ids = UInt32Column::create();
    be_ids->append_datum(1);
    auto seg_ids = UInt32Column::create();
    seg_ids->append_datum(1);
    auto ord_ids = UInt32Column::create();
    ord_ids->append_datum(1);

    row_id_column = RowIdColumn::create(be_ids, seg_ids, ord_ids);
    ASSERT_EQ(row_id_column->size(), 1);
    ASSERT_EQ(row_id_column->capacity(), 1);
    ASSERT_EQ(row_id_column->type_size(), 12);
    ASSERT_EQ(row_id_column->byte_size(), 12);
    ASSERT_EQ(row_id_column->byte_size(0, 1), 12);
    ASSERT_EQ(row_id_column->byte_size(0), 12);

    row_id_column->assign(3, 0);
    ASSERT_EQ(row_id_column->size(), 3);
}

PARALLEL_TEST(RowIdColumnTest, test_append_data) {
    auto row_id_column = RowIdColumn::create();
    row_id_column->reserve(3);
    row_id_column->append_datum(DatumRowId(1, 1, 1));
    row_id_column->append_datum(DatumRowId(2, 2, 2));
    row_id_column->append_datum(DatumRowId(3, 3, 3));
    auto src_column = row_id_column->clone();

    ASSERT_EQ(row_id_column->size(), 3);
    ASSERT_EQ(row_id_column->get(0), DatumRowId(1, 1, 1));
    ASSERT_EQ(row_id_column->get(1), DatumRowId(2, 2, 2));
    ASSERT_EQ(row_id_column->get(2), DatumRowId(3, 3, 3));
    ASSERT_EQ(row_id_column->debug_string(), "(1,1,1),(2,2,2),(3,3,3)");

    row_id_column->remove_first_n_values(1);
    ASSERT_EQ(row_id_column->size(), 2);
    ASSERT_EQ(row_id_column->get(0), DatumRowId(2, 2, 2));
    ASSERT_EQ(row_id_column->get(1), DatumRowId(3, 3, 3));

    row_id_column->append(*row_id_column, 0, 1);
    ASSERT_EQ(row_id_column->size(), 3);
    ASSERT_EQ(row_id_column->get(2), DatumRowId(2, 2, 2));

    std::vector<uint32_t> indexes = {1};
    row_id_column->append_selective(*src_column, indexes.data(), 0, 1);
    ASSERT_EQ(row_id_column->size(), 4);
    ASSERT_EQ(row_id_column->get(3), DatumRowId(2, 2, 2));

    row_id_column->resize(1);
    row_id_column->append_value_multiple_times(*src_column, 0, 2);
    ASSERT_EQ(row_id_column->size(), 3);
    ASSERT_EQ(row_id_column->get(1), DatumRowId(1, 1, 1));

    row_id_column->append_default();
    ASSERT_EQ(row_id_column->size(), 4);
    ASSERT_EQ(row_id_column->get(3), DatumRowId(0, 0, 0));
}

PARALLEL_TEST(RowIdColumnTest, test_serialize) {
    auto row_id_column = RowIdColumn::create();
    row_id_column->reserve(3);
    row_id_column->append_datum(DatumRowId(1, 1, 1));
    row_id_column->append_datum(DatumRowId(2, 2, 2));
    row_id_column->append_datum(DatumRowId(3, 3, 3));

    ASSERT_EQ(row_id_column->max_one_element_serialize_size(), 12);
    ASSERT_EQ(serde::ColumnArraySerde::max_serialized_size(*row_id_column), 48);
    uint8_t buffer[48];
    const uint8_t* pos = serde::ColumnArraySerde::serialize(*row_id_column, buffer);
    ASSERT_EQ(pos - buffer, 48);

    auto ret = RowIdColumn::create();
    pos = serde::ColumnArraySerde::deserialize(buffer, ret.get());
    ASSERT_EQ(pos - buffer, 48);
    ASSERT_EQ(ret->size(), 3);
    ASSERT_EQ(ret->debug_string(), "(1,1,1),(2,2,2),(3,3,3)");
}

PARALLEL_TEST(RowIdColumnTest, test_column_iterator) {
    const uint32_t mock_be_id = 1;
    const uint32_t mock_seg_id = 2;
    auto iter = std::make_unique<GlobalRowIdColumnIterator>(mock_be_id, mock_seg_id);
    ASSERT_OK(iter->init({}));
    ASSERT_OK(iter->seek_to_first());
    ASSERT_EQ(iter->get_current_ordinal(), 0);
    ASSERT_OK(iter->seek_to_ordinal(10));
    ASSERT_EQ(iter->get_current_ordinal(), 10);

    auto result = RowIdColumn::create();
    size_t n = 5;
    ASSERT_OK(iter->next_batch(&n, result.get()));
    ASSERT_EQ(result->size(), 5);
    ASSERT_EQ(result->debug_string(), "(1,2,10),(1,2,11),(1,2,12),(1,2,13),(1,2,14)");
    SparseRange<rowid_t> range(15, 16);
    result->reset_column();
    ASSERT_OK(iter->next_batch(range, result.get()));
    ASSERT_EQ(result->debug_string(), "(1,2,15)");
    std::vector<rowid_t> rowids{1, 2, 3};
    result->reset_column();
    ASSERT_OK(iter->fetch_values_by_rowid(rowids.data(), 3, result.get()));
    ASSERT_EQ(result->debug_string(), "(1,2,1),(1,2,2),(1,2,3)");
}
} // namespace starrocks