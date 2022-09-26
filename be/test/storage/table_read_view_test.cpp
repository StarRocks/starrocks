// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/table_read_view.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include "column/datum_convert.h"
#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/datum_row.h"
#include "storage/kv_store.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "storage/wrapper_field.h"
#include "testutil/assert.h"

#include <iostream>

namespace starrocks {

using DatumRowVector = std::vector<DatumRow>;
using RowSharedPtrVector = std::vector<RowSharedPtr>;

class TableReadViewTest : public testing::Test {
public:

    static void create_row(vector<DatumRow>& data, int64_t c0, int32_t c1, int32_t c2, int16_t c3, int32_t c4) {
        DatumRow row(5);
        row.set_int64(0, c0);
        row.set_int32(1, c1);
        row.set_int32(2, c2);
        row.set_int16(3, c3);
        row.set_int32(4, c4);
        data.push_back(row);
    }

    void create_rowset(const TabletSharedPtr& tablet, const vector<DatumRow>& data, int version) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = &tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema_to_format_v2(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, data.size());
        auto& cols = chunk->columns();
        for (const DatumRow& row : data) {
            DatumRow tmp_row(row.size());
            for (size_t i = 0; i < row.size(); i++) {
                cols[i]->append_datum(row.get_datum(i));
            }
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        auto row_set = *writer->build();
        ASSERT_TRUE(_tablet->rowset_commit(version, row_set).ok());
        ASSERT_EQ(version, _tablet->updates()->max_version());
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn pk1;
        pk1.column_name = "pk1_bigint";
        pk1.__set_is_key(true);
        pk1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(pk1);
        TColumn pk2;
        pk2.column_name = "pk2_int";
        pk2.__set_is_key(true);
        pk2.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(pk2);
        TColumn pk3;
        pk3.column_name = "pk3_int";
        pk3.__set_is_key(true);
        pk3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(pk3);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void SetUp() override { }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    TabletSharedPtr _tablet;
};

void collect_row_iterator_result(StatusOr<RowIteratorSharedPtr> status_or, DatumRowVector& result) {
    if (!status_or.status().ok()) {
        ASSERT_TRUE(status_or.status().is_end_of_file());
    }
    RowIteratorSharedPtr iterator = status_or.value();
    ASSERT_TRUE(iterator != nullptr);
    StatusOr<RowSharedPtr> row_status = iterator->get_next();
    while (row_status.status().ok()) {
        RowSharedPtr row = row_status.value();
        ASSERT_TRUE(row != nullptr);
        result.push_back(DatumRow(row->size()));
        for (size_t i = 0; i < row->size(); i++) {
            result.back().set_datum(i, row->get_datum(i));
        }
        row_status = iterator->get_next();
    }
    ASSERT_TRUE(row_status.status().is_end_of_file());
    iterator->close();
}

void collect_chunk_iterator_result(StatusOr<ChunkIteratorPtr> status_or, DatumRowVector& result) {
    if (!status_or.status().ok()) {
        ASSERT_TRUE(status_or.status().is_end_of_file());
    }
    ChunkIteratorPtr iterator = status_or.value();
    ASSERT_TRUE(iterator != nullptr);
    std::shared_ptr<vectorized::Chunk> chunk = ChunkHelper::new_chunk(iterator->schema(), 2);
    Status status = iterator->get_next(chunk.get());
    while (status.ok()) {
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            result.push_back(DatumRow(chunk->num_columns()));
            for (size_t j = 0; j < chunk->num_columns(); j++) {
                result.back().set_datum(j, chunk->columns().at(j)->get(i));
            }
        }
        chunk.reset();
        status = iterator->get_next(chunk.get());
    }
    ASSERT_TRUE(status.is_end_of_file());
    iterator->close();
}

void verify_result(const vectorized::Schema& schema, DatumRowVector& expect_result, DatumRowVector& actual_result) {
    ASSERT_EQ(expect_result.size(), actual_result.size());
    for (size_t i = 0; i < expect_result.size(); i++) {
        DatumRow& expect = expect_result[i];
        DatumRow& actual = actual_result[i];
        ASSERT_EQ(schema.num_fields(), actual.size());
        ASSERT_EQ(expect.size(), actual.size());
        for (size_t j = 0; j < expect.size(); j++) {
            int cmp = schema.field(j)->type()->cmp(expect.get_datum(j), actual.get_datum(j));
            ASSERT_EQ(0, cmp);
        }
    }
}

DatumRowVector get_expect_output(DatumRowVector& rows, std::vector<int> output_index) {
    DatumRowVector result;
    for (DatumRow& row : rows) {
        DatumRow output(output_index.size());
        for (size_t i = 0; i < output_index.size(); i++) {
            output.set_datum(i, row.get_datum(output_index[i]));
        }
        result.push_back(output);
    }
    return result;
}

TEST_F(TableReadViewTest, test_basic_read) {
    srand(GetCurrentTimeMicros());
    _tablet = create_tablet(rand(), rand());

    // 1. generate data, and write to tablet
    // TODO test more types
    DatumRowVector rows;
    create_row(rows, (int64_t) 1, (int32_t) 1, (int32_t) 1, (int16_t) 1, (int32_t) 1);
    create_row(rows, (int64_t) 1, (int32_t) 1, (int32_t) 2, (int16_t) 1, (int32_t) 1);
    create_row(rows, (int64_t) 1, (int32_t) 1, (int32_t) 3, (int16_t) 1, (int32_t) 1);
    create_rowset(_tablet, rows, 2);
    create_row(rows, (int64_t) 2, (int32_t) 1, (int32_t) 1, (int16_t) 1, (int32_t) 1);
    create_row(rows, (int64_t) 2, (int32_t) 1, (int32_t) 2, (int16_t) 2, (int32_t) 1);
    create_row(rows, (int64_t) 2, (int32_t) 1, (int32_t) 3, (int16_t) 3, (int32_t) 1);
    create_rowset(_tablet, rows, 3);
    create_row(rows, (int64_t) 3, (int32_t) 3, (int32_t) 4, (int16_t) 1, (int32_t) 1);
    create_row(rows, (int64_t) 4, (int32_t) 2, (int32_t) 3, (int16_t) 2, (int32_t) 1);
    create_row(rows, (int64_t) 5, (int32_t) 1, (int32_t) 5, (int16_t) 3, (int32_t) 1);
    create_rowset(_tablet, rows, 4);

    // 2. build TableReadViewParams, and create TableReadView
    TableReadViewParams params;
    // 2.1 data version to read
    params.version = Version(0, 4);

    vectorized::Schema tablet_schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    // 2.2 sort key schema
    params.sort_key_schema.append(tablet_schema.field(0));
    params.sort_key_schema.append(tablet_schema.field(1));
    params.sort_key_schema.append(tablet_schema.field(2));

    // 2.3 output schema
    params.output_schema.append(tablet_schema.field(0));
    params.output_schema.append(tablet_schema.field(2));
    params.output_schema.append(tablet_schema.field(4));
    // 2.4 create TableReadView
    TableReadViewSharedPtr table_read_view = StorageEngine::instance()->get_table(_tablet->tablet_id())->create_table_read_view(params);

    DatumRowVector expect_results = get_expect_output(rows, std::vector<int>{0, 2, 4});
    ReadOption read_option;
    // 3. test to look up a sort key
    for (size_t i = 0; i < rows.size(); i++) {
        DatumRow& row = rows[i];
        DatumRow key(3);
        for (size_t j = 0; j < 3; j++) {
            key.set_datum(j, row.get_datum(j));
        }
        DatumRowVector expect{expect_results[i]};
        DatumRowVector result;
        // iterate result Row by Row
        collect_row_iterator_result(table_read_view->get(key, read_option), result);
        verify_result(params.output_schema, expect, result);
        DatumRowVector chunk_result;
        // iterate result Chunk by Chunk
        collect_chunk_iterator_result(table_read_view->get_chunk(key, read_option), chunk_result);
        verify_result(params.output_schema, expect, chunk_result);
    }

    // 4. test to look up a prefix key
    DatumRow prefix_key1(2);
    prefix_key1.set_int64(0, (int64_t) 1);
    prefix_key1.set_int32(1, (int32_t) 1);
    DatumRowVector expect1{expect_results[0], expect_results[1], expect_results[2]};
    DatumRowVector result1;
    collect_row_iterator_result(table_read_view->get(prefix_key1, read_option), result1);
    verify_result(params.output_schema, expect1, result1);
    DatumRowVector chunk_result1;
    collect_chunk_iterator_result(table_read_view->get_chunk(prefix_key1, read_option), chunk_result1);
    verify_result(params.output_schema, expect1, chunk_result1);

    DatumRow prefix_key2(2);
    prefix_key2.set_int64(0, (int64_t) 2);
    prefix_key2.set_int32(1, (int32_t) 1);
    DatumRowVector expect2{expect_results[3], expect_results[4], expect_results[5]};
    DatumRowVector result2;
    collect_row_iterator_result(table_read_view->get(prefix_key2, read_option), result2);
    verify_result(params.output_schema, expect2, result2);
    DatumRowVector chunk_result2;
    collect_chunk_iterator_result(table_read_view->get_chunk(prefix_key2, read_option), chunk_result2);
    verify_result(params.output_schema, expect2, chunk_result2);

    for (size_t i = 6; i < 9; i++) {
        DatumRow prefix_key(2);
        prefix_key.set_datum(0, rows[i].get_datum(0));
        prefix_key.set_datum(1, rows[i].get_datum(1));
        DatumRowVector expect{expect_results[i]};
        DatumRowVector result;
        collect_row_iterator_result(table_read_view->get(prefix_key, read_option), result);
        verify_result(params.output_schema, expect, result);
        DatumRowVector chunk_result;
        collect_chunk_iterator_result(table_read_view->get_chunk(prefix_key, read_option), chunk_result);
        verify_result(params.output_schema, expect, chunk_result);
    }

    table_read_view->close();
}

} // namespace starrocks
