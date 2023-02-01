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

#include "storage/table_reader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <memory>

#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

using DatumTupleVector = std::vector<DatumTuple>;

class TableReaderTest : public testing::Test {
public:
    static void create_row(vector<DatumTuple>& data, int64_t c0, int32_t c1, int32_t c2, int16_t c3, int32_t c4) {
        DatumTuple tuple;
        tuple.append(Datum(c0));
        tuple.append(Datum(c1));
        tuple.append(Datum(c2));
        tuple.append(Datum(c3));
        tuple.append(Datum(c4));
        data.push_back(tuple);
    }

    static void create_rowset(const TabletSharedPtr& tablet, int version, const vector<DatumTuple>& data, int start_pos,
                              int end_pos) {
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
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, data.size());
        auto& cols = chunk->columns();
        for (int pos = start_pos; pos < end_pos; pos++) {
            const DatumTuple& row = data[pos];
            DatumTuple tmp_row;
            for (size_t i = 0; i < row.size(); i++) {
                cols[i]->append_datum(row.get(i));
            }
        }
        CHECK_OK(writer->flush_chunk(*chunk));
        auto row_set = *writer->build();
        ASSERT_TRUE(tablet->rowset_commit(version, row_set).ok());
        ASSERT_EQ(version, tablet->updates()->max_version());
    }

    static TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
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

    void build_multi_get_request(DatumTuple& tuple, Chunk* key_chunk, std::vector<bool>& found, Chunk* value_chunk) {
        key_chunk->get_column_by_index(0)->append_datum(tuple.get(0));
        key_chunk->get_column_by_index(1)->append_datum(tuple.get(1));
        key_chunk->get_column_by_index(2)->append_datum(tuple.get(2));
        found.push_back(true);
        value_chunk->get_column_by_index(0)->append_datum(tuple.get(3));
        value_chunk->get_column_by_index(1)->append_datum(tuple.get(4));
    }

    void build_eq_predicates(Schema& scan_key_schema, DatumTuple& tuple,
                             std::vector<const ColumnPredicate*>& predicates) {
        predicates.clear();
        for (size_t i = 0; i < scan_key_schema.num_fields(); i++) {
            const FieldPtr& field = scan_key_schema.field(i);
            ColumnPredicate* predicate = new_column_eq_predicate(field->type(), field->id(),
                                                                 datum_to_string(field->type().get(), tuple.get(i)));
            _object_pool.add(predicate);
            predicates.push_back(predicate);
        }
    }

    void build_scan_request(DatumTupleVector& tuples, Schema& scan_key_schema, std::vector<int> tuple_index,
                            std::vector<int> value_column_index, std::vector<const ColumnPredicate*>& predicates,
                            Chunk* result) {
        build_eq_predicates(scan_key_schema, tuples[tuple_index[0]], predicates);
        result->reset();
        for (int i : tuple_index) {
            DatumTuple tuple = tuples[i];
            for (int j = 0; j < value_column_index.size(); j++) {
                Datum datum = tuple.get(value_column_index[j]);
                result->get_column_by_index(j)->append_datum(datum);
            }
        }
    }

    void SetUp() override {
        srand(GetCurrentTimeMicros());
        db_id = 1;
        table_name = "table_reader_test";
        table_id = 2;
        version = 10;
        for (int i = 0; i < 3; i++) {
            TabletSharedPtr tablet = create_tablet(rand(), rand());
            _tablets.push_back(tablet);
        }
        _key_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {0, 1, 2});
        _value_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {3, 4});
    }

    void TearDown() override {
        for (TabletSharedPtr& tablet : _tablets) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
            tablet.reset();
        }
    }

protected:
    int64_t db_id;
    std::string table_name;
    int64_t table_id;
    int64_t version;
    std::vector<TabletSharedPtr> _tablets;
    ObjectPool _object_pool;
    Schema _key_schema;
    Schema _value_schema;
};

void collect_chunk_iterator_result(StatusOr<ChunkIteratorPtr>& status_or, Chunk& result) {
    if (!status_or.status().ok()) {
        ASSERT_TRUE(status_or.status().is_end_of_file());
    }
    ChunkIteratorPtr iterator = status_or.value();
    std::shared_ptr<Chunk> chunk = ChunkHelper::new_chunk(iterator->schema(), 2);
    Status status = iterator->get_next(chunk.get());
    result.reset();
    while (status.ok()) {
        result.append(*chunk);
        chunk->reset();
        status = iterator->get_next(chunk.get());
    }
    ASSERT_TRUE(status.is_end_of_file());
    iterator->close();
}

void verify_chunk_eq(Schema& schema, Chunk* expect_chunk, Chunk* actual_chunk) {
    ASSERT_EQ(expect_chunk->num_rows(), actual_chunk->num_rows());
    for (int i = 0; i < expect_chunk->num_rows(); i++) {
        DatumTuple expect_tuple = expect_chunk->get(i);
        DatumTuple actual_tuple = actual_chunk->get(i);
        for (size_t j = 0; j < schema.num_fields(); j++) {
            int cmp = schema.field(j)->type()->cmp(expect_tuple.get(j), actual_tuple.get(j));
            ASSERT_EQ(0, cmp);
        }
    }
}

TEST_F(TableReaderTest, test_basic_read) {
    // 1. generate data, and write to tablet
    DatumTupleVector rows;
    create_row(rows, (int64_t)1, (int32_t)1, (int32_t)1, (int16_t)1, (int32_t)1);
    create_row(rows, (int64_t)1, (int32_t)1, (int32_t)2, (int16_t)2, (int32_t)1);
    create_row(rows, (int64_t)1, (int32_t)1, (int32_t)3, (int16_t)1, (int32_t)1);
    create_rowset(_tablets[0], 2, rows, 0, 3);
    create_row(rows, (int64_t)2, (int32_t)1, (int32_t)1, (int16_t)1, (int32_t)1);
    create_row(rows, (int64_t)2, (int32_t)1, (int32_t)2, (int16_t)2, (int32_t)1);
    create_row(rows, (int64_t)2, (int32_t)1, (int32_t)3, (int16_t)3, (int32_t)1);
    create_rowset(_tablets[0], 3, rows, 3, 6);
    create_row(rows, (int64_t)3, (int32_t)3, (int32_t)4, (int16_t)1, (int32_t)1);
    create_row(rows, (int64_t)4, (int32_t)2, (int32_t)3, (int16_t)2, (int32_t)1);
    create_row(rows, (int64_t)5, (int32_t)1, (int32_t)5, (int16_t)3, (int32_t)1);
    create_rowset(_tablets[0], 4, rows, 6, 9);
    while (true) {
        std::vector<RowsetSharedPtr> dummy_rowsets;
        EditVersion full_version;
        ASSERT_TRUE(_tablets[0]->updates()->get_applied_rowsets(4, &dummy_rowsets, &full_version).ok());
        if (full_version.major() == 4) {
            break;
        }
        std::cerr << "waiting for version 4\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 2. build TableReaderParams, and create TableReader
    LocalTableReaderParams params;
    // 2.1 data version to read
    params.version = 4;
    params.tablet_id = _tablets[0]->tablet_id();

    // 2.3 create TableReader
    std::shared_ptr<TableReader> table_reader = std::make_shared<TableReader>();
    EXPECT_TRUE(table_reader->init(params).ok());

    // 3. verify multi_get
    ChunkPtr key_chunk = ChunkHelper::new_chunk(_key_schema, 10);
    std::vector<bool> expected_found;
    ChunkPtr expected_value_chunk = ChunkHelper::new_chunk(_value_schema, 10);

    // key (1, 1, 2), value (2, 1)
    build_multi_get_request(rows[1], key_chunk.get(), expected_found, expected_value_chunk.get());

    // key (1, 1, 4) not found
    key_chunk->get_column_by_index(0)->append_datum(Datum((int64_t)1));
    key_chunk->get_column_by_index(1)->append_datum(Datum((int32_t)1));
    key_chunk->get_column_by_index(2)->append_datum(Datum((int32_t)4));
    expected_found.push_back(false);

    // key (2, 1, 3), value (3, 1)
    build_multi_get_request(rows[5], key_chunk.get(), expected_found, expected_value_chunk.get());

    // key (3, 3, 4), value (1, 1)
    build_multi_get_request(rows[6], key_chunk.get(), expected_found, expected_value_chunk.get());

    // key (5, 8, 9) not found
    key_chunk->get_column_by_index(0)->append_datum(Datum((int64_t)5));
    key_chunk->get_column_by_index(1)->append_datum(Datum((int32_t)8));
    key_chunk->get_column_by_index(2)->append_datum(Datum((int32_t)9));
    expected_found.push_back(false);

    std::vector<bool> found;
    ChunkPtr value_chunk = ChunkHelper::new_chunk(_value_schema, 10);
    Status status = table_reader->multi_get(*key_chunk, {"v1", "v2"}, found, *value_chunk);
    ASSERT_OK(status);
    ASSERT_EQ(expected_found.size(), found.size());
    for (int i = 0; i < expected_found.size(); i++) {
        ASSERT_EQ(expected_found[i], found[i]);
    }
    verify_chunk_eq(_value_schema, expected_value_chunk.get(), value_chunk.get());

    // 4. verify scan
    Schema scan_key_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {0, 1});
    Schema scan_value_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {2, 3, 4});

    ChunkPtr expect_scan_result = ChunkHelper::new_chunk(scan_value_schema, 0);
    ChunkPtr scan_result = ChunkHelper::new_chunk(scan_value_schema, 0);
    std::vector<const ColumnPredicate*> predicates;

    // scan prefix key (1, 1)
    build_scan_request(rows, scan_key_schema, {0, 1, 2}, {2, 3, 4}, predicates, expect_scan_result.get());
    StatusOr<ChunkIteratorPtr> status_or = table_reader->scan({"pk3_int", "v1", "v2"}, predicates);
    collect_chunk_iterator_result(status_or, *scan_result);
    verify_chunk_eq(scan_value_schema, expect_scan_result.get(), scan_result.get());

    // scan prefix key (2, 1)
    build_scan_request(rows, scan_key_schema, {3, 4, 5}, {2, 3, 4}, predicates, expect_scan_result.get());
    status_or = table_reader->scan({"pk3_int", "v1", "v2"}, predicates);
    collect_chunk_iterator_result(status_or, *scan_result);
    verify_chunk_eq(scan_value_schema, expect_scan_result.get(), scan_result.get());

    // scan prefix key (6, 7), no data
    expect_scan_result->reset();
    DatumTuple prefix_key;
    prefix_key.append(Datum((int64_t)6));
    prefix_key.append(Datum((int32_t)7));
    build_eq_predicates(scan_key_schema, prefix_key, predicates);
    status_or = table_reader->scan({"pk3_int", "v1", "v2"}, predicates);
    collect_chunk_iterator_result(status_or, *scan_result);
    verify_chunk_eq(scan_value_schema, expect_scan_result.get(), scan_result.get());
}

} // namespace starrocks
