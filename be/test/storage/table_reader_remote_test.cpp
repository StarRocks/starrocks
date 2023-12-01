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

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <memory>

#include "column/column_helper.h"
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
#include "storage/table_reader.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks {

using DatumTupleVector = std::vector<DatumTuple>;

class TableReaderRemoteTest : public testing::Test {
public:
    static void create_row(vector<DatumTuple>& data, int64_t pk1, int64_t v1, int32_t v2) {
        DatumTuple tuple;
        tuple.append(Datum(pk1));
        tuple.append(Datum(v1));
        tuple.append(Datum(v2));
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
        writer_context.tablet_schema = tablet->tablet_schema();
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
        ASSERT_TRUE(tablet->rowset_commit(version, row_set, 0).ok());
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
        pk1.column_name = "pk1";
        pk1.__set_is_key(true);
        pk1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(pk1);

        TColumn v1;
        v1.column_name = "v1";
        v1.__set_is_key(false);
        v1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(v1);

        TColumn v2;
        v2.column_name = "v2";
        v2.__set_is_key(false);
        v2.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(v2);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void create_table_reader_params(TableReaderParams& params, int64_t version) {
        params.schema.db_id = db_id;
        params.schema.table_id = table_id;
        params.schema.version = version;
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("pk1").column_pos(0).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("v1").column_pos(1).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("v2").column_pos(2).build());

        tuple_builder.build(&dtb);

        TDescriptorTable t_desc_tbl = dtb.desc_tbl();
        params.schema.slot_descs = t_desc_tbl.slotDescriptors;
        params.schema.tuple_desc = t_desc_tbl.tupleDescriptors[0];
        params.schema.indexes.resize(1);
        params.schema.indexes[0].id = 1111;
        params.schema.indexes[0].columns = {"pk1", "v1", "v2"};

        params.partition_param.db_id = db_id;
        params.partition_param.table_id = table_id;
        params.partition_param.version = version;
        params.partition_param.__set_distributed_columns({"pk1"});
        params.partition_param.partitions.resize(1);
        params.partition_param.partitions[0].id = 1;
        params.partition_param.partitions[0].num_buckets = num_buckets;
        params.partition_param.partitions[0].indexes.resize(1);
        params.partition_param.partitions[0].indexes[0].index_id = 1111;
        params.partition_param.partitions[0].indexes[0].tablets = _tablet_ids;
        params.partition_param.distributed_columns = {"pk1"};
        params.partition_versions[1] = version;
    }

protected:
    void SetUp() override { StoragePageCache::create_global_cache(&_tracker, 1000000000); }

    void TearDown() override {
        for (TabletSharedPtr& tablet : _tablets) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(tablet->tablet_id());
            tablet.reset();
        }
        StoragePageCache::release_global_cache();
    }

    void run_multi_get() {}

protected:
    int64_t db_id;
    std::string table_name;
    int64_t table_id;
    size_t num_buckets = 4;
    std::vector<int64_t> _tablet_ids;
    std::vector<TabletSharedPtr> _tablets;
    ObjectPool _object_pool;
    Schema _key_schema;
    Schema _value_schema;
    MemTracker _tracker;
};

TEST_F(TableReaderRemoteTest, test_multi_get_1_tablet) {
    num_buckets = 1;
    const size_t pk_size = 1000;
    const int64_t multi_get_size = 500;

    srand(GetCurrentTimeMicros());
    db_id = 1;
    table_name = "table_reader_remote_1_tablet_test";
    table_id = 2;
    int64_t tablet_id = rand();
    for (int i = 0; i < num_buckets; i++) {
        TabletSharedPtr tablet = create_tablet(tablet_id + i, rand());
        _tablets.push_back(tablet);
        _tablet_ids.push_back(tablet_id + i);
    }
    _key_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {0});
    _value_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {1, 2});

    std::set<int64_t> all_ints;
    for (size_t i = 0; i < pk_size; i++) {
        all_ints.insert(i);
    }
    vector<int64_t> pk_array(all_ints.begin(), all_ints.end());
    auto pk_column = Int64Column::create();
    for (int64_t i : pk_array) {
        pk_column->append(i);
    }
    std::vector<uint32_t> buckets(pk_array.size(), 0);
    pk_column->crc32_hash(buckets.data(), 0, all_ints.size());
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = buckets[i] % num_buckets;
    }
    for (size_t i = 0; i < num_buckets; i++) {
        vector<DatumTuple> data;
        for (size_t j = 0; j < pk_column->size(); j++) {
            if (buckets[j] == i) {
                int64_t pk = pk_column->get(j).get_int64();
                create_row(data, pk, pk * 2, pk * 3);
            }
        }
        create_rowset(_tablets[i], 2, data, 0, data.size());
    }
    DatumTupleVector rows;
    while (true) {
        bool ok = true;
        for (int i = 0; i < num_buckets; i++) {
            std::vector<RowsetSharedPtr> dummy_rowsets;
            EditVersion full_version;
            ASSERT_TRUE(_tablets[0]->updates()->get_applied_rowsets(2, &dummy_rowsets, &full_version).ok());
            if (full_version.major_number() < 2) {
                ok = false;
                break;
            }
        }
        if (ok) {
            break;
        }
        std::cerr << "waiting for version 2\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    TableReaderParams read_params;
    create_table_reader_params(read_params, 2);
    std::shared_ptr<TableReader> table_reader = std::make_shared<TableReader>();
    EXPECT_TRUE(table_reader->init(read_params).ok());

    // construct multi_get
    ChunkPtr key_chunk = std::make_shared<Chunk>();
    TypeDescriptor key_type = TypeDescriptor::from_thrift(read_params.schema.slot_descs[0].slotType);
    key_chunk->append_column(ColumnHelper::create_column(key_type, false), read_params.schema.slot_descs[0].id);
    key_chunk->get_column_by_index(0)->reserve(multi_get_size);
    ChunkPtr values_chunk = ChunkHelper::new_chunk(_value_schema, multi_get_size);
    vector<int64_t> expected_values;
    vector<bool> expected_found(multi_get_size, false);
    for (int64_t i = 0; i < multi_get_size; i++) {
        int64_t pk = i * 2;
        key_chunk->get_column_by_index(0)->append_datum(Datum(pk));
        if (all_ints.find(pk) != all_ints.end()) {
            expected_found[i] = true;
            expected_values.push_back(pk * 2);
        }
    }
    vector<bool> found;
    EXPECT_TRUE(table_reader->multi_get(*key_chunk, {"v1", "v2"}, found, *values_chunk).ok());
    ASSERT_EQ(expected_found.size(), found.size());
    ASSERT_EQ(expected_found, found);
    ASSERT_EQ(expected_values.size(), values_chunk->num_rows());
    for (size_t i = 0; i < expected_values.size(); i++) {
        ASSERT_EQ(expected_values[i], values_chunk->get_column_by_index(0)->get(i).get_int64());
    }
}

TEST_F(TableReaderRemoteTest, test_multi_get_4_tablet) {
    num_buckets = 4;
    const int64_t pk_range = 1000000;
    const int64_t pk_num = 10000;
    const int64_t multi_get_size = 1000;
    const size_t multi_get_time = 10;

    srand(GetCurrentTimeMicros());
    db_id = 1;
    table_name = "table_reader_remote_4_tablet_test";
    table_id = 2;
    int64_t tablet_id = rand();
    for (int i = 0; i < num_buckets; i++) {
        TabletSharedPtr tablet = create_tablet(tablet_id + i, rand());
        _tablets.push_back(tablet);
        _tablet_ids.push_back(tablet_id + i);
    }
    _key_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {0});
    _value_schema = ChunkHelper::convert_schema(_tablets[0]->tablet_schema(), {1, 2});

    srand(0);
    std::set<int64_t> all_ints;
    for (int i = 0; i < pk_num; i++) {
        all_ints.insert(rand() % pk_range);
    }
    vector<int64_t> pk_array(all_ints.begin(), all_ints.end());
    auto pk_column = Int64Column::create();
    for (int64_t i : pk_array) {
        pk_column->append(i);
    }
    std::vector<uint32_t> buckets(pk_array.size(), 0);
    pk_column->crc32_hash(buckets.data(), 0, all_ints.size());
    for (int i = 0; i < buckets.size(); i++) {
        buckets[i] = buckets[i] % num_buckets;
    }
    for (size_t i = 0; i < num_buckets; i++) {
        vector<DatumTuple> data;
        for (size_t j = 0; j < pk_column->size(); j++) {
            if (buckets[j] == i) {
                int64_t pk = pk_column->get(j).get_int64();
                create_row(data, pk, pk * 2, pk * 3);
            }
        }
        create_rowset(_tablets[i], 2, data, 0, data.size());
    }
    DatumTupleVector rows;
    while (true) {
        bool ok = true;
        for (int i = 0; i < num_buckets; i++) {
            std::vector<RowsetSharedPtr> dummy_rowsets;
            EditVersion full_version;
            ASSERT_TRUE(_tablets[0]->updates()->get_applied_rowsets(2, &dummy_rowsets, &full_version).ok());
            if (full_version.major_number() < 2) {
                ok = false;
                break;
            }
        }
        if (ok) {
            break;
        }
        std::cerr << "waiting for version 2\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    TableReaderParams read_params;
    create_table_reader_params(read_params, 2);
    std::shared_ptr<TableReader> table_reader = std::make_shared<TableReader>();
    EXPECT_TRUE(table_reader->init(read_params).ok());

    for (size_t i = 0; i < multi_get_time; i++) {
        // construct multi_get
        ChunkPtr key_chunk = std::make_shared<Chunk>();
        TypeDescriptor key_type = TypeDescriptor::from_thrift(read_params.schema.slot_descs[0].slotType);
        key_chunk->append_column(ColumnHelper::create_column(key_type, false), read_params.schema.slot_descs[0].id);
        key_chunk->get_column_by_index(0)->reserve(multi_get_size);
        ChunkPtr values_chunk = ChunkHelper::new_chunk(_value_schema, multi_get_size);
        vector<int64_t> expected_values;
        vector<bool> expected_found(multi_get_size, false);
        for (int64_t i = 0; i < multi_get_size; i++) {
            int64_t pk = rand() % pk_range;
            key_chunk->get_column_by_index(0)->append_datum(Datum(pk));
            if (all_ints.find(pk) != all_ints.end()) {
                expected_found[i] = true;
                expected_values.push_back(pk * 2);
            }
        }
        vector<bool> found;
        EXPECT_TRUE(table_reader->multi_get(*key_chunk, {"v1", "v2"}, found, *values_chunk).ok());
        ASSERT_EQ(expected_found.size(), found.size());
        ASSERT_EQ(expected_found, found);
        ASSERT_EQ(expected_values.size(), values_chunk->num_rows());
        for (size_t i = 0; i < expected_values.size(); i++) {
            ASSERT_EQ(expected_values[i], values_chunk->get_column_by_index(0)->get(i).get_int64());
        }
    }
}

} // namespace starrocks
