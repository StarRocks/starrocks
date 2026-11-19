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

#include "storage/schema_change.h"

#include <algorithm>
#include <utility>

#include "column/datum_convert.h"
#include "fs/fs_util.h"
#include "gen_cpp/Exprs_types.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/delta_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/schema_test_helper.h"
#include "testutil/tablet_test_helper.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/logging.h"

namespace starrocks {

class SchemaChangeTest : public testing::Test {
    void SetUp() override {
        _storage_engine = StorageEngine::instance();
        _txn_mgr = _storage_engine->txn_manager();
        _tablet_mgr = _storage_engine->tablet_manager();
    }

    void TearDown() override {}

protected:
    void add_key_column(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type);
    void add_value_column(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type,
                          TKeysType::type keys_type = TKeysType::DUP_KEYS);
    void add_value_column_with_index(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type,
                                     TKeysType::type keys_type = TKeysType::DUP_KEYS);

    void create_base_tablet(TTabletId tablet_id, TKeysType::type type, TStorageType::type);
    void create_dest_tablet_with_index(TTabletId base_tablet_id, TTabletId new_tablet_id, TKeysType::type type);

    void write_data_to_base_tablet(TTabletId tablet_id, Version version);

    TabletSchemaSPtr gen_tablet_schema(const std::string& name, const std::string& type, const std::string& aggregation,
                                       uint32_t length);
    template <typename T>
    void test_convert_to_varchar(LogicalType type, int type_size, T val, const std::string& expect_val);
    template <typename T>
    void test_convert_from_varchar(LogicalType type, int type_size, std::string val, T expect_val);
    std::vector<SlotDescriptor*> gen_base_slots();
    TAlterTabletReqV2 gen_alter_tablet_req(TTabletId base_tablet_id, TTabletId new_tablet_id, Version version);
    RowsetId next_rowset_id() const { return _storage_engine->next_rowset_id(); }

    StorageEngine* _storage_engine;
    TabletManager* _tablet_mgr;
    TxnManager* _txn_mgr;
    MemPool _mem_pool;
    std::vector<SlotDescriptor> _slots;
    MemTracker _mem_tracker;
    OlapReaderStatistics _stats;
    const TypeInfoPtr _json_type = get_type_info(TYPE_JSON);
    const TypeInfoPtr _varchar_type = get_type_info(TYPE_VARCHAR);
    const int64_t _mem_limit = config::memory_limitation_per_thread_for_schema_change * 1024 * 1024 * 1024;
};

TAlterTabletReqV2 SchemaChangeTest::gen_alter_tablet_req(TTabletId base_tablet_id, TTabletId new_tablet_id,
                                                         Version version) {
    TAlterTabletReqV2 req;
    req.__set_base_tablet_id(base_tablet_id);
    req.__set_new_tablet_id(new_tablet_id);
    req.__set_base_schema_hash(0);
    req.__set_new_schema_hash(0);
    req.__set_alter_version(version.second);
    req.__set_alter_job_type(TAlterJobType::SCHEMA_CHANGE);

    return req;
}

std::vector<SlotDescriptor*> SchemaChangeTest::gen_base_slots() {
    for (size_t i = 0; i < 4; i++) {
        TSlotDescriptorBuilder slot_desc_builder;
        auto slot = slot_desc_builder.type(TYPE_INT)
                            .column_name("c" + std::to_string(i))
                            .column_pos(i)
                            .nullable(false)
                            .id(i)
                            .build();
        _slots.emplace_back(slot);
    }

    std::vector<SlotDescriptor*> slots;
    for (size_t i = 0; i < 4; i++) {
        slots.emplace_back(&_slots[i]);
    }
    return slots;
}

void SchemaChangeTest::add_key_column(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type) {
    TColumn col = SchemaTestHelper::gen_key_column(column_name, type);
    request->tablet_schema.columns.push_back(col);
}

void SchemaChangeTest::add_value_column(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type,
                                        TKeysType::type keys_type) {
    TColumn col;
    if (keys_type == TKeysType::DUP_KEYS) {
        col = SchemaTestHelper::gen_value_column_for_dup_table(column_name, type);
    } else {
        col = SchemaTestHelper::gen_value_column_for_agg_table(column_name, type);
    }
    request->tablet_schema.columns.push_back(col);
}

void SchemaChangeTest::add_value_column_with_index(TCreateTabletReq* request, std::string column_name,
                                                   TPrimitiveType::type type, TKeysType::type keys_type) {
    TColumn col;
    if (keys_type == TKeysType::DUP_KEYS) {
        col = SchemaTestHelper::gen_value_column_for_dup_table(column_name, type);
    } else {
        col = SchemaTestHelper::gen_value_column_for_agg_table(column_name, type);
    }
    col.__set_has_bitmap_index(true);
    request->tablet_schema.columns.push_back(col);
}

void SchemaChangeTest::create_base_tablet(TTabletId tablet_id, TKeysType::type type, TStorageType::type storage_type) {
    auto create_tablet_req = TabletTestHelper::gen_create_tablet_req(tablet_id, type, storage_type);

    add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
    add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
    add_value_column(&create_tablet_req, "v1", TPrimitiveType::INT);
    add_value_column(&create_tablet_req, "v2", TPrimitiveType::INT);

    ASSERT_OK(_storage_engine->create_tablet(create_tablet_req));
}

void SchemaChangeTest::create_dest_tablet_with_index(TTabletId base_tablet_id, TTabletId new_tablet_id,
                                                     TKeysType::type type) {
    auto create_tablet_req = TabletTestHelper::gen_create_tablet_req(new_tablet_id, type, TStorageType::COLUMN);
    create_tablet_req.__set_base_tablet_id(base_tablet_id);

    add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
    add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
    add_value_column(&create_tablet_req, "v1", TPrimitiveType::INT);
    add_value_column_with_index(&create_tablet_req, "v2", TPrimitiveType::INT);

    ASSERT_OK(_storage_engine->create_tablet(create_tablet_req));
}

void SchemaChangeTest::write_data_to_base_tablet(TTabletId tablet_id, Version version) {
    auto tablet = _tablet_mgr->get_tablet(tablet_id);
    Schema base_schema = *tablet->tablet_schema()->schema();
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
    for (size_t i = 0; i < 4; ++i) {
        ColumnPtr& base_col = base_chunk->get_column_by_index(i);
        base_col = ColumnTestHelper::build_column<int32_t>({0, 1, 2, 3});
    }

    auto rowset_writer = TabletTestHelper::create_rowset_writer(*tablet, next_rowset_id(), version);

    ASSERT_OK(rowset_writer->add_chunk(*base_chunk));
    ASSERT_OK(rowset_writer->flush());
    RowsetSharedPtr new_rowset = *rowset_writer->build();
    ASSERT_TRUE(new_rowset != nullptr);
    ASSERT_OK(tablet->add_rowset(new_rowset, false));
}

TabletSchemaSPtr SchemaChangeTest::gen_tablet_schema(const std::string& name, const std::string& type,
                                                     const std::string& aggregation, uint32_t length) {
    TabletSchemaPB tablet_schema_pb;
    SchemaTestHelper::add_column_pb_to_tablet_schema(&tablet_schema_pb, name, type, aggregation, length);
    return std::make_shared<TabletSchema>(tablet_schema_pb);
}

template <typename T>
void SchemaChangeTest::test_convert_to_varchar(LogicalType type, int type_size, T val, const std::string& expect_val) {
    auto src_tablet_schema = gen_tablet_schema("c1", logical_type_to_string(type), "REPLACE", type_size);
    auto dst_tablet_schema = gen_tablet_schema("c2", "VARCHAR", "REPLACE", 255);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum(val);
    Datum dst_datum;

    auto converter = get_type_converter(type, TYPE_VARCHAR);
    ASSERT_OK(converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool));
    EXPECT_EQ(expect_val, dst_datum.get_slice().to_string());
}

template <typename T>
void SchemaChangeTest::test_convert_from_varchar(LogicalType type, int type_size, std::string val, T expect_val) {
    auto src_tablet_schema = gen_tablet_schema("c1", "VARCHAR", "REPLACE", 255);
    auto dst_tablet_schema = gen_tablet_schema("c2", logical_type_to_string(type), "REPLACE", type_size);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum(Slice((char*)val.data(), val.size()));
    Datum dst_datum;

    auto converter = get_type_converter(TYPE_VARCHAR, type);
    ASSERT_OK(converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool));
    EXPECT_EQ(expect_val, dst_datum.get<T>());
}

TEST_F(SchemaChangeTest, column_with_row) {
    int64_t base_tablet_id = 1415001;
    int64_t new_tablet_id = 1415002;

    create_base_tablet(base_tablet_id, TKeysType::PRIMARY_KEYS, TStorageType::COLUMN_WITH_ROW);

    {
        auto create_tablet_req = TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::PRIMARY_KEYS,
                                                                         TStorageType::COLUMN_WITH_ROW);
        create_tablet_req.__set_base_tablet_id(base_tablet_id);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v0", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v3", TPrimitiveType::INT);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    ASSERT_TRUE(base_tablet != nullptr);
    ASSERT_EQ(base_tablet->tablet_schema()->columns().back().name(), Schema::FULL_ROW_COLUMN);
    ASSERT_EQ(base_tablet->tablet_schema()->columns().back().unique_id(), 4);

    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    ASSERT_TRUE(new_tablet != nullptr);
    ASSERT_EQ(new_tablet->tablet_schema()->columns().back().name(), Schema::FULL_ROW_COLUMN);
    ASSERT_EQ(new_tablet->tablet_schema()->columns().back().unique_id(), 6);
}

TEST_F(SchemaChangeTest, convert_tinyint_to_varchar) {
    test_convert_to_varchar<int8_t>(TYPE_TINYINT, 1, 127, "127");
}

TEST_F(SchemaChangeTest, convert_smallint_to_varchar) {
    test_convert_to_varchar<int16_t>(TYPE_SMALLINT, 2, 32767, "32767");
}

TEST_F(SchemaChangeTest, convert_int_to_varchar) {
    test_convert_to_varchar<int32_t>(TYPE_INT, 4, 2147483647, "2147483647");
}

TEST_F(SchemaChangeTest, convert_bigint_to_varchar) {
    test_convert_to_varchar<int64_t>(TYPE_BIGINT, 8, 9223372036854775807, "9223372036854775807");
}

TEST_F(SchemaChangeTest, convert_largeint_to_varchar) {
    test_convert_to_varchar<int128_t>(TYPE_LARGEINT, 16, 1701411834604690, "1701411834604690");
}

TEST_F(SchemaChangeTest, convert_float_to_varchar) {
    test_convert_to_varchar<float>(TYPE_FLOAT, 4, 3.40282e+38, "3.40282e+38");
}

TEST_F(SchemaChangeTest, convert_double_to_varchar) {
    test_convert_to_varchar<double>(TYPE_DOUBLE, 8, 123.456, "123.456");
}

TEST_F(SchemaChangeTest, convert_varchar_to_tinyint) {
    test_convert_from_varchar<int8_t>(TYPE_TINYINT, 1, "127", 127);
}

TEST_F(SchemaChangeTest, convert_varchar_to_smallint) {
    test_convert_from_varchar<int16_t>(TYPE_SMALLINT, 2, "32767", 32767);
}

TEST_F(SchemaChangeTest, convert_varchar_to_int) {
    test_convert_from_varchar<int32_t>(TYPE_INT, 4, "2147483647", 2147483647);
}

TEST_F(SchemaChangeTest, convert_varchar_to_bigint) {
    test_convert_from_varchar<int64_t>(TYPE_BIGINT, 8, "9223372036854775807", 9223372036854775807);
}

TEST_F(SchemaChangeTest, convert_varchar_to_largeint) {
    test_convert_from_varchar<int128_t>(TYPE_LARGEINT, 16, "1701411834604690", 1701411834604690);
}

TEST_F(SchemaChangeTest, convert_varchar_to_float) {
    test_convert_from_varchar<float>(TYPE_FLOAT, 4, "3.40282e+38", 3.40282e+38);
}

TEST_F(SchemaChangeTest, convert_varchar_to_double) {
    test_convert_from_varchar<double>(TYPE_DOUBLE, 8, "123.456", 123.456);
}

TEST_F(SchemaChangeTest, convert_float_to_double) {
    auto src_tablet_schema = gen_tablet_schema("c1", "FLOAT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DOUBLE", "REPLACE", 8);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum((float)(1.2345));
    Datum dst_datum;

    auto converter = get_type_converter(TYPE_FLOAT, TYPE_DOUBLE);
    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);

    ASSERT_TRUE(st.ok());
    EXPECT_EQ(1.2345, dst_datum.get_double());
}

TEST_F(SchemaChangeTest, convert_datetime_to_date) {
    auto src_tablet_schema = gen_tablet_schema("c1", "DATETIME", "REPLACE", 8);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DATE", "REPLACE", 3);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string origin_val = "2021-09-28 16:07:00";

    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);
    int64_t value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) * 1000000L +
                    time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
    src_datum.set_int64(value);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_DATETIME_V1, TYPE_DATE_V1);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    EXPECT_EQ(dst_value, (int)dst_datum.get_uint24());
}

TEST_F(SchemaChangeTest, convert_date_to_datetime) {
    auto src_tablet_schema = gen_tablet_schema("c1", "DATE", "REPLACE", 3);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DATETIME", "REPLACE", 8);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));
    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    src_datum.set_uint24(value);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_DATE_V1, TYPE_DATETIME_V1);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    int64_t dst_value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) * 1000000L;
    EXPECT_EQ(dst_value, dst_datum.get_int64());
}

TEST_F(SchemaChangeTest, convert_int_to_date_v2) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DATE V2", "REPLACE", 3);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);
    src_datum.set_int32(20210928);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_INT, TYPE_DATE);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    EXPECT_EQ("2021-09-28", dst_datum.get_date().to_string());
}

TEST_F(SchemaChangeTest, convert_int_to_date) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DATE", "REPLACE", 3);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);
    src_datum.set_int32(20210928);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_INT, TYPE_DATE_V1);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    EXPECT_EQ(dst_value, (int)dst_datum.get_uint24());
}

TEST_F(SchemaChangeTest, convert_int_to_bitmap) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "OBJECT", "BITMAP_UNION", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_INT, OLAP_MATERIALIZE_TYPE_BITMAP);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const BitmapValue* bitmap_val = dst_datum.get_bitmap();
    EXPECT_EQ(bitmap_val->cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_varchar_to_hll) {
    auto src_tablet_schema = gen_tablet_schema("c1", "VARCHAR", "REPLACE", 255);
    auto dst_tablet_schema = gen_tablet_schema("c2", "HLL", "HLL_UNION", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string str = "test string";
    Slice slice(str.data(), str.size());
    src_datum.set_slice(slice);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_VARCHAR, OLAP_MATERIALIZE_TYPE_HLL);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const HyperLogLog* hll = dst_datum.get_hyperloglog();
    EXPECT_EQ(hll->estimate_cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_int_to_count) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "BIGINT", "SUM", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_INT, OLAP_MATERIALIZE_TYPE_COUNT);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    EXPECT_EQ(dst_datum.get_int64(), 1);
}

TEST_F(SchemaChangeTest, schema_change_with_directing_v2) {
    TTabletId base_tablet_id = 1101;
    TTabletId new_tablet_id = 1102;
    Version version(3, 3);

    create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        auto create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::BIGINT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::VARCHAR);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    auto base_tablet_schema = base_tablet->tablet_schema();
    auto new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    for (size_t i = 0; i < 4; ++i) {
        ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(i);
        column_mapping->ref_column = i;
    }
    ASSERT_OK(chunk_changer.prepare());
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), chunk_changer.get_selected_column_indexes());

    auto sc_procedure = std::make_unique<SchemaChangeDirectly>(&chunk_changer);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_OK(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, schema_change_with_sorting_v2) {
    TTabletId base_tablet_id = 1103;
    TTabletId new_tablet_id = 1104;
    Version version(3, 3);

    create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        TCreateTabletReq create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::BIGINT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::VARCHAR);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    auto base_tablet_schema = base_tablet->tablet_schema();
    auto new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    column_mapping = chunk_changer.get_mutable_column_mapping(3);
    column_mapping->ref_column = 3;
    ASSERT_TRUE(chunk_changer.prepare().ok());

    auto sc_procedure = std::make_unique<SchemaChangeWithSorting>(&chunk_changer, _mem_limit);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    Schema base_schema = ChunkHelper::convert_schema(base_tablet_schema, chunk_changer.get_selected_column_indexes());

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_OK(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));
    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, schema_change_with_agg_key_reorder) {
    TTabletId base_tablet_id = 1203;
    TTabletId new_tablet_id = 1204;
    Version version(3, 3);

    create_base_tablet(base_tablet_id, TKeysType::AGG_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        TCreateTabletReq create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::AGG_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::BIGINT, TKeysType::AGG_KEYS);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    auto base_tablet_schema = base_tablet->tablet_schema();
    auto new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    ASSERT_TRUE(chunk_changer.prepare().ok());

    auto sc_procedure = std::make_unique<SchemaChangeWithSorting>(&chunk_changer, _mem_limit);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    Schema base_schema = ChunkHelper::convert_schema(base_tablet_schema, chunk_changer.get_selected_column_indexes());

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, version);
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_OK(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));
    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, convert_varchar_to_json) {
    std::vector<std::string> test_cases = {"{\"a\": 1}", "null", "[1,2,3]"};
    for (const auto& json_str : test_cases) {
        JsonValue expected = JsonValue::parse(json_str).value();

        BinaryColumn::Ptr src_column = BinaryColumn::create();
        JsonColumn::Ptr dst_column = JsonColumn::create();
        src_column->append(json_str);

        auto converter = get_type_converter(TYPE_VARCHAR, TYPE_JSON);
        Status st = converter->convert_column(_varchar_type.get(), *src_column, _json_type.get(), dst_column.get(),
                                              &_mem_pool);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(*dst_column->get_object(0), expected);
    }
}

TEST_F(SchemaChangeTest, convert_json_to_varchar) {
    std::string json_str = "{\"a\": 1}";
    JsonValue json = JsonValue::parse(json_str).value();
    JsonColumn::Ptr src_column = JsonColumn::create();
    BinaryColumn::Ptr dst_column = BinaryColumn::create();
    src_column->append(&json);

    auto converter = get_type_converter(TYPE_JSON, TYPE_VARCHAR);
    Status st =
            converter->convert_column(_json_type.get(), *src_column, _varchar_type.get(), dst_column.get(), &_mem_pool);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(dst_column->get_slice(0), json_str);
}

TEST_F(SchemaChangeTest, schema_change_with_materialized_column_old_style) {
    TTabletId base_tablet_id = 1301;
    TTabletId new_tablet_id = 1302;
    Version version(3, 3);

    create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        TCreateTabletReq create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::INT);

        create_tablet_req.tablet_schema.columns.back().__set_is_allow_null(true);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    auto base_tablet_schema = base_tablet->tablet_schema();
    auto new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    for (size_t i = 0; i < 4; ++i) {
        ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(i);
        column_mapping->ref_column = i;
    }
    ASSERT_TRUE(chunk_changer.prepare().ok());

    std::vector<TExprNode> nodes;

    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.type = gen_type_desc(TPrimitiveType::INT);
    node.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node.__set_slot_ref(t_slot_ref);
    node.is_nullable = true;
    nodes.emplace_back(node);

    TExpr t_expr;
    t_expr.nodes = nodes;

    chunk_changer.init_runtime_state(TQueryOptions(), TQueryGlobals());

    ExprContext* ctx = nullptr;

    Status st =
            Expr::create_expr_tree(chunk_changer.get_object_pool(), t_expr, &ctx, chunk_changer.get_runtime_state());
    DCHECK(st.ok()) << st.message();
    st = ctx->prepare(chunk_changer.get_runtime_state());
    DCHECK(st.ok()) << st.message();
    st = ctx->open(chunk_changer.get_runtime_state());
    DCHECK(st.ok()) << st.message();

    chunk_changer.get_gc_exprs()->insert({3, ctx});

    auto sc_procedure = std::make_unique<SchemaChangeDirectly>(&chunk_changer);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    Schema base_schema = ChunkHelper::convert_schema(base_tablet_schema, chunk_changer.get_selected_column_indexes());

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, version);
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_OK(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, schema_change_with_materialized_column_optimization) {
    TTabletId base_tablet_id = 1401;
    TTabletId new_tablet_id = 1402;
    Version version(2, 2);

    create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        TCreateTabletReq create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "newcol", TPrimitiveType::INT);

        create_tablet_req.tablet_schema.columns.back().__set_is_allow_null(true);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    (void)new_tablet->set_tablet_state(TABLET_NOTREADY);

    TAlterTabletReqV2 request;
    TAlterTabletMaterializedColumnReq mc_request;

    std::vector<TExprNode> nodes;

    TExprNode node;
    node.node_type = TExprNodeType::SLOT_REF;
    node.type = gen_type_desc(TPrimitiveType::INT);
    node.num_children = 0;
    TSlotRef t_slot_ref = TSlotRef();
    t_slot_ref.slot_id = 0;
    t_slot_ref.tuple_id = 0;
    node.__set_slot_ref(t_slot_ref);
    node.is_nullable = true;
    nodes.emplace_back(node);

    TExpr t_expr;
    t_expr.nodes = nodes;

    std::map<int32_t, starrocks::TExpr> m_expr;
    m_expr.insert({4, t_expr});

    mc_request.__set_query_globals(TQueryGlobals());
    mc_request.__set_query_options(TQueryOptions());
    mc_request.__set_mc_exprs(m_expr);

    request.__set_base_schema_hash(base_tablet->schema_hash());
    request.__set_new_schema_hash(new_tablet->schema_hash());
    request.__set_base_tablet_id(base_tablet_id);
    request.__set_new_tablet_id(new_tablet_id);
    request.__set_alter_version(base_tablet->max_version().second);
    request.__set_tablet_type(TTabletType::TABLET_TYPE_DISK);
    request.__set_materialized_column_req(mc_request);
    request.__set_txn_id(99);
    request.__set_job_id(999);

    std::string alter_msg_header = strings::Substitute("[Alter Job:$0, tablet:$1]: ", 999, 1401);
    SchemaChangeHandler handler;
    handler.set_alter_msg_header(alter_msg_header);
    auto res = handler.process_alter_tablet(request);
    ASSERT_TRUE(res.ok()) << res.to_string();

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, overlapping_direct_schema_change) {
    TTabletId base_tablet_id = 1403;
    TTabletId new_tablet_id = 1404;
    Version version(2, 2);
    TPartitionId partition_id = 1;
    TTransactionId txn_id = 1;

    {
        create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
        auto slots = gen_base_slots();
        auto delta_writer = TabletTestHelper::create_delta_writer(base_tablet_id, slots, &_mem_tracker);

        Chunk chunk;
        auto c0 = ColumnTestHelper::build_column<int32_t>({0, 1, 2, 3});
        auto c1 = ColumnTestHelper::build_column<int32_t>({0, 1, 2, 3});
        auto c2 = ColumnTestHelper::build_column<int32_t>({0, 1, 2, 3});
        auto c3 = ColumnTestHelper::build_column<int32_t>({0, 1, 2, 3});
        chunk.append_column(std::move(c0), 0);
        chunk.append_column(std::move(c1), 1);
        chunk.append_column(std::move(c2), 2);
        chunk.append_column(std::move(c3), 3);

        std::vector<uint32_t> idxs{0, 1, 2, 3};
        ASSERT_OK(delta_writer->write(chunk, idxs.data(), 0, 4));
        ASSERT_OK(delta_writer->flush_memtable_async(false));
        ASSERT_OK(delta_writer->write(chunk, idxs.data(), 0, 4));
        ASSERT_OK(delta_writer->close());
        ASSERT_OK(delta_writer->commit());
    }

    auto base_tablet = _tablet_mgr->get_tablet(base_tablet_id);

    {
        std::map<TabletInfo, std::pair<RowsetSharedPtr, bool>> tablet_related_rs;
        _txn_mgr->get_txn_related_tablets(txn_id, partition_id, &tablet_related_rs);
        for (auto& tablet_rs : tablet_related_rs) {
            ASSERT_OK(_txn_mgr->publish_txn(partition_id, base_tablet, txn_id, version.second, tablet_rs.second.first,
                                            10000));
        }
    }

    auto base_rowset = base_tablet->get_inc_rowset_by_version(version);
    ASSERT_TRUE(base_rowset != nullptr);
    ASSERT_TRUE(base_rowset->is_overlapped());

    create_dest_tablet_with_index(base_tablet_id, new_tablet_id, TKeysType::DUP_KEYS);

    {
        SchemaChangeHandler handler;
        TAlterTabletReqV2 req1 = gen_alter_tablet_req(100003, 100004, version);
        ASSERT_FALSE(handler.process_alter_tablet(req1).ok());

        TAlterTabletReqV2 req2 = gen_alter_tablet_req(base_tablet_id, 100004, version);
        ASSERT_FALSE(handler.process_alter_tablet(req2).ok());

        TTabletId new_tablet_id2 = 1405;
        create_dest_tablet_with_index(base_tablet_id, new_tablet_id2, TKeysType::DUP_KEYS);
        auto new_tablet = _tablet_mgr->get_tablet(new_tablet_id2);
        ASSERT_TRUE(new_tablet != nullptr);
        new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);

        TAlterTabletReqV2 req3 = gen_alter_tablet_req(base_tablet_id, new_tablet_id2, version);
        ASSERT_FALSE(handler.process_alter_tablet(req3).ok());
        new_tablet->set_tablet_state(TabletState::TABLET_NOTREADY);

        PFailPointTriggerMode trigger_mode;
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("add_rowset_failed");
        fp->setMode(trigger_mode);
        ASSERT_FALSE(handler.process_alter_tablet(req3).ok());

        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(trigger_mode);
        fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("add_rowset_already_exist");
        trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
        fp->setMode(trigger_mode);
        ASSERT_TRUE(handler.process_alter_tablet(req3).ok());
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(trigger_mode);
    }

    TAlterTabletReqV2 req = gen_alter_tablet_req(base_tablet_id, new_tablet_id, version);

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(req));

    auto new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    auto new_tablet_schema = new_tablet->tablet_schema();

    auto new_rowset = new_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(new_rowset != nullptr);
    ASSERT_EQ(new_rowset->num_segments(), 1);

    auto seg_iters = TabletTestHelper::create_segment_iterators(*new_tablet, version, &_stats);
    ASSERT_EQ(seg_iters.size(), 1);

    Chunk result_chunk;
    ASSERT_OK(seg_iters[0]->get_next(&result_chunk));
    ASSERT_EQ(result_chunk.num_rows(), 8);

    ASSERT_EQ("[0, 0, 0, 0]", result_chunk.debug_row(0));
    ASSERT_EQ("[0, 0, 0, 0]", result_chunk.debug_row(1));
    ASSERT_EQ("[1, 1, 1, 1]", result_chunk.debug_row(2));
    ASSERT_EQ("[1, 1, 1, 1]", result_chunk.debug_row(3));
    ASSERT_EQ("[2, 2, 2, 2]", result_chunk.debug_row(4));
    ASSERT_EQ("[2, 2, 2, 2]", result_chunk.debug_row(5));
    ASSERT_EQ("[3, 3, 3, 3]", result_chunk.debug_row(6));
    ASSERT_EQ("[3, 3, 3, 3]", result_chunk.debug_row(7));

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

namespace {
TabletSchemaPB order_schema(const std::string& type, int changed_column, KeysType keys_type = AGG_KEYS) {
    auto pb = SchemaTestHelper::gen_schema_pb_of_dup(TYPE_INT, 0, 3, 2);
    pb.set_keys_type(keys_type);
    for (int i = 0; i < 3; ++i) {
        auto* col = pb.mutable_column(i);
        col->set_length(4);
        col->set_aggregation(i < 2 || keys_type == DUP_KEYS ? "NONE" : "SUM");
    }
    auto* col = pb.mutable_column(changed_column);
    col->set_type(type);
    col->set_length(type == "VARCHAR" || type == "CHAR" ? 32 : 16);
    if (type == "DECIMAL32" || type == "DECIMAL64" || type == "DECIMAL128") {
        col->set_precision(8);
        col->set_frac(2);
    }
    return pb;
}

void check_order_dispatch(const TabletSchemaPB& before, const TabletSchemaPB& after, bool expected_sorting) {
    auto base = TabletSchema::create(before);
    auto target = TabletSchema::create(after);
    ChunkChanger changer(target);
    bool sorting = false;
    bool directly = false;
    ASSERT_OK(
            SchemaChangeUtils::parse_request(base, target, &changer, {}, nullptr, false, &sorting, &directly, nullptr));
    EXPECT_EQ(expected_sorting, sorting);
    if (expected_sorting) {
        EXPECT_FALSE(directly);
    }
}
} // namespace

TEST_F(SchemaChangeTest, order_changing_key_conversions) {
    for (const auto& types : std::vector<std::pair<std::string, std::string>>{
                 {"TINYINT", "VARCHAR"},   {"SMALLINT", "VARCHAR"},  {"INT", "VARCHAR"},
                 {"BIGINT", "VARCHAR"},    {"LARGEINT", "VARCHAR"},  {"DECIMAL_V2", "VARCHAR"},
                 {"DECIMAL32", "VARCHAR"}, {"DECIMAL64", "VARCHAR"}, {"DECIMAL128", "VARCHAR"},
                 {"VARCHAR", "DECIMAL32"}, {"VARCHAR", "DECIMAL64"}, {"VARCHAR", "DECIMAL128"},
                 {"VARCHAR", "TINYINT"},   {"VARCHAR", "SMALLINT"},  {"VARCHAR", "INT"},
                 {"VARCHAR", "BIGINT"},    {"VARCHAR", "LARGEINT"},  {"VARCHAR", "DATE"},
                 {"INT", "DATE"},          {"DATETIME", "DATE"}}) {
        SCOPED_TRACE(types.first + " -> " + types.second);
        for (int position : {0, 1, 2}) {
            for (bool change_short_keys : {false, true}) {
                auto keys_type = position == 2 ? DUP_KEYS : AGG_KEYS;
                auto before = order_schema(types.first, position, keys_type);
                auto after = order_schema(types.second, position, keys_type);
                if (change_short_keys) after.set_num_short_key_columns(1);
                check_order_dispatch(before, after, position < 2);
            }
        }
    }
}

TEST_F(SchemaChangeTest, order_preserving_key_conversions) {
    const std::vector<std::string> integers{"TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"};
    for (int i = 0; i < integers.size(); ++i) {
        for (int j = i; j < integers.size(); ++j) {
            SCOPED_TRACE(integers[i] + " -> " + integers[j]);
            check_order_dispatch(order_schema(integers[i], 0), order_schema(integers[j], 0), false);
        }
    }
    for (const auto& types : std::vector<std::pair<std::string, std::string>>{
                 {"DATE", "DATETIME"}, {"CHAR", "VARCHAR"}, {"CHAR", "CHAR"}, {"VARCHAR", "VARCHAR"}}) {
        auto before = order_schema(types.first, 0);
        auto after = order_schema(types.second, 0);
        after.mutable_column(0)->set_length(64);
        after.mutable_column(0)->set_is_nullable(true);
        before.mutable_column(0)->set_is_nullable(false);
        check_order_dispatch(before, after, false);
    }
    const std::vector<std::pair<std::string, int>> decimals{{"DECIMAL32", 9}, {"DECIMAL64", 18}, {"DECIMAL128", 38}};
    for (int i = 0; i < decimals.size(); ++i) {
        for (int j = 0; j < decimals.size(); ++j) {
            for (int scale_increase : {0, 1}) {
                auto before = order_schema(decimals[i].first, 0);
                auto after = order_schema(decimals[j].first, 0);
                before.mutable_column(0)->set_precision(std::min(decimals[i].second, decimals[j].second) - 1);
                before.mutable_column(0)->set_frac(2);
                after.mutable_column(0)->set_precision(decimals[j].second);
                after.mutable_column(0)->set_frac(2 + scale_increase);
                check_order_dispatch(before, after, false);
            }
        }
    }
    auto before = order_schema("DECIMAL_V2", 0);
    auto after = order_schema("DECIMAL128", 0);
    after.mutable_column(0)->set_precision(27);
    after.mutable_column(0)->set_frac(9);
    check_order_dispatch(before, after, false);
}

TEST_F(SchemaChangeTest, order_conversion_uses_sort_keys) {
    auto before = order_schema("BIGINT", 2, DUP_KEYS);
    auto after = order_schema("VARCHAR", 2, DUP_KEYS);
    before.add_sort_key_idxes(2);
    before.add_sort_key_idxes(0);
    after.add_sort_key_idxes(2);
    after.add_sort_key_idxes(0);
    check_order_dispatch(before, after, true);
    before = order_schema("BIGINT", 0, DUP_KEYS);
    after = order_schema("VARCHAR", 0, DUP_KEYS);
    before.add_sort_key_idxes(1);
    after.add_sort_key_idxes(1);
    before.set_num_short_key_columns(1);
    after.set_num_short_key_columns(1);
    check_order_dispatch(before, after, false);

    // A benign earlier conversion must not hide a later order-changing one.
    before = order_schema("BIGINT", 1);
    after = order_schema("VARCHAR", 1);
    after.mutable_column(0)->set_type("BIGINT");
    check_order_dispatch(before, after, true);

    // Losing time within the last DUP sort key preserves order; a following key does not.
    for (int position : {0, 1}) {
        check_order_dispatch(order_schema("DATETIME", position, DUP_KEYS), order_schema("DATE", position, DUP_KEYS),
                             position == 0);
    }
}

TEST_F(SchemaChangeTest, historical_key_conversion_order) {
    const TTabletId base_id = 1415101;
    const TTabletId target_id = 1415102;
    const Version version(2, 2);
    for (bool target : {false, true}) {
        auto req = TabletTestHelper::gen_create_tablet_req(target ? target_id : base_id, TKeysType::AGG_KEYS,
                                                           TStorageType::COLUMN);
        if (target) req.__set_base_tablet_id(base_id);
        add_key_column(&req, "k1", target ? TPrimitiveType::VARCHAR : TPrimitiveType::BIGINT);
        add_key_column(&req, "k2", TPrimitiveType::INT);
        add_value_column(&req, "v", TPrimitiveType::BIGINT, TKeysType::AGG_KEYS);
        if (target) {
            req.tablet_schema.columns[0].column_type.__set_len(32);
            req.tablet_schema.__set_short_key_column_count(1);
        }
        ASSERT_OK(_storage_engine->create_tablet(req));
    }
    auto base = _tablet_mgr->get_tablet(base_id);
    auto target = _tablet_mgr->get_tablet(target_id);
    DeferOp cleanup([&] {
        (void)_tablet_mgr->drop_tablet(base_id);
        (void)_tablet_mgr->drop_tablet(target_id);
    });
    auto chunk = ChunkHelper::new_chunk(*base->tablet_schema()->schema(), 16384);
    for (int64_t k1 : {9, 10, 90, 100}) {
        for (int32_t k2 = 0; k2 < 4096; ++k2) {
            chunk->get_column_by_index(0)->append_datum(Datum(k1));
            chunk->get_column_by_index(1)->append_datum(Datum(k2));
            chunk->get_column_by_index(2)->append_datum(Datum(int64_t{1}));
        }
    }
    auto writer = TabletTestHelper::create_rowset_writer(*base, next_rowset_id(), version);
    ASSERT_OK(writer->add_chunk(*chunk));
    ASSERT_OK(writer->flush());
    auto rowset = writer->build();
    ASSERT_OK(rowset.status());
    ASSERT_OK(base->add_rowset(*rowset, false));

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(gen_alter_tablet_req(base_id, target_id, version)));
    auto converted = target->get_rowset_by_version(version);
    ASSERT_NE(nullptr, converted);
    ASSERT_OK(converted->load());
    ASSERT_EQ(1, converted->num_segments());
    std::vector<std::string> short_keys;
    ASSERT_OK(converted->segments()[0]->get_short_key_index(&short_keys));
    ASSERT_GT(short_keys.size(), 4);
    EXPECT_TRUE(std::is_sorted(short_keys.begin(), short_keys.end()));
    auto iters = TabletTestHelper::create_segment_iterators(*target, version, &_stats);
    ASSERT_EQ(1, iters.size());
    auto result = ChunkHelper::new_chunk(*target->tablet_schema()->schema(), 4096);
    std::vector<std::string> expected{"10", "100", "9", "90"};
    size_t count = 0;
    while (true) {
        result->reset();
        auto st = iters[0]->get_next(result.get());
        if (st.is_end_of_file()) break;
        ASSERT_OK(st);
        for (size_t i = 0; i < result->num_rows(); ++i, ++count) {
            ASSERT_LT(count, 16384);
            ASSERT_EQ(expected[count / 4096], result->get_column_by_index(0)->get(i).get_slice().to_string());
            EXPECT_EQ(count % 4096, result->get_column_by_index(1)->get(i).get_int32());
            EXPECT_EQ(1, result->get_column_by_index(2)->get(i).get_int64());
        }
    }
    EXPECT_EQ(16384, count);
    iters[0]->close();
}

TEST_F(SchemaChangeTest, order_conversion_comparator_evidence) {
    auto check = [&](TabletSchemaPB before, TabletSchemaPB after, const std::vector<std::string>& values,
                     bool strictly_ordered) {
        auto base = TabletSchema::create(before);
        auto target = TabletSchema::create(after);
        auto base_schema = ChunkHelper::convert_schema(base);
        auto target_schema = ChunkHelper::convert_schema(target);
        ChunkChanger changer(target);
        bool sorting = false, directly = false;
        ASSERT_OK(SchemaChangeUtils::parse_request(base, target, &changer, {}, nullptr, false, &sorting, &directly,
                                                   nullptr));
        ChunkPtr input = ChunkHelper::new_chunk(base_schema, values.size() + 1);
        for (int i = -1; i < static_cast<int>(values.size()); ++i) {
            Datum value;
            if (i >= 0) {
                ASSERT_OK(datum_from_string(base_schema.field(0)->type().get(), &value, values[i], &_mem_pool));
            }
            input->get_column_by_index(0)->append_datum(value);
            input->get_column_by_index(1)->append_datum(Datum(int32_t(values.size() - i)));
            input->get_column_by_index(2)->append_datum(Datum(int32_t{1}));
        }
        ChunkPtr output = ChunkHelper::new_chunk(target_schema, input->num_rows());
        ASSERT_TRUE(changer.change_chunk_v2(input, output, base_schema, target_schema, &_mem_pool));
        bool preserves = true;
        for (int i = 1; i < input->num_rows(); ++i) {
            ASSERT_LT(input->get_column_by_index(0)->compare_at(i - 1, i, *input->get_column_by_index(0), -1), 0);
            preserves &= output->get_column_by_index(0)->compare_at(i - 1, i, *output->get_column_by_index(0), -1) < 0;
        }
        EXPECT_EQ(strictly_ordered, preserves);
        EXPECT_EQ(!strictly_ordered, sorting);
    };
    const std::vector<std::string> ints{"TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"};
    const std::vector<std::vector<std::string>> limits{{"-128", "-1", "0", "1", "127"},
                                                       {"-32768", "-1", "0", "1", "32767"},
                                                       {"-2147483648", "-1", "0", "1", "2147483647"},
                                                       {"-9223372036854775808", "-1", "0", "1", "9223372036854775807"}};
    for (int i = 0; i < limits.size(); ++i) {
        for (int j = i + 1; j < ints.size(); ++j) {
            SCOPED_TRACE(ints[i] + " -> " + ints[j]);
            check(order_schema(ints[i], 0), order_schema(ints[j], 0), limits[i], true);
        }
        if (i < 3) check(order_schema(ints[i], 0), order_schema("DOUBLE", 0), limits[i], true);
    }
    check(order_schema("CHAR", 0), order_schema("VARCHAR", 0), {"", "a", "aa", "z"}, true);
    auto wider_string = order_schema("VARCHAR", 0);
    wider_string.mutable_column(0)->set_length(64);
    check(order_schema("VARCHAR", 0), wider_string, {"", "a", "aa", "z"}, true);
    check(order_schema("DATE", 0), order_schema("DATETIME", 0), {"1000-01-01", "2000-02-29", "9999-12-31"}, true);
    check(order_schema("BIGINT", 0), order_schema("DOUBLE", 0), {"9007199254740992", "9007199254740993"}, false);
    check(order_schema("FLOAT", 0), order_schema("DOUBLE", 0), {"0.0000001", "0.0000002"}, false);
    check(order_schema("BIGINT", 0), order_schema("VARCHAR", 0), {"9", "10", "90", "100"}, false);
    check(order_schema("VARCHAR", 0), order_schema("BIGINT", 0), {"10", "100", "9", "90", "bad"}, false);
    check(order_schema("INT", 0), order_schema("DATE", 0), {"691231", "700101"}, false);
    check(order_schema("DATETIME", 0), order_schema("DATE", 0), {"2026-11-17 01:00:00", "2026-11-17 02:00:00"}, false);
    for (const auto& type : {"DECIMAL32", "DECIMAL64", "DECIMAL128"}) {
        auto before = order_schema(type, 0);
        before.mutable_column(0)->set_precision(8);
        before.mutable_column(0)->set_frac(2);
        auto after = order_schema("DECIMAL128", 0);
        after.mutable_column(0)->set_precision(38);
        after.mutable_column(0)->set_frac(9);
        check(before, after, {"-999999.99", "-0.01", "0", "0.01", "999999.99"}, true);
        for (const auto& target_type : {"DECIMAL32", "DECIMAL64", "DECIMAL128"}) {
            auto exact_target = order_schema(target_type, 0);
            exact_target.mutable_column(0)->set_precision(9);
            exact_target.mutable_column(0)->set_frac(3);
            check(before, exact_target, {"-999999.99", "-0.01", "0", "0.01", "999999.99"}, true);
        }
        after = order_schema("VARCHAR", 0);
        check(before, after, {"9.00", "10.00"}, false);
        check(after, before, {"10.00", "100.00", "9.00"}, false);
    }
    auto before = order_schema("DECIMAL_V2", 0);
    auto after = order_schema("DECIMAL128", 0);
    after.mutable_column(0)->set_precision(27);
    after.mutable_column(0)->set_frac(9);
    check(before, after, {"-999999999999999999.999999999", "-0.000000001", "0", "999999999999999999.999999999"}, true);
}

TEST_F(SchemaChangeTest, historical_key_conversion_order_and_aggregation) {
    TTabletId next_id = 1415200;
    for (bool strings : {false, true}) {
        const TTabletId base_id = next_id++;
        const TTabletId target_id = next_id++;
        const Version version(2, 2);
        for (bool target : {false, true}) {
            auto req = TabletTestHelper::gen_create_tablet_req(target ? target_id : base_id, TKeysType::AGG_KEYS,
                                                               TStorageType::COLUMN);
            if (target) req.__set_base_tablet_id(base_id);
            auto type = strings ? (target ? TPrimitiveType::BIGINT : TPrimitiveType::VARCHAR)
                                : (target ? TPrimitiveType::DATE : TPrimitiveType::DATETIME);
            add_key_column(&req, "k1", type);
            add_key_column(&req, "k2", TPrimitiveType::INT);
            add_value_column(&req, "v", TPrimitiveType::BIGINT, TKeysType::AGG_KEYS);
            req.tablet_schema.columns[0].__set_is_allow_null(true);
            if (type == TPrimitiveType::VARCHAR) req.tablet_schema.columns[0].column_type.__set_len(32);
            req.tablet_schema.__set_short_key_column_count(1);
            ASSERT_OK(_storage_engine->create_tablet(req));
        }
        auto base = _tablet_mgr->get_tablet(base_id);
        auto target = _tablet_mgr->get_tablet(target_id);
        DeferOp cleanup([&] {
            (void)_tablet_mgr->drop_tablet(base_id);
            (void)_tablet_mgr->drop_tablet(target_id);
        });
        std::vector<std::string> keys =
                strings ? std::vector<std::string>{"01", "1", "bad"}
                        : std::vector<std::string>{"2026-11-17 01:00:00", "2026-11-17 02:00:00", "2026-11-18 00:00:00"};
        auto chunk = ChunkHelper::new_chunk(*base->tablet_schema()->schema(), 12288);
        auto field_type = get_type_info(base->tablet_schema()->column(0));
        for (const auto& text : keys) {
            Datum key;
            ASSERT_OK(datum_from_string(field_type.get(), &key, text, &_mem_pool));
            for (int32_t suffix = 0; suffix < 4096; ++suffix) {
                chunk->get_column_by_index(0)->append_datum(key);
                chunk->get_column_by_index(1)->append_datum(Datum(suffix));
                chunk->get_column_by_index(2)->append_datum(Datum(int64_t{1}));
            }
        }
        auto writer = TabletTestHelper::create_rowset_writer(*base, next_rowset_id(), version);
        ASSERT_OK(writer->add_chunk(*chunk));
        ASSERT_OK(writer->flush());
        auto rowset = writer->build();
        ASSERT_OK(rowset.status());
        ASSERT_OK(base->add_rowset(*rowset, false));
        SchemaChangeHandler handler;
        ASSERT_OK(handler.process_alter_tablet(gen_alter_tablet_req(base_id, target_id, version)));
        auto converted = target->get_rowset_by_version(version);
        ASSERT_NE(nullptr, converted);
        ASSERT_EQ(8192, converted->num_rows());
        ASSERT_OK(converted->load());
        ASSERT_EQ(1, converted->num_segments());
        std::vector<std::string> short_keys;
        ASSERT_OK(converted->segments()[0]->get_short_key_index(&short_keys));
        EXPECT_TRUE(std::is_sorted(short_keys.begin(), short_keys.end()));
        auto iters = TabletTestHelper::create_segment_iterators(*target, version, &_stats);
        ASSERT_EQ(1, iters.size());
        auto result = ChunkHelper::new_chunk(*target->tablet_schema()->schema(), 4096);
        size_t count = 0;
        while (true) {
            result->reset();
            auto st = iters[0]->get_next(result.get());
            if (st.is_end_of_file()) break;
            ASSERT_OK(st);
            for (size_t i = 0; i < result->num_rows(); ++i, ++count) {
                ASSERT_LT(count, 8192);
                auto key = result->get_column_by_index(0)->get(i);
                if (strings) {
                    // branch-4.0 converts the unparseable "bad" to 0, not NULL (#75538 and #76707 are not on 4.0).
                    ASSERT_FALSE(key.is_null());
                    ASSERT_EQ(count < 4096 ? 0 : 1, key.get_int64());
                } else {
                    ASSERT_EQ(count < 4096 ? "2026-11-17" : "2026-11-18", key.get_date().to_string());
                }
                ASSERT_EQ(count % 4096, result->get_column_by_index(1)->get(i).get_int32());
                const bool merged_group = strings ? count >= 4096 : count < 4096;
                ASSERT_EQ(merged_group ? 2 : 1, result->get_column_by_index(2)->get(i).get_int64());
            }
        }
        EXPECT_EQ(8192, count);
        iters[0]->close();
    }
}

TEST_F(SchemaChangeTest, sorted_key_conversion_generated_columns) {
    TTabletId next_id = 1415300;
    for (int mode = 0; mode < 4; ++mode) {
        SCOPED_TRACE(mode); // ADD, MODIFY, constant NULL, invalid target index.
        const auto base_id = next_id++;
        const auto target_id = next_id++;
        const Version version(2, 2);
        for (bool target : {false, true}) {
            auto req = TabletTestHelper::gen_create_tablet_req(target ? target_id : base_id, TKeysType::DUP_KEYS,
                                                               TStorageType::COLUMN);
            if (target) req.__set_base_tablet_id(base_id);
            add_key_column(&req, "k", target ? TPrimitiveType::VARCHAR : TPrimitiveType::BIGINT);
            if (target) req.tablet_schema.columns[0].column_type.__set_len(32);
            add_value_column(&req, "v", TPrimitiveType::INT);
            req.tablet_schema.columns.back().__set_is_allow_null(true);
            if (target || mode == 1) {
                add_value_column(&req, "g", TPrimitiveType::INT);
                req.tablet_schema.columns.back().__set_is_allow_null(true);
            }
            req.tablet_schema.__set_short_key_column_count(1);
            ASSERT_OK(_storage_engine->create_tablet(req));
        }
        DeferOp cleanup([&] {
            (void)_tablet_mgr->drop_tablet(base_id);
            (void)_tablet_mgr->drop_tablet(target_id);
        });
        auto base = _tablet_mgr->get_tablet(base_id);
        auto target = _tablet_mgr->get_tablet(target_id);
        auto chunk = ChunkHelper::new_chunk(*base->tablet_schema()->schema(), 4);
        const std::vector<int64_t> keys{9, 10, 90, 100};
        for (size_t i = 0; i < keys.size(); ++i) {
            chunk->get_column_by_index(0)->append_datum(Datum(keys[i]));
            chunk->get_column_by_index(1)->append_datum(i == 1 ? Datum() : Datum(static_cast<int32_t>(i + 1)));
            if (mode == 1) chunk->get_column_by_index(2)->append_datum(Datum(int32_t{99}));
        }
        auto writer = TabletTestHelper::create_rowset_writer(*base, next_rowset_id(), version);
        ASSERT_OK(writer->add_chunk(*chunk));
        ASSERT_OK(writer->flush());
        auto rowset = writer->build();
        ASSERT_OK(rowset.status());
        ASSERT_OK(base->add_rowset(*rowset, false));

        TExprNode node;
        node.__set_node_type(mode == 2 ? TExprNodeType::NULL_LITERAL : TExprNodeType::SLOT_REF);
        node.__set_type(gen_type_desc(TPrimitiveType::INT));
        node.__set_num_children(0);
        node.__set_is_nullable(true);
        if (mode != 2) {
            TSlotRef slot;
            slot.__set_slot_id(1);
            slot.__set_tuple_id(0);
            node.__set_slot_ref(slot);
        }
        TExpr expr;
        expr.__set_nodes({node});
        TAlterTabletMaterializedColumnReq materialized;
        materialized.__set_query_options(TQueryOptions());
        materialized.__set_query_globals(TQueryGlobals());
        materialized.__set_mc_exprs({{mode == 3 ? 3 : 2, expr}});
        auto request = gen_alter_tablet_req(base_id, target_id, version);
        request.__set_materialized_column_req(materialized);
        SchemaChangeHandler handler;
        auto status = handler.process_alter_tablet(request);
        if (mode == 3) {
            ASSERT_FALSE(status.ok());
            EXPECT_NE(std::string::npos, status.message().find("generated column index out of range"));
            EXPECT_EQ(TABLET_NOTREADY, target->tablet_state());
            continue;
        }
        ASSERT_OK(status);
        auto iters = TabletTestHelper::create_segment_iterators(*target, version, &_stats);
        ASSERT_EQ(1, iters.size());
        auto result = ChunkHelper::new_chunk(*target->tablet_schema()->schema(), 4);
        ASSERT_OK(iters[0]->get_next(result.get()));
        ASSERT_EQ(4, result->num_rows());
        const std::vector<std::string> expected_keys{"10", "100", "9", "90"};
        const std::vector<int32_t> expected_values{0, 4, 1, 3};
        for (size_t i = 0; i < 4; ++i) {
            EXPECT_EQ(expected_keys[i], result->get_column_by_index(0)->get(i).get_slice().to_string());
            const auto value = result->get_column_by_index(2)->get(i);
            ASSERT_EQ(mode == 2 || i == 0, value.is_null());
            if (!value.is_null()) {
                EXPECT_EQ(expected_values[i], value.get_int32());
            }
        }
        result->reset();
        EXPECT_TRUE(iters[0]->get_next(result.get()).is_end_of_file());
        iters[0]->close();
    }
}

TEST_F(SchemaChangeTest, order_conversion_exact_and_lossy_numeric) {
    for (const auto& type : {"TINYINT", "SMALLINT", "INT"}) {
        check_order_dispatch(order_schema(type, 0), order_schema("DOUBLE", 0), false);
    }
    for (const auto& type : {"BIGINT", "FLOAT"}) {
        check_order_dispatch(order_schema(type, 0), order_schema("DOUBLE", 0), true);
        check_order_dispatch(order_schema(type, 1, DUP_KEYS), order_schema("DOUBLE", 1, DUP_KEYS), false);
    }
    auto before = order_schema("VARCHAR", 0);
    auto after = order_schema("BIGINT", 0);
    before.mutable_column(0)->set_is_nullable(false);
    after.mutable_column(0)->set_is_nullable(false);
    auto base = TabletSchema::create(before);
    auto target = TabletSchema::create(after);
    ChunkChanger changer(target);
    bool sorting = false, directly = false;
    ASSERT_OK(
            SchemaChangeUtils::parse_request(base, target, &changer, {}, nullptr, false, &sorting, &directly, nullptr));
    EXPECT_TRUE(sorting);
    // The main-branch check that an unparseable string fails a NOT NULL numeric conversion is
    // dropped: branch-4.0 converts it to 0 (#75538 and #76707 are not on 4.0).
}

} // namespace starrocks
