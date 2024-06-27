// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/schema_change.h"

#include <utility>

#include "column/datum_convert.h"
#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/delta_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/schema_test_helper.h"
#include "testutil/tablet_test_helper.h"
#include "util/logging.h"

namespace starrocks::vectorized {

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

    std::shared_ptr<TabletSchema> gen_tablet_schema(const std::string& name, const std::string& type,
                                                    const std::string& aggregation, uint32_t length);
    template <typename T>
    void test_convert_to_varchar(FieldType type, int type_size, T val, const std::string& expect_val);
    template <typename T>
    void test_convert_from_varchar(FieldType type, int type_size, std::string val, T expect_val);
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
    const TypeInfoPtr _json_type = get_type_info(FieldType::OLAP_FIELD_TYPE_JSON);
    const TypeInfoPtr _varchar_type = get_type_info(FieldType::OLAP_FIELD_TYPE_VARCHAR);
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
    col.__set_is_bloom_filter_column(true);
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
    Schema base_schema = *tablet->tablet_schema().schema();
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

std::shared_ptr<TabletSchema> SchemaChangeTest::gen_tablet_schema(const std::string& name, const std::string& type,
                                                                  const std::string& aggregation, uint32_t length) {
    TabletSchemaPB tablet_schema_pb;
    SchemaTestHelper::add_column_pb_to_tablet_schema(&tablet_schema_pb, name, type, aggregation, length);
    return std::make_shared<TabletSchema>(tablet_schema_pb);
}

template <typename T>
void SchemaChangeTest::test_convert_to_varchar(FieldType type, int type_size, T val, const std::string& expect_val) {
    auto src_tablet_schema = gen_tablet_schema("c1", field_type_to_string(type), "REPLACE", type_size);
    auto dst_tablet_schema = gen_tablet_schema("c2", "VARCHAR", "REPLACE", 255);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum(val);
    Datum dst_datum;

    auto converter = get_type_converter(type, OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool));
    EXPECT_EQ(expect_val, dst_datum.get_slice().to_string());
}

template <typename T>
void SchemaChangeTest::test_convert_from_varchar(FieldType type, int type_size, std::string val, T expect_val) {
    auto src_tablet_schema = gen_tablet_schema("c1", "VARCHAR", "REPLACE", 255);
    auto dst_tablet_schema = gen_tablet_schema("c2", field_type_to_string(type), "REPLACE", type_size);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum(Slice((char*)val.data(), val.size()));
    Datum dst_datum;

    auto converter = get_type_converter(OLAP_FIELD_TYPE_VARCHAR, type);
    ASSERT_OK(converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool));
    EXPECT_EQ(expect_val, dst_datum.get<T>());
}

TEST_F(SchemaChangeTest, convert_tinyint_to_varchar) {
    test_convert_to_varchar<int8_t>(OLAP_FIELD_TYPE_TINYINT, 1, 127, "127");
}

TEST_F(SchemaChangeTest, convert_smallint_to_varchar) {
    test_convert_to_varchar<int16_t>(OLAP_FIELD_TYPE_SMALLINT, 2, 32767, "32767");
}

TEST_F(SchemaChangeTest, convert_int_to_varchar) {
    test_convert_to_varchar<int32_t>(OLAP_FIELD_TYPE_INT, 4, 2147483647, "2147483647");
}

TEST_F(SchemaChangeTest, convert_bigint_to_varchar) {
    test_convert_to_varchar<int64_t>(OLAP_FIELD_TYPE_BIGINT, 8, 9223372036854775807, "9223372036854775807");
}

TEST_F(SchemaChangeTest, convert_largeint_to_varchar) {
    test_convert_to_varchar<int128_t>(OLAP_FIELD_TYPE_LARGEINT, 16, 1701411834604690, "1701411834604690");
}

TEST_F(SchemaChangeTest, convert_float_to_varchar) {
    test_convert_to_varchar<float>(OLAP_FIELD_TYPE_FLOAT, 4, 3.40282e+38, "3.40282e+38");
}

TEST_F(SchemaChangeTest, convert_double_to_varchar) {
    test_convert_to_varchar<double>(OLAP_FIELD_TYPE_DOUBLE, 8, 123.456, "123.456");
}

TEST_F(SchemaChangeTest, convert_varchar_to_tinyint) {
    test_convert_from_varchar<int8_t>(OLAP_FIELD_TYPE_TINYINT, 1, "127", 127);
}

TEST_F(SchemaChangeTest, convert_varchar_to_smallint) {
    test_convert_from_varchar<int16_t>(OLAP_FIELD_TYPE_SMALLINT, 2, "32767", 32767);
}

TEST_F(SchemaChangeTest, convert_varchar_to_int) {
    test_convert_from_varchar<int32_t>(OLAP_FIELD_TYPE_INT, 4, "2147483647", 2147483647);
}

TEST_F(SchemaChangeTest, convert_varchar_to_bigint) {
    test_convert_from_varchar<int64_t>(OLAP_FIELD_TYPE_BIGINT, 8, "9223372036854775807", 9223372036854775807);
}

TEST_F(SchemaChangeTest, convert_varchar_to_largeint) {
    test_convert_from_varchar<int128_t>(OLAP_FIELD_TYPE_LARGEINT, 16, "1701411834604690", 1701411834604690);
}

TEST_F(SchemaChangeTest, convert_varchar_to_float) {
    test_convert_from_varchar<float>(OLAP_FIELD_TYPE_FLOAT, 4, "3.40282e+38", 3.40282e+38);
}

TEST_F(SchemaChangeTest, convert_varchar_to_double) {
    test_convert_from_varchar<double>(OLAP_FIELD_TYPE_DOUBLE, 8, "123.456", 123.456);
}

TEST_F(SchemaChangeTest, convert_float_to_double) {
    auto src_tablet_schema = gen_tablet_schema("c1", "FLOAT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "DOUBLE", "REPLACE", 8);

    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum((float)(1.2345));
    Datum dst_datum;

    auto converter = get_type_converter(OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_DOUBLE);
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
    auto converter = vectorized::get_type_converter(OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_DATE);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    ASSERT_EQ(dst_value, dst_datum.get_uint24());
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
    auto converter = vectorized::get_type_converter(OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_DATETIME);

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
    auto converter = vectorized::get_type_converter(OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE_V2);

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
    auto converter = vectorized::get_type_converter(OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE);

    Status st = converter->convert_datum(f1.type().get(), src_datum, f2.type().get(), &dst_datum, &_mem_pool);
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    EXPECT_EQ(dst_value, dst_datum.get_uint24());
}

TEST_F(SchemaChangeTest, convert_int_to_bitmap) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "OBJECT", "BITMAP_UNION", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(OLAP_FIELD_TYPE_INT, OLAP_MATERIALIZE_TYPE_BITMAP);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const BitmapValue* bitmap_val = dst_datum.get_bitmap();
    EXPECT_EQ(bitmap_val->cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_varchar_to_hll) {
    auto src_tablet_schema = gen_tablet_schema("c1", "VARCHAR", "REPLACE", 255);
    auto dst_tablet_schema = gen_tablet_schema("c2", "HLL", "HLL_UNION", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string str = "test string";
    Slice slice(str.data(), str.size());
    src_datum.set_slice(slice);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(OLAP_FIELD_TYPE_VARCHAR, OLAP_MATERIALIZE_TYPE_HLL);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const HyperLogLog* hll = dst_datum.get_hyperloglog();
    EXPECT_EQ(hll->estimate_cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_int_to_count) {
    auto src_tablet_schema = gen_tablet_schema("c1", "INT", "REPLACE", 4);
    auto dst_tablet_schema = gen_tablet_schema("c2", "BIGINT", "SUM", 8);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema_to_format_v2(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f1 = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(OLAP_FIELD_TYPE_INT, OLAP_MATERIALIZE_TYPE_COUNT);
    Status st = converter->convert_materialized(src_col, dst_col, f1.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    EXPECT_EQ(dst_datum.get_int64(), 1);
}

TEST_F(SchemaChangeTest, convert_from) {
    TTabletId base_tablet_id = 1001;
    TTabletId new_tablet_id = 1002;
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
    const auto& base_tablet_schema = base_tablet->tablet_schema();
    const auto& new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    auto indexs = chunk_changer.get_mutable_selected_column_indexs();
    for (size_t i = 0; i < 4; ++i) {
        ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(i);
        column_mapping->ref_column = i;
        column_mapping->ref_base_reader_column_index = i;
        indexs->emplace_back(i);
    }
    auto sc_procedure = std::make_shared<SchemaChangeDirectly>(&chunk_changer);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    auto base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet_schema);

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_TRUE(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
}

TEST_F(SchemaChangeTest, schema_change_with_sorting) {
    TTabletId base_tablet_id = 1003;
    TTabletId new_tablet_id = 1004;
    Version version(3, 3);

    create_base_tablet(base_tablet_id, TKeysType::DUP_KEYS, TStorageType::COLUMN);
    write_data_to_base_tablet(base_tablet_id, version);

    {
        TCreateTabletReq create_tablet_req =
                TabletTestHelper::gen_create_tablet_req(new_tablet_id, TKeysType::UNIQUE_KEYS, TStorageType::COLUMN);
        add_key_column(&create_tablet_req, "k1", TPrimitiveType::INT);
        add_key_column(&create_tablet_req, "k2", TPrimitiveType::INT);
        add_value_column(&create_tablet_req, "v1", TPrimitiveType::BIGINT);
        add_value_column(&create_tablet_req, "v2", TPrimitiveType::VARCHAR);
        add_value_column(&create_tablet_req, "v3", TPrimitiveType::HLL);
        Status res = _storage_engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok()) << res.to_string();
    }

    TabletSharedPtr base_tablet = _tablet_mgr->get_tablet(base_tablet_id);
    TabletSharedPtr new_tablet = _tablet_mgr->get_tablet(new_tablet_id);
    const auto& base_tablet_schema = base_tablet->tablet_schema();
    const auto& new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    auto indexs = chunk_changer.get_mutable_selected_column_indexs();

    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping->ref_base_reader_column_index = 0;
    indexs->emplace_back(0);
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping->ref_base_reader_column_index = 1;
    indexs->emplace_back(1);
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    column_mapping->ref_base_reader_column_index = 2;
    indexs->emplace_back(2);
    column_mapping = chunk_changer.get_mutable_column_mapping(3);
    column_mapping->ref_column = 3;
    column_mapping->ref_base_reader_column_index = 3;
    indexs->emplace_back(3);
    column_mapping = chunk_changer.get_mutable_column_mapping(4);
    column_mapping->ref_column = -1;
    SchemaChangeUtils::init_column_mapping(column_mapping, new_tablet_schema.column(4), "");

    auto sc_procedure = std::make_shared<SchemaChangeWithSorting>(&chunk_changer, _mem_limit);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    auto base_schema = ChunkHelper::convert_schema_to_format_v2(base_tablet_schema);

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_TRUE(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));

    (void)_tablet_mgr->drop_tablet(base_tablet_id);
    (void)_tablet_mgr->drop_tablet(new_tablet_id);
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
    const auto& base_tablet_schema = base_tablet->tablet_schema();
    const auto& new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    auto indexs = chunk_changer.get_mutable_selected_column_indexs();
    for (size_t i = 0; i < 4; ++i) {
        ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(i);
        column_mapping->ref_column = i;
        column_mapping->ref_base_reader_column_index = i;
        indexs->emplace_back(i);
    }
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet_schema, *chunk_changer.get_mutable_selected_column_indexs());

    auto sc_procedure = std::make_unique<SchemaChangeDirectly>(&chunk_changer);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_TRUE(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));

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
    const auto& base_tablet_schema = base_tablet->tablet_schema();
    const auto& new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    auto indexs = chunk_changer.get_mutable_selected_column_indexs();
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping->ref_base_reader_column_index = 0;
    indexs->emplace_back(0);
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping->ref_base_reader_column_index = 1;
    indexs->emplace_back(1);
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    column_mapping->ref_base_reader_column_index = 2;
    indexs->emplace_back(2);
    column_mapping = chunk_changer.get_mutable_column_mapping(3);
    column_mapping->ref_column = 3;
    column_mapping->ref_base_reader_column_index = 3;
    indexs->emplace_back(3);

    auto sc_procedure = std::make_unique<SchemaChangeWithSorting>(&chunk_changer, _mem_limit);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet_schema, *chunk_changer.get_mutable_selected_column_indexs());

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, rowset->version());
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_TRUE(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));
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
    const auto& base_tablet_schema = base_tablet->tablet_schema();
    const auto& new_tablet_schema = new_tablet->tablet_schema();

    ChunkChanger chunk_changer(new_tablet_schema);
    auto indexs = chunk_changer.get_mutable_selected_column_indexs();
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping->ref_base_reader_column_index = 0;
    indexs->emplace_back(0);
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping->ref_base_reader_column_index = 1;
    indexs->emplace_back(1);
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    column_mapping->ref_base_reader_column_index = 2;
    indexs->emplace_back(2);

    auto sc_procedure = std::make_unique<SchemaChangeWithSorting>(&chunk_changer, _mem_limit);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet_schema, *chunk_changer.get_mutable_selected_column_indexs());

    auto rowset_reader = TabletTestHelper::create_rowset_reader(base_tablet, base_schema, version);
    auto rowset_writer = TabletTestHelper::create_rowset_writer(*new_tablet, next_rowset_id(), version);

    ASSERT_TRUE(sc_procedure->process(rowset_reader.get(), rowset_writer.get(), new_tablet, base_tablet, rowset));
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

        auto converter = get_type_converter(OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_JSON);
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

    auto converter = get_type_converter(OLAP_FIELD_TYPE_JSON, OLAP_FIELD_TYPE_VARCHAR);
    Status st =
            converter->convert_column(_json_type.get(), *src_column, _varchar_type.get(), dst_column.get(), &_mem_pool);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(dst_column->get_slice(0), json_str);
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
        chunk.append_column(c0, 0);
        chunk.append_column(c1, 1);
        chunk.append_column(c2, 2);
        chunk.append_column(c3, 3);

        std::vector<uint32_t> idxs{0, 1, 2, 3};
        ASSERT_OK(delta_writer->write(chunk, idxs.data(), 0, 4));
        ASSERT_OK(delta_writer->_flush_memtable_async(false));
        ASSERT_OK(delta_writer->write(chunk, idxs.data(), 0, 4));
        ASSERT_OK(delta_writer->close());
        ASSERT_OK(delta_writer->commit());
    }

    auto base_tablet = _tablet_mgr->get_tablet(base_tablet_id);

    {
        std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        _txn_mgr->get_txn_related_tablets(txn_id, partition_id, &tablet_related_rs);
        for (auto& tablet_rs : tablet_related_rs) {
            ASSERT_OK(
                    _txn_mgr->publish_txn(partition_id, base_tablet, txn_id, version.second, tablet_rs.second, 10000));
        }
    }

    auto base_rowset = base_tablet->get_inc_rowset_by_version(version);
    ASSERT_TRUE(base_rowset != nullptr);
    ASSERT_TRUE(base_rowset->is_overlapped());

    create_dest_tablet_with_index(base_tablet_id, new_tablet_id, TKeysType::DUP_KEYS);

    TAlterTabletReqV2 req = gen_alter_tablet_req(base_tablet_id, new_tablet_id, version);

    SchemaChangeHandler handler;
    ASSERT_OK(handler.process_alter_tablet(req));

    auto new_tablet = _tablet_mgr->get_tablet(new_tablet_id);

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

} // namespace starrocks::vectorized
