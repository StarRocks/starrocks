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

#include <utility>

#include "column/datum_convert.h"
#include "fs/fs_util.h"
#include "gen_cpp/Exprs_types.h"
#include "gtest/gtest.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"
#include "util/logging.h"

namespace starrocks {

class SchemaChangeTest : public testing::Test {
    void SetUp() override { _sc_procedure = nullptr; }

    void TearDown() override {
        if (_sc_procedure != nullptr) {
            delete _sc_procedure;
        }
    }

protected:
    void SetCreateTabletReq(TCreateTabletReq* request, int64_t tablet_id, TKeysType::type type = TKeysType::DUP_KEYS) {
        request->tablet_id = tablet_id;
        request->__set_version(1);
        request->__set_version_hash(0);
        request->tablet_schema.schema_hash = 270068375;
        request->tablet_schema.short_key_column_count = 2;
        request->tablet_schema.keys_type = type;
        request->tablet_schema.storage_type = TStorageType::COLUMN;
    }

    void AddColumn(TCreateTabletReq* request, std::string column_name, TPrimitiveType::type type, bool is_key,
                   TKeysType::type keys_type = TKeysType::DUP_KEYS) {
        TColumn c;
        c.column_name = std::move(column_name);
        c.__set_is_key(is_key);
        c.column_type.type = type;
        if (!is_key && keys_type == TKeysType::AGG_KEYS) {
            c.__set_aggregation_type(TAggregationType::SUM);
        }
        request->tablet_schema.columns.push_back(c);
    }

    void CreateSrcTablet(TTabletId tablet_id, TKeysType::type type = TKeysType::DUP_KEYS) {
        StorageEngine* engine = StorageEngine::instance();
        TCreateTabletReq create_tablet_req;
        SetCreateTabletReq(&create_tablet_req, tablet_id, type);
        AddColumn(&create_tablet_req, "k1", TPrimitiveType::INT, true);
        AddColumn(&create_tablet_req, "k2", TPrimitiveType::INT, true);
        AddColumn(&create_tablet_req, "v1", TPrimitiveType::INT, false);
        AddColumn(&create_tablet_req, "v2", TPrimitiveType::INT, false);
        Status res = engine->create_tablet(create_tablet_req);
        ASSERT_TRUE(res.ok());
        TabletSharedPtr tablet = engine->tablet_manager()->get_tablet(create_tablet_req.tablet_id);
        Schema base_schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);
        for (size_t i = 0; i < 4; ++i) {
            ColumnPtr& base_col = base_chunk->get_column_by_index(i);
            for (size_t j = 0; j < 4; ++j) {
                Datum datum;
                if (i != 1) {
                    datum.set_int32(i + 1);
                } else {
                    datum.set_int32(4 - j);
                }
                base_col->append_datum(datum);
            }
        }
        RowsetWriterContext writer_context;
        writer_context.rowset_id = engine->next_rowset_id();
        writer_context.tablet_uid = tablet->tablet_uid();
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.tablet_schema = &(tablet->tablet_schema());
        writer_context.rowset_state = VISIBLE;
        writer_context.version = Version(3, 3);
        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());
        CHECK_OK(rowset_writer->add_chunk(*base_chunk));
        CHECK_OK(rowset_writer->flush());
        RowsetSharedPtr new_rowset = *rowset_writer->build();
        ASSERT_TRUE(new_rowset != nullptr);
        ASSERT_TRUE(tablet->add_rowset(new_rowset, false).ok());
    }

    std::shared_ptr<TabletSchema> SetTabletSchema(const std::string& name, const std::string& type,
                                                  const std::string& aggregation, uint32_t length, bool is_allow_null,
                                                  bool is_key) {
        TabletSchemaPB tablet_schema_pb;
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(0);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        return std::make_shared<TabletSchema>(tablet_schema_pb);
    }

    template <typename T>
    void test_convert_to_varchar(LogicalType type, int type_size, T val, const std::string& expect_val) {
        auto src_tablet_schema =
                SetTabletSchema("SrcColumn", logical_type_to_string(type), "REPLACE", type_size, false, false);
        auto dst_tablet_schema = SetTabletSchema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false);

        Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
        Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

        Datum src_datum;
        src_datum.set<T>(val);
        Datum dst_datum;
        auto converter = get_type_converter(type, TYPE_VARCHAR);
        std::unique_ptr<MemPool> mem_pool(new MemPool());
        Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
        ASSERT_TRUE(st.ok());

        EXPECT_EQ(expect_val, dst_datum.get_slice().to_string());
    }

    template <typename T>
    void test_convert_from_varchar(LogicalType type, int type_size, std::string val, T expect_val) {
        auto src_tablet_schema = SetTabletSchema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false);
        auto dst_tablet_schema =
                SetTabletSchema("DstColumn", logical_type_to_string(type), "REPLACE", type_size, false, false);

        Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
        Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

        Datum src_datum;
        Slice slice;
        slice.data = (char*)val.data();
        slice.size = val.size();
        src_datum.set_slice(slice);
        Datum dst_datum;
        auto converter = get_type_converter(TYPE_VARCHAR, type);
        std::unique_ptr<MemPool> mem_pool(new MemPool());
        Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
        ASSERT_TRUE(st.ok());

        EXPECT_EQ(expect_val, dst_datum.get<T>());
    }

    SchemaChange* _sc_procedure;
};

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
    auto src_tablet_schema = SetTabletSchema("SrcColumn", "FLOAT", "REPLACE", 4, false, false);
    auto dst_tablet_schema = SetTabletSchema("VarcharColumn", "DOUBLE", "REPLACE", 8, false, false);

    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_float(1.2345);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_FLOAT, TYPE_DOUBLE);
    std::unique_ptr<MemPool> mem_pool(new MemPool());
    Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
    ASSERT_TRUE(st.ok());

    EXPECT_EQ(1.2345, dst_datum.get_double());
}

TEST_F(SchemaChangeTest, convert_datetime_to_date) {
    auto src_tablet_schema = SetTabletSchema("DateTimeColumn", "DATETIME", "REPLACE", 8, false, false);
    auto dst_tablet_schema = SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false);

    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    Datum src_datum;
    std::string origin_val = "2021-09-28 16:07:00";

    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);
    int64_t value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) * 1000000L +
                    time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
    src_datum.set_int64(value);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_DATETIME_V1, TYPE_DATE_V1);

    Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    EXPECT_EQ(dst_value, (int)dst_datum.get_uint24());
}

TEST_F(SchemaChangeTest, convert_date_to_datetime) {
    auto src_tablet_schema = SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false);
    auto dst_tablet_schema = SetTabletSchema("DateTimeColumn", "DATETIME", "REPLACE", 8, false, false);

    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));
    std::unique_ptr<MemPool> mem_pool(new MemPool());
    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    src_datum.set_uint24(value);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_DATE_V1, TYPE_DATETIME_V1);

    Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
    ASSERT_TRUE(st.ok());

    int64_t dst_value = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) * 1000000L;
    EXPECT_EQ(dst_value, dst_datum.get_int64());
}

TEST_F(SchemaChangeTest, convert_int_to_date_v2) {
    auto src_tablet_schema = SetTabletSchema("IntColumn", "INT", "REPLACE", 4, false, false);
    auto dst_tablet_schema = SetTabletSchema("DateColumn", "DATE V2", "REPLACE", 3, false, false);

    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);
    src_datum.set_int32(20210928);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_INT, TYPE_DATE);

    Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
    ASSERT_TRUE(st.ok());

    EXPECT_EQ("2021-09-28", dst_datum.get_date().to_string());
}

TEST_F(SchemaChangeTest, convert_int_to_date) {
    auto src_tablet_schema = SetTabletSchema("IntColumn", "INT", "REPLACE", 4, false, false);
    auto dst_tablet_schema = SetTabletSchema("DateColumn", "DATE", "REPLACE", 3, false, false);

    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    std::unique_ptr<MemPool> mem_pool(new MemPool());
    Datum src_datum;
    std::string origin_val = "2021-09-28";
    tm time_tm;
    strptime(origin_val.c_str(), "%Y-%m-%d", &time_tm);
    src_datum.set_int32(20210928);
    Datum dst_datum;
    auto converter = get_type_converter(TYPE_INT, TYPE_DATE_V1);

    Status st = converter->convert_datum(f.type().get(), src_datum, f2.type().get(), &dst_datum, mem_pool.get());
    ASSERT_TRUE(st.ok());

    int dst_value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    EXPECT_EQ(dst_value, (int)dst_datum.get_uint24());
}

TEST_F(SchemaChangeTest, convert_int_to_bitmap) {
    auto src_tablet_schema = SetTabletSchema("IntColumn", "INT", "REPLACE", 4, false, false);
    auto dst_tablet_schema = SetTabletSchema("BitmapColumn", "OBJECT", "BITMAP_UNION", 8, false, false);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_INT, OLAP_MATERIALIZE_TYPE_BITMAP);
    Status st = converter->convert_materialized(src_col, dst_col, f.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const BitmapValue* bitmap_val = dst_datum.get_bitmap();
    EXPECT_EQ(bitmap_val->cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_varchar_to_hll) {
    auto src_tablet_schema = SetTabletSchema("IntColumn", "VARCHAR", "REPLACE", 255, false, false);
    auto dst_tablet_schema = SetTabletSchema("HLLColumn", "HLL", "HLL_UNION", 8, false, false);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    std::string str = "test string";
    Slice slice(str.data(), str.size());
    src_datum.set_slice(slice);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_VARCHAR, OLAP_MATERIALIZE_TYPE_HLL);
    Status st = converter->convert_materialized(src_col, dst_col, f.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    const HyperLogLog* hll = dst_datum.get_hyperloglog();
    EXPECT_EQ(hll->estimate_cardinality(), 1);
}

TEST_F(SchemaChangeTest, convert_int_to_count) {
    auto src_tablet_schema = SetTabletSchema("IntColumn", "INT", "REPLACE", 4, false, false);
    auto dst_tablet_schema = SetTabletSchema("CountColumn", "BIGINT", "SUM", 8, false, false);

    ChunkPtr src_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*src_tablet_schema), 4096);
    ChunkPtr dst_chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(*dst_tablet_schema), 4096);
    ColumnPtr& src_col = src_chunk->get_column_by_index(0);
    ColumnPtr& dst_col = dst_chunk->get_column_by_index(0);
    Field f = ChunkHelper::convert_field(0, src_tablet_schema->column(0));
    Field f2 = ChunkHelper::convert_field(0, dst_tablet_schema->column(0));

    Datum src_datum;
    src_datum.set_int32(2);
    src_col->append_datum(src_datum);

    auto converter = get_materialized_converter(TYPE_INT, OLAP_MATERIALIZE_TYPE_COUNT);
    Status st = converter->convert_materialized(src_col, dst_col, f.type().get());
    ASSERT_TRUE(st.ok());

    Datum dst_datum = dst_col->get(0);
    EXPECT_EQ(dst_datum.get_int64(), 1);
}

TEST_F(SchemaChangeTest, schema_change_with_directing_v2) {
    CreateSrcTablet(1101);
    StorageEngine* engine = StorageEngine::instance();
    TCreateTabletReq create_tablet_req;
    SetCreateTabletReq(&create_tablet_req, 1102, TKeysType::DUP_KEYS);
    AddColumn(&create_tablet_req, "k1", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "k2", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "v1", TPrimitiveType::BIGINT, false);
    AddColumn(&create_tablet_req, "v2", TPrimitiveType::VARCHAR, false);
    Status res = engine->create_tablet(create_tablet_req);
    ASSERT_TRUE(res.ok()) << res.to_string();
    TabletSharedPtr new_tablet = engine->tablet_manager()->get_tablet(create_tablet_req.tablet_id);
    TabletSharedPtr base_tablet = engine->tablet_manager()->get_tablet(1101);

    ChunkChanger chunk_changer(new_tablet->tablet_schema());
    for (size_t i = 0; i < 4; ++i) {
        ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(i);
        column_mapping->ref_column = i;
    }
    ASSERT_TRUE(chunk_changer.prepare().ok());
    _sc_procedure = new (std::nothrow) SchemaChangeDirectly(&chunk_changer);
    Version version(3, 3);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), chunk_changer.get_selected_column_indexes());
    auto* tablet_rowset_reader = new TabletReader(base_tablet, rowset->version(), base_schema);
    ASSERT_TRUE(tablet_rowset_reader != nullptr);
    ASSERT_TRUE(tablet_rowset_reader->prepare().ok());
    ASSERT_TRUE(tablet_rowset_reader->open(read_params).ok());

    RowsetWriterContext writer_context;
    writer_context.rowset_id = engine->next_rowset_id();
    writer_context.tablet_uid = new_tablet->tablet_uid();
    writer_context.tablet_id = new_tablet->tablet_id();
    writer_context.tablet_schema_hash = new_tablet->schema_hash();
    writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
    writer_context.tablet_schema = &(new_tablet->tablet_schema());
    writer_context.rowset_state = VISIBLE;
    writer_context.version = Version(3, 3);
    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    ASSERT_TRUE(
            _sc_procedure->process(tablet_rowset_reader, rowset_writer.get(), new_tablet, base_tablet, rowset).ok());
    delete tablet_rowset_reader;
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1101);
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1102);
}

TEST_F(SchemaChangeTest, schema_change_with_sorting_v2) {
    CreateSrcTablet(1103);
    StorageEngine* engine = StorageEngine::instance();
    TCreateTabletReq create_tablet_req;
    SetCreateTabletReq(&create_tablet_req, 1104, TKeysType::DUP_KEYS);
    AddColumn(&create_tablet_req, "k1", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "k2", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "v1", TPrimitiveType::BIGINT, false);
    AddColumn(&create_tablet_req, "v2", TPrimitiveType::VARCHAR, false);
    Status res = engine->create_tablet(create_tablet_req);
    ASSERT_TRUE(res.ok()) << res.to_string();
    TabletSharedPtr new_tablet = engine->tablet_manager()->get_tablet(create_tablet_req.tablet_id,
                                                                      create_tablet_req.tablet_schema.schema_hash);
    TabletSharedPtr base_tablet = engine->tablet_manager()->get_tablet(1103);

    ChunkChanger chunk_changer(new_tablet->tablet_schema());
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    column_mapping = chunk_changer.get_mutable_column_mapping(3);
    column_mapping->ref_column = 3;
    ASSERT_TRUE(chunk_changer.prepare().ok());

    _sc_procedure = new (std::nothrow) SchemaChangeWithSorting(
            &chunk_changer, config::memory_limitation_per_thread_for_schema_change * 1024 * 1024 * 1024);
    Version version(3, 3);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), chunk_changer.get_selected_column_indexes());
    auto* tablet_rowset_reader = new TabletReader(base_tablet, rowset->version(), base_schema);
    ASSERT_TRUE(tablet_rowset_reader != nullptr);
    ASSERT_TRUE(tablet_rowset_reader->prepare().ok());
    ASSERT_TRUE(tablet_rowset_reader->open(read_params).ok());

    RowsetWriterContext writer_context;
    writer_context.rowset_id = engine->next_rowset_id();
    writer_context.tablet_uid = new_tablet->tablet_uid();
    writer_context.tablet_id = new_tablet->tablet_id();
    writer_context.tablet_schema_hash = new_tablet->schema_hash();
    writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
    writer_context.tablet_schema = &(new_tablet->tablet_schema());
    writer_context.rowset_state = VISIBLE;
    writer_context.version = Version(3, 3);
    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    ASSERT_TRUE(
            _sc_procedure->process(tablet_rowset_reader, rowset_writer.get(), new_tablet, base_tablet, rowset).ok());
    delete tablet_rowset_reader;
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1103);
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1104);
}

TEST_F(SchemaChangeTest, schema_change_with_agg_key_reorder) {
    CreateSrcTablet(1203, TKeysType::AGG_KEYS);
    StorageEngine* engine = StorageEngine::instance();
    TCreateTabletReq create_tablet_req;
    SetCreateTabletReq(&create_tablet_req, 1204, TKeysType::AGG_KEYS);
    AddColumn(&create_tablet_req, "k1", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "k2", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "v1", TPrimitiveType::BIGINT, false, TKeysType::AGG_KEYS);
    Status res = engine->create_tablet(create_tablet_req);
    ASSERT_TRUE(res.ok()) << res.to_string();
    TabletSharedPtr new_tablet = engine->tablet_manager()->get_tablet(create_tablet_req.tablet_id,
                                                                      create_tablet_req.tablet_schema.schema_hash);
    TabletSharedPtr base_tablet = engine->tablet_manager()->get_tablet(1203);

    ChunkChanger chunk_changer(new_tablet->tablet_schema());
    ColumnMapping* column_mapping = chunk_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 1;
    column_mapping = chunk_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 0;
    column_mapping = chunk_changer.get_mutable_column_mapping(2);
    column_mapping->ref_column = 2;
    ASSERT_TRUE(chunk_changer.prepare().ok());

    _sc_procedure = new (std::nothrow) SchemaChangeWithSorting(
            &chunk_changer, config::memory_limitation_per_thread_for_schema_change * 1024 * 1024 * 1024);
    Version version(3, 3);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), chunk_changer.get_selected_column_indexes());
    auto* tablet_rowset_reader = new TabletReader(base_tablet, rowset->version(), base_schema);
    ASSERT_TRUE(tablet_rowset_reader != nullptr);
    ASSERT_TRUE(tablet_rowset_reader->prepare().ok());
    ASSERT_TRUE(tablet_rowset_reader->open(read_params).ok());

    RowsetWriterContext writer_context;
    writer_context.rowset_id = engine->next_rowset_id();
    writer_context.tablet_uid = new_tablet->tablet_uid();
    writer_context.tablet_id = new_tablet->tablet_id();
    writer_context.tablet_schema_hash = new_tablet->schema_hash();
    writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
    writer_context.tablet_schema = &(new_tablet->tablet_schema());
    writer_context.rowset_state = VISIBLE;
    writer_context.version = Version(3, 3);
    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    ASSERT_TRUE(
            _sc_procedure->process(tablet_rowset_reader, rowset_writer.get(), new_tablet, base_tablet, rowset).ok());
    delete tablet_rowset_reader;
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1203);
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1204);
}

TEST_F(SchemaChangeTest, convert_varchar_to_json) {
    auto mem_pool = std::make_unique<MemPool>();
    std::vector<std::string> test_cases = {"{\"a\": 1}", "null", "[1,2,3]"};
    for (const auto& json_str : test_cases) {
        JsonValue expected = JsonValue::parse(json_str).value();

        BinaryColumn::Ptr src_column = BinaryColumn::create();
        JsonColumn::Ptr dst_column = JsonColumn::create();
        src_column->append(json_str);

        auto converter = get_type_converter(TYPE_VARCHAR, TYPE_JSON);
        TypeInfoPtr type1 = get_type_info(TYPE_VARCHAR);
        TypeInfoPtr type2 = get_type_info(TYPE_JSON);
        Status st = converter->convert_column(type1.get(), *src_column, type2.get(), dst_column.get(), mem_pool.get());
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
    auto mem_pool = std::make_unique<MemPool>();
    TypeInfoPtr type1 = get_type_info(TYPE_JSON);
    TypeInfoPtr type2 = get_type_info(TYPE_VARCHAR);
    Status st = converter->convert_column(type1.get(), *src_column, type2.get(), dst_column.get(), mem_pool.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(dst_column->get_slice(0), json_str);
}

TEST_F(SchemaChangeTest, schema_change_with_materialized_column) {
    CreateSrcTablet(1301);
    StorageEngine* engine = StorageEngine::instance();
    TCreateTabletReq create_tablet_req;
    SetCreateTabletReq(&create_tablet_req, 1302, TKeysType::DUP_KEYS);
    AddColumn(&create_tablet_req, "k1", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "k2", TPrimitiveType::INT, true);
    AddColumn(&create_tablet_req, "v1", TPrimitiveType::INT, false);
    AddColumn(&create_tablet_req, "v2", TPrimitiveType::INT, false);

    create_tablet_req.tablet_schema.columns.back().__set_is_allow_null(true);
    Status res = engine->create_tablet(create_tablet_req);
    ASSERT_TRUE(res.ok()) << res.to_string();
    TabletSharedPtr new_tablet = engine->tablet_manager()->get_tablet(create_tablet_req.tablet_id);
    TabletSharedPtr base_tablet = engine->tablet_manager()->get_tablet(1301);

    ChunkChanger chunk_changer(new_tablet->tablet_schema());
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
    DCHECK(st.ok()) << st.get_error_msg();
    st = ctx->prepare(chunk_changer.get_runtime_state());
    DCHECK(st.ok()) << st.get_error_msg();
    st = ctx->open(chunk_changer.get_runtime_state());
    DCHECK(st.ok()) << st.get_error_msg();

    chunk_changer.get_mc_exprs()->insert({3, ctx});

    _sc_procedure = new (std::nothrow) SchemaChangeDirectly(&chunk_changer);
    Version version(3, 3);
    RowsetSharedPtr rowset = base_tablet->get_rowset_by_version(version);
    ASSERT_TRUE(rowset != nullptr);

    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;
    Schema base_schema =
            ChunkHelper::convert_schema(base_tablet->tablet_schema(), chunk_changer.get_selected_column_indexes());
    auto* tablet_rowset_reader = new TabletReader(base_tablet, rowset->version(), base_schema);
    ASSERT_TRUE(tablet_rowset_reader != nullptr);
    ASSERT_TRUE(tablet_rowset_reader->prepare().ok());
    ASSERT_TRUE(tablet_rowset_reader->open(read_params).ok());

    RowsetWriterContext writer_context;
    writer_context.rowset_id = engine->next_rowset_id();
    writer_context.tablet_uid = new_tablet->tablet_uid();
    writer_context.tablet_id = new_tablet->tablet_id();
    writer_context.tablet_schema_hash = new_tablet->schema_hash();
    writer_context.rowset_path_prefix = new_tablet->schema_hash_path();
    writer_context.tablet_schema = &(new_tablet->tablet_schema());
    writer_context.rowset_state = VISIBLE;
    writer_context.version = Version(3, 3);
    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    ASSERT_TRUE(
            _sc_procedure->process(tablet_rowset_reader, rowset_writer.get(), new_tablet, base_tablet, rowset).ok());
    delete tablet_rowset_reader;
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1101);
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(1102);
}

} // namespace starrocks
