// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/beta_rowset_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include <string>
#include <vector>

#include "column/datum_tuple.h"
#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/comparison_predicate.h"
#include "storage/data_dir.h"
#include "storage/row_block.h"
#include "storage/row_cursor.h"
#include "storage/rowset/beta_rowset_reader.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/column_predicate.h"
#include "util/defer_op.h"
#include "util/file_utils.h"

using std::string;

namespace starrocks {

static StorageEngine* k_engine = nullptr;

class BetaRowsetTest : public testing::Test {
protected:
    OlapReaderStatistics _stats;

    void SetUp() override {
        _tablet_meta_mem_tracker = std::make_unique<MemTracker>();
        _schema_change_mem_tracker = std::make_unique<MemTracker>();
        _page_cache_mem_tracker = std::make_unique<MemTracker>();
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        static int i = 0;
        config::storage_root_path = std::filesystem::current_path().string() + "/data_test_" + std::to_string(i);

        ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        options.tablet_meta_mem_tracker = _tablet_meta_mem_tracker.get();
        options.schema_change_mem_tracker = _schema_change_mem_tracker.get();
        Status s = starrocks::StorageEngine::open(options, &k_engine);
        ASSERT_TRUE(s.ok()) << s.to_string();

        ExecEnv* exec_env = starrocks::ExecEnv::GetInstance();
        exec_env->set_storage_engine(k_engine);

        const std::string rowset_dir = config::storage_root_path + "/data/beta_rowset_test";
        ASSERT_TRUE(FileUtils::create_dir(rowset_dir).ok());
        StoragePageCache::create_global_cache(_page_cache_mem_tracker.get(), 1000000000);
        i++;
    }

    void TearDown() override {
        delete k_engine;
        k_engine = nullptr;
        if (FileUtils::check_exist(config::storage_root_path)) {
            ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
        }
        StoragePageCache::release_global_cache();
    }

    // (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
    void create_tablet_schema(TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(true);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type("INT"); // TODO change to varchar(20) when dict encoding for string is supported
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(true);
        column_2->set_is_nullable(true);
        column_2->set_is_bf_column(false);

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name("v1");
        column_3->set_type("INT");
        column_3->set_length(4);
        column_3->set_is_key(false);
        column_3->set_is_nullable(false);
        column_3->set_is_bf_column(false);
        column_3->set_aggregation("SUM");

        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_primary_tablet_schema(TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(PRIMARY_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_key(true);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);

        ColumnPB* column_3 = tablet_schema_pb.add_column();
        column_3->set_unique_id(3);
        column_3->set_name("v1");
        column_3->set_type("INT");
        column_3->set_length(4);
        column_3->set_is_key(false);
        column_3->set_is_nullable(false);
        column_3->set_is_bf_column(false);
        column_3->set_aggregation("REPLACE");

        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_rowset_writer_context(const TabletSchema* tablet_schema, RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->mem_tracker = _tablet_meta_mem_tracker.get();
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->rowset_path_prefix = config::storage_root_path + "/data/beta_rowset_test";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 0;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context, RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        ASSERT_EQ(OLAP_SUCCESS, s);
        ASSERT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        ASSERT_EQ(OLAP_SUCCESS, s);
    }

private:
    std::unique_ptr<MemTracker> _tablet_meta_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _schema_change_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
};

TEST_F(BetaRowsetTest, BasicFunctionTest) {
    OLAPStatus s;
    TabletSchema tablet_schema;
    create_tablet_schema(&tablet_schema);
    RowsetSharedPtr rowset;
    const int num_segments = 3;
    const uint32_t rows_per_segment = 4096;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context(kDataFormatUnknown, kDataFormatV2);
        create_rowset_writer_context(&tablet_schema, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        ASSERT_EQ(OLAP_SUCCESS, s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := 4096 * i + rid
        for (int i = 0; i < num_segments; ++i) {
            MemTracker mem_tracker(-1);
            MemPool mem_pool(&mem_tracker);
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 10 + i;
                uint32_t k2 = k1 * 10;
                uint32_t k3 = rows_per_segment * i + rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                ASSERT_EQ(OLAP_SUCCESS, s);
            }
            s = rowset_writer->flush();
            ASSERT_EQ(OLAP_SUCCESS, s);
        }

        rowset = rowset_writer->build();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(num_segments, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(num_segments * rows_per_segment, rowset->rowset_meta()->num_rows());
    }

    { // test return ordered results and return k1 and k2
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = &tablet_schema;
        reader_context.need_ordered_result = true;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;
        reader_context.seek_columns = &return_columns;
        reader_context.stats = &_stats;

        // without predicates
        {
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);
            RowBlock* output_block;
            uint32_t num_rows_read = 0;
            while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
                ASSERT_TRUE(output_block != nullptr);
                ASSERT_GT(output_block->row_num(), 0);
                ASSERT_EQ(0, output_block->pos());
                ASSERT_EQ(output_block->row_num(), output_block->limit());
                ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
                // after sort merge segments, k1 will be 0, 1, 2, 10, 11, 12, 20, 21, 22, ..., 40950, 40951, 40952
                for (int i = 0; i < output_block->row_num(); ++i) {
                    char* field1 = output_block->field_ptr(i, 0);
                    char* field2 = output_block->field_ptr(i, 1);
                    // test null bit
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field1));
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field2));
                    uint32_t k1 = *reinterpret_cast<uint32_t*>(field1 + 1);
                    uint32_t k2 = *reinterpret_cast<uint32_t*>(field2 + 1);
                    ASSERT_EQ(k1 * 10, k2);

                    int rid = num_rows_read / 3;
                    int seg_id = num_rows_read % 3;
                    ASSERT_EQ(rid * 10 + seg_id, k1);
                    num_rows_read++;
                }
            }
            EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
            EXPECT_TRUE(output_block == nullptr);
            EXPECT_EQ(rowset->rowset_meta()->num_rows(), num_rows_read);
        }

        // merge segments with predicates
        {
            std::vector<const ColumnPredicate*> column_predicates;
            // column predicate: k1 = 10
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            column_predicates.emplace_back(predicate.get());
            reader_context.predicates = &column_predicates;
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);
            RowBlock* output_block;
            uint32_t num_rows_read = 0;
            while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
                ASSERT_TRUE(output_block != nullptr);
                ASSERT_EQ(1, output_block->row_num());
                ASSERT_EQ(0, output_block->pos());
                ASSERT_EQ(output_block->row_num(), output_block->limit());
                ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
                // after sort merge segments, k1 will be 10
                for (int i = 0; i < output_block->row_num(); ++i) {
                    char* field1 = output_block->field_ptr(i, 0);
                    char* field2 = output_block->field_ptr(i, 1);
                    // test null bit
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field1));
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field2));
                    uint32_t k1 = *reinterpret_cast<uint32_t*>(field1 + 1);
                    uint32_t k2 = *reinterpret_cast<uint32_t*>(field2 + 1);
                    ASSERT_EQ(10, k1);
                    ASSERT_EQ(k1 * 10, k2);
                    num_rows_read++;
                }
            }
            EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
            EXPECT_TRUE(output_block == nullptr);
            EXPECT_EQ(1, num_rows_read);
        }
    }

    { // test return unordered data and only k3
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = &tablet_schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {2};
        reader_context.return_columns = &return_columns;
        reader_context.seek_columns = &return_columns;
        reader_context.stats = &_stats;

        // without predicate
        {
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);

            RowBlock* output_block;
            uint32_t num_rows_read = 0;
            while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
                ASSERT_TRUE(output_block != nullptr);
                ASSERT_GT(output_block->row_num(), 0);
                ASSERT_EQ(0, output_block->pos());
                ASSERT_EQ(output_block->row_num(), output_block->limit());
                ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
                // for unordered result, k3 will be 0, 1, 2, ..., 4096*3-1
                for (int i = 0; i < output_block->row_num(); ++i) {
                    char* field3 = output_block->field_ptr(i, 2);
                    // test null bit
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field3));
                    uint32_t k3 = *reinterpret_cast<uint32_t*>(field3 + 1);
                    ASSERT_EQ(num_rows_read, k3);
                    num_rows_read++;
                }
            }
            EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
            EXPECT_TRUE(output_block == nullptr);
            EXPECT_EQ(rowset->rowset_meta()->num_rows(), num_rows_read);
        }

        // with predicate
        {
            std::vector<const ColumnPredicate*> column_predicates;
            // column predicate: k3 < 100
            ColumnPredicate* predicate = new LessPredicate<int32_t>(2, 100);
            column_predicates.emplace_back(predicate);
            reader_context.predicates = &column_predicates;
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);

            RowBlock* output_block;
            uint32_t num_rows_read = 0;
            while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
                ASSERT_TRUE(output_block != nullptr);
                ASSERT_LE(output_block->row_num(), 100);
                ASSERT_EQ(0, output_block->pos());
                ASSERT_EQ(output_block->row_num(), output_block->limit());
                ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
                // for unordered result, k3 will be 0, 1, 2, ..., 99
                for (int i = 0; i < output_block->row_num(); ++i) {
                    char* field3 = output_block->field_ptr(i, 2);
                    // test null bit
                    ASSERT_FALSE(*reinterpret_cast<bool*>(field3));
                    uint32_t k3 = *reinterpret_cast<uint32_t*>(field3 + 1);
                    ASSERT_EQ(num_rows_read, k3);
                    num_rows_read++;
                }
            }
            EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
            EXPECT_TRUE(output_block == nullptr);
            EXPECT_EQ(100, num_rows_read);
            delete predicate;
        }
    }
}

TEST_F(BetaRowsetTest, DiscontinuousRowidTest) {
    OLAPStatus s;
    TabletSchema tablet_schema;
    create_tablet_schema(&tablet_schema);
    RowsetSharedPtr rowset;
    const int num_segments = 1;
    const uint32_t rows_per_segment = 4096;

    {
        // write num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context(kDataFormatUnknown, kDataFormatV2);
        create_rowset_writer_context(&tablet_schema, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        ASSERT_EQ(OLAP_SUCCESS, s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        for (int i = 0; i < num_segments; ++i) {
            MemTracker mem_tracker(-1);
            MemPool mem_pool(&mem_tracker);
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid;
                uint32_t k2 = rid;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                ASSERT_EQ(OLAP_SUCCESS, s);
            }
            s = rowset_writer->flush();
            ASSERT_EQ(OLAP_SUCCESS, s);
        }

        rowset = rowset_writer->build();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(num_segments, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(num_segments * rows_per_segment, rowset->rowset_meta()->num_rows());
    }

    {
        auto& schema = rowset->schema();
        vector<uint32_t> pk_columns;
        for (size_t i = 0; i < schema.num_key_columns(); i++) {
            pk_columns.push_back((uint32_t)i);
        }
        vectorized::Schema pkey_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);

        OlapReaderStatistics stats;
        vectorized::RowsetReadOptions rs_opts;

        rs_opts.is_primary_keys = false;
        rs_opts.sorted = false;
        rs_opts.version = 0;
        rs_opts.stats = &stats;

        std::unique_ptr<vectorized::ColumnPredicate> predicate(
                vectorized::new_column_ne_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, "1"));

        rs_opts.predicates[predicate->column_id()].emplace_back(predicate.get());

        auto res = rowset->new_iterator(pkey_schema, rs_opts);
        if (!res.ok()) {
            ASSERT_TRUE(res.ok()) << res.status().to_string();
        }

        // TODO(cbl): refactor chunkandrowid
        //            while (true) {
        //                vectorized::ChunkAndRowid chunk_and_rowid;
        //                chunk_and_rowid.chunk =
        //                        vectorized::ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
        //
        //                Status st = itr->get_next(&chunk_and_rowid);
        //
        //                if (chunk_and_rowid.rowids_offset_col.empty()) {
        //                    if (i == 0) {
        //                        DCHECK_NE(UINT32_MAX, chunk_and_rowid.start_rowid);
        //                        ASSERT_EQ(chunk_and_rowid.start_rowid, 0);
        //                    }
        //                } else {
        //                    ASSERT_EQ(chunk_and_rowid.rowids_offset_col.size(), 4095);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[0],
        //                              0);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[1],
        //                              2);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[2],
        //                              3);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[4094],
        //                              4095);
        //                }
        //                DCHECK_NE(UINT32_MAX, chunk_and_rowid.segment_id);
        //
        //                i++;
        //
        //                if (st.is_end_of_file()) {
        //                    break;
        //                } else if (!st.ok()) {
        //                    ASSERT_TRUE(st.ok()) << st.to_string();
        //                }
        //            }
    }
}

TEST_F(BetaRowsetTest, DiscontinuousRowid1Test) {
    OLAPStatus s;
    TabletSchema tablet_schema;
    create_tablet_schema(&tablet_schema);
    RowsetSharedPtr rowset;
    const int num_segments = 1;
    const uint32_t rows_per_segment = 4096 * 3;

    {
        // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context(kDataFormatUnknown, kDataFormatV2);
        create_rowset_writer_context(&tablet_schema, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        ASSERT_EQ(OLAP_SUCCESS, s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        for (int i = 0; i < num_segments; ++i) {
            MemTracker mem_tracker(-1);
            MemPool mem_pool(&mem_tracker);
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid;
                uint32_t k2 = rid;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                ASSERT_EQ(OLAP_SUCCESS, s);
            }
            s = rowset_writer->flush();
            ASSERT_EQ(OLAP_SUCCESS, s);
        }

        rowset = rowset_writer->build();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(num_segments, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(num_segments * rows_per_segment, rowset->rowset_meta()->num_rows());
    }

    {
        auto& schema = rowset->schema();
        vector<uint32_t> pk_columns;
        for (size_t i = 0; i < schema.num_key_columns(); i++) {
            pk_columns.push_back((uint32_t)i);
        }
        vectorized::Schema pkey_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);

        OlapReaderStatistics stats;
        vectorized::RowsetReadOptions rs_opts;

        rs_opts.is_primary_keys = false;
        rs_opts.sorted = false;
        rs_opts.version = 0;
        rs_opts.stats = &stats;

        std::unique_ptr<vectorized::ColumnPredicate> predicate(
                vectorized::new_column_ne_predicate(get_type_info(OLAP_FIELD_TYPE_INT), 0, "1"));

        rs_opts.predicates[predicate->column_id()].emplace_back(predicate.get());

        auto res = rowset->new_iterator(pkey_schema, rs_opts);
        if (!res.ok()) {
            ASSERT_TRUE(res.ok()) << res.status().to_string();
        }

        // TODO(cbl): refactor chunkandrowid
        //            while (true) {
        //                vectorized::ChunkAndRowid chunk_and_rowid;
        //                chunk_and_rowid.chunk =
        //                        vectorized::ChunkHelper::new_chunk(pkey_schema, config::vector_chunk_size);
        //
        //                Status st = itr->get_next(&chunk_and_rowid);
        //
        //                if (chunk_and_rowid.rowids_offset_col.empty()) {
        //                    switch (i) {
        //                    case 0:
        //                        ASSERT_EQ(chunk_and_rowid.start_rowid, 0);
        //                        break;
        //                    case 1:
        //                        ASSERT_EQ(chunk_and_rowid.start_rowid, 4097);
        //                        break;
        //                    case 2:
        //                        ASSERT_EQ(chunk_and_rowid.start_rowid, 8193);
        //                        break;
        //                    }
        //                } else {
        //                    ASSERT_EQ(chunk_and_rowid.rowids_offset_col.size(), 4095);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[0],
        //                              0);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[1],
        //                              2);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[2],
        //                              3);
        //                    ASSERT_EQ(chunk_and_rowid.start_rowid +
        //                                      chunk_and_rowid.rowids_offset_col.get_data()[4094],
        //                              4095);
        //                }
        //                DCHECK_NE(UINT32_MAX, chunk_and_rowid.segment_id);
        //
        //                i++;
        //
        //                if (st.is_end_of_file()) {
        //                    break;
        //                } else if (!st.ok()) {
        //                    ASSERT_TRUE(st.ok()) << st.to_string();
        //                }
        //            }
    }
}

TEST_F(BetaRowsetTest, FinalMergeTest) {
    TabletSchema tablet_schema;
    create_primary_tablet_schema(&tablet_schema);
    RowsetSharedPtr rowset;
    const uint32_t rows_per_segment = 1024;
    {
        RowsetWriterContext writer_context(kDataFormatV2, kDataFormatV2);
        create_rowset_writer_context(&tablet_schema, &writer_context);
        writer_context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_EQ(OLAP_SUCCESS, RowsetFactory::create_rowset_writer(writer_context, &rowset_writer));

        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema);

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = 0; i < rows_per_segment; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(1)));
            }
            rowset_writer->add_chunk(*chunk.get());
            EXPECT_EQ(OLAP_SUCCESS, rowset_writer->flush());
        }

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = rows_per_segment / 2; i < rows_per_segment + rows_per_segment / 2; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(2)));
            }
            rowset_writer->add_chunk(*chunk.get());
            EXPECT_EQ(OLAP_SUCCESS, rowset_writer->flush());
        }

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = rows_per_segment; i < rows_per_segment * 2; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(3)));
            }
            rowset_writer->add_chunk(*chunk.get());
            EXPECT_EQ(OLAP_SUCCESS, rowset_writer->flush());
        }

        rowset = rowset_writer->build();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(1, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(rows_per_segment * 2, rowset->rowset_meta()->num_rows());

        vectorized::SegmentReadOptions seg_options;
        seg_options.block_mgr = fs::fs_util::block_manager();
        seg_options.stats = &_stats;

        MemTracker tracker;
        DeferOp memory_tracker_releaser([&tracker] { return tracker.release(tracker.consumption()); });

        std::string segment_file =
                BetaRowset::segment_file_path(writer_context.rowset_path_prefix, writer_context.rowset_id, 0);

        auto segment =
                *segment_v2::Segment::open(&tracker, fs::fs_util::block_manager(), segment_file, 0, &tablet_schema);
        ASSERT_NE(segment->num_rows(), 0);
        auto res = segment->new_iterator(schema, seg_options);
        ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
        auto seg_iterator = res.value();

        auto chunk = vectorized::ChunkHelper::new_chunk(seg_iterator->schema(), 100);

        size_t count = 0;

        while (true) {
            auto st = seg_iterator->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_FALSE(!st.ok());
            for (auto i = 0; i < chunk->num_rows(); i++) {
                auto index = count + i;
                if (0 <= index && index < rows_per_segment / 2) {
                    EXPECT_EQ(1, chunk->get(i)[2].get_int32());
                } else if (rows_per_segment / 2 <= index && index < rows_per_segment) {
                    EXPECT_EQ(2, chunk->get(i)[2].get_int32());
                } else if (rows_per_segment <= index && index < rows_per_segment * 2) {
                    EXPECT_EQ(3, chunk->get(i)[2].get_int32());
                }
            }
            count += chunk->num_rows();
            chunk->reset();
        }
        EXPECT_EQ(count, rows_per_segment * 2);
    }
}

} // namespace starrocks
