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

#include "storage/rowset/beta_rowset.h"

#include <string>
#include <vector>

#include "column/datum_tuple.h"
#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/column_predicate.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/file_utils.h"

using std::string;

namespace starrocks {

static StorageEngine* k_engine = nullptr;

class BetaRowsetTest : public testing::Test {
protected:
    OlapReaderStatistics _stats;

    void SetUp() override {
        _metadata_mem_tracker = std::make_unique<MemTracker>();
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
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        options.metadata_mem_tracker = _metadata_mem_tracker.get();
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
        k_engine->stop();
        delete k_engine;
        k_engine = nullptr;
        starrocks::ExecEnv::GetInstance()->set_storage_engine(nullptr);
        if (FileUtils::check_exist(config::storage_root_path)) {
            ASSERT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
        }
        StoragePageCache::release_global_cache();
    }

    std::shared_ptr<TabletSchema> create_primary_tablet_schema() {
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

        return std::make_shared<TabletSchema>(tablet_schema_pb);
    }

    void create_rowset_writer_context(const TabletSchema* tablet_schema, RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
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

private:
    std::unique_ptr<MemTracker> _metadata_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _schema_change_mem_tracker = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
};

TEST_F(BetaRowsetTest, FinalMergeTest) {
    auto tablet_schema = create_primary_tablet_schema();
    RowsetSharedPtr rowset;
    const uint32_t rows_per_segment = 1024;
    {
        RowsetWriterContext writer_context(kDataFormatV2, kDataFormatV2);
        create_rowset_writer_context(tablet_schema.get(), &writer_context);
        writer_context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*tablet_schema);

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = 0; i < rows_per_segment; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(1)));
            }
            ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
            ASSERT_OK(rowset_writer->flush());
        }

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = rows_per_segment / 2; i < rows_per_segment + rows_per_segment / 2; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(2)));
            }
            ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
            ASSERT_OK(rowset_writer->flush());
        }

        {
            auto chunk = vectorized::ChunkHelper::new_chunk(schema, config::vector_chunk_size);
            auto& cols = chunk->columns();
            for (auto i = rows_per_segment; i < rows_per_segment * 2; i++) {
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i)));
                cols[2]->append_datum(vectorized::Datum(static_cast<int32_t>(3)));
            }
            ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
            ASSERT_OK(rowset_writer->flush());
        }

        rowset = rowset_writer->build().value();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(1, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(rows_per_segment * 2, rowset->rowset_meta()->num_rows());

        vectorized::SegmentReadOptions seg_options;
        seg_options.block_mgr = fs::fs_util::block_manager();
        seg_options.stats = &_stats;

        std::string segment_file =
                BetaRowset::segment_file_path(writer_context.rowset_path_prefix, writer_context.rowset_id, 0);

        auto segment = *Segment::open(fs::fs_util::block_manager(), segment_file, 0, tablet_schema.get());
        ASSERT_NE(segment->num_rows(), 0);
        auto res = segment->new_iterator(schema, seg_options);
        ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
        auto seg_iterator = res.value();

        seg_iterator->init_encoded_schema(vectorized::EMPTY_GLOBAL_DICTMAPS);

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

TEST_F(BetaRowsetTest, VerticalWriteTest) {
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    RowsetWriterContext writer_context(kDataFormatV2, kDataFormatV2);
    create_rowset_writer_context(tablet_schema.get(), &writer_context);
    writer_context.max_rows_per_segment = 5000;
    writer_context.writer_type = kVertical;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    int32_t chunk_size = 3000;
    size_t num_rows = 10000;

    {
        // k1 k2
        std::vector<uint32_t> column_indexes{0, 1};
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*tablet_schema, column_indexes);
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size; ++j) {
                if (i * chunk_size + j >= num_rows) {
                    break;
                }
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(vectorized::Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            }
            ASSERT_OK(rowset_writer->add_columns(*chunk, column_indexes, true));
        }
        ASSERT_OK(rowset_writer->flush_columns());
    }

    {
        // v1
        std::vector<uint32_t> column_indexes{2};
        auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*tablet_schema, column_indexes);
        auto chunk = vectorized::ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size; ++j) {
                if (i * chunk_size + j >= num_rows) {
                    break;
                }
                cols[0]->append_datum(vectorized::Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            }
            ASSERT_OK(rowset_writer->add_columns(*chunk, column_indexes, false));
        }
        ASSERT_OK(rowset_writer->flush_columns());
    }
    ASSERT_OK(rowset_writer->final_flush());

    // check rowset
    RowsetSharedPtr rowset = rowset_writer->build().value();
    ASSERT_EQ(num_rows, rowset->rowset_meta()->num_rows());
    ASSERT_EQ(3, rowset->rowset_meta()->num_segments());

    vectorized::RowsetReadOptions rs_opts;
    rs_opts.is_primary_keys = false;
    rs_opts.sorted = true;
    rs_opts.version = 0;
    rs_opts.stats = &_stats;
    auto schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*tablet_schema);
    auto res = rowset->new_iterator(schema, rs_opts);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

    auto iterator = res.value();
    int count = 0;
    auto chunk = vectorized::ChunkHelper::new_chunk(schema, chunk_size);
    while (true) {
        chunk->reset();
        auto st = iterator->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_FALSE(!st.ok());
        for (auto i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(count, chunk->get(i)[0].get_int32());
            EXPECT_EQ(count + 1, chunk->get(i)[1].get_int32());
            EXPECT_EQ(count + 2, chunk->get(i)[2].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(count, num_rows);
}

} // namespace starrocks
