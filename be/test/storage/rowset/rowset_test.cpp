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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/rowset_test.cpp

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

#include "storage/rowset/rowset.h"

#include <butil/iobuf.h>

#include <string>
#include <vector>

#include "column/datum_tuple.h"
#include "fs/fs_util.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/empty_iterator.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_rewriter.h"
#include "storage/rowset_update_state.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

using std::string;

namespace starrocks {

static StorageEngine* k_engine = nullptr;

class RowsetTest : public testing::Test {
protected:
    OlapReaderStatistics _stats;

    void SetUp() override {
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        static int i = 0;
        _default_storage_root_path = config::storage_root_path;
        config::storage_root_path = std::filesystem::current_path().string() + "/data_test_" + std::to_string(i);

        ASSERT_OK(fs::remove_all(config::storage_root_path));
        ASSERT_TRUE(fs::create_directories(config::storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path);

        starrocks::EngineOptions options;
        options.store_paths = paths;
        Status s = starrocks::StorageEngine::open(options, &k_engine);
        ASSERT_TRUE(s.ok()) << s.to_string();

        const std::string rowset_dir = config::storage_root_path + "/data/rowset_test";
        ASSERT_TRUE(fs::create_directories(rowset_dir).ok());
        ASSERT_TRUE(fs::create_directories(config::storage_root_path + "/data/rowset_test_seg").ok());
        ASSERT_TRUE(fs::create_directories(config::storage_root_path + "/data/rowset_test_delete").ok());
        i++;
    }

    void TearDown() override {
        k_engine->stop();
        delete k_engine;
        k_engine = nullptr;
        if (fs::path_exist(config::storage_root_path)) {
            ASSERT_TRUE(fs::remove_all(config::storage_root_path).ok());
        }
        StoragePageCache::instance()->prune();
        config::storage_root_path = _default_storage_root_path;
    }

    std::shared_ptr<TabletSchema> create_primary_tablet_schema() {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(PRIMARY_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
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

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 2;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "k1";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "k2";
        k2.__set_is_key(true);
        k2.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k2);

        TColumn v1;
        v1.column_name = "v1";
        v1.__set_is_key(false);
        v1.column_type.type = TPrimitiveType::INT;
        v1.aggregation_type = TAggregationType::REPLACE;
        request.tablet_schema.columns.push_back(v1);

        TColumn v2;
        v2.column_name = "v2";
        v2.__set_is_key(false);
        v2.column_type.type = TPrimitiveType::INT;
        v2.aggregation_type = TAggregationType::REPLACE;
        request.tablet_schema.columns.push_back(v2);

        TColumn v3;
        v3.column_name = "v3";
        v3.__set_is_key(false);
        v3.column_type.type = TPrimitiveType::INT;
        v3.aggregation_type = TAggregationType::REPLACE;
        request.tablet_schema.columns.push_back(v3);

        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void create_rowset_writer_context(int64_t tablet_id, const TabletSchemaCSPtr& tablet_schema,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = tablet_id;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_path_prefix = config::storage_root_path + "/data/rowset_test";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 0;
    }

    void create_partial_rowset_writer_context(int64_t tablet_id, const std::vector<int32_t>& column_indexes,
                                              const std::shared_ptr<TabletSchema>& partial_schema,
                                              const TabletSchemaCSPtr& full_schema,
                                              RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = tablet_id;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_path_prefix = config::storage_root_path + "/data/rowset_test";
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = partial_schema;
        rowset_writer_context->full_tablet_schema = full_schema;
        rowset_writer_context->is_partial_update = true;
        rowset_writer_context->referenced_column_ids = column_indexes;
        rowset_writer_context->version.first = 0;
        rowset_writer_context->version.second = 0;
    }

    void test_final_merge(bool has_merge_condition);

private:
    std::string _default_storage_root_path;
};

static ChunkIteratorPtr create_tablet_iterator(TabletReader& reader, Schema& schema) {
    TabletReaderParams params;
    if (!reader.prepare().ok()) {
        LOG(ERROR) << "reader prepare failed";
        return nullptr;
    }
    std::vector<ChunkIteratorPtr> seg_iters;
    if (!reader.get_segment_iterators(params, &seg_iters).ok()) {
        LOG(ERROR) << "reader get segment iterators fail";
        return nullptr;
    }
    if (seg_iters.empty()) {
        return new_empty_iterator(schema, DEFAULT_CHUNK_SIZE);
    }
    return new_union_iterator(seg_iters);
}

void RowsetTest::test_final_merge(bool has_merge_condition = false) {
    auto tablet = create_tablet(12421, 53242);

    RowsetSharedPtr rowset;
    const uint32_t rows_per_segment = 1024;
    RowsetWriterContext writer_context;
    create_rowset_writer_context(12421, tablet->tablet_schema(), &writer_context);
    writer_context.segments_overlap = OVERLAP_UNKNOWN;
    if (has_merge_condition) {
        writer_context.merge_condition = "v1";
    }

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = 0; i < rows_per_segment; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(1)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment / 2; i < rows_per_segment + rows_per_segment / 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(2)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(2)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(2)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment; i < rows_per_segment * 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(3)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(3)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(3)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    rowset = rowset_writer->build().value();
    ASSERT_TRUE(rowset != nullptr);
    ASSERT_EQ(3, rowset->rowset_meta()->num_segments());
    ASSERT_EQ(OVERLAP_UNKNOWN, rowset->rowset_meta()->segments_overlap());
    ASSERT_EQ(rows_per_segment * 3, rowset->rowset_meta()->num_rows());

    {
        size_t count = 0;
        for (size_t seg_id = 0; seg_id < rowset->rowset_meta()->num_segments(); seg_id++) {
            SegmentReadOptions seg_options;
            ASSIGN_OR_ABORT(seg_options.fs, FileSystem::CreateSharedFromString("posix://"));
            seg_options.stats = &_stats;
            std::string segment_file =
                    Rowset::segment_file_path(writer_context.rowset_path_prefix, writer_context.rowset_id, seg_id);
            auto segment = *Segment::open(seg_options.fs, segment_file, 0, tablet->tablet_schema());
            ASSERT_NE(segment->num_rows(), 0);
            auto res = segment->new_iterator(schema, seg_options);
            ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
            auto seg_iterator = res.value();

            ASSERT_TRUE(seg_iterator->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

            auto chunk = ChunkHelper::new_chunk(schema, 100);
            while (true) {
                auto st = seg_iterator->get_next(chunk.get());
                if (st.is_end_of_file()) {
                    break;
                }
                ASSERT_FALSE(!st.ok());
                for (auto i = 0; i < chunk->num_rows(); i++) {
                    auto index = count + i;
                    if (0 <= index && index < rows_per_segment) {
                        EXPECT_EQ(1, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(1, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(1, chunk->get(i)[4].get_int32());
                    } else if (rows_per_segment <= index && index < rows_per_segment * 2) {
                        EXPECT_EQ(2, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(2, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(2, chunk->get(i)[4].get_int32());
                    } else if (rows_per_segment * 2 <= index && index < rows_per_segment * 3) {
                        EXPECT_EQ(3, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(3, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(3, chunk->get(i)[4].get_int32());
                    }
                }
                count += chunk->num_rows();
                chunk->reset();
            }
        }
        EXPECT_EQ(count, rows_per_segment * 3);
    }

    ASSERT_EQ(1, tablet->updates()->version_history_count());
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    auto st = tablet->rowset_commit(2, rowset, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LE(pool->num_threads(), 1);
    ASSERT_EQ(2, tablet->updates()->max_version());
    ASSERT_EQ(2, tablet->updates()->version_history_count());

    {
        TabletReader reader(tablet, Version(0, 2), schema);
        auto iter = create_tablet_iterator(reader, schema);
        ASSERT_TRUE(iter != nullptr);
        auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
        size_t count = 0;
        while (true) {
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_FALSE(!st.ok());
            for (auto i = 0; i < chunk->num_rows(); i++) {
                auto index = count + i;
                if (0 <= index && index < rows_per_segment / 2) {
                    EXPECT_EQ(1, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(1, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(1, chunk->get(i)[4].get_int32());
                } else if (rows_per_segment / 2 <= index && index < rows_per_segment) {
                    EXPECT_EQ(2, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(2, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(2, chunk->get(i)[4].get_int32());
                } else if (rows_per_segment <= index && index < rows_per_segment * 2) {
                    EXPECT_EQ(3, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(3, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(3, chunk->get(i)[4].get_int32());
                }
            }
            count += chunk->num_rows();
            chunk->reset();
        }
        EXPECT_EQ(count, rows_per_segment * 2);
    }
}

TEST_F(RowsetTest, FinalMergeTest) {
    test_final_merge(false);
}

TEST_F(RowsetTest, ConditionUpdateWithMultipleSegmentsTest) {
    test_final_merge(true);
}

TEST_F(RowsetTest, FinalMergeVerticalTest) {
    auto tablet = create_tablet(12345, 1111);
    RowsetSharedPtr rowset;
    const uint32_t rows_per_segment = 1024;
    config::vertical_compaction_max_columns_per_group = 1;
    RowsetWriterContext writer_context;
    create_rowset_writer_context(12345, tablet->tablet_schema(), &writer_context);
    writer_context.segments_overlap = OVERLAP_UNKNOWN;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = 0; i < rows_per_segment; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(1)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment / 2; i < rows_per_segment + rows_per_segment / 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(2)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(2)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(2)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment; i < rows_per_segment * 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(3)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(3)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(3)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    rowset = rowset_writer->build().value();
    ASSERT_TRUE(rowset != nullptr);
    ASSERT_EQ(3, rowset->rowset_meta()->num_segments());
    ASSERT_EQ(OVERLAP_UNKNOWN, rowset->rowset_meta()->segments_overlap());
    ASSERT_EQ(rows_per_segment * 3, rowset->rowset_meta()->num_rows());

    {
        size_t count = 0;
        for (size_t seg_id = 0; seg_id < rowset->rowset_meta()->num_segments(); seg_id++) {
            SegmentReadOptions seg_options;
            ASSIGN_OR_ABORT(seg_options.fs, FileSystem::CreateSharedFromString("posix://"));
            seg_options.stats = &_stats;

            std::string segment_file =
                    Rowset::segment_file_path(writer_context.rowset_path_prefix, writer_context.rowset_id, seg_id);
            auto segment = *Segment::open(seg_options.fs, segment_file, 0, tablet->tablet_schema());

            ASSERT_NE(segment->num_rows(), 0);
            auto res = segment->new_iterator(schema, seg_options);
            ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

            auto seg_iterator = res.value();
            ASSERT_TRUE(seg_iterator->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
            auto chunk = ChunkHelper::new_chunk(seg_iterator->schema(), 100);
            while (true) {
                auto st = seg_iterator->get_next(chunk.get());
                if (st.is_end_of_file()) {
                    break;
                }
                ASSERT_FALSE(!st.ok());
                for (auto i = 0; i < chunk->num_rows(); i++) {
                    auto index = count + i;
                    if (0 <= index && index < rows_per_segment) {
                        EXPECT_EQ(1, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(1, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(1, chunk->get(i)[4].get_int32());
                    } else if (rows_per_segment <= index && index < rows_per_segment * 2) {
                        EXPECT_EQ(2, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(2, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(2, chunk->get(i)[4].get_int32());
                    } else if (rows_per_segment * 2 <= index && index < rows_per_segment * 3) {
                        EXPECT_EQ(3, chunk->get(i)[2].get_int32());
                        EXPECT_EQ(3, chunk->get(i)[3].get_int32());
                        EXPECT_EQ(3, chunk->get(i)[4].get_int32());
                    }
                }
                count += chunk->num_rows();
                chunk->reset();
            }
        }
        EXPECT_EQ(count, rows_per_segment * 3);
    }

    ASSERT_EQ(1, tablet->updates()->version_history_count());
    auto pool = StorageEngine::instance()->update_manager()->apply_thread_pool();
    auto st = tablet->rowset_commit(2, rowset, 0);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_LE(pool->num_threads(), 1);
    ASSERT_EQ(2, tablet->updates()->max_version());
    ASSERT_EQ(2, tablet->updates()->version_history_count());

    {
        TabletReader reader(tablet, Version(0, 2), schema);
        auto iter = create_tablet_iterator(reader, schema);
        ASSERT_TRUE(iter != nullptr);
        auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
        size_t count = 0;
        while (true) {
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_FALSE(!st.ok());
            for (auto i = 0; i < chunk->num_rows(); i++) {
                auto index = count + i;
                if (0 <= index && index < rows_per_segment / 2) {
                    EXPECT_EQ(1, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(1, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(1, chunk->get(i)[4].get_int32());
                } else if (rows_per_segment / 2 <= index && index < rows_per_segment) {
                    EXPECT_EQ(2, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(2, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(2, chunk->get(i)[4].get_int32());
                } else if (rows_per_segment <= index && index < rows_per_segment * 2) {
                    EXPECT_EQ(3, chunk->get(i)[2].get_int32());
                    EXPECT_EQ(3, chunk->get(i)[3].get_int32());
                    EXPECT_EQ(3, chunk->get(i)[4].get_int32());
                }
            }
            count += chunk->num_rows();
            chunk->reset();
        }
        EXPECT_EQ(count, rows_per_segment * 2);
    }
}

static ssize_t read_and_compare(const ChunkIteratorPtr& iter, int64_t nkeys) {
    auto full_chunk = ChunkHelper::new_chunk(iter->schema(), nkeys);
    auto& cols = full_chunk->columns();
    for (size_t i = 0; i < nkeys / 4; i++) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[2]->append_datum(Datum(static_cast<int32_t>(1)));
        cols[3]->append_datum(Datum(static_cast<int32_t>(1)));
    }
    for (size_t i = nkeys / 4; i < nkeys / 2; i++) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[2]->append_datum(Datum(static_cast<int32_t>(2)));
        cols[3]->append_datum(Datum(static_cast<int32_t>(2)));
    }
    for (size_t i = nkeys / 2; i < nkeys; i++) {
        cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
        cols[2]->append_datum(Datum(static_cast<int32_t>(3)));
        cols[3]->append_datum(Datum(static_cast<int32_t>(3)));
    }
    size_t count = 0;
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            for (auto i = 0; i < chunk->num_rows(); i++) {
                EXPECT_EQ(full_chunk->get(count + i).compare(iter->schema(), chunk->get(i)), 0);
            }
            count += chunk->num_rows();
            chunk->reset();
        } else {
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet_and_compare(const TabletSharedPtr& tablet,
                                       const std::shared_ptr<TabletSchema>& partial_schema, int64_t version,
                                       int64_t nkeys) {
    Schema schema = ChunkHelper::convert_schema(partial_schema);
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_and_compare(iter, nkeys);
}

TEST_F(RowsetTest, FinalMergeVerticalPartialTest) {
    auto tablet = create_tablet(12345, 1111);
    const uint32_t rows_per_segment = 1024;
    config::vertical_compaction_max_columns_per_group = 1;
    RowsetWriterContext writer_context;
    std::vector<int32_t> column_indexes = {0, 1, 2, 3};
    std::shared_ptr<TabletSchema> partial_schema = TabletSchema::create(tablet->tablet_schema(), column_indexes);
    create_partial_rowset_writer_context(12345, column_indexes, partial_schema, tablet->tablet_schema(),
                                         &writer_context);
    writer_context.segments_overlap = OVERLAP_UNKNOWN;
    writer_context.rowset_path_prefix = tablet->schema_hash_path();

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    auto schema = ChunkHelper::convert_schema(partial_schema);

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = 0; i < rows_per_segment; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(1)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment / 2; i < rows_per_segment + rows_per_segment / 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(2)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(2)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = rows_per_segment; i < rows_per_segment * 2; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(3)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(3)));
        }
        ASSERT_OK(rowset_writer->add_chunk(*chunk.get()));
        ASSERT_OK(rowset_writer->flush());
    }

    auto rowset = rowset_writer->build().value();
    rowset->set_schema(tablet->tablet_schema());
    ASSERT_TRUE(rowset != nullptr);
    ASSERT_EQ(3, rowset->rowset_meta()->num_segments());
    ASSERT_EQ(rows_per_segment * 3, rowset->rowset_meta()->num_rows());

    ASSERT_TRUE(tablet->rowset_commit(2, rowset, 0).ok());
    EXPECT_EQ(rows_per_segment * 2, read_tablet_and_compare(tablet, partial_schema, 2, rows_per_segment * 2));
    ASSERT_OK(starrocks::StorageEngine::instance()->update_manager()->on_rowset_finished(tablet.get(), rowset.get()));
}

TEST_F(RowsetTest, VerticalWriteTest) {
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    RowsetWriterContext writer_context;
    create_rowset_writer_context(12345, tablet_schema, &writer_context);
    writer_context.max_rows_per_segment = 5000;
    writer_context.writer_type = kVertical;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    int32_t chunk_size = 3000;
    size_t num_rows = 10000;

    {
        // k1 k2
        std::vector<uint32_t> column_indexes{0, 1};
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            }
            ASSERT_OK(rowset_writer->add_columns(*chunk, column_indexes, true));
        }
        ASSERT_OK(rowset_writer->flush_columns());
    }

    {
        // v1
        std::vector<uint32_t> column_indexes{2};
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
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

    RowsetReadOptions rs_opts;
    rs_opts.is_primary_keys = false;
    rs_opts.sorted = true;
    rs_opts.version = 0;
    rs_opts.stats = &_stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto res = rowset->new_iterator(schema, rs_opts);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

    auto iterator = res.value();
    int count = 0;
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
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

TEST_F(RowsetTest, SegmentWriteTest) {
    auto tablet_schema = TabletSchemaHelper::create_tablet_schema();

    RowsetWriterContext writer_context;
    create_rowset_writer_context(12345, tablet_schema, &writer_context);
    writer_context.writer_type = kHorizontal;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    int32_t chunk_size = 3000;
    size_t num_rows = 10000;

    std::vector<std::unique_ptr<SegmentPB>> seg_infos;
    {
        // k1 k2 v
        std::vector<uint32_t> column_indexes{0, 1, 2};
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows / chunk_size + 1; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
                cols[2]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            }
            seg_infos.emplace_back(std::make_unique<SegmentPB>());
            ASSERT_OK(rowset_writer->flush_chunk(*chunk, seg_infos.back().get()));
        }
    }

    // check rowset
    RowsetSharedPtr rowset = rowset_writer->build().value();
    ASSERT_EQ(num_rows, rowset->rowset_meta()->num_rows());
    ASSERT_EQ(4, rowset->rowset_meta()->num_segments());

    RowsetReadOptions rs_opts;
    rs_opts.is_primary_keys = false;
    rs_opts.sorted = true;
    rs_opts.version = 0;
    rs_opts.stats = &_stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto res = rowset->new_iterator(schema, rs_opts);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

    auto iterator = res.value();
    int count = 0;
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
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

    std::unique_ptr<RowsetWriter> segment_rowset_writer;
    writer_context.rowset_path_prefix = config::storage_root_path + "/data/rowset_test_seg";
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &segment_rowset_writer).ok());

    std::shared_ptr<FileSystem> fs = FileSystem::CreateSharedFromString(rowset->rowset_path()).value();

    for (int i = 0; i < seg_infos.size(); ++i) {
        auto& seg_info = seg_infos[i];
        auto seg_path = rowset->segment_file_path(rowset->rowset_path(), rowset->rowset_id(), i);
        auto rfile = std::move(fs->new_random_access_file(seg_path).value());

        butil::IOBuf data;
        auto buf = new uint8[seg_info->data_size()];
        data.append_user_data(buf, seg_info->data_size(), [](void* buf) { delete[](uint8*) buf; });

        ASSERT_TRUE(rfile->read_fully(buf, seg_info->data_size()).ok());
        auto st = segment_rowset_writer->flush_segment(*seg_info, data);
        LOG(INFO) << st;
        ASSERT_TRUE(st.ok());
    }

    // check rowset
    {
        RowsetSharedPtr rowset = segment_rowset_writer->build().value();
        ASSERT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        ASSERT_EQ(4, rowset->rowset_meta()->num_segments());

        RowsetReadOptions rs_opts;
        rs_opts.is_primary_keys = false;
        rs_opts.sorted = true;
        rs_opts.version = 0;
        rs_opts.stats = &_stats;
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto res = rowset->new_iterator(schema, rs_opts);
        ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);

        auto iterator = res.value();
        int count = 0;
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
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
}

TEST_F(RowsetTest, SegmentRewriterAutoIncrementTest) {
    std::shared_ptr<TabletSchema> partial_tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_key_pb(3)});

    RowsetWriterContext writer_context;
    create_rowset_writer_context(12345, partial_tablet_schema, &writer_context);
    writer_context.writer_type = kHorizontal;

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    int32_t chunk_size = 3000;
    size_t num_rows = 3000;

    std::vector<std::unique_ptr<SegmentPB>> seg_infos;
    {
        std::vector<uint32_t> column_indexes{0, 1, 2};
        auto schema = ChunkHelper::convert_schema(partial_tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows / chunk_size + 1; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
                cols[2]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            }
            seg_infos.emplace_back(std::make_unique<SegmentPB>());
            ASSERT_OK(rowset_writer->flush_chunk(*chunk, seg_infos.back().get()));
        }
    }

    RowsetSharedPtr rowset = rowset_writer->build().value();
    ASSERT_EQ(num_rows, rowset->rowset_meta()->num_rows());
    ASSERT_EQ(2, rowset->rowset_meta()->num_segments());
    rowset->load();

    std::shared_ptr<FileSystem> fs = FileSystem::CreateSharedFromString(rowset->rowset_path()).value();
    std::string file_name = Rowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), 0);

    auto partial_segment = *Segment::open(fs, file_name, 0, partial_tablet_schema);
    ASSERT_EQ(partial_segment->num_rows(), num_rows);

    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3), create_int_value_pb(4)});
    std::vector<uint32_t> read_column_ids{2, 3};
    std::vector<std::unique_ptr<Column>> write_columns(read_column_ids.size());
    for (auto i = 0; i < read_column_ids.size(); ++i) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tablet_schema->column(read_column_id);
        auto column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        write_columns[i] = column->clone_empty();
        for (auto j = 0; j < num_rows; ++j) {
            write_columns[i]->append_datum(Datum(static_cast<int32_t>(j + read_column_ids[i])));
        }
    }

    AutoIncrementPartialUpdateState auto_increment_partial_update_state;
    auto_increment_partial_update_state.init(rowset.get(), partial_tablet_schema, 2, 0);
    auto_increment_partial_update_state.write_column.reset(write_columns[0].release());
    write_columns.erase(write_columns.begin());
    auto dst_file_name = Rowset::segment_temp_file_path(rowset->rowset_path(), rowset->rowset_id(), 0);

    std::vector<uint32_t> column_ids{3};
    ASSERT_OK(SegmentRewriter::rewrite(file_name, dst_file_name, tablet_schema, auto_increment_partial_update_state,
                                       column_ids, &write_columns));

    auto segment = *Segment::open(fs, dst_file_name, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);
}

TEST_F(RowsetTest, SegmentDeleteWriteTest) {
    auto tablet = create_tablet(12345, 1111);
    int64_t num_rows = 1024;
    RowsetWriterContext writer_context;
    create_rowset_writer_context(12345, tablet->tablet_schema(), &writer_context);

    std::unique_ptr<RowsetWriter> rowset_writer;
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &rowset_writer).ok());

    auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

    Int64Column deletes;
    std::unique_ptr<SegmentPB> seg_info = std::make_unique<SegmentPB>();
    {
        auto chunk = ChunkHelper::new_chunk(schema, config::vector_chunk_size);
        auto& cols = chunk->columns();
        for (auto i = 0; i < num_rows; i++) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(1)));
            cols[4]->append_datum(Datum(static_cast<int32_t>(1)));
            if (i % 2 == 1) {
                deletes.append(i);
            }
        }
        ASSERT_OK(rowset_writer->flush_chunk_with_deletes(*chunk, deletes, seg_info.get()));

        LOG(INFO) << "segment " << seg_info->data_size() << " delete " << seg_info->delete_data_size();
    }

    RowsetSharedPtr rowset = rowset_writer->build().value();
    ASSERT_EQ(num_rows, rowset->rowset_meta()->num_rows());
    ASSERT_EQ(1, rowset->rowset_meta()->num_segments());

    std::unique_ptr<RowsetWriter> segment_rowset_writer;
    writer_context.rowset_path_prefix = config::storage_root_path + "/data/rowset_test_delete";
    ASSERT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &segment_rowset_writer).ok());

    std::shared_ptr<FileSystem> fs = FileSystem::CreateSharedFromString(rowset->rowset_path()).value();

    auto seg_path = rowset->segment_file_path(rowset->rowset_path(), rowset->rowset_id(), 0);
    auto seg_del_path = rowset->segment_del_file_path(rowset->rowset_path(), rowset->rowset_id(), 0);

    auto rfile = std::move(fs->new_random_access_file(seg_path).value());
    auto dfile = std::move(fs->new_random_access_file(seg_del_path).value());

    butil::IOBuf data;
    auto buf = new uint8[seg_info->data_size()];
    ASSERT_TRUE(rfile->read_fully(buf, seg_info->data_size()).ok());
    data.append_user_data(buf, seg_info->data_size(), [](void* buf) { delete[](uint8*) buf; });

    auto del_buf = new uint8[seg_info->delete_data_size()];
    ASSERT_TRUE(dfile->read_fully(del_buf, seg_info->delete_data_size()).ok());
    data.append_user_data(del_buf, seg_info->delete_data_size(), [](void* buf) { delete[](uint8*) buf; });

    auto st = segment_rowset_writer->flush_segment(*seg_info, data);
    LOG(INFO) << st;
    ASSERT_TRUE(st.ok());
}
} // namespace starrocks
