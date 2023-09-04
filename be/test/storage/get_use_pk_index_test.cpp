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
#include <memory>
#include <string>
#include <thread>

#include "column/datum_tuple.h"
#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/kv_store.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema_change.h"
#include "storage/snapshot_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

namespace starrocks {

using vectorized::Datum;

class GetUsePkIndexTest : public testing::Test {
public:
    using Row = std::vector<Datum>;
    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const std::vector<std::vector<Row>>& segments,
                                  SegmentsOverlapPB overlap) {
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
        writer_context.segments_overlap = overlap;
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        for (size_t i = 0; i < segments.size(); i++) {
            auto& segment = segments[i];
            auto chunk = ChunkHelper::new_chunk(schema, segment.size());
            auto& cols = chunk->columns();
            for (auto& row : segment) {
                CHECK(cols.size() == row.size());
                for (size_t j = 0; j < row.size(); j++) {
                    cols[j]->append_datum(row[j]);
                }
            }
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

    static int64_t get_pk1(int64_t key) { return key % 100; }

    static int64_t get_pk2(int64_t key) { return key / 100; }

    static int64_t get_v1(int64_t key) { return static_cast<int64_t>((key * 7919) % 7883); }

    static int64_t get_v2(int64_t key) { return static_cast<int64_t>((key * 6361) % 6373); }

    void generate_data(int64_t key_start, size_t num_row, size_t num_segment, bool multi_column_pk, bool sort_key,
                       std::vector<std::vector<Row>>& segments) {
        size_t num_row_per_segment = (num_row + num_segment - 1) / num_segment;
        std::vector<int64_t> keys(num_row, 0);
        for (size_t i = 0; i < num_row; i++) {
            keys[i] = key_start + i;
        }
        std::random_shuffle(keys.begin(), keys.end());
        for (size_t i = 0; i < num_segment; i++) {
            auto& segment = segments.emplace_back();
            size_t start = i * num_row_per_segment;
            size_t end = std::min((i + 1) * num_row_per_segment, num_row);
            std::sort(keys.begin() + start, keys.begin() + end);
            for (size_t j = start; j < end; j++) {
                auto& row = segment.emplace_back();
                auto key = keys[j];
                if (multi_column_pk) {
                    row.push_back(Datum(get_pk1(key)));
                    row.push_back(Datum(get_pk2(key)));
                } else {
                    row.push_back(Datum(key));
                }
                row.push_back(Datum(get_v1(key)));
                row.push_back(Datum(get_v2(key)));
            }
            if (sort_key) {
                const size_t sort_key_idx = multi_column_pk ? 2 : 1;
                std::vector<size_t> ind(segment.size());
                std::iota(ind.begin(), ind.end(), 0);
                std::sort(ind.begin(), ind.end(), [&](size_t a, size_t b) {
                    return segment[a][sort_key_idx].get_int64() < segment[b][sort_key_idx].get_int64();
                });
                std::vector<Row> sorted_segment;
                sorted_segment.reserve(segment.size());
                for (auto idx : ind) {
                    sorted_segment.emplace_back(segment[idx]);
                }
                std::swap(segment, sorted_segment);
            }
        }
    }

    void print_data(const std::vector<std::vector<Row>>& segments) {
        for (size_t i = 0; i < segments.size(); i++) {
            std::cout << "segment " << i << std::endl;
            for (auto& row : segments[i]) {
                for (auto& datum : row) {
                    std::cout << "  " << datum.get_int64();
                }
                std::cout << std::endl;
            }
        }
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, bool multi_column_pk, bool sort_key) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        if (sort_key) {
            if (multi_column_pk) {
                request.tablet_schema.sort_key_idxes.push_back(2);
            } else {
                request.tablet_schema.sort_key_idxes.push_back(1);
            }
        }

        if (multi_column_pk) {
            TColumn pk1;
            pk1.column_name = "pk1";
            pk1.__set_is_key(true);
            pk1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(pk1);
            TColumn pk2;
            pk2.column_name = "pk2";
            pk2.__set_is_key(true);
            pk2.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(pk2);
        } else {
            TColumn k1;
            k1.column_name = "pk";
            k1.__set_is_key(true);
            k1.column_type.type = TPrimitiveType::BIGINT;
            request.tablet_schema.columns.push_back(k1);
        }

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k3);
        auto st = StorageEngine::instance()->create_tablet(request);
        CHECK(st.ok()) << st.to_string();
        return StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, false);
    }

    void wait_for_version(int64_t version, int64_t timeout_ms = 10000) {
        while (true) {
            timeout_ms -= 200;
            if (timeout_ms <= 0) {
                ASSERT_TRUE(false) << "timeout";
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            std::vector<RowsetSharedPtr> rowsets;
            EditVersion full_version;
            ASSERT_TRUE(_tablet->updates()->get_applied_rowsets(version, &rowsets, &full_version).ok());
            if (full_version.major() >= version) {
                break;
            }
            std::cerr << "waiting for version " << version << std::endl;
        }
    }

    void read_using_pk_index(int64_t key, int64_t version, bool multi_column_pk, bool expect_exist) {
        const vectorized::Schema& schema = *_tablet->tablet_schema().schema();
        vectorized::TabletReader reader(_tablet, Version(0, version), schema);
        vectorized::TabletReaderParams params;
        params.is_pipeline = true;
        params.reader_type = READER_QUERY;
        params.use_page_cache = false;
        params.use_pk_index = true;
        std::string pk_str = std::to_string(key);
        std::unique_ptr<vectorized::ColumnPredicate> pk_eq(
                vectorized::new_column_eq_predicate(get_type_info(FieldType::OLAP_FIELD_TYPE_BIGINT), 0, pk_str));
        std::string pk1_str = std::to_string(get_pk1(key));
        std::unique_ptr<vectorized::ColumnPredicate> pk1_eq(
                vectorized::new_column_eq_predicate(get_type_info(FieldType::OLAP_FIELD_TYPE_BIGINT), 0, pk1_str));
        std::string pk2_str = std::to_string(get_pk2(key));
        std::unique_ptr<vectorized::ColumnPredicate> pk2_eq(
                vectorized::new_column_eq_predicate(get_type_info(FieldType::OLAP_FIELD_TYPE_BIGINT), 1, pk2_str));
        if (multi_column_pk) {
            params.predicates.emplace_back(pk2_eq.get());
            params.predicates.emplace_back(pk1_eq.get());
        } else {
            params.predicates.emplace_back(pk_eq.get());
        }
        ASSERT_OK(reader.prepare());
        ASSERT_OK(reader.open(params));
        auto chunk = ChunkHelper::new_chunk(reader.schema(), 1);
        if (expect_exist) {
            ASSERT_OK(reader.do_get_next(chunk.get()));
            ASSERT_EQ(1, chunk->num_rows());
            if (multi_column_pk) {
                ASSERT_EQ(get_pk1(key), chunk->get_column_by_index(0)->get(0).get_int64());
                ASSERT_EQ(get_pk2(key), chunk->get_column_by_index(1)->get(0).get_int64());
                ASSERT_EQ(get_v1(key), chunk->get_column_by_index(2)->get(0).get_int64());
                ASSERT_EQ(get_v2(key), chunk->get_column_by_index(3)->get(0).get_int64());
            } else {
                ASSERT_EQ(key, chunk->get_column_by_index(0)->get(0).get_int64());
                ASSERT_EQ(get_v1(key), chunk->get_column_by_index(1)->get(0).get_int64());
                ASSERT_EQ(get_v2(key), chunk->get_column_by_index(2)->get(0).get_int64());
            }
            chunk->reset();
            ASSERT_TRUE(reader.do_get_next(chunk.get()).is_end_of_file());
        } else {
            ASSERT_TRUE(reader.do_get_next(chunk.get()).is_end_of_file());
        }
        reader.close();
    }

    void test_single_rowset_read(int32_t seed, bool multi_column_pk, bool sort_key) {
        const int64_t key_start = 0;
        const int num_row = _num_row;
        const int num_get = std::min(20, num_row / 1000);
        LOG(INFO) << "seed=" << seed << ", rowset=1, segment=" << _num_segment
                  << ", multi_column_pk=" << multi_column_pk << ", sort_key=" << sort_key << ", "
                  << "num_row=" << num_row << ", num_get=" << num_get;
        srand(GetCurrentTimeMicros());
        _tablet = create_tablet(rand(), rand(), multi_column_pk, sort_key);
        std::vector<std::vector<Row>> segments;
        std::srand(seed);
        generate_data(key_start, num_row, _num_segment, multi_column_pk, sort_key, segments);
        auto rs = create_rowset(_tablet, segments, SegmentsOverlapPB::OVERLAPPING);
        ASSERT_TRUE(_tablet->rowset_commit(2, rs).ok());
        wait_for_version(2);
        ASSERT_EQ(2, _tablet->updates()->max_version());
        for (int i = 0; i < num_get; i++) {
            int64_t get_key = std::rand() % num_row + key_start;
            read_using_pk_index(get_key, 2, multi_column_pk, true);
        }
        read_using_pk_index(key_start - 1, 2, multi_column_pk, false);
        read_using_pk_index(key_start + num_row, 2, multi_column_pk, false);
    }

    void test_multi_rowset_read(int32_t seed, bool multi_column_pk, bool sort_key) {
        const int64_t key_start = 0;
        const int num_row = _num_row;
        const int num_get = std::min(20, num_row / 1000);
        LOG(INFO) << "seed=" << seed << ", rowset=2, segment=" << _num_segment
                  << ", multi_column_pk=" << multi_column_pk << ", sort_key=" << sort_key << ", "
                  << "num_row=" << num_row << ", num_get=" << num_get;
        srand(GetCurrentTimeMicros());
        _tablet = create_tablet(rand(), rand(), multi_column_pk, sort_key);
        std::srand(seed);
        std::vector<std::vector<Row>> segments1;
        generate_data(key_start, num_row / 2, _num_segment, multi_column_pk, sort_key, segments1);
        auto rs1 = create_rowset(_tablet, segments1, SegmentsOverlapPB::OVERLAPPING);
        ASSERT_TRUE(_tablet->rowset_commit(2, rs1).ok());
        std::vector<std::vector<Row>> segments2;
        generate_data(key_start + num_row / 2, num_row - num_row / 2, _num_segment, multi_column_pk, sort_key,
                      segments2);
        auto rs2 = create_rowset(_tablet, segments2, SegmentsOverlapPB::OVERLAPPING);
        ASSERT_TRUE(_tablet->rowset_commit(3, rs2).ok());
        wait_for_version(3);
        ASSERT_EQ(3, _tablet->updates()->max_version());
        for (int i = 0; i < num_get; i++) {
            int64_t get_key = std::rand() % num_row + key_start;
            read_using_pk_index(get_key, 2, multi_column_pk, true);
        }
        read_using_pk_index(key_start - 1, 2, multi_column_pk, false);
        read_using_pk_index(key_start + num_row, 2, multi_column_pk, false);
    }

protected:
    TabletSharedPtr _tablet;
    size_t _num_segment = 1;
    size_t _num_row = 10000;
};

TEST_F(GetUsePkIndexTest, single_segment) {
    const int32_t seed = 0;
    _num_row = 10000;
    _num_segment = 1;
    test_single_rowset_read(seed, false, false);
    test_single_rowset_read(seed, true, false);
    test_single_rowset_read(seed, false, true);
    test_single_rowset_read(seed, true, true);
}

TEST_F(GetUsePkIndexTest, multi_segment) {
    const int32_t seed = 0;
    _num_row = 10000;
    _num_segment = 3;
    test_single_rowset_read(seed, false, false);
    test_single_rowset_read(seed, true, false);
    test_single_rowset_read(seed, false, true);
    test_single_rowset_read(seed, true, true);
}

TEST_F(GetUsePkIndexTest, multi_segment_multi_rowset) {
    const int32_t seed = 0;
    _num_row = 10000;
    _num_segment = 3;
    test_multi_rowset_read(seed, false, false);
    test_multi_rowset_read(seed, true, false);
    test_multi_rowset_read(seed, false, true);
    test_multi_rowset_read(seed, true, true);
}

}; // namespace starrocks
