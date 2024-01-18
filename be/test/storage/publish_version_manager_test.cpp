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

#include "storage/publish_version_manager.h"

#include <gtest/gtest.h>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "agent/publish_version.h"
#include "butil/file_util.h"
#include "column/column_helper.h"
#include "column/column_pool.h"
#include "common/config.h"
#include "exec/pipeline/query_context.h"
#include "fs/fs_util.h"
#include "gtest/gtest.h"
#include "storage/chunk_helper.h"
#include "storage/delta_writer.h"
#include "storage/empty_iterator.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_reader.h"
#include "storage/txn_manager.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "testutil/assert.h"

namespace starrocks::vectorized {

class PublishVersionManagerTest : public testing::Test {
public:
    void SetUp() override {
        _publish_version_manager = starrocks::StorageEngine::instance()->publish_version_manager();
        _finish_publish_version_thread = std::thread([this] { _finish_publish_version_thread_callback(nullptr); });
        Thread::set_thread_name(_finish_publish_version_thread, "finish_publish_version");
    }

    void TearDown() override {
        if (_tablet) {
            StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
        _stopped.store(true, std::memory_order_release);
        _finish_publish_version_cv.notify_all();
        if (_finish_publish_version_thread.joinable()) {
            _finish_publish_version_thread.join();
        }
    }

    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

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

    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys,
                                  Column* one_delete = nullptr, bool empty = false, bool has_merge_condition = false) {
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
        if (has_merge_condition) {
            writer_context.merge_condition = "v2";
        }
        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());
        if (empty) {
            return *writer->build();
        }
        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto& cols = chunk->columns();
        for (int64_t key : keys) {
            if (schema.num_key_fields() == 1) {
                cols[0]->append_datum(Datum(key));
            } else {
                cols[0]->append_datum(Datum(key));
                string v = fmt::to_string(key * 234234342345);
                cols[1]->append_datum(Datum(Slice(v)));
                cols[2]->append_datum(Datum((int32_t)key));
            }
            int vcol_start = schema.num_key_fields();
            cols[vcol_start]->append_datum(Datum((int16_t)(key % 100 + 1)));
            if (cols[vcol_start + 1]->is_binary()) {
                string v = fmt::to_string(key % 1000 + 2);
                cols[vcol_start + 1]->append_datum(Datum(Slice(v)));
            } else {
                cols[vcol_start + 1]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }
        }
        if (one_delete == nullptr && !keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        } else if (one_delete == nullptr) {
            CHECK_OK(writer->flush());
        } else if (one_delete != nullptr) {
            CHECK_OK(writer->flush_chunk_with_deletes(*chunk, *one_delete));
        }
        return *writer->build();
    }

    void* _finish_publish_version_thread_callback(void* arg) {
        while (!_stopped.load(std::memory_order_consume)) {
            int32_t interval = config::finish_publish_version_internal;
            {
                std::unique_lock<std::mutex> wl(_finish_publish_version_mutex);
                CHECK(_publish_version_manager != nullptr);
                while (!_publish_version_manager->has_pending_task() && !_stopped.load(std::memory_order_consume)) {
                    _finish_publish_version_cv.wait(wl);
                }
                _publish_version_manager->finish_publish_version_task();
                if (interval <= 0) {
                    LOG(WARNING) << "finish_publish_version_internal config is illegal: " << interval
                                 << ", force set to 1";
                    interval = 1000;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }

        return nullptr;
    }

public:
    TabletSharedPtr _tablet;
    std::thread _finish_publish_version_thread;
    std::mutex _finish_publish_version_mutex;
    std::condition_variable _finish_publish_version_cv;
    std::atomic<bool> _stopped{false};
    PublishVersionManager* _publish_version_manager;
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

static ssize_t read_until_eof(const ChunkIteratorPtr& iter) {
    auto chunk = ChunkHelper::new_chunk(iter->schema(), 100);
    size_t count = 0;
    while (true) {
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        } else if (st.ok()) {
            count += chunk->num_rows();
            chunk->reset();
        } else {
            LOG(WARNING) << "read error: " << st.to_string();
            return -1;
        }
    }
    return count;
}

static ssize_t read_tablet(const TabletSharedPtr& tablet, int64_t version) {
    Schema schema = ChunkHelper::convert_schema(tablet->tablet_schema());
    TabletReader reader(tablet, Version(0, version), schema);
    auto iter = create_tablet_iterator(reader, schema);
    if (iter == nullptr) {
        return -1;
    }
    return read_until_eof(iter);
}

TEST_F(PublishVersionManagerTest, test_publish_task) {
    _tablet = create_tablet(rand(), rand());
    // write
    const int N = 1000;
    std::vector<int64_t> keys;
    for (int i = 0; i < N; i++) {
        keys.push_back(i);
    }
    auto rs0 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(2, rs0).ok());
    _tablet->updates()->stop_apply(true);
    auto rs1 = create_rowset(_tablet, keys);
    ASSERT_TRUE(_tablet->rowset_commit(3, rs1).ok());
    std::vector<TFinishTaskRequest> finish_task_requests;
    auto& finish_task_request = finish_task_requests.emplace_back();
    finish_task_request.signature = 2222;
    auto& tablet_publish_versions = finish_task_request.tablet_publish_versions;
    auto& pair = tablet_publish_versions.emplace_back();
    pair.__set_tablet_id(_tablet->tablet_id());
    pair.__set_version(3);
    _publish_version_manager->wait_publish_task_apply_finish(std::move(finish_task_requests));
    _finish_publish_version_cv.notify_one();

    ASSERT_EQ(0, _publish_version_manager->finish_task_requests_size());
    ASSERT_EQ(1, _publish_version_manager->waitting_finish_task_requests_size());
    _tablet->updates()->stop_apply(false);
    _tablet->updates()->check_for_apply();
    ASSERT_EQ(N, read_tablet(_tablet, 3));

    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT_EQ(0, _publish_version_manager->finish_task_requests_size());
    ASSERT_EQ(0, _publish_version_manager->waitting_finish_task_requests_size());
}

} // namespace starrocks