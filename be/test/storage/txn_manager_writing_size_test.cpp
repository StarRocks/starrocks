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

#include <atomic>
#include <thread>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/schema.h"
#include "storage/chunk_helper.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/txn_manager.h"

namespace starrocks {

class TxnManagerWritingSizeTest : public testing::TestWithParam<TKeysType::type> {
public:
    void SetUp() override {
        static std::atomic<int64_t> next_tablet_id{89123000};
        TCreateTabletReq request;
        request.tablet_id = next_tablet_id.fetch_add(1);
        request.__set_partition_id(request.tablet_id);
        request.__set_version(1);
        request.tablet_schema.schema_hash = 1234;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = GetParam();
        request.tablet_schema.storage_type = TStorageType::COLUMN;
        TColumn key;
        key.column_name = "k";
        key.__set_is_key(true);
        key.__set_is_allow_null(false);
        key.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(key);
        ASSERT_OK(StorageEngine::instance()->create_tablet(request));
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
        ASSERT_NE(nullptr, _tablet);
        _txn_mgr = StorageEngine::instance()->txn_manager();
        _load_id.set_hi(1);
        _load_id.set_lo(request.tablet_id);
    }

    void TearDown() override {
        if (_tablet != nullptr) {
            _txn_mgr->force_rollback_tablet_related_txns(_tablet->data_dir()->get_meta(), _tablet->tablet_id(),
                                                         _tablet->schema_hash(), _tablet->tablet_uid());
            ASSERT_OK(StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id()));
            _tablet.reset();
        }
    }

    void prepare(int64_t txn_id) {
        ASSERT_OK(_txn_mgr->prepare_txn(_tablet->partition_id(), _tablet, txn_id, _load_id));
    }

    void add(int64_t txn_id, int64_t bytes) {
        _txn_mgr->add_in_writing_data_size(_tablet->partition_id(), _tablet, txn_id, bytes);
    }

    RowsetSharedPtr commit(int64_t txn_id, bool with_data = false) {
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_uid = _tablet->tablet_uid();
        context.tablet_schema_hash = _tablet->schema_hash();
        context.partition_id = _tablet->partition_id();
        context.rowset_path_prefix = _tablet->schema_hash_path();
        context.tablet_schema = _tablet->tablet_schema();
        context.txn_id = txn_id;
        context.load_id = _load_id;
        context.rowset_state = PREPARED;
        std::unique_ptr<RowsetWriter> writer;
        CHECK_OK(RowsetFactory::create_rowset_writer(context, &writer));
        if (with_data) {
            auto schema = ChunkHelper::convert_schema(_tablet->tablet_schema());
            auto chunk = ChunkFactory::new_chunk(schema, 1);
            chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(txn_id)));
            CHECK_OK(writer->add_chunk(*chunk));
        }
        CHECK_OK(writer->flush());
        auto result = writer->build();
        CHECK(result.ok()) << result.status();
        auto rowset = result.value();
        if (with_data) {
            add(txn_id, rowset->data_disk_size());
        }
        CHECK_OK(_txn_mgr->commit_txn(_tablet->partition_id(), _tablet, txn_id, _load_id, rowset, false, false));
        return rowset;
    }

    void expect_empty() {
        EXPECT_EQ(0, _tablet->in_writing_data_size());
        // A zero byte entry still allocates a map node; checking only the sum misses this leak.
        EXPECT_TRUE(_tablet->_in_writing_txn_size.empty());
    }

protected:
    TabletSharedPtr _tablet;
    TxnManager* _txn_mgr = nullptr;
    PUniqueId _load_id;
};

TEST_P(TxnManagerWritingSizeTest, PublishReleasesEntriesForEveryTransaction) {
    for (int64_t txn_id = 1; txn_id <= 20; ++txn_id) {
        prepare(txn_id);
        add(txn_id, 10);
        add(txn_id, 20);
        EXPECT_EQ(30, _tablet->in_writing_data_size());
        auto rowset = commit(txn_id);
        // A committed writer can be destroyed before FE publishes. Do not lose these bytes early.
        EXPECT_EQ(30, _tablet->in_writing_data_size());
        add(txn_id, 100); // A delayed/duplicate flush must not change a committed transaction.
        EXPECT_EQ(30, _tablet->in_writing_data_size());
        ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, txn_id, txn_id + 1, rowset));
        expect_empty();
        ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, txn_id, txn_id + 1, rowset));
        add(txn_id, 100);
        expect_empty();
    }
}

TEST_P(TxnManagerWritingSizeTest, OverwriteReleasesEntry) {
    prepare(1);
    add(1, 10);
    auto rowset = commit(1);
    ASSERT_OK(_txn_mgr->publish_overwrite_txn(_tablet->partition_id(), _tablet, 1, 2, rowset, 0));
    expect_empty();
    add(1, 100);
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, PendingPublishPreservesCapacityUntilVersionGapIsFilled) {
    if (GetParam() != TKeysType::PRIMARY_KEYS) {
        GTEST_SKIP() << "Only PK tablets have pending commits";
    }
    const auto initial_data_size = _tablet->data_size();
    prepare(1);
    auto later_rowset = commit(1, true);
    const auto later_bytes = later_rowset->data_disk_size();
    ASSERT_GT(later_bytes, 0);
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());
    // Version 2 has not arrived. A successful publish of version 3 only enters pending commits.
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset));
    EXPECT_EQ(1, _tablet->updates()->num_pending());
    EXPECT_TRUE(_tablet->_in_writing_txn_size.empty());
    EXPECT_EQ(initial_data_size, _tablet->data_size());
    EXPECT_EQ(later_bytes, _tablet->updates()->pending_data_size());
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset));
    EXPECT_TRUE(_tablet->_in_writing_txn_size.empty());
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());

    prepare(2);
    auto earlier_rowset = commit(2, true);
    const auto earlier_bytes = earlier_rowset->data_disk_size();
    ASSERT_GT(earlier_bytes, 0);
    EXPECT_EQ(later_bytes + earlier_bytes, _tablet->in_writing_data_size());
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 2, 2, earlier_rowset));
    EXPECT_EQ(0, _tablet->updates()->num_pending());
    EXPECT_EQ(0, _tablet->updates()->pending_data_size());
    expect_empty();
    // data_size also includes index/column files; all committed rowset bytes must be present.
    EXPECT_GE(_tablet->data_size(), initial_data_size + later_bytes + earlier_bytes);
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 2, 2, earlier_rowset));
    expect_empty();
    EXPECT_GE(_tablet->data_size(), initial_data_size + later_bytes + earlier_bytes);
}

TEST_P(TxnManagerWritingSizeTest, PendingMetadataFailureKeepsSingleEstimateForRetry) {
    if (GetParam() != TKeysType::PRIMARY_KEYS) {
        GTEST_SKIP() << "Only PK tablets have pending commits";
    }
    const auto initial_data_size = _tablet->data_size();
    prepare(1);
    auto later_rowset = commit(1, true);
    const auto later_bytes = later_rowset->data_disk_size();
    ASSERT_GT(later_bytes, 0);
    {
        DeferOp restore_error([] {
            TEST_DISABLE_ERROR_POINT("TabletMetaManager::pending_rowset_commit");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->EnableProcessing();
        TEST_ENABLE_ERROR_POINT("TabletMetaManager::pending_rowset_commit", Status::IOError("injected pending write"));
        for (int attempt = 0; attempt < 2; ++attempt) {
            auto st = _txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset);
            ASSERT_TRUE(st.is_io_error()) << st;
            EXPECT_EQ(0, _tablet->updates()->num_pending());
            EXPECT_EQ(0, _tablet->updates()->pending_data_size());
            EXPECT_EQ(initial_data_size, _tablet->data_size());
            EXPECT_EQ(1, _tablet->_in_writing_txn_size.size());
            EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());
        }
    }
    // Retry must persist the pending rowset, not mistake the failed insertion for success.
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset));
    EXPECT_EQ(1, _tablet->updates()->num_pending());
    EXPECT_TRUE(_tablet->_in_writing_txn_size.empty());
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset));
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());

    prepare(2);
    auto earlier_rowset = commit(2, true);
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 2, 2, earlier_rowset));
    EXPECT_EQ(0, _tablet->updates()->num_pending());
    expect_empty();
    EXPECT_GE(_tablet->data_size(), initial_data_size + later_bytes + earlier_rowset->data_disk_size());
}

TEST_P(TxnManagerWritingSizeTest, FailedPendingPublishIsNotAppliedWhenGapIsFilled) {
    if (GetParam() != TKeysType::PRIMARY_KEYS) {
        GTEST_SKIP() << "Only PK tablets have pending commits";
    }
    prepare(1);
    auto later_rowset = commit(1, true);
    const auto later_bytes = later_rowset->data_disk_size();
    ASSERT_GT(later_bytes, 0);
    {
        DeferOp restore_error([] {
            TEST_DISABLE_ERROR_POINT("TabletMetaManager::pending_rowset_commit");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        SyncPoint::GetInstance()->EnableProcessing();
        TEST_ENABLE_ERROR_POINT("TabletMetaManager::pending_rowset_commit", Status::IOError("injected pending write"));
        auto st = _txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset);
        ASSERT_TRUE(st.is_io_error()) << st;
    }
    prepare(2);
    auto earlier_rowset = commit(2, true);
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 2, 2, earlier_rowset));
    EXPECT_EQ(0, _tablet->updates()->num_pending());
    EXPECT_EQ(2, _tablet->updates()->max_version());
    EXPECT_EQ(1, _tablet->_in_writing_txn_size.size());
    EXPECT_EQ(later_bytes, _tablet->in_writing_data_size());
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 3, later_rowset));
    EXPECT_EQ(3, _tablet->updates()->max_version());
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, PublishFailureKeepsEntryForRetry) {
    if (GetParam() != TKeysType::PRIMARY_KEYS) {
        GTEST_SKIP() << "Inject the PK rowset_commit error branch";
    }
    prepare(1);
    add(1, 10);
    auto rowset = commit(1);
    Status status;
    {
        DeferOp restore_error([&] { _tablet->updates()->_error.store(false); });
        _tablet->updates()->_error.store(true);
        status = _txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 2, rowset);
    }
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(10, _tablet->in_writing_data_size());
    EXPECT_EQ(1, _tablet->_in_writing_txn_size.size());
    ASSERT_OK(_txn_mgr->publish_txn(_tablet->partition_id(), _tablet, 1, 2, rowset));
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, OverwriteFailureKeepsEntryForRetry) {
    if (GetParam() != TKeysType::PRIMARY_KEYS) {
        GTEST_SKIP() << "Inject the PK overwrite rowset_commit error branch";
    }
    prepare(1);
    add(1, 10);
    auto rowset = commit(1);
    Status status;
    {
        DeferOp restore_error([&] { _tablet->updates()->_error.store(false); });
        _tablet->updates()->_error.store(true);
        status = _txn_mgr->publish_overwrite_txn(_tablet->partition_id(), _tablet, 1, 2, rowset, 0);
    }
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(10, _tablet->in_writing_data_size());
    ASSERT_OK(_txn_mgr->publish_overwrite_txn(_tablet->partition_id(), _tablet, 1, 2, rowset, 0));
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, EarlyRollbackReleasesEntryAndRejectsLateFlush) {
    prepare(1);
    add(1, 10);
    ASSERT_OK(_txn_mgr->rollback_txn(_tablet->partition_id(), _tablet, 1));
    expect_empty();
    add(1, 100);
    ASSERT_OK(_txn_mgr->rollback_txn(_tablet->partition_id(), _tablet, 1));
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, CommittedRollbackKeepsEntryUntilClearTransaction) {
    prepare(1);
    add(1, 10);
    auto rowset = commit(1);
    ASSERT_TRUE(_txn_mgr->rollback_txn(_tablet->partition_id(), _tablet, 1).is_already_exist());
    EXPECT_EQ(10, _tablet->in_writing_data_size());
    // FE can abort after the local writer committed (e.g. a late max_filter_ratio failure).
    StorageEngine::instance()->clear_transaction_task(1, {_tablet->partition_id()});
    expect_empty();
    StorageEngine::instance()->clear_transaction_task(1, {_tablet->partition_id()});
    add(1, 100);
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, ClearPreparedEntryIncludingZeroBytes) {
    prepare(1);
    add(1, 0);
    ASSERT_EQ(1, _tablet->_in_writing_txn_size.size());
    ASSERT_OK(_txn_mgr->delete_txn(_tablet->partition_id(), _tablet, 1));
    add(1, 0);
    expect_empty();
    EXPECT_TRUE(_txn_mgr->delete_txn(_tablet->partition_id(), _tablet, 1).is_not_found());
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, ConcurrentFlushAndClearCannotRecreateEntry) {
    prepare(1);
    add(1, 1);
    std::atomic<bool> started{false};
    std::thread flush([&] {
        started.store(true);
        for (int i = 0; i < 10000; ++i) {
            add(1, 1);
        }
    });
    while (!started.load()) {
        std::this_thread::yield();
    }
    auto status = _txn_mgr->delete_txn(_tablet->partition_id(), _tablet, 1);
    flush.join();
    ASSERT_OK(status);
    expect_empty();
    add(1, 100);
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, ReprepareStartsANewAccountingLifetime) {
    prepare(1);
    add(1, 10);
    ASSERT_OK(_txn_mgr->rollback_txn(_tablet->partition_id(), _tablet, 1));
    expect_empty();
    add(1, 100); // Absent transactions reject late flushes.
    expect_empty();
    // prepare_txn intentionally allows retries. The guard is keyed by tablet/txn, not by a
    // writer-attempt generation; calls after reprepare belong to the new accounting lifetime.
    prepare(1);
    add(1, 20);
    EXPECT_EQ(20, _tablet->in_writing_data_size());
    auto rowset = commit(1);
    ASSERT_OK(_txn_mgr->delete_txn(_tablet->partition_id(), _tablet, 1));
    expect_empty();
}

TEST_P(TxnManagerWritingSizeTest, RejectedDeletePreservesWritingEstimate) {
    prepare(1);
    add(1, 10);
    auto rowset = commit(1);
    rowset->make_visible(Version(2, 2));
    ASSERT_FALSE(_txn_mgr->delete_txn(_tablet->partition_id(), _tablet, 1).ok());
    EXPECT_EQ(10, _tablet->in_writing_data_size());
    EXPECT_EQ(1, _tablet->_in_writing_txn_size.size());
}

TEST_P(TxnManagerWritingSizeTest, ForceRollbackReleasesPreparedAndCommittedEntries) {
    prepare(1);
    add(1, 10);
    prepare(2);
    add(2, 20);
    auto rowset = commit(2);
    _txn_mgr->force_rollback_tablet_related_txns(_tablet->data_dir()->get_meta(), _tablet->tablet_id(),
                                                 _tablet->schema_hash(), _tablet->tablet_uid());
    add(1, 100);
    add(2, 100);
    expect_empty();
}

INSTANTIATE_TEST_SUITE_P(SharedNothing, TxnManagerWritingSizeTest,
                         testing::Values(TKeysType::PRIMARY_KEYS, TKeysType::DUP_KEYS));

} // namespace starrocks
