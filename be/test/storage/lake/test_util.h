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

#pragma once
#include <gtest/gtest.h>

#include <utility>

#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_meta_manager.h"
#include "testutil/assert.h"

namespace starrocks::lake {

StatusOr<TabletMetadataPtr> TEST_publish_single_version(TabletManager* tablet_mgr, int64_t tablet_id,
                                                        int64_t new_version, int64_t txn_id);

Status TEST_publish_single_log_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id,
                                       int64_t log_version);

class TestBase : public ::testing::Test {
public:
    ~TestBase() override {
        // Wait for all vacuum tasks finished processing before destroying
        // _tablet_mgr.
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
        (void)fs::remove_all(_test_dir);
    }

protected:
    explicit TestBase(std::string test_dir, int64_t cache_limit = 1024 * 1024)
            : _test_dir(std::move(test_dir)),
              _parent_tracker(std::make_unique<MemTracker>(-1)),
              _mem_tracker(std::make_unique<MemTracker>(-1, "", _parent_tracker.get())),
              _lp(std::make_unique<FixedLocationProvider>(_test_dir)),
              _update_mgr(std::make_unique<UpdateManager>(_lp.get())),
              _tablet_mgr(std::make_unique<TabletManager>(_lp.get(), _update_mgr.get(), cache_limit)) {}

    void remove_test_dir_or_die() { ASSERT_OK(fs::remove_all(_test_dir)); }

    void remove_test_dir_ignore_error() { (void)fs::remove_all(_test_dir); }

    void clear_and_init_test_dir() {
        remove_test_dir_ignore_error();
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kTxnLogDirectoryName)));
    }

    void check_local_persistent_index_meta(int64_t tablet_id, int64_t expected_version) {
        PersistentIndexMetaPB index_meta;
        DataDir* data_dir = StorageEngine::instance()->get_persistent_index_store(tablet_id);
        CHECK_OK(TabletMetaManager::get_persistent_index_meta(data_dir, tablet_id, &index_meta));
        ASSERT_TRUE(index_meta.version().major_number() == expected_version);
    }

    StatusOr<TabletMetadataPtr> publish_single_version(int64_t tablet_id, int64_t new_version, int64_t txn_id);

    Status publish_single_log_version(int64_t tablet_id, int64_t txn_id, int64_t log_version);

    std::string _test_dir;
    std::unique_ptr<MemTracker> _parent_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<LocationProvider> _lp;
    std::unique_ptr<UpdateManager> _update_mgr;
    std::unique_ptr<TabletManager> _tablet_mgr;
};

struct PrimaryKeyParam {
    bool enable_persistent_index = false;
};

inline StatusOr<TabletMetadataPtr> TEST_publish_single_version(TabletManager* tablet_mgr, int64_t tablet_id,
                                                               int64_t new_version, int64_t txn_id) {
    lake::PublishVersionRequest request;
    lake::PublishVersionResponse response;

    request.add_tablet_ids(tablet_id);
    request.add_txn_ids(txn_id);
    request.set_base_version(new_version - 1);
    request.set_new_version(new_version);
    request.set_commit_time(time(nullptr));

    auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), tablet_mgr);
    lake_service.publish_version(nullptr, &request, &response, nullptr);

    if (response.failed_tablets_size() == 0) {
        return tablet_mgr->get_tablet_metadata(tablet_id, new_version);
    } else {
        return Status::InternalError(fmt::format("failed to publish version. tablet_id={} txn_id={} new_version={}",
                                                 tablet_id, txn_id, new_version));
    }
}

inline Status TEST_publish_single_log_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id,
                                              int64_t log_version) {
    lake::PublishLogVersionRequest request;
    lake::PublishLogVersionResponse response;

    request.add_tablet_ids(tablet_id);
    request.add_txn_ids(txn_id);
    request.add_versions(log_version);

    auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), tablet_mgr);
    lake_service.publish_log_version(nullptr, &request, &response, nullptr);

    if (response.failed_tablets_size() == 0) {
        return Status::OK();
    } else {
        return Status::InternalError(fmt::format("failed to publish log version. tablet_id={} txn_id={} version={}",
                                                 tablet_id, txn_id, log_version));
    }
}

inline StatusOr<TabletMetadataPtr> TestBase::publish_single_version(int64_t tablet_id, int64_t new_version,
                                                                    int64_t txn_id) {
    return TEST_publish_single_version(_tablet_mgr.get(), tablet_id, new_version, txn_id);
}

inline Status TestBase::publish_single_log_version(int64_t tablet_id, int64_t txn_id, int64_t log_version) {
    return TEST_publish_single_log_version(_tablet_mgr.get(), tablet_id, txn_id, log_version);
}

} // namespace starrocks::lake
