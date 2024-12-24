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

#include <memory>
#include <utility>

#include "connector/connector.h"
#include "fs/fs_util.h"
#include "gutil/strings/join.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "service/service_be/lake_service.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/transactions.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_meta_manager.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

StatusOr<TabletMetadataPtr> TEST_publish_single_version(TabletManager* tablet_mgr, int64_t tablet_id,
                                                        int64_t new_version, int64_t txn_id,
                                                        bool rebuild_pindex = false);

Status TEST_publish_single_log_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id,
                                       int64_t log_version);

StatusOr<TabletMetadataPtr> TEST_batch_publish(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                               int64_t new_version, std::vector<int64_t>& txn_ids);

std::shared_ptr<RuntimeState> create_runtime_state();

std::shared_ptr<RuntimeState> create_runtime_state(const TQueryOptions& query_options);

DescriptorTbl* create_table_desc(RuntimeState* runtime_state, const std::vector<TypeDescriptor>& types);

std::shared_ptr<TPlanNode> create_tplan_node_cloud();

std::vector<TScanRangeParams> create_scan_ranges_cloud(std::vector<TabletMetadata*>& tablet_metas);

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
              _mem_tracker(std::make_unique<MemTracker>(1024 * 1024, "", _parent_tracker.get())),
              _lp(std::make_shared<FixedLocationProvider>(_test_dir)),
              _update_mgr(std::make_unique<UpdateManager>(_lp, _mem_tracker.get())),
              _tablet_mgr(std::make_unique<TabletManager>(_lp, _update_mgr.get(), cache_limit)) {}

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

    StatusOr<TabletMetadataPtr> publish_single_version(int64_t tablet_id, int64_t new_version, int64_t txn_id,
                                                       bool rebuild_pindex = false);

    Status publish_single_log_version(int64_t tablet_id, int64_t txn_id, int64_t log_version);

    StatusOr<TabletMetadataPtr> batch_publish(int64_t tablet_id, int64_t base_version, int64_t new_version,
                                              std::vector<int64_t>& txn_ids);

    std::string _test_dir;
    std::unique_ptr<MemTracker> _parent_tracker;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<LocationProvider> _lp;
    std::unique_ptr<UpdateManager> _update_mgr;
    std::unique_ptr<TabletManager> _tablet_mgr;
};

struct PrimaryKeyParam {
    bool enable_persistent_index = false;
    PersistentIndexTypePB persistent_index_type = PersistentIndexTypePB::LOCAL;
    PartialUpdateMode partial_update_mode = PartialUpdateMode::ROW_MODE;
    bool enable_transparent_data_encryption = false;
};

inline StatusOr<TabletMetadataPtr> TEST_publish_single_version(TabletManager* tablet_mgr, int64_t tablet_id,
                                                               int64_t new_version, int64_t txn_id,
                                                               bool rebuild_pindex) {
    PublishVersionRequest request;
    PublishVersionResponse response;

    request.add_tablet_ids(tablet_id);
    request.add_txn_ids(txn_id);
    request.set_base_version(new_version - 1);
    request.set_new_version(new_version);
    request.set_commit_time(time(nullptr));
    if (rebuild_pindex) {
        request.add_rebuild_pindex_tablet_ids(tablet_id);
    }

    auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), tablet_mgr);
    lake_service.publish_version(nullptr, &request, &response, nullptr);

    if (response.failed_tablets_size() == 0) {
        return tablet_mgr->get_tablet_metadata(tablet_id, new_version);
    } else {
        return Status::InternalError(fmt::format("failed to publish version. tablet_id={} txn_id={} new_version={}",
                                                 tablet_id, txn_id, new_version));
    }
}

inline StatusOr<TabletMetadataPtr> TEST_batch_publish(TabletManager* tablet_mgr, int64_t tablet_id,
                                                      int64_t base_version, int64_t new_version,
                                                      std::vector<int64_t>& txn_ids) {
    PublishVersionRequest request;
    PublishVersionResponse response;

    request.add_tablet_ids(tablet_id);
    for (auto& txn_id : txn_ids) {
        request.add_txn_ids(txn_id);
    }
    request.set_base_version(base_version);
    request.set_new_version(new_version);
    request.set_commit_time(time(nullptr));

    auto lake_service = LakeServiceImpl(ExecEnv::GetInstance(), tablet_mgr);
    lake_service.publish_version(nullptr, &request, &response, nullptr);

    if (response.failed_tablets_size() == 0) {
        return tablet_mgr->get_tablet_metadata(tablet_id, new_version);
    } else {
        return Status::InternalError(
                fmt::format("failed to publish version. tablet_id={} txn_ids={} base_version={} new_version={}",
                            tablet_id, JoinInts(txn_ids, ","), base_version, new_version));
    }
}

inline Status TEST_publish_single_log_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t txn_id,
                                              int64_t log_version) {
    PublishLogVersionRequest request;
    PublishLogVersionResponse response;

    request.add_tablet_ids(tablet_id);
    request.set_txn_id(txn_id);
    request.set_version(log_version);

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
                                                                    int64_t txn_id, bool rebuild_pindex) {
    return TEST_publish_single_version(_tablet_mgr.get(), tablet_id, new_version, txn_id, rebuild_pindex);
}

inline Status TestBase::publish_single_log_version(int64_t tablet_id, int64_t txn_id, int64_t log_version) {
    return TEST_publish_single_log_version(_tablet_mgr.get(), tablet_id, txn_id, log_version);
}

inline StatusOr<TabletMetadataPtr> TestBase::batch_publish(int64_t tablet_id, int64_t base_version, int64_t new_version,
                                                           std::vector<int64_t>& txn_ids) {
    return TEST_batch_publish(_tablet_mgr.get(), tablet_id, base_version, new_version, txn_ids);
}

inline std::shared_ptr<TabletMetadataPB> generate_simple_tablet_metadata(KeysType keys_type) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);
    //
    //  | column | type | KEY | NULL |
    //  +--------+------+-----+------+
    //  |   c0   |  INT | YES |  NO  |
    //  |   c1   |  INT | NO  |  NO  |
    auto schema = metadata->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
    }
    return metadata;
}

inline std::shared_ptr<RuntimeState> create_runtime_state() {
    TQueryOptions query_options;
    return create_runtime_state(query_options);
}

inline std::shared_ptr<RuntimeState> create_runtime_state(const TQueryOptions& query_options) {
    TUniqueId fragment_id;
    TQueryGlobals query_globals;
    std::shared_ptr<RuntimeState> runtime_state =
            std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, ExecEnv::GetInstance());
    TUniqueId id;
    runtime_state->init_mem_trackers(id);
    return runtime_state;
}

inline DescriptorTbl* create_table_desc(RuntimeState* runtime_state, const std::vector<TypeDescriptor>& types) {
    /// Init DescriptorTable
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (auto& t : types) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(runtime_state, runtime_state->obj_pool(), desc_tbl_builder.desc_tbl(), &tbl,
                                config::vector_chunk_size)
                  .ok());

    runtime_state->set_desc_tbl(tbl);
    return tbl;
}

inline std::shared_ptr<TPlanNode> create_tplan_node_cloud() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};

    auto tnode = std::make_shared<TPlanNode>();
    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::LAKE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::LAKE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

inline std::vector<TScanRangeParams> create_scan_ranges_cloud(std::vector<TabletMetadata*>& tablet_metas) {
    std::vector<TScanRangeParams> scan_ranges;

    for (auto tablet_meta : tablet_metas) {
        TInternalScanRange internal_scan_range;
        internal_scan_range.__set_tablet_id(tablet_meta->id());
        internal_scan_range.__set_version(std::to_string(tablet_meta->version()));

        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        scan_ranges.push_back(param);
    }

    return scan_ranges;
}

} // namespace starrocks::lake
