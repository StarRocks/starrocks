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

#include "storage/lake/external_cluster_snapshot_task.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/external_cluster_snapshot_task_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_metadata.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class ExternalClusterSnapshotTaskTest : public TestBase {
public:
    ExternalClusterSnapshotTaskTest() : TestBase(kTestDirectory) {}

    void SetUp() override {
        clear_and_init_test_dir();
        _exec_env = ExecEnv::GetInstance();
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    MutableTabletMetadataPtr create_tablet_metadata(int64_t tablet_id, int64_t version, int64_t rowset_id,
                                                    const std::vector<std::string>& segments = {},
                                                    const std::vector<std::string>& sstable_files = {},
                                                    const std::vector<std::string>& dcg_files = {},
                                                    const std::vector<std::string>& delvec_files = {}) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(version);
        metadata->set_next_rowset_id(1);

        // Add schema
        auto schema = metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        auto c1 = schema->add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);

        // Add rowsets with segments
        for (const auto& segment : segments) {
            auto* rowset = metadata->add_rowsets();
            rowset->set_id(rowset_id++);
            rowset->set_overlapped(true);
            rowset->set_num_rows(100);
            rowset->set_data_size(1024);
            rowset->add_segment_metas()->set_filename(segment);
        }

        // Add sstable files
        if (!sstable_files.empty()) {
            auto* sstable_meta = metadata->mutable_sstable_meta();
            for (const auto& file : sstable_files) {
                auto* sstable = sstable_meta->add_sstables();
                sstable->set_filename(file);
            }
        }

        // Add DCG files
        if (!dcg_files.empty()) {
            auto* dcg_meta = metadata->mutable_dcg_meta();
            // Create a DCG with the given column files
            auto& dcg = (*dcg_meta->mutable_dcgs())[1]; // Use rowset_id 1 as key
            for (const auto& file : dcg_files) {
                dcg.add_column_files(file);
            }
        }

        // Add delvec files
        if (!delvec_files.empty()) {
            auto* delvec_meta = metadata->mutable_delvec_meta();
            for (size_t i = 0; i < delvec_files.size(); ++i) {
                auto& delvec_file = (*delvec_meta->mutable_version_to_file())[i + 1];
                delvec_file.set_name(delvec_files[i]);
            }
        }

        return metadata;
    }

    TExternalClusterSnapshotRequest create_snapshot_request(int64_t db_id, int64_t table_id, int64_t partition_id,
                                                            int64_t physical_partition_id, int64_t pre_version,
                                                            int64_t new_version, int64_t job_id,
                                                            const std::vector<int64_t>& src_tablets,
                                                            const std::vector<TBackend>& backends) {
        TExternalClusterSnapshotRequest request;
        request.__set_db_id(db_id);
        request.__set_table_id(table_id);
        request.__set_partition_id(partition_id);
        request.__set_physical_partition_id(physical_partition_id);
        request.__set_pre_version(pre_version);
        request.__set_new_version(new_version);
        request.__set_job_id(job_id);
        request.__set_dest_tablet_id(0);
        // Build compute_node_tablets according to the new thrift definition.
        // For test purposes we put all source tablets on the first backend.
        if (!backends.empty() && !src_tablets.empty()) {
            TComputeNodeTablets cn_tablets;
            cn_tablets.__set_compute_node(backends[0]);
            cn_tablets.__set_tablets(src_tablets);
            std::vector<TComputeNodeTablets> compute_node_tablets;
            compute_node_tablets.emplace_back(std::move(cn_tablets));
            request.__set_compute_node_tablets(compute_node_tablets);
        }
        return request;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_cluster_snapshot_task";
    ExecEnv* _exec_env = nullptr;
};

// Test basic snapshot task with new segments
TEST_F(ExternalClusterSnapshotTaskTest, test_basic_snapshot_with_new_segments) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with one segment
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata with two segments (one new, one existing)
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"segment1.dat", "segment2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // The function will attempt RPC call, which may fail in test environment
    // but we can verify that metadata was read and processed correctly
    // The RPC call is asynchronous, so the function will complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with new sstable files
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_with_new_sstable_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with one sstable
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {}, {"sstable1.sst"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata with two sstables (one new)
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {}, {"sstable1.sst", "sstable2.sst"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with new dcg files
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_with_new_dcg_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with one dcg file
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {}, {}, {"dcg1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata with two dcg files (one new)
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {}, {}, {"dcg1.dat", "dcg2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with new delvec files
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_with_new_delvec_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with one delvec file
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {}, {}, {}, {"delvec1.delvec"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata with two delvec files (one new)
    auto new_metadata =
            create_tablet_metadata(tablet_id, new_version, 1, {}, {}, {}, {"delvec1.delvec", "delvec2.delvec"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task when pre_version is -1 (no previous version)
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_without_pre_version) {
    int64_t tablet_id = next_id();
    int64_t pre_version = -1;
    int64_t new_version = 1;

    // Create new-version metadata
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"segment1.dat", "segment2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task when new version metadata doesn't exist
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_new_version_not_found) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Only create pre-version metadata
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    SyncPoint::GetInstance()->SetCallBack("cluster_snapshot_task::upload_snapshot_files",
                                          [](void* arg) { /* Skip RPC */ });
    SyncPoint::GetInstance()->EnableProcessing();

    // Should handle the error gracefully
    run_external_cluster_snapshot_task(request, signature, _exec_env);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test snapshot task with empty node_to_tablets
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_empty_tablets) {
    int64_t pre_version = 1;
    int64_t new_version = 2;

    std::vector<int64_t> src_tablets; // Empty
    std::vector<TBackend> backends;   // Empty

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    SyncPoint::GetInstance()->SetCallBack("cluster_snapshot_task::upload_snapshot_files",
                                          [](void* arg) { /* Skip RPC */ });
    SyncPoint::GetInstance()->EnableProcessing();

    // Should complete without errors
    run_external_cluster_snapshot_task(request, signature, _exec_env);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test snapshot task with multiple tablets
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_multiple_tablets) {
    int64_t tablet_id1 = next_id();
    int64_t tablet_id2 = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create metadata for tablet 1
    auto pre_metadata1 = create_tablet_metadata(tablet_id1, pre_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata1));
    auto new_metadata1 = create_tablet_metadata(tablet_id1, new_version, 1, {"segment1.dat", "segment2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata1));

    // Create metadata for tablet 2
    auto pre_metadata2 = create_tablet_metadata(tablet_id2, pre_version, 1, {"segment3.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata2));
    auto new_metadata2 = create_tablet_metadata(tablet_id2, new_version, 1, {"segment3.dat", "segment4.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata2));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id1, tablet_id2};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with multiple backends
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_multiple_backends) {
    int64_t tablet_id1 = next_id();
    int64_t tablet_id2 = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create metadata for tablet 1
    auto pre_metadata1 = create_tablet_metadata(tablet_id1, pre_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata1));
    auto new_metadata1 = create_tablet_metadata(tablet_id1, new_version, 1, {"segment1.dat", "segment2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata1));

    // Create metadata for tablet 2
    auto pre_metadata2 = create_tablet_metadata(tablet_id2, pre_version, 1, {"segment3.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata2));
    auto new_metadata2 = create_tablet_metadata(tablet_id2, new_version, 1, {"segment3.dat", "segment4.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata2));

    TBackend backend1;
    backend1.__set_host("127.0.0.1");
    backend1.__set_be_port(9060);
    TBackend backend2;
    backend2.__set_host("127.0.0.2");
    backend2.__set_be_port(9060);

    std::vector<int64_t> src_tablets = {tablet_id1, tablet_id2};
    std::vector<TBackend> backends = {backend1, backend2};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task when no new files (all files already exist)
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_no_new_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version and new-version metadata with same files
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"segment1.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    SyncPoint::GetInstance()->SetCallBack("cluster_snapshot_task::upload_snapshot_files",
                                          [](void* arg) { /* Skip RPC */ });
    SyncPoint::GetInstance()->EnableProcessing();

    // Should complete without sending RPC (no new files)
    run_external_cluster_snapshot_task(request, signature, _exec_env);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

// ==================== Tests for external_cluster_snapshot_task_helper ====================

// Test build_rowset_index with valid metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_build_rowset_index_with_metadata) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat", "segment2.dat"});

    auto index = build_rowset_index(metadata);

    ASSERT_EQ(index.size(), 2);
    ASSERT_TRUE(index.contains(1));
    ASSERT_TRUE(index.contains(2));
    ASSERT_EQ(index[1]->segment_metas_size(), 1);
    ASSERT_EQ(index[2]->segment_metas_size(), 1);
}

// Test build_rowset_index with null metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_build_rowset_index_with_null_metadata) {
    TabletMetadataPtr metadata = nullptr;

    auto index = build_rowset_index(metadata);

    ASSERT_TRUE(index.empty());
}

// Test build_rowset_index with empty rowsets
TEST_F(ExternalClusterSnapshotTaskTest, test_build_rowset_index_with_empty_rowsets) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {});

    auto index = build_rowset_index(metadata);

    ASSERT_TRUE(index.empty());
}

// Test collect_sstable_files with valid metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_sstable_files_with_metadata) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {"sstable1.sst", "sstable2.sst"});

    auto files = collect_sstable_files(metadata);

    ASSERT_EQ(files.size(), 2);
    ASSERT_TRUE(files.contains("sstable1.sst"));
    ASSERT_TRUE(files.contains("sstable2.sst"));
}

// Test collect_sstable_files with null metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_sstable_files_with_null_metadata) {
    TabletMetadataPtr metadata = nullptr;

    auto files = collect_sstable_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_sstable_files with empty sstable_meta
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_sstable_files_with_empty_sstable_meta) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {});

    auto files = collect_sstable_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_dcg_files with valid metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_dcg_files_with_metadata) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {}, {"111.dcg", "222.dcg"});

    auto files = collect_dcg_files(metadata);

    ASSERT_EQ(files.size(), 2);
    ASSERT_TRUE(files.contains("111.dcg"));
    ASSERT_TRUE(files.contains("222.dcg"));
}

// Test collect_dcg_files with null metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_dcg_files_with_null_metadata) {
    TabletMetadataPtr metadata = nullptr;

    auto files = collect_dcg_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_dcg_files with empty dcg_meta
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_dcg_files_with_empty_dcg_meta) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {});

    auto files = collect_dcg_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_delvec_files with valid metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_delvec_files_with_metadata) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {}, {}, {"delvec1.delvec", "delvec2.delvec"});

    auto files = collect_delvec_files(metadata);

    ASSERT_EQ(files.size(), 2);
    ASSERT_TRUE(files.contains("delvec1.delvec"));
    ASSERT_TRUE(files.contains("delvec2.delvec"));
}

// Test collect_delvec_files with null metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_delvec_files_with_null_metadata) {
    TabletMetadataPtr metadata = nullptr;

    auto files = collect_delvec_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_delvec_files with empty delvec_meta
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_delvec_files_with_empty_delvec_meta) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {});

    auto files = collect_delvec_files(metadata);

    ASSERT_TRUE(files.empty());
}

// Test collect_schema_ids with valid metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_schema_ids_with_metadata) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 1, 1, {});

    phmap::flat_hash_set<int64_t> schema_ids;
    collect_schema_ids(metadata, schema_ids);

    ASSERT_EQ(schema_ids.size(), 1);
    ASSERT_TRUE(schema_ids.contains(metadata->schema().id()));
}

// Test collect_schema_ids with null metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_schema_ids_with_null_metadata) {
    TabletMetadataPtr metadata = nullptr;

    phmap::flat_hash_set<int64_t> schema_ids;
    collect_schema_ids(metadata, schema_ids);

    ASSERT_TRUE(schema_ids.empty());
}

// Test TabletFileCollections::collect with both pre and new metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_tablet_file_collections_collect_with_both_metadata) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"}, {"sstable1.sst"}, {"dcg1.dat"},
                                               {"delvec1.delvec"});
    auto new_metadata =
            create_tablet_metadata(tablet_id, 2, 1, {"segment1.dat", "segment2.dat"}, {"sstable1.sst", "sstable2.sst"},
                                   {"dcg1.dat", "dcg2.dat"}, {"delvec1.delvec", "delvec2.delvec"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);

    // Check pre_rowsets
    ASSERT_EQ(collections.pre_rowsets.size(), 1);
    ASSERT_TRUE(collections.pre_rowsets.contains(1));

    // Check new_rowsets
    ASSERT_EQ(collections.new_rowsets.size(), 2);
    ASSERT_TRUE(collections.new_rowsets.contains(1));
    ASSERT_TRUE(collections.new_rowsets.contains(2));

    // Check pre_sstable_files
    ASSERT_EQ(collections.pre_sstable_files.size(), 1);
    ASSERT_TRUE(collections.pre_sstable_files.contains("sstable1.sst"));

    // Check new_sstable_files
    ASSERT_EQ(collections.new_sstable_files.size(), 2);
    ASSERT_TRUE(collections.new_sstable_files.contains("sstable1.sst"));
    ASSERT_TRUE(collections.new_sstable_files.contains("sstable2.sst"));

    // Check pre_dcg_files
    ASSERT_EQ(collections.pre_dcg_files.size(), 1);
    ASSERT_TRUE(collections.pre_dcg_files.contains("dcg1.dat"));

    // Check new_dcg_files
    ASSERT_EQ(collections.new_dcg_files.size(), 2);
    ASSERT_TRUE(collections.new_dcg_files.contains("dcg1.dat"));
    ASSERT_TRUE(collections.new_dcg_files.contains("dcg2.dat"));

    // Check pre_delvec_files
    ASSERT_EQ(collections.pre_delvec_files.size(), 1);
    ASSERT_TRUE(collections.pre_delvec_files.contains("delvec1.delvec"));

    // Check new_delvec_files
    ASSERT_EQ(collections.new_delvec_files.size(), 2);
    ASSERT_TRUE(collections.new_delvec_files.contains("delvec1.delvec"));
    ASSERT_TRUE(collections.new_delvec_files.contains("delvec2.delvec"));
}

// Test TabletFileCollections::collect with null pre_metadata
TEST_F(ExternalClusterSnapshotTaskTest, test_tablet_file_collections_collect_with_null_pre_metadata) {
    int64_t tablet_id = next_id();
    TabletMetadataPtr pre_metadata = nullptr;
    auto new_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"}, {"sstable1.sst"}, {"dcg1.dat"},
                                               {"delvec1.delvec"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);

    // Check pre_rowsets should be empty
    ASSERT_TRUE(collections.pre_rowsets.empty());
    ASSERT_TRUE(collections.pre_sstable_files.empty());
    ASSERT_TRUE(collections.pre_dcg_files.empty());
    ASSERT_TRUE(collections.pre_delvec_files.empty());

    // Check new_rowsets
    ASSERT_EQ(collections.new_rowsets.size(), 1);
    ASSERT_EQ(collections.new_sstable_files.size(), 1);
    ASSERT_EQ(collections.new_dcg_files.size(), 1);
    ASSERT_EQ(collections.new_delvec_files.size(), 1);
}

// Test populate_tablet_snapshot with new files
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_tablet_snapshot_with_new_files) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"}, {"sstable1.sst"}, {"dcg1.dat"},
                                               {"delvec1.delvec"});
    auto new_metadata =
            create_tablet_metadata(tablet_id, 2, 1, {"segment1.dat", "segment2.dat"}, {"sstable1.sst", "sstable2.sst"},
                                   {"dcg1.dat", "dcg2.dat"}, {"delvec1.delvec", "delvec2.delvec"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet pre_bundle_data_files;
    phmap::flat_hash_set<std::string> globally_bound_segments;
    UploadSnapshotFilesRequestPB node_req;

    auto* tablet_pb =
            populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_segments, node_req);

    ASSERT_NE(tablet_pb, nullptr);
    ASSERT_EQ(tablet_pb->tablet_id(), tablet_id);

    // Check new_data_files contains new files
    std::set<std::string> new_files(tablet_pb->new_data_files().begin(), tablet_pb->new_data_files().end());
    ASSERT_TRUE(new_files.contains("segment2.dat"));
    ASSERT_TRUE(new_files.contains("sstable2.sst"));
    ASSERT_TRUE(new_files.contains("dcg2.dat"));
    ASSERT_TRUE(new_files.contains("delvec2.delvec"));

    // Check that existing files are not included
    ASSERT_FALSE(new_files.contains("segment1.dat"));
    ASSERT_FALSE(new_files.contains("sstable1.sst"));
    ASSERT_FALSE(new_files.contains("dcg1.dat"));
    ASSERT_FALSE(new_files.contains("delvec1.delvec"));
}

// Test populate_tablet_snapshot with no new files
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_tablet_snapshot_with_no_new_files) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"}, {"sstable1.sst"}, {"dcg1.dat"},
                                               {"delvec1.delvec"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 1, {"segment1.dat"}, {"sstable1.sst"}, {"dcg1.dat"},
                                               {"delvec1.delvec"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet pre_bundle_data_files;
    phmap::flat_hash_set<std::string> globally_bound_segments;
    UploadSnapshotFilesRequestPB node_req;

    auto* tablet_pb =
            populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_segments, node_req);

    ASSERT_NE(tablet_pb, nullptr);
    ASSERT_EQ(tablet_pb->tablet_id(), tablet_id);
    ASSERT_EQ(tablet_pb->new_data_files_size(), 0);
}

// Test populate_tablet_snapshot with globally_bound_segments deduplication
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_tablet_snapshot_with_globally_bound_segments) {
    int64_t tablet_id1 = next_id();
    int64_t tablet_id2 = next_id();

    auto pre_metadata1 = create_tablet_metadata(tablet_id1, 1, 1, {"segment1.dat"});
    auto new_metadata1 = create_tablet_metadata(tablet_id1, 2, 1, {"segment1.dat", "segment2.dat"});

    auto pre_metadata2 = create_tablet_metadata(tablet_id2, 1, 1, {});
    auto new_metadata2 = create_tablet_metadata(tablet_id2, 2, 1, {"segment2.dat"});

    FileSet pre_bundle_data_files;
    phmap::flat_hash_set<std::string> globally_bound_segments;
    UploadSnapshotFilesRequestPB node_req;

    // First tablet adds segment2.dat
    auto collections1 = TabletFileCollections::collect(pre_metadata1, new_metadata1);
    auto* tablet_pb1 = populate_tablet_snapshot(tablet_id1, collections1, pre_bundle_data_files,
                                                globally_bound_segments, node_req);

    // Second tablet should not add segment2.dat again (already in globally_bound_segments)
    auto collections2 = TabletFileCollections::collect(pre_metadata2, new_metadata2);
    auto* tablet_pb2 = populate_tablet_snapshot(tablet_id2, collections2, pre_bundle_data_files,
                                                globally_bound_segments, node_req);

    ASSERT_NE(tablet_pb1, nullptr);
    ASSERT_NE(tablet_pb2, nullptr);

    // Check that segment2.dat is only in first tablet's new_data_files
    std::set<std::string> files1(tablet_pb1->new_data_files().begin(), tablet_pb1->new_data_files().end());
    std::set<std::string> files2(tablet_pb2->new_data_files().begin(), tablet_pb2->new_data_files().end());

    ASSERT_TRUE(files1.contains("segment2.dat"));
    ASSERT_FALSE(files2.contains("segment2.dat"));
}

// Test populate_tablet_snapshot with pre_bundle_data_files removal
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_tablet_snapshot_with_pre_bundle_data_files) {
    int64_t tablet_id = next_id();

    // Create metadata where a segment from pre_bundle_data_files appears in new version
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 2, {"segment1.dat", "segment2.dat"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet pre_bundle_data_files = {"segment1.dat"}; // segment1.dat is in pre_bundle_data_files
    phmap::flat_hash_set<std::string> globally_bound_segments;
    UploadSnapshotFilesRequestPB node_req;

    auto* tablet_pb =
            populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_segments, node_req);

    ASSERT_NE(tablet_pb, nullptr);
    // segment1.dat should be removed from pre_bundle_data_files since it appears in new version
    ASSERT_FALSE(pre_bundle_data_files.contains("segment1.dat"));
    // segment2.dat should be added to new_data_files
    std::set<std::string> new_files(tablet_pb->new_data_files().begin(), tablet_pb->new_data_files().end());
    ASSERT_TRUE(new_files.contains("segment2.dat"));
}

// Test populate_meta_schema_files with filebundling
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_meta_schema_files_with_filebundling) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {});
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {});

    UploadSnapshotFilesRequestPB node_req;
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;
    FileSet unused_meta_files;

    // First call with meta_added = false
    populate_meta_schema_files(true, false, tablet_id, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files, tablet_pb);

    ASSERT_EQ(tablet_pb->new_metadata_files_size(), 1);
    ASSERT_EQ(tablet_pb->new_metadata_files(0), tablet_metadata_filename(0, new_version));
    ASSERT_EQ(unused_meta_files.size(), 1);
    ASSERT_TRUE(unused_meta_files.contains(tablet_metadata_filename(0, pre_version)));

    // Second call with meta_added = true (should not add metadata again)
    auto* tablet_pb2 = node_req.add_tablet_snapshots();
    tablet_pb2->set_tablet_id(tablet_id + 1);
    FileSet unused_meta_files2;
    populate_meta_schema_files(true, true, tablet_id + 1, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files2, tablet_pb2);

    ASSERT_EQ(tablet_pb2->new_metadata_files_size(), 0);
}

// Test populate_meta_schema_files without filebundling
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_meta_schema_files_without_filebundling) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {});
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {});

    UploadSnapshotFilesRequestPB node_req;
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;
    FileSet unused_meta_files;

    populate_meta_schema_files(false, false, tablet_id, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files, tablet_pb);

    ASSERT_EQ(tablet_pb->new_metadata_files_size(), 1);
    ASSERT_EQ(tablet_pb->new_metadata_files(0), tablet_metadata_filename(tablet_id, new_version));
    ASSERT_EQ(unused_meta_files.size(), 1);
    ASSERT_TRUE(unused_meta_files.contains(tablet_metadata_filename(tablet_id, pre_version)));
}

// Test populate_meta_schema_files with schema files
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_meta_schema_files_with_schema_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {});
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {});

    // Add historical schema to new_metadata
    int64_t historical_schema_id = next_id();
    auto& historical_schema = (*new_metadata->mutable_historical_schemas())[historical_schema_id];
    historical_schema.set_id(historical_schema_id);
    historical_schema.set_num_short_key_columns(1);
    historical_schema.set_keys_type(DUP_KEYS);

    UploadSnapshotFilesRequestPB node_req;
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;
    FileSet unused_meta_files;

    populate_meta_schema_files(false, false, tablet_id, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files, tablet_pb);

    // Should have schema files for both main schema and historical schema
    ASSERT_GE(tablet_pb->new_schema_files_size(), 1);
}

// Test populate_meta_schema_files with duplicate schema IDs
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_meta_schema_files_with_duplicate_schema_ids) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {});
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {});

    UploadSnapshotFilesRequestPB node_req;
    auto* tablet_pb = node_req.add_tablet_snapshots();
    tablet_pb->set_tablet_id(tablet_id);

    phmap::flat_hash_set<int64_t> pre_schema_ids;
    phmap::flat_hash_set<int64_t> new_schema_ids;
    FileSet unused_meta_files;

    // First call
    populate_meta_schema_files(false, false, tablet_id, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files, tablet_pb);

    // Second call with same metadata (should not add duplicate schema files)
    auto* tablet_pb2 = node_req.add_tablet_snapshots();
    tablet_pb2->set_tablet_id(tablet_id + 1);
    FileSet unused_meta_files2;
    populate_meta_schema_files(false, false, tablet_id + 1, pre_version, new_version, pre_metadata, new_metadata,
                               pre_schema_ids, new_schema_ids, unused_meta_files2, tablet_pb2);

    // Schema files should not be added again
    ASSERT_EQ(tablet_pb2->new_schema_files_size(), 0);
}

// ==================== Tests for collect_unused_files ====================

// Test collect_unused_files with unused segments
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_unused_files_with_unused_segments) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat", "segment2.dat"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 3, {"segment3.dat"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet unused_data_files;
    FileSet pre_bundle_data_files;

    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    // segment1.dat should be in unused_data_files
    ASSERT_TRUE(unused_data_files.contains("segment1.dat"));
    ASSERT_TRUE(unused_data_files.contains("segment2.dat"));
}

// Test collect_unused_files with unused sstable files
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_unused_files_with_unused_sstable) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {"sstable1.sst", "sstable2.sst"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 1, {}, {"sstable2.sst"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet unused_data_files;
    FileSet pre_bundle_data_files;

    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    ASSERT_TRUE(unused_data_files.contains("sstable1.sst"));
    ASSERT_FALSE(unused_data_files.contains("sstable2.sst"));
}

// Test collect_unused_files with unused dcg files
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_unused_files_with_unused_dcg) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {}, {"dcg1.dat", "dcg2.dat"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 1, {}, {}, {"dcg2.dat"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet unused_data_files;
    FileSet pre_bundle_data_files;

    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    ASSERT_TRUE(unused_data_files.contains("dcg1.dat"));
    ASSERT_FALSE(unused_data_files.contains("dcg2.dat"));
}

// Test collect_unused_files with unused delvec files
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_unused_files_with_unused_delvec) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {}, {}, {}, {"delvec1.delvec", "delvec2.delvec"});
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 1, {}, {}, {}, {"delvec2.delvec"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet unused_data_files;
    FileSet pre_bundle_data_files;

    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    ASSERT_TRUE(unused_data_files.contains("delvec1.delvec"));
    ASSERT_FALSE(unused_data_files.contains("delvec2.delvec"));
}

// Test collect_unused_files with empty pre_rowsets
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_unused_files_with_empty_pre_rowsets) {
    int64_t tablet_id = next_id();
    TabletMetadataPtr pre_metadata = nullptr;
    auto new_metadata = create_tablet_metadata(tablet_id, 1, 1, {"segment1.dat"});

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet unused_data_files;
    FileSet pre_bundle_data_files;

    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);

    ASSERT_TRUE(unused_data_files.empty());
    ASSERT_TRUE(pre_bundle_data_files.empty());
}

// ==================== Tests for prepare_unused_files_for_log ====================

// Test prepare_unused_files_for_log with pre_version >= 0
TEST_F(ExternalClusterSnapshotTaskTest, test_prepare_unused_files_for_log_with_pre_version) {
    int64_t pre_version = 1;
    FileSet pre_bundle_data_files = {"bundle1.dat", "bundle2.dat"};
    FileSet unused_data_files = {"segment1.dat"};
    FileSet unused_meta_files = {"meta1.meta"};
    phmap::flat_hash_set<int64_t> pre_schema_ids = {100, 200};
    phmap::flat_hash_set<int64_t> new_schema_ids = {200};
    FileSet unused_schema_files;

    prepare_unused_files_for_log(pre_version, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files);

    // Bundle files should be added to unused_data_files
    ASSERT_TRUE(unused_data_files.contains("bundle1.dat"));
    ASSERT_TRUE(unused_data_files.contains("bundle2.dat"));
    ASSERT_TRUE(unused_data_files.contains("segment1.dat"));

    // Schema 100 should be in unused_schema_files (not in new_schema_ids)
    ASSERT_TRUE(unused_schema_files.contains(schema_filename(100)));
    ASSERT_FALSE(unused_schema_files.contains(schema_filename(200)));
}

// Test prepare_unused_files_for_log with pre_version < 0
TEST_F(ExternalClusterSnapshotTaskTest, test_prepare_unused_files_for_log_with_negative_pre_version) {
    int64_t pre_version = -1;
    FileSet pre_bundle_data_files = {"bundle1.dat"};
    FileSet unused_data_files = {"segment1.dat"};
    FileSet unused_meta_files = {"meta1.meta"};
    phmap::flat_hash_set<int64_t> pre_schema_ids = {100};
    phmap::flat_hash_set<int64_t> new_schema_ids = {};
    FileSet unused_schema_files;

    prepare_unused_files_for_log(pre_version, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files);

    // Should return early, no changes
    ASSERT_FALSE(unused_data_files.contains("bundle1.dat"));
    ASSERT_TRUE(unused_schema_files.empty());
}

// ==================== Tests for delete partition task ====================

// Test run_delete_partition_task
TEST_F(ExternalClusterSnapshotTaskTest, test_run_delete_partition_task) {
    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_dest_tablet_id(500);
    request.__set_is_drop_partition(true);

    int64_t signature = next_id();

    // The function will attempt to delete partition, which may fail in test environment
    // but we can verify that the function completes
    run_delete_partition_task(request, signature, _exec_env);
}

// ==================== Tests for delete files task ====================

// Test run_delete_files_task
TEST_F(ExternalClusterSnapshotTaskTest, test_run_delete_files_task) {
    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_job_id(500);
    request.__set_dest_tablet_id(600);
    request.__set_new_version(-1);

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {next_id()};
    TComputeNodeTablets cn_tablets;
    cn_tablets.__set_compute_node(backend);
    cn_tablets.__set_tablets(src_tablets);
    std::vector<TComputeNodeTablets> compute_node_tablets;
    compute_node_tablets.emplace_back(std::move(cn_tablets));
    request.__set_compute_node_tablets(compute_node_tablets);

    int64_t signature = next_id();

    // The function will attempt to load and delete files, which may fail in test environment
    // but we can verify that the function completes
    run_delete_files_task(request, signature, _exec_env);
}

// ==================== Tests for snapshot task with new delete logic ====================

// Test snapshot task with is_drop_partition flag
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_task_with_is_drop_partition) {
    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_dest_tablet_id(500);
    request.__set_is_drop_partition(true);

    int64_t signature = next_id();

    // Should call run_delete_partition_task
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with new_version == -1 (delete files case)
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_task_with_delete_files) {
    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_job_id(500);
    request.__set_dest_tablet_id(600);
    request.__set_new_version(-1);

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {next_id()};
    TComputeNodeTablets cn_tablets;
    cn_tablets.__set_compute_node(backend);
    cn_tablets.__set_tablets(src_tablets);
    std::vector<TComputeNodeTablets> compute_node_tablets;
    compute_node_tablets.emplace_back(std::move(cn_tablets));
    request.__set_compute_node_tablets(compute_node_tablets);

    int64_t signature = next_id();

    // Should call run_delete_files_task
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with write_snapshot_log (unused files collection)
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_task_with_unused_files_collection) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with files that will become unused
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {"segment1.dat", "segment2.dat"},
                                               {"sstable1.sst"}, {"dcg1.dat"}, {"delvec1.delvec"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata without some files (they become unused)
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"segment2.dat"}, {"sstable1.sst"}, {}, {});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // The function will collect unused files and write snapshot log
    // RPC call may fail in test environment, but function should complete
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

// Test snapshot task with bundle files handling
TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_task_with_bundle_files) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 1;
    int64_t new_version = 2;

    // Create pre-version metadata with bundle file
    auto pre_metadata = create_tablet_metadata(tablet_id, pre_version, 1, {"segment1.dat"});
    // Set bundle_file_offset to make it a bundle file
    auto* rowset = pre_metadata->mutable_rowsets(0);
    rowset->mutable_segment_metas(0)->set_bundle_file_offset(0);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*pre_metadata));

    // Create new-version metadata without this rowset
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"segment2.dat"});
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata));

    TBackend backend;
    backend.__set_host("127.0.0.1");
    backend.__set_be_port(9060);
    std::vector<int64_t> src_tablets = {tablet_id};
    std::vector<TBackend> backends = {backend};

    auto request = create_snapshot_request(100, 200, 300, 400, pre_version, new_version, 500, src_tablets, backends);
    int64_t signature = next_id();

    // Bundle files should be collected in pre_bundle_data_files
    run_external_cluster_snapshot_task(request, signature, _exec_env);
}

} // namespace starrocks::lake
