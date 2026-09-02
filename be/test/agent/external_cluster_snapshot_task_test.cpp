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

#include "agent/external_cluster_snapshot_task.h"

#include <gtest/gtest.h>

#include "agent/external_cluster_snapshot_task_helper.h"
#include "agent/external_cluster_snapshot_task_rpc.h"
#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "exec/exec_env.h"
#include "fs/fs_memory.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/lake_service.pb.h"
#include "storage/lake/filenames.h"
#include "storage/lake/snapshot_file_syncer.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/test_util.h"
#include "storage/protobuf_file.h"
#include "storage/storage_env.h"

namespace starrocks::lake {

using namespace starrocks;

class SnapshotDeleteFileSystem final : public MemoryFileSystem {
public:
    void set_delete_dir_status(Status status) { _delete_dir_status = std::move(status); }
    void set_delete_files_status(Status status) { _delete_files_status = std::move(status); }

    Status delete_dir_recursive(const std::string& dirname) override {
        _deleted_dir = dirname;
        return _delete_dir_status;
    }

    Status delete_files(std::span<const std::string> paths) override {
        _deleted_files.assign(paths.begin(), paths.end());
        return _delete_files_status;
    }

    const std::string& deleted_dir() const { return _deleted_dir; }
    const std::vector<std::string>& deleted_files() const { return _deleted_files; }

private:
    Status _delete_dir_status = Status::OK();
    Status _delete_files_status = Status::OK();
    std::string _deleted_dir;
    std::vector<std::string> _deleted_files;
};

class DeleteLogFileSystem final : public MemoryFileSystem {
public:
    void set_delete_file_status(Status status) { _delete_file_status = std::move(status); }

    Status delete_file(const std::string& path) override {
        if (!_delete_file_status.ok()) {
            return _delete_file_status;
        }
        return MemoryFileSystem::delete_file(path);
    }

private:
    Status _delete_file_status = Status::OK();
};

class ExternalClusterSnapshotTaskTest : public TestBase {
public:
    ExternalClusterSnapshotTaskTest() : TestBase(kTestDirectory) {}

    void SetUp() override {
        clear_and_init_test_dir();
        _exec_env = ExecEnv::GetInstance();
        _snapshot_delete_fs = std::make_shared<SnapshotDeleteFileSystem>();
        _snapshot_delete_fs->set_delete_dir_status(Status::NotFound("already deleted"));
        _snapshot_delete_fs->set_delete_files_status(Status::NotFound("already deleted"));
        SyncPoint::GetInstance()->SetCallBack("SnapshotFileSyncer::file_system", [&](void* arg) {
            auto* fs_or = reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg);
            *fs_or = _snapshot_delete_fs;
        });
        SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::tablet_manager", [&](void* arg) {
            *reinterpret_cast<TabletManager**>(arg) = _tablet_mgr.get();
        });
        SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::skip",
                                              [](void* arg) { *reinterpret_cast<bool*>(arg) = true; });
        SyncPoint::GetInstance()->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        remove_test_dir_ignore_error();
    }

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
        request.__set_dest_tablet_id(9000);
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
    std::shared_ptr<SnapshotDeleteFileSystem> _snapshot_delete_fs;
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
                                          [](void* arg) { *reinterpret_cast<bool*>(arg) = true; });
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
                                          [](void* arg) { *reinterpret_cast<bool*>(arg) = true; });
    SyncPoint::GetInstance()->EnableProcessing();

    // Should complete without errors
    run_external_cluster_snapshot_task(request, signature, _exec_env);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_empty_tablets_uses_default_tablet_manager) {
    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_pre_version(-1);
    request.__set_new_version(1);

    TStatusCode::type reported_status = TStatusCode::RUNTIME_ERROR;
    SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::input", [&](void* arg) {
        auto* finish_request = reinterpret_cast<TFinishTaskRequest*>(arg);
        reported_status = finish_request->task_status.status_code;
    });
    run_external_cluster_snapshot_task(request, next_id(), _exec_env);
    SyncPoint::GetInstance()->ClearCallBack("FinishAgentTask::input");
    ASSERT_EQ(TStatusCode::OK, reported_status);
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
                                          [](void* arg) { *reinterpret_cast<bool*>(arg) = true; });
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
    FileSet partition_live_files;

    prepare_unused_files_for_log(pre_version, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files, partition_live_files);

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
    FileSet partition_live_files;

    prepare_unused_files_for_log(pre_version, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files, partition_live_files);

    // Should return early, no changes
    ASSERT_FALSE(unused_data_files.contains("bundle1.dat"));
    ASSERT_TRUE(unused_schema_files.empty());
}

TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_log_tablet_id_is_stable_across_cn_regrouping) {
    TExternalClusterSnapshotRequest request;
    TBackend backend1;
    backend1.__set_host("127.0.0.1");
    backend1.__set_be_port(9060);
    TBackend backend2;
    backend2.__set_host("127.0.0.2");
    backend2.__set_be_port(9060);

    TComputeNodeTablets group1;
    group1.__set_compute_node(backend1);
    group1.__set_tablets({9, 3});
    TComputeNodeTablets group2;
    group2.__set_compute_node(backend2);
    group2.__set_tablets({7, 5});
    request.__set_compute_node_tablets({group1, group2});
    ASSERT_EQ(3, get_snapshot_log_tablet_id(request).value());
    ASSERT_EQ((std::vector<int64_t>{3, 5, 7, 9}), get_snapshot_log_tablet_ids(request));

    group1.__set_tablets({7, 9});
    group2.__set_tablets({5, 3});
    request.__set_compute_node_tablets({group2, group1});
    ASSERT_EQ(3, get_snapshot_log_tablet_id(request).value());

    request.__set_dest_tablet_id(11);
    ASSERT_EQ(11, get_snapshot_log_tablet_id(request).value());
    ASSERT_EQ((std::vector<int64_t>{11, 3, 5, 7, 9}), get_snapshot_log_tablet_ids(request));

    request.__isset.dest_tablet_id = false;
    request.__set_compute_node_tablets({});
    ASSERT_FALSE(get_snapshot_log_tablet_id(request).has_value());
    ASSERT_TRUE(get_snapshot_log_tablet_ids(request).empty());
}

// ==================== Tests for delete partition task ====================

TEST_F(ExternalClusterSnapshotTaskTest, test_snapshot_file_syncer_delete_statuses) {
    auto fs = std::make_shared<SnapshotDeleteFileSystem>();
    SyncPoint::GetInstance()->SetCallBack("SnapshotFileSyncer::file_system", [&](void* arg) {
        auto* fs_or = reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg);
        *fs_or = fs;
    });

    SnapshotFileSyncer syncer;
    fs->set_delete_dir_status(Status::NotFound("already deleted"));
    ASSERT_OK(syncer.delete_partition(500, 100, 200, 300, 400));
    ASSERT_EQ("staros://500/db100/200/400", fs->deleted_dir());

    fs->set_delete_dir_status(Status::IOError("delete directory failed"));
    auto st = syncer.delete_partition(500, 100, 200, 300, 400);
    ASSERT_TRUE(st.is_io_error());

    ExternalClusterSnapshotLogPB log_pb;
    log_pb.set_db_id(100);
    log_pb.set_table_id(200);
    log_pb.set_physical_partition_id(400);
    log_pb.add_delete_data_files("segment.dat");
    log_pb.add_delete_meta_files("meta.pb");
    log_pb.add_delete_schema_files("schema.pb");

    fs->set_delete_files_status(Status::NotFound("already deleted"));
    ASSERT_OK(syncer.delete_files(600, log_pb));
    ASSERT_EQ(3, fs->deleted_files().size());

    fs->set_delete_files_status(Status::IOError("delete files failed"));
    st = syncer.delete_files(600, log_pb);
    ASSERT_TRUE(st.is_io_error());
    SyncPoint::GetInstance()->ClearCallBack("SnapshotFileSyncer::file_system");
}

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

    TStatusCode::type reported_status = TStatusCode::RUNTIME_ERROR;
    SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::input", [&](void* arg) {
        auto* finish_request = reinterpret_cast<TFinishTaskRequest*>(arg);
        reported_status = finish_request->task_status.status_code;
    });

    // No candidate root has a delete log, which means an earlier idempotent cleanup consumed it.
    run_delete_files_task(request, signature, _exec_env);
    SyncPoint::GetInstance()->ClearCallBack("FinishAgentTask::input");
    ASSERT_EQ(TStatusCode::OK, reported_status);
}

TEST_F(ExternalClusterSnapshotTaskTest, test_run_delete_files_task_error_and_success_paths) {
    auto log_fs = std::make_shared<DeleteLogFileSystem>();
    auto log_location_provider = std::make_shared<FixedLocationProvider>("/snapshot");
    auto delete_fs = std::make_shared<SnapshotDeleteFileSystem>();
    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::delete_log_location_provider", [&](void* arg) {
        auto* provider = reinterpret_cast<std::shared_ptr<LocationProvider>*>(arg);
        *provider = log_location_provider;
    });
    SyncPoint::GetInstance()->SetCallBack("SnapshotFileSyncer::file_system", [&](void* arg) {
        auto* fs_or = reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg);
        *fs_or = delete_fs;
    });

    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_dest_tablet_id(600);
    request.__set_new_version(-1);
    TComputeNodeTablets cn_tablets;
    cn_tablets.__set_tablets({500});
    request.__set_compute_node_tablets({cn_tablets});

    TStatusCode::type reported_status = TStatusCode::OK;
    std::string reported_error;
    SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::input", [&](void* arg) {
        auto* finish_request = reinterpret_cast<TFinishTaskRequest*>(arg);
        reported_status = finish_request->task_status.status_code;
        reported_error =
                finish_request->task_status.error_msgs.empty() ? "" : finish_request->task_status.error_msgs.front();
    });

    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::delete_log_file_system", [](void* arg) {
        auto* fs_or = reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg);
        *fs_or = Status::IOError("create delete-log filesystem failed");
    });
    request.__set_job_id(501);
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::RUNTIME_ERROR, reported_status);
    ASSERT_NE(std::string::npos, reported_error.find("create delete-log filesystem failed"));

    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::delete_log_file_system", [&](void* arg) {
        auto* fs_or = reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg);
        *fs_or = log_fs;
    });
    request.__set_job_id(502);
    auto log_path = log_location_provider->snapshot_log_location(request.dest_tablet_id, request.job_id,
                                                                 request.physical_partition_id);
    ASSERT_OK(log_fs->create_dir_recursive(log_location_provider->snapshot_log_root_location(request.dest_tablet_id)));
    ASSERT_OK(log_fs->create_file(log_path));
    ASSERT_OK(log_fs->append_file(log_path, Slice("\xff", 1)));
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::RUNTIME_ERROR, reported_status);
    ASSERT_NE(std::string::npos, reported_error.find("failed to parse protobuf"));

    ExternalClusterSnapshotLogPB log_pb;
    log_pb.set_db_id(request.db_id);
    log_pb.set_table_id(request.table_id);
    log_pb.set_physical_partition_id(request.physical_partition_id);
    log_pb.add_delete_data_files("segment.dat");
    log_pb.add_delete_meta_files("meta.pb");
    log_pb.add_delete_schema_files("schema.pb");

    request.__set_job_id(503);
    log_path = log_location_provider->snapshot_log_location(request.dest_tablet_id, request.job_id,
                                                            request.physical_partition_id);
    ASSERT_OK(ProtobufFile(log_path, log_fs).save(log_pb, false));
    delete_fs->set_delete_files_status(Status::IOError("delete snapshot files failed"));
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::RUNTIME_ERROR, reported_status);
    ASSERT_NE(std::string::npos, reported_error.find("delete snapshot files failed"));
    ASSERT_OK(log_fs->path_exists(log_path));

    request.__set_job_id(504);
    log_path = log_location_provider->snapshot_log_location(request.dest_tablet_id, request.job_id,
                                                            request.physical_partition_id);
    ASSERT_OK(ProtobufFile(log_path, log_fs).save(log_pb, false));
    delete_fs->set_delete_files_status(Status::OK());
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::OK, reported_status);
    ASSERT_TRUE(log_fs->path_exists(log_path).is_not_found());

    SyncPoint::GetInstance()->ClearCallBack("FinishAgentTask::input");
    SyncPoint::GetInstance()->ClearCallBack("SnapshotFileSyncer::file_system");
    SyncPoint::GetInstance()->ClearCallBack("ExternalClusterSnapshotTask::delete_log_file_system");
    SyncPoint::GetInstance()->ClearCallBack("ExternalClusterSnapshotTask::delete_log_location_provider");
}

TEST_F(ExternalClusterSnapshotTaskTest, test_delete_log_failure_is_retryable) {
    auto log_fs = std::make_shared<DeleteLogFileSystem>();
    auto log_location_provider = std::make_shared<FixedLocationProvider>("/snapshot");
    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::delete_log_location_provider", [&](void* arg) {
        *reinterpret_cast<std::shared_ptr<LocationProvider>*>(arg) = log_location_provider;
    });
    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::delete_log_file_system", [&](void* arg) {
        *reinterpret_cast<StatusOr<std::shared_ptr<FileSystem>>*>(arg) = log_fs;
    });

    TExternalClusterSnapshotRequest request;
    request.__set_db_id(100);
    request.__set_table_id(200);
    request.__set_partition_id(300);
    request.__set_physical_partition_id(400);
    request.__set_job_id(505);
    request.__set_dest_tablet_id(600);
    request.__set_new_version(-1);

    ExternalClusterSnapshotLogPB log_pb;
    log_pb.set_db_id(request.db_id);
    log_pb.set_table_id(request.table_id);
    log_pb.set_physical_partition_id(request.physical_partition_id);
    auto log_path = log_location_provider->snapshot_log_location(request.dest_tablet_id, request.job_id,
                                                                 request.physical_partition_id);
    ASSERT_OK(log_fs->create_dir_recursive(log_location_provider->snapshot_log_root_location(request.dest_tablet_id)));
    ASSERT_OK(ProtobufFile(log_path, log_fs).save(log_pb, false));

    TStatusCode::type reported_status = TStatusCode::OK;
    SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::input", [&](void* arg) {
        reported_status = reinterpret_cast<TFinishTaskRequest*>(arg)->task_status.status_code;
    });

    log_fs->set_delete_file_status(Status::IOError("delete snapshot log failed"));
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::RUNTIME_ERROR, reported_status);
    ASSERT_OK(log_fs->path_exists(log_path));

    log_fs->set_delete_file_status(Status::OK());
    run_delete_files_task(request, next_id(), _exec_env);
    ASSERT_EQ(TStatusCode::OK, reported_status);
    ASSERT_TRUE(log_fs->path_exists(log_path).is_not_found());
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

    auto log_fs = std::make_shared<MemoryFileSystem>();
    auto log_location_provider = std::make_shared<FixedLocationProvider>("/snapshot");
    ASSERT_OK(log_fs->create_dir_recursive(log_location_provider->snapshot_log_root_location(request.dest_tablet_id)));
    SyncPoint::GetInstance()->SetCallBack("cluster_snapshot_task::upload_snapshot_files",
                                          [](void* arg) { *reinterpret_cast<bool*>(arg) = true; });
    SyncPoint::GetInstance()->SetCallBack(
            "ExternalClusterSnapshotTask::snapshot_log_location_provider", [&](void* arg) {
                auto* provider = reinterpret_cast<std::shared_ptr<LocationProvider>*>(arg);
                *provider = log_location_provider;
            });
    SyncPoint::GetInstance()->SetCallBack("ExternalClusterSnapshotTask::snapshot_log_file_system", [&](void* arg) {
        *reinterpret_cast<std::shared_ptr<FileSystem>*>(arg) = log_fs;
    });
    TStatusCode::type reported_status = TStatusCode::RUNTIME_ERROR;
    SyncPoint::GetInstance()->SetCallBack("FinishAgentTask::input", [&](void* arg) {
        auto* finish_request = reinterpret_cast<TFinishTaskRequest*>(arg);
        reported_status = finish_request->task_status.status_code;
    });

    run_external_cluster_snapshot_task(request, signature, _exec_env);

    auto log_path = log_location_provider->snapshot_log_location(request.dest_tablet_id, request.job_id,
                                                                 request.physical_partition_id);
    ExternalClusterSnapshotLogPB log_pb;
    ASSERT_OK(ProtobufFile(log_path, log_fs).load(&log_pb, false));
    ASSERT_EQ(request.job_id, log_pb.job_id());
    ASSERT_GT(log_pb.delete_data_files_size(), 0);
    ASSERT_EQ(TStatusCode::OK, reported_status);

    // Rolling-upgrade compatibility: requests without the optional destination tablet keep using
    // the stable minimum source-tablet root used by older FEs.
    request.__set_job_id(501);
    request.__isset.dest_tablet_id = false;
    ASSERT_OK(log_fs->create_dir_recursive(log_location_provider->snapshot_log_root_location(tablet_id)));
    reported_status = TStatusCode::RUNTIME_ERROR;
    run_external_cluster_snapshot_task(request, next_id(), _exec_env);
    auto legacy_log_path =
            log_location_provider->snapshot_log_location(tablet_id, request.job_id, request.physical_partition_id);
    ASSERT_OK(ProtobufFile(legacy_log_path, log_fs).load(&log_pb, false));
    ASSERT_EQ(request.job_id, log_pb.job_id());
    ASSERT_EQ(TStatusCode::OK, reported_status);

    SyncPoint::GetInstance()->ClearCallBack("FinishAgentTask::input");
    SyncPoint::GetInstance()->ClearCallBack("ExternalClusterSnapshotTask::snapshot_log_file_system");
    SyncPoint::GetInstance()->ClearCallBack("ExternalClusterSnapshotTask::snapshot_log_location_provider");
    SyncPoint::GetInstance()->ClearCallBack("cluster_snapshot_task::upload_snapshot_files");
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

// ==================== Reshard-boundary / shared-file tests ====================

// .del files are collected, including from a segmentless delete-only rowset.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_del_files) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 2, 1, {"seg1.dat"});
    metadata->mutable_rowsets(0)->add_del_files()->set_name("del1.del");
    auto* del_only = metadata->add_rowsets(); // delete-only rowset (no segments)
    del_only->set_id(50);
    del_only->add_del_files()->set_name("del2.del");

    auto files = collect_del_files(metadata);
    EXPECT_EQ(2, files.size());
    EXPECT_TRUE(files.contains("del1.del"));
    EXPECT_TRUE(files.contains("del2.del"));

    EXPECT_TRUE(collect_del_files(nullptr).empty());
    EXPECT_TRUE(collect_del_files(create_tablet_metadata(next_id(), 2, 1, {"seg.dat"})).empty());
}

// collect_live_data_files returns every current live data filename across all classes.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_live_data_files) {
    int64_t tablet_id = next_id();
    auto metadata = create_tablet_metadata(tablet_id, 2, 1, {"seg.dat"}, {"s.sst"}, {"c.cols"}, {"d.delvec"});
    metadata->mutable_rowsets(0)->add_del_files()->set_name("x.del");
    auto collections = TabletFileCollections::collect(nullptr, metadata);

    auto files = collect_live_data_files(collections);
    EXPECT_EQ(5, files.size());
    EXPECT_TRUE(files.contains("seg.dat"));
    EXPECT_TRUE(files.contains("x.del"));
    EXPECT_TRUE(files.contains("s.sst"));
    EXPECT_TRUE(files.contains("c.cols"));
    EXPECT_TRUE(files.contains("d.delvec"));
    EXPECT_TRUE(collect_live_data_files(TabletFileCollections()).empty());
}

// .del files are uploaded in FULL (all current), even when also present in pre metadata, so existing
// snapshots that predate .del tracking are backfilled.
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_uploads_all_del_files_full) {
    int64_t tablet_id = next_id();
    auto pre_metadata = create_tablet_metadata(tablet_id, 1, 1, {"seg1.dat"});
    pre_metadata->mutable_rowsets(0)->add_del_files()->set_name("shared.del");
    auto new_metadata = create_tablet_metadata(tablet_id, 2, 1, {"seg1.dat"});
    new_metadata->mutable_rowsets(0)->add_del_files()->set_name("shared.del");

    auto collections = TabletFileCollections::collect(pre_metadata, new_metadata);
    FileSet globally_bound_files;
    FileSet pre_bundle_data_files;
    UploadSnapshotFilesRequestPB node_req;
    auto* tablet_pb =
            populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_files, node_req);

    bool found = false;
    for (const auto& f : tablet_pb->new_data_files()) {
        if (f == "shared.del") found = true;
    }
    EXPECT_TRUE(found); // present in both pre and new, still uploaded (full)
}

// A shared file referenced by two split children is uploaded at most once per partition cycle.
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_dedups_shared_sstable_across_tablets) {
    int64_t t1 = next_id();
    int64_t t2 = next_id();
    auto new1 = create_tablet_metadata(t1, 2, 1, {}, {"shared.sst"});
    auto new2 = create_tablet_metadata(t2, 2, 1, {}, {"shared.sst"});
    auto coll1 = TabletFileCollections::collect(nullptr, new1);
    auto coll2 = TabletFileCollections::collect(nullptr, new2);

    FileSet globally_bound_files;
    FileSet pre_bundle_data_files;
    UploadSnapshotFilesRequestPB node_req;
    auto* pb1 = populate_tablet_snapshot(t1, coll1, pre_bundle_data_files, globally_bound_files, node_req);
    auto* pb2 = populate_tablet_snapshot(t2, coll2, pre_bundle_data_files, globally_bound_files, node_req);

    int count = 0;
    for (const auto& f : pb1->new_data_files()) {
        if (f == "shared.sst") count++;
    }
    for (const auto& f : pb2->new_data_files()) {
        if (f == "shared.sst") count++;
    }
    EXPECT_EQ(1, count);
}

// A shared file compacted away by one child is NOT deleted while a sibling still references it: the
// partition-wide live-file subtraction removes it from the delete candidates. Uses a bundled segment
// to also confirm subtraction happens after bundle expansion.
TEST_F(ExternalClusterSnapshotTaskTest, test_shared_file_not_deleted_when_sibling_references) {
    // Child A drops shared bundle segment F (F in A's pre rowset, gone from A's new).
    int64_t ta = next_id();
    auto a_pre = create_tablet_metadata(ta, 1, /*rowset_id=*/1, {"F.dat"});
    a_pre->mutable_rowsets(0)->mutable_segment_metas(0)->set_bundle_file_offset(0); // bundle file
    auto a_new = create_tablet_metadata(ta, 2, /*rowset_id=*/2, {"A_own.dat"});
    auto a_coll = TabletFileCollections::collect(a_pre, a_new);

    FileSet unused_data_files;
    FileSet pre_bundle_data_files;
    collect_unused_files(a_coll, unused_data_files, pre_bundle_data_files);
    EXPECT_TRUE(pre_bundle_data_files.contains("F.dat")); // child A dropped F (a bundle file)

    // Partition-wide live set = A's new + sibling B's new; B still references F.
    int64_t tb = next_id();
    auto b_new = create_tablet_metadata(tb, 2, 1, {"F.dat"});
    FileSet partition_live_files = collect_live_data_files(a_coll);
    for (const auto& f : collect_live_data_files(TabletFileCollections::collect(nullptr, b_new))) {
        partition_live_files.emplace(f);
    }

    // prepare_unused_files_for_log folds bundle files into unused, THEN subtracts the live set.
    FileSet unused_meta_files, unused_schema_files;
    phmap::flat_hash_set<int64_t> pre_schema_ids, new_schema_ids;
    prepare_unused_files_for_log(/*pre_version=*/1, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files, partition_live_files);
    EXPECT_FALSE(unused_data_files.contains("F.dat")); // kept: sibling B still references it
}

// Reshard boundary: a child with no metadata at pre_version does a full re-sync instead of failing.
TEST_F(ExternalClusterSnapshotTaskTest, test_process_tablet_pre_not_found_full_resync) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 3; // below the child's earliest metadata version
    int64_t new_version = 6; // the reshard publish version
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"seg_new.dat"}, {"s_new.sst"});
    new_metadata->mutable_rowsets(0)->add_del_files()->set_name("d_new.del");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata)); // pre_version 3 is intentionally absent

    FileSet globally_bound_files, pre_bundle_data_files, unused_data_files, unused_meta_files, partition_live_files;
    phmap::flat_hash_set<int64_t> pre_schema_ids, new_schema_ids;
    UploadSnapshotFilesRequestPB node_req;

    auto st = process_tablet_for_snapshot(_tablet_mgr.get(), tablet_id, pre_version, new_version,
                                          /*is_filebundling=*/false, /*meta_added=*/false, pre_bundle_data_files,
                                          unused_data_files, unused_meta_files, pre_schema_ids, new_schema_ids,
                                          globally_bound_files, partition_live_files, node_req);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, node_req.tablet_snapshots_size());

    FileSet uploaded;
    for (const auto& f : node_req.tablet_snapshots(0).new_data_files()) {
        uploaded.emplace(f);
    }
    EXPECT_TRUE(uploaded.contains("seg_new.dat")); // full re-sync uploads all current data files
    EXPECT_TRUE(uploaded.contains("s_new.sst"));
    EXPECT_TRUE(uploaded.contains("d_new.del"));

    bool has_new_meta = false;
    for (const auto& f : node_req.tablet_snapshots(0).new_metadata_files()) {
        if (f == tablet_metadata_filename(tablet_id, new_version)) has_new_meta = true;
    }
    EXPECT_TRUE(has_new_meta); // new metadata still uploaded
    // The child never had a {tablet}_{pre_version}.meta, so it must not be queued for deletion.
    EXPECT_FALSE(unused_meta_files.contains(tablet_metadata_filename(tablet_id, pre_version)));
    EXPECT_TRUE(partition_live_files.contains("seg_new.dat")); // recorded for deletion safety
}

// ==================== Index sidecar (.vi / .idx) collection ====================

// collect_vector_index_files: every recorded .vi, with NO async-build watermark gate -- a .vi whose
// rowset version is above vector_index_built_version is still collected (completeness); a not-yet-
// built .vi is tolerated at upload time, not filtered here. Filename matches
// gen_vector_index_filename_for_segment.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_vector_index_files) {
    int64_t tablet_id = next_id();
    auto md = create_tablet_metadata(tablet_id, 10, 1, {"s1.dat", "s2.dat"});
    md->mutable_rowsets(0)->set_version(2);
    md->mutable_rowsets(1)->set_version(10);
    for (int i = 0; i < 2; ++i) {
        auto* seg = md->mutable_rowsets(i)->mutable_segment_metas(0);
        seg->set_segment_vector_index_uid(9);
        seg->add_vector_index_ids(7);
    }
    md->set_vector_index_built_version(3); // below rowset v10 -- must NOT filter s2's .vi out

    auto files = collect_vector_index_files(md);
    EXPECT_EQ(2, files.size());
    EXPECT_TRUE(files.contains("s1_9_7.vi"));
    EXPECT_TRUE(files.contains("s2_9_7.vi")); // above the watermark, still collected (no gate)

    EXPECT_TRUE(collect_vector_index_files(nullptr).empty());
    // A segment without vector_index_ids contributes nothing.
    EXPECT_TRUE(collect_vector_index_files(create_tablet_metadata(next_id(), 5, 1, {"seg.dat"})).empty());
}

// A legacy segment written before segment_vector_index_uid existed carries vector_index_ids but no
// uid. gen_vector_index_filename_for_segment DCHECKs the uid, so such a segment must be skipped (its
// .vi is unresolvable by the read path too) rather than crash a debug build or emit a uid-0 name.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_vector_index_files_skips_legacy_segment_without_uid) {
    auto md = create_tablet_metadata(next_id(), 10, 1, {"legacy.dat", "modern.dat"});
    // rowset 0: vector_index_ids but NO segment_vector_index_uid (pre-uid segment).
    md->mutable_rowsets(0)->mutable_segment_metas(0)->add_vector_index_ids(7);
    // rowset 1: a normal post-uid segment.
    auto* modern = md->mutable_rowsets(1)->mutable_segment_metas(0);
    modern->set_segment_vector_index_uid(9);
    modern->add_vector_index_ids(7);

    auto files = collect_vector_index_files(md);
    EXPECT_EQ(1, files.size());
    EXPECT_TRUE(files.contains("modern_9_7.vi"));
    EXPECT_FALSE(files.contains("legacy_0_7.vi")); // legacy segment skipped, not named by uid 0
}

// collect_inverted_index_files: each flat .idx from idg_meta; a GIN entry (directory artifact) is
// skipped (the file-based syncer cannot transport a directory).
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_inverted_index_files) {
    int64_t tablet_id = next_id();
    auto md = create_tablet_metadata(tablet_id, 2, 1, {"seg1.dat"});
    auto& idg0 = (*md->mutable_idg_meta()->mutable_idgs())[0]; // key = segment global rssid
    idg0.add_entries()->set_index_file("0000000000000001_uuid.idx");
    // A GIN entry names a directory artifact the file syncer cannot transport -> skipped.
    auto* gin = idg0.add_entries();
    gin->set_index_file("gin_artifact_dir");
    gin->add_keys()->set_index_type(IndexType::GIN);
    (*md->mutable_idg_meta()->mutable_idgs())[1].add_entries()->set_index_file("0000000000000002_uuid.idx");

    auto files = collect_inverted_index_files(md);
    EXPECT_EQ(2, files.size());
    EXPECT_TRUE(files.contains("0000000000000001_uuid.idx"));
    EXPECT_TRUE(files.contains("0000000000000002_uuid.idx"));
    EXPECT_FALSE(files.contains("gin_artifact_dir"));

    EXPECT_TRUE(collect_inverted_index_files(nullptr).empty());
    EXPECT_TRUE(collect_inverted_index_files(create_tablet_metadata(next_id(), 2, 1, {"s.dat"})).empty());
}

// A flat index artifact whose name is not ".idx" (a hypothetical future flat index type) is still
// collected -- the skip is keyed on the GIN index_type, not a filename suffix, so a transportable
// flat file is never silently dropped.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_inverted_index_files_keeps_non_idx_flat) {
    int64_t tablet_id = next_id();
    auto md = create_tablet_metadata(tablet_id, 2, 1, {"seg1.dat"});
    auto* e = (*md->mutable_idg_meta()->mutable_idgs())[0].add_entries();
    e->set_index_file("future_flat_index.bin");
    e->add_keys()->set_index_type(IndexType::BITMAP);
    EXPECT_TRUE(collect_inverted_index_files(md).contains("future_flat_index.bin"));
}

// TabletFileCollections::collect merges .vi and .idx into pre_/new_index_files; null pre -> empty pre.
TEST_F(ExternalClusterSnapshotTaskTest, test_collect_populates_index_files) {
    int64_t tablet_id = next_id();
    auto mk = [&](int64_t version) {
        auto md = create_tablet_metadata(tablet_id, version, 1, {"seg1.dat"});
        auto* seg = md->mutable_rowsets(0)->mutable_segment_metas(0);
        seg->set_segment_vector_index_uid(9);
        seg->add_vector_index_ids(7);
        (*md->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("aa.idx");
        return md;
    };
    auto collections = TabletFileCollections::collect(mk(1), mk(2));
    EXPECT_TRUE(collections.pre_index_files.contains("seg1_9_7.vi"));
    EXPECT_TRUE(collections.pre_index_files.contains("aa.idx"));
    EXPECT_TRUE(collections.new_index_files.contains("seg1_9_7.vi"));
    EXPECT_TRUE(collections.new_index_files.contains("aa.idx"));

    auto c2 = TabletFileCollections::collect(nullptr, mk(2));
    EXPECT_TRUE(c2.pre_index_files.empty());
    EXPECT_EQ(2, c2.new_index_files.size());
}

// Index files upload in FULL: a file present in BOTH pre and new is still uploaded (backfill).
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_uploads_index_files_full) {
    int64_t tablet_id = next_id();
    auto mk = [&](int64_t v) {
        auto md = create_tablet_metadata(tablet_id, v, 1, {"seg1.dat"});
        auto* seg = md->mutable_rowsets(0)->mutable_segment_metas(0);
        seg->set_segment_vector_index_uid(9);
        seg->add_vector_index_ids(7);
        (*md->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("shared.idx");
        return md;
    };
    auto collections = TabletFileCollections::collect(mk(1), mk(2));
    FileSet globally_bound_files, pre_bundle_data_files;
    UploadSnapshotFilesRequestPB node_req;
    auto* pb = populate_tablet_snapshot(tablet_id, collections, pre_bundle_data_files, globally_bound_files, node_req);
    // Index files go to the tolerant new_index_data_files list, not new_data_files.
    std::set<std::string> uploaded(pb->new_index_data_files().begin(), pb->new_index_data_files().end());
    EXPECT_TRUE(uploaded.contains("seg1_9_7.vi")); // present in both pre and new, still uploaded (full)
    EXPECT_TRUE(uploaded.contains("shared.idx"));
    // ...and not in the intolerant data-file list.
    std::set<std::string> data(pb->new_data_files().begin(), pb->new_data_files().end());
    EXPECT_FALSE(data.contains("seg1_9_7.vi"));
    EXPECT_FALSE(data.contains("shared.idx"));
}

// A shared index file referenced by two split children is uploaded at most once per partition cycle.
TEST_F(ExternalClusterSnapshotTaskTest, test_populate_dedups_shared_index_across_tablets) {
    int64_t t1 = next_id(), t2 = next_id();
    auto mk = [&](int64_t tid) {
        auto md = create_tablet_metadata(tid, 2, 1, {"S.dat"});
        auto* seg = md->mutable_rowsets(0)->mutable_segment_metas(0);
        seg->set_segment_vector_index_uid(9); // shared writer uid -> both children resolve the same .vi
        seg->add_vector_index_ids(7);
        (*md->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("shared.idx");
        return md;
    };
    FileSet globally_bound_files, pre_bundle_data_files;
    UploadSnapshotFilesRequestPB node_req;
    auto* pb1 = populate_tablet_snapshot(t1, TabletFileCollections::collect(nullptr, mk(t1)), pre_bundle_data_files,
                                         globally_bound_files, node_req);
    auto* pb2 = populate_tablet_snapshot(t2, TabletFileCollections::collect(nullptr, mk(t2)), pre_bundle_data_files,
                                         globally_bound_files, node_req);
    int vi = 0, idx = 0;
    for (const auto& f : pb1->new_index_data_files()) {
        vi += (f == "S_9_7.vi");
        idx += (f == "shared.idx");
    }
    for (const auto& f : pb2->new_index_data_files()) {
        vi += (f == "S_9_7.vi");
        idx += (f == "shared.idx");
    }
    EXPECT_EQ(1, vi);
    EXPECT_EQ(1, idx);
}

// A pre-only (dropped) index file is a deletion candidate; collect_live_data_files includes index files.
TEST_F(ExternalClusterSnapshotTaskTest, test_index_files_unused_and_live) {
    int64_t tablet_id = next_id();
    auto pre = create_tablet_metadata(tablet_id, 1, 1, {"seg1.dat"});
    auto* pre_seg = pre->mutable_rowsets(0)->mutable_segment_metas(0);
    pre_seg->set_segment_vector_index_uid(9);
    pre_seg->add_vector_index_ids(7);
    (*pre->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("a.idx");
    // new drops the vector-indexed segment (different rowset) and the idg entry.
    auto nw = create_tablet_metadata(tablet_id, 2, 2, {"seg2.dat"});

    auto collections = TabletFileCollections::collect(pre, nw);
    FileSet unused_data_files, pre_bundle_data_files;
    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);
    EXPECT_TRUE(unused_data_files.contains("seg1_9_7.vi"));
    EXPECT_TRUE(unused_data_files.contains("a.idx"));

    auto live = collect_live_data_files(collections);
    EXPECT_FALSE(live.contains("seg1_9_7.vi")); // no longer live in new
    EXPECT_FALSE(live.contains("a.idx"));
    // A tablet that still references index files reports them live.
    auto nlive = create_tablet_metadata(next_id(), 2, 1, {"segN.dat"});
    auto* nseg = nlive->mutable_rowsets(0)->mutable_segment_metas(0);
    nseg->set_segment_vector_index_uid(3);
    nseg->add_vector_index_ids(5);
    (*nlive->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("live.idx");
    auto live2 = collect_live_data_files(TabletFileCollections::collect(nullptr, nlive));
    EXPECT_TRUE(live2.contains("segN_3_5.vi"));
    EXPECT_TRUE(live2.contains("live.idx"));
}

// A shared index file one child drops is NOT deleted while a sibling still references it (partition-
// wide live subtraction), covering both a shared .vi (same recorded uid) and a shared .idx.
TEST_F(ExternalClusterSnapshotTaskTest, test_shared_index_file_not_deleted_when_sibling_references) {
    int64_t ta = next_id();
    auto a_pre = create_tablet_metadata(ta, 1, 1, {"S.dat"});
    auto* a_seg = a_pre->mutable_rowsets(0)->mutable_segment_metas(0);
    a_seg->set_segment_vector_index_uid(9);
    a_seg->add_vector_index_ids(7);
    (*a_pre->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("shared.idx");
    auto a_new = create_tablet_metadata(ta, 2, 2, {"A_own.dat"}); // child A drops both
    auto a_coll = TabletFileCollections::collect(a_pre, a_new);

    FileSet unused_data_files, pre_bundle_data_files;
    collect_unused_files(a_coll, unused_data_files, pre_bundle_data_files);
    EXPECT_TRUE(unused_data_files.contains("S_9_7.vi"));
    EXPECT_TRUE(unused_data_files.contains("shared.idx"));

    // Sibling B still references the same shared .vi (same segment + uid) and shared.idx.
    int64_t tb = next_id();
    auto b_new = create_tablet_metadata(tb, 2, 1, {"S.dat"});
    auto* b_seg = b_new->mutable_rowsets(0)->mutable_segment_metas(0);
    b_seg->set_segment_vector_index_uid(9);
    b_seg->add_vector_index_ids(7);
    (*b_new->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("shared.idx");
    FileSet partition_live_files = collect_live_data_files(a_coll);
    for (const auto& f : collect_live_data_files(TabletFileCollections::collect(nullptr, b_new))) {
        partition_live_files.emplace(f);
    }

    FileSet unused_meta_files, unused_schema_files;
    phmap::flat_hash_set<int64_t> pre_schema_ids, new_schema_ids;
    prepare_unused_files_for_log(/*pre_version=*/1, pre_bundle_data_files, unused_data_files, unused_meta_files,
                                 pre_schema_ids, new_schema_ids, unused_schema_files, partition_live_files);
    EXPECT_FALSE(unused_data_files.contains("S_9_7.vi"));   // kept: sibling B references it
    EXPECT_FALSE(unused_data_files.contains("shared.idx")); // kept: sibling B references it
}

// A fully-removed pre-only IDG entry becomes a deletion candidate; an entry with empty index_file
// (fully tombstoned) is not collected.
TEST_F(ExternalClusterSnapshotTaskTest, test_dropped_and_empty_idg_entries) {
    int64_t tablet_id = next_id();
    auto pre = create_tablet_metadata(tablet_id, 1, 1, {"seg1.dat"});
    (*pre->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("dropped.idx");
    auto nw = create_tablet_metadata(tablet_id, 2, 1, {"seg1.dat"});
    (*nw->mutable_idg_meta()->mutable_idgs())[0].add_entries(); // entry with no index_file -> skipped

    EXPECT_TRUE(collect_inverted_index_files(nw).empty());

    auto collections = TabletFileCollections::collect(pre, nw);
    FileSet unused_data_files, pre_bundle_data_files;
    collect_unused_files(collections, unused_data_files, pre_bundle_data_files);
    EXPECT_TRUE(unused_data_files.contains("dropped.idx"));
}

// Reshard boundary: a child with no pre metadata does a full re-sync that includes its .vi and .idx,
// queues neither for deletion, and records both as partition-live.
TEST_F(ExternalClusterSnapshotTaskTest, test_process_tablet_pre_not_found_full_resync_index) {
    int64_t tablet_id = next_id();
    int64_t pre_version = 3; // below the child's earliest metadata version
    int64_t new_version = 6; // reshard publish version
    auto new_metadata = create_tablet_metadata(tablet_id, new_version, 1, {"seg_new.dat"});
    auto* seg = new_metadata->mutable_rowsets(0)->mutable_segment_metas(0);
    seg->set_segment_vector_index_uid(9);
    seg->add_vector_index_ids(7);
    (*new_metadata->mutable_idg_meta()->mutable_idgs())[0].add_entries()->set_index_file("n.idx");
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*new_metadata)); // pre_version 3 intentionally absent

    FileSet globally_bound_files, pre_bundle_data_files, unused_data_files, unused_meta_files, partition_live_files;
    phmap::flat_hash_set<int64_t> pre_schema_ids, new_schema_ids;
    UploadSnapshotFilesRequestPB node_req;

    auto st = process_tablet_for_snapshot(_tablet_mgr.get(), tablet_id, pre_version, new_version,
                                          /*is_filebundling=*/false, /*meta_added=*/false, pre_bundle_data_files,
                                          unused_data_files, unused_meta_files, pre_schema_ids, new_schema_ids,
                                          globally_bound_files, partition_live_files, node_req);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1, node_req.tablet_snapshots_size());

    FileSet uploaded;
    for (const auto& f : node_req.tablet_snapshots(0).new_index_data_files()) {
        uploaded.emplace(f);
    }
    EXPECT_TRUE(uploaded.contains("seg_new_9_7.vi")); // full re-sync uploads the .vi
    EXPECT_TRUE(uploaded.contains("n.idx"));          // and the .idx
    // The child never had a pre-version metadata, so its index files must not be queued for deletion.
    EXPECT_FALSE(unused_data_files.contains("seg_new_9_7.vi"));
    EXPECT_FALSE(unused_data_files.contains("n.idx"));
    EXPECT_TRUE(partition_live_files.contains("seg_new_9_7.vi")); // recorded for deletion safety
    EXPECT_TRUE(partition_live_files.contains("n.idx"));
}

} // namespace starrocks::lake
