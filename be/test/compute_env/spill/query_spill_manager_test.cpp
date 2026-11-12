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

#include "compute_env/spill/query_spill_manager.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/config_storage_fwd.h"
#include "compute_env/spill/dir_manager.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "fs/remote_read_context.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks::spill {

class QuerySpillManagerCoreTest : public ::testing::Test {
public:
    void SetUp() override {
        _query_id = generate_uuid();
        _local_path = fmt::format("{}/query_spill_manager", config::storage_root_path);
        _fs_handle = std::shared_ptr<FileSystem>(&_fs, [](FileSystem*) {});
        ASSERT_OK(_fs.create_dir_recursive(_local_path));
        _local_dir = std::make_shared<Dir>(_local_path, _fs_handle, 1024);
        _dir_mgr = std::make_unique<DirManager>(std::vector<DirPtr>{_local_dir});
    }

protected:
    MemoryFileSystem _fs;
    std::shared_ptr<FileSystem> _fs_handle;
    TUniqueId _query_id;
    std::string _local_path;
    DirPtr _local_dir;
    std::unique_ptr<DirManager> _dir_mgr;
    GlobalSpillManager _global_mgr;
};

TEST_F(QuerySpillManagerCoreTest, LocalSpillUsesInjectedDirManager) {
    QuerySpillManager query_spill_manager(_query_id, &_global_mgr, _dir_mgr.get());
    TQueryOptions query_options;
    ASSERT_OK(query_spill_manager.init_block_manager(query_options, RemoteReadContext{}));
    ASSERT_NE(nullptr, query_spill_manager.block_manager());

    AcquireBlockOptions block_options;
    block_options.query_id = _query_id;
    block_options.fragment_instance_id = _query_id;
    block_options.plan_node_id = 1;
    block_options.name = "local";
    block_options.block_size = 16;
    auto block_or = query_spill_manager.block_manager()->acquire_block(block_options);
    ASSERT_TRUE(block_or.ok()) << block_or.status();
    auto block = block_or.value();
    EXPECT_EQ(fmt::format("LogBlock[container={}/{}/{}-{}]", _local_path, print_id(_query_id), print_id(_query_id),
                          "local-1-0"),
              block->debug_string());
}

TEST_F(QuerySpillManagerCoreTest, CountersForwardToGlobalSpillManager) {
    QuerySpillManager query_spill_manager(_query_id, &_global_mgr, _dir_mgr.get());

    query_spill_manager.increase_spillable_operators();
    query_spill_manager.inc_reserve_bytes(128);
    EXPECT_EQ(1, _global_mgr.spillable_operators());
    EXPECT_EQ(128, _global_mgr.spill_expected_reserved_bytes());
    EXPECT_EQ(128, query_spill_manager.spill_expected_reserved_bytes());

    query_spill_manager.dec_reserve_bytes(64);
    query_spill_manager.decrease_spillable_operators();
    EXPECT_EQ(0, _global_mgr.spillable_operators());
    EXPECT_EQ(64, _global_mgr.spill_expected_reserved_bytes());
    EXPECT_EQ(64, query_spill_manager.spill_expected_reserved_bytes());
}

namespace {
std::vector<RemoteReadContext> g_query_spill_reads;

Status record_query_spill_read(const RemoteReadContext& ctx, int64_t, const std::function<int64_t()>& read) {
    g_query_spill_reads.push_back(ctx);
    read();
    return Status::OK();
}
} // namespace

// The spill of a query on remote storage is read back as that query: the context it was given, which the fragment
// takes from query_remote_read_context() like the query's scans.
TEST_F(QuerySpillManagerCoreTest, RemoteSpillReadsAsTheQuery) {
    const std::string remote_path = "./query_spill_manager_remote_test";
    ASSERT_OK(FileSystem::Default()->create_dir_recursive(remote_path));
    g_scheduled_remote_read.store(&record_query_spill_read);
    DeferOp reset([&]() {
        g_scheduled_remote_read.store(nullptr);
        g_query_spill_reads.clear();
        (void)FileSystem::Default()->delete_dir_recursive(remote_path);
    });

    TQueryOptions query_options;
    query_options.spill_options.__set_enable_spill_to_remote_storage(true);
    TSpillToRemoteStorageOptions remote_options;
    remote_options.__set_remote_storage_paths({remote_path});
    remote_options.__set_remote_storage_conf(TCloudConfiguration());
    remote_options.__set_disable_spill_to_local_disk(true);
    query_options.spill_options.__set_spill_to_remote_storage_options(remote_options);

    const auto query = RemoteReadContext::for_query(7, 16, /*group_config_version=*/3,
                                                    RemoteReadContext::flow_id_of(_query_id.hi, _query_id.lo));
    QuerySpillManager query_spill_manager(_query_id, &_global_mgr, _dir_mgr.get());
    ASSERT_OK(query_spill_manager.init_block_manager(query_options, query));

    AcquireBlockOptions block_options;
    block_options.query_id = _query_id;
    block_options.fragment_instance_id = _query_id;
    block_options.plan_node_id = 1;
    block_options.name = "remote";
    block_options.block_size = 4;
    ASSIGN_OR_ABORT(auto block, query_spill_manager.block_manager()->acquire_block(block_options));
    ASSERT_TRUE(block->is_remote());
    ASSERT_OK(block->append({Slice("abcd")}));
    ASSERT_OK(query_spill_manager.block_manager()->release_block(block));
    ASSIGN_OR_ABORT(auto stream, block->get_readable());
    char buf[4];
    ASSERT_OK(stream->read_fully(buf, sizeof(buf)));
    ASSERT_EQ("abcd", std::string(buf, sizeof(buf)));

    ASSERT_FALSE(g_query_spill_reads.empty());
    for (const auto& ctx : g_query_spill_reads) {
        EXPECT_EQ(RemoteReadContext::Workload::QUERY, ctx.workload);
        EXPECT_EQ(query.group_id, ctx.group_id);
        EXPECT_EQ(query.group_weight, ctx.group_weight);
        EXPECT_EQ(query.group_config_version, ctx.group_config_version);
        EXPECT_EQ(query.flow_id, ctx.flow_id);
    }
}

TEST_F(QuerySpillManagerCoreTest, ExpectedReservedBytesToleratesMissingGlobalManager) {
    QuerySpillManager query_spill_manager(_query_id, nullptr, _dir_mgr.get());

    EXPECT_EQ(0, query_spill_manager.spill_expected_reserved_bytes());
}

} // namespace starrocks::spill
