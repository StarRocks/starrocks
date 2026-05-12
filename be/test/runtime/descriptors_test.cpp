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

#include "runtime/descriptors.h"

#include <gtest/gtest.h>

#include <thread>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors_ext.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class HiveTableDescriptorAddPartitionTest : public ::testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        _pool = std::make_unique<ObjectPool>();

        TTableDescriptor tdesc;
        tdesc.id = 1;
        tdesc.tableType = TTableType::HDFS_TABLE;
        _table_desc = _pool->add(new HdfsTableDescriptor(tdesc, _pool.get()));

        _runtime_state = _make_runtime_state();
    }

    std::shared_ptr<RuntimeState> _make_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        auto rs = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
        TUniqueId query_id;
        rs->init_mem_trackers(query_id);
        return rs;
    }

    // Build a minimal THdfsPartition. partition_key_exprs is left empty by default;
    // callers can override with custom thrift to exercise the dedup-conflict branch.
    static THdfsPartition _make_partition(std::vector<TExpr> partition_key_exprs = {}) {
        THdfsPartition p;
        p.file_format = THdfsFileFormat::TEXT;
        p.location.suffix = "";
        p.partition_key_exprs = std::move(partition_key_exprs);
        return p;
    }

    // Returns a TExpr describing a valid INT_LITERAL so it can survive both the
    // thrift inequality check on the fast path and a real ExprFactory::create_expr_trees
    // call on the slow path. The literal value is parameterised so callers can produce
    // distinct-but-valid exprs.
    static TExpr _make_int_literal_thrift_expr(int64_t value) {
        TExprNode node;
        node.node_type = TExprNodeType::INT_LITERAL;
        node.type = gen_type_desc(TPrimitiveType::INT);
        node.num_children = 0;
        node.is_nullable = false;
        node.has_nullable_child = false;
        TIntLiteral lit;
        lit.value = value;
        node.__set_int_literal(lit);

        TExpr e;
        e.nodes.push_back(node);
        return e;
    }

protected:
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<ObjectPool> _pool;
    HdfsTableDescriptor* _table_desc = nullptr;
    std::shared_ptr<RuntimeState> _runtime_state;
};

// First call inserts a new partition descriptor; get_partition returns it.
TEST_F(HiveTableDescriptorAddPartitionTest, FirstCallInsertsNewEntry) {
    constexpr int64_t partition_id = 1;
    THdfsPartition thrift = _make_partition();

    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift));

    HdfsPartitionDescriptor* desc = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, desc);
    ASSERT_EQ(thrift.partition_key_exprs, desc->thrift_partition_key_exprs());
}

// Second call with the same partition_id and same thrift is a dedup hit on the fast
// path. The previously inserted descriptor must remain in the map (no replacement).
TEST_F(HiveTableDescriptorAddPartitionTest, SamePartitionIdSameThriftIsDedupHit) {
    constexpr int64_t partition_id = 2;
    THdfsPartition thrift = _make_partition();

    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift));
    HdfsPartitionDescriptor* first = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, first);

    // Same thrift: must succeed and must NOT replace the existing entry.
    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift));
    HdfsPartitionDescriptor* second = _table_desc->get_partition(partition_id);
    ASSERT_EQ(first, second);
}

// Second call with the same partition_id but different thrift must fail, and the
// error message must surface enough context (both the new thrift and the existing
// partition's debug string) to help diagnose the mismatch.
TEST_F(HiveTableDescriptorAddPartitionTest, SamePartitionIdDifferentThriftFails) {
    constexpr int64_t partition_id = 3;
    THdfsPartition base = _make_partition();
    THdfsPartition conflict = _make_partition({_make_int_literal_thrift_expr(1)});

    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, base));

    Status s = _table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, conflict);
    ASSERT_FALSE(s.ok());
    ASSERT_TRUE(s.is_internal_error()) << s.to_string();

    // The mismatch error must include id, both descriptors, and explicitly mention
    // partition_key_exprs. The format is unified across fast-path and slow-path race
    // branches, so this assertion is deterministic regardless of which branch fires.
    const std::string msg = s.to_string();
    EXPECT_NE(std::string::npos, msg.find(std::to_string(partition_id))) << msg;
    EXPECT_NE(std::string::npos, msg.find("partition_key_exprs")) << msg;
    EXPECT_NE(std::string::npos, msg.find("new partition (thrift)")) << msg;
    EXPECT_NE(std::string::npos, msg.find("old_partition")) << msg;

    // Existing entry must be unchanged.
    HdfsPartitionDescriptor* desc = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, desc);
    ASSERT_EQ(base.partition_key_exprs, desc->thrift_partition_key_exprs());
}

// Different partition_ids should produce independent descriptors.
TEST_F(HiveTableDescriptorAddPartitionTest, DifferentPartitionIdsCoexist) {
    THdfsPartition p1 = _make_partition();
    THdfsPartition p2 = _make_partition({_make_int_literal_thrift_expr(42)});

    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), 10, p1));
    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), 11, p2));

    HdfsPartitionDescriptor* d10 = _table_desc->get_partition(10);
    HdfsPartitionDescriptor* d11 = _table_desc->get_partition(11);
    ASSERT_NE(nullptr, d10);
    ASSERT_NE(nullptr, d11);
    ASSERT_NE(d10, d11);
}

// Many sequential dedup-hits on the same partition_id must all succeed and never
// replace the original entry. Exercises the shared_lock fast path.
TEST_F(HiveTableDescriptorAddPartitionTest, RepeatedDedupHitsKeepFirstEntry) {
    constexpr int64_t partition_id = 42;
    THdfsPartition thrift = _make_partition();

    ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift));
    HdfsPartitionDescriptor* first = _table_desc->get_partition(partition_id);

    for (int i = 0; i < 100; ++i) {
        ASSERT_OK(_table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift));
    }
    ASSERT_EQ(first, _table_desc->get_partition(partition_id));
}

// Concurrent add_partition_value with the same id + same thrift from many threads
// must yield exactly one descriptor in the map. The races between the shared_lock
// fast path miss and the unique_lock slow path insert must all resolve to the same
// surviving entry without errors.
TEST_F(HiveTableDescriptorAddPartitionTest, ConcurrentInsertSameIdConvergesToOne) {
    constexpr int64_t partition_id = 7;
    THdfsPartition thrift = _make_partition();

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    std::vector<Status> statuses(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i]() {
            statuses[i] = _table_desc->add_partition_value(_runtime_state.get(), _pool.get(), partition_id, thrift);
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    for (const auto& s : statuses) {
        ASSERT_OK(s);
    }

    HdfsPartitionDescriptor* desc = _table_desc->get_partition(partition_id);
    ASSERT_NE(nullptr, desc);
    ASSERT_EQ(thrift.partition_key_exprs, desc->thrift_partition_key_exprs());
}

} // namespace starrocks