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

// Whether a hash join may leave a mismatched pair of inputs alone (local_shuffle_matches_sender) is
// decided from what the input that will be locally shuffled reports about its hash. These tests walk
// the inputs of a second bucket shuffle join over bucket-transformed tables: its probe side is the
// local shuffle the first join interpolated, against a bucket shuffle exchange.

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/pipeline_builder_operators.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/pipeline_builder_context.h"
#include "exec_primitive/pipeline/operator.h"
#include "exec_primitive/pipeline/source_operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

constexpr int32_t kDop = 4;
constexpr int32_t kPlanNodeId = 100;

class TestSourceOperator final : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
};

// Stands in for a scan of a bucket table that is split across drivers: it can be locally shuffled, and
// like every scan it names BUCKET_SHUFFLE_HASH_PARTITIONED as the hash to shuffle it by.
class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id, const std::vector<TBucketProperty>& bucket_properties)
            : SourceOperatorFactory(id, "test_source", plan_node_id) {
        set_degree_of_parallelism(kDop);
        set_could_local_shuffle(true);
        set_partition_type(TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED);
        set_bucket_properties(bucket_properties);
    }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

std::vector<TBucketProperty> murmur_bucket_properties() {
    TBucketProperty bucket;
    bucket.__set_bucket_func(TBucketFunction::MURMUR3_X86_32);
    bucket.__set_bucket_num(17);
    return {bucket};
}

class LocalShuffleAlignmentTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = std::make_unique<FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>(TQueryGlobals{}));
        _context = std::make_unique<PipelineBuilderContext>(_fragment_context.get(), kDop, kDop);
        _texchange_node.__set_partition_type(TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED);
    }

    RuntimeState* runtime_state() { return _fragment_context->runtime_state(); }

    // The probe side of the second join when the first join re-partitioned its inputs by plain hash:
    // what #78920 does for a bucket-transformed scan against a bucket shuffle exchange, or for a UNION
    // over per-driver scans against an exchange bound to drivers.
    OpFactories probe_side_forced_by_first_join(const std::vector<TBucketProperty>& bucket_properties) {
        OpFactories ops{std::make_shared<TestSourceOperatorFactory>(_context->next_operator_id(), kPlanNodeId,
                                                                    bucket_properties)};
        return builder::interpolate_local_forced_shuffle_exchange(_context.get(), runtime_state(), kPlanNodeId, ops, {},
                                                                  TPartitionType::HASH_PARTITIONED, {});
    }

    // The probe side of the second join when the first join shuffled its scan the usual way: a plain
    // bucket table whose scan ranges were not assigned per driver.
    OpFactories probe_side_shuffled_by_first_join(const std::vector<TBucketProperty>& bucket_properties) {
        OpFactories ops{std::make_shared<TestSourceOperatorFactory>(_context->next_operator_id(), kPlanNodeId,
                                                                    bucket_properties)};
        return builder::maybe_interpolate_local_shuffle_exchange(_context.get(), runtime_state(), kPlanNodeId, ops,
                                                                 std::vector<ExprContext*>{});
    }

    // The build side of the second join: a bucket shuffle exchange, already partitioned per driver by
    // its sender.
    OpFactories build_side_bucket_shuffle_exchange() {
        auto exchange = std::make_shared<ExchangeSourceOperatorFactory>(_context->next_operator_id(), kPlanNodeId + 1,
                                                                        _texchange_node, 1, RecordDescriptor(),
                                                                        /*enable_pipeline_level_shuffle=*/true);
        exchange->set_degree_of_parallelism(kDop);
        return {exchange};
    }

    std::unique_ptr<FragmentContext> _fragment_context;
    std::unique_ptr<PipelineBuilderContext> _context;
    TExchangeNode _texchange_node;
};

TEST_F(LocalShuffleAlignmentTest, LocalShuffleKeepsTheBucketTransformOfItsInput) {
    auto shuffled = probe_side_forced_by_first_join(murmur_bucket_properties());
    auto* source = _context->source_operator(shuffled);

    ASSERT_NE(nullptr, dynamic_cast<LocalExchangeSourceOperatorFactory*>(source));
    EXPECT_TRUE(source->could_local_shuffle());
    // A later local shuffle of this source has to hash like the fragment's rows are spread over
    // instances: by the bucket transform, not by the crc32 the partition type alone would name.
    EXPECT_EQ(TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED, source->partition_type());
    EXPECT_EQ(murmur_bucket_properties(), source->get_bucket_properties());
}

TEST_F(LocalShuffleAlignmentTest, BucketTransformedInputIsNotTakenToMatchTheSender) {
    auto shuffled = probe_side_forced_by_first_join(murmur_bucket_properties());
    auto received = build_side_bucket_shuffle_exchange();
    ASSERT_FALSE(_context->could_local_shuffle(received));

    // The exchange cannot tell whether its sender hashed by the bucket transform, so an input carrying
    // one must be re-partitioned together with the exchange. Losing the bucket properties on the way
    // made this true, and the join then shuffled its probe side by crc32 against a sender hashing by
    // murmur3 (test_iceberg_bucket_aware_execution).
    EXPECT_FALSE(builder::local_shuffle_matches_sender(_context.get(), shuffled, received));
}

TEST_F(LocalShuffleAlignmentTest, PlainBucketInputMatchesAnUnboundSender) {
    auto shuffled = probe_side_shuffled_by_first_join({});
    auto received = build_side_bucket_shuffle_exchange();

    ASSERT_TRUE(_context->could_local_shuffle(shuffled));
    // Both sides end up on xorshift32(crc32) % dop, so the join needs no extra shuffle.
    EXPECT_TRUE(builder::local_shuffle_matches_sender(_context.get(), shuffled, received));
}

TEST_F(LocalShuffleAlignmentTest, PlainBucketInputDoesNotMatchASenderBoundToDrivers) {
    _fragment_context->set_has_per_driver_scan_ranges();
    auto shuffled = probe_side_forced_by_first_join({});
    auto received = build_side_bucket_shuffle_exchange();

    // With per-driver scan ranges the sender follows the FE's bucket-to-driver assignment instead.
    EXPECT_FALSE(builder::local_shuffle_matches_sender(_context.get(), shuffled, received));
}

} // namespace
} // namespace starrocks::pipeline
