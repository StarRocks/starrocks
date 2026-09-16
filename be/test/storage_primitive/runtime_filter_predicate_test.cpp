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

#include "storage_primitive/runtime_filter_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "runtime/bucket_aware_partition.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_layout.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {
namespace {

constexpr SlotId kProbeSlotId = 1;
constexpr TPlanNodeId kProbeNodeId = 11;

// Three buckets mapped onto three instances in a deliberately non-identity order, so a
// path that forgets the bucketseq_to_instance indirection cannot accidentally agree.
const std::vector<int32_t> kBucketSeqToInstance = {2, 0, 1};

TRuntimeFilterLayout make_bucket_aware_layout(int32_t filter_id) {
    TBucketProperty bucket_property;
    bucket_property.bucket_func = TBucketFunction::MURMUR3_X86_32;
    bucket_property.bucket_num = kBucketSeqToInstance.size();

    TRuntimeFilterLayout layout;
    layout.__set_filter_id(filter_id);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L);
    layout.__set_pipeline_level_multi_partitioned(false);
    layout.__set_num_instances(kBucketSeqToInstance.size());
    layout.__set_num_drivers_per_instance(1);
    layout.__set_bucketseq_to_instance(kBucketSeqToInstance);
    layout.__set_bucket_properties({bucket_property});
    return layout;
}

// What FE emits for a bucket-shuffle join over an Iceberg table with a bucket transform
// under enable_bucket_aware_execution_on_lake.
TRuntimeFilterDescription make_local_hash_bucket_desc(int32_t filter_id) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_has_remote_targets(true);
    desc.__set_build_plan_node_id(5);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_layout(make_bucket_aware_layout(filter_id));
    desc.__set_plan_node_id_to_target_expr(
            {{kProbeNodeId, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(kProbeSlotId, true)}});
    return desc;
}

// The sub-filter index the operator-level probe assigns to each row: the murmur3 bucket
// transform, then bucketseq_to_instance.
std::vector<uint32_t> operator_partitions(const RuntimeFilterLayout& layout, const Column* column) {
    std::vector<uint32_t> hash_values;
    std::vector<uint32_t> round_hashes;
    std::vector<uint32_t> bucket_ids;
    std::vector<uint32_t> round_ids;
    BucketAwarePartitionCtx bctx(layout.bucket_properties(), hash_values, round_hashes, bucket_ids, round_ids);
    calc_hash_values_and_bucket_ids({column}, bctx);

    std::vector<uint32_t> partitions;
    partitions.reserve(bucket_ids.size());
    for (uint32_t bucket_id : bucket_ids) {
        EXPECT_LT(bucket_id, kBucketSeqToInstance.size());
        partitions.push_back(static_cast<uint32_t>(kBucketSeqToInstance[bucket_id]));
    }
    return partitions;
}

} // namespace

class RuntimeFilterPredicateTest : public ::testing::Test {
protected:
    static ColumnPtr make_probe_column(size_t num_rows) {
        auto column = Int32Column::create();
        for (size_t i = 0; i < num_rows; ++i) {
            column->append(static_cast<int32_t>(i) * 7 + 3);
        }
        return column;
    }

    RuntimeFilterProbeDescriptor* make_desc(int32_t filter_id) {
        auto* desc = _pool.add(new RuntimeFilterProbeDescriptor());
        CHECK(desc->init(&_pool, make_local_hash_bucket_desc(filter_id), kProbeNodeId, &_runtime_state).ok());
        return desc;
    }

    // A partitioned bloom filter whose sub-filter i holds exactly the values the
    // operator-level probe routes to partition i -- i.e. the filter a correct build side
    // would have produced.
    TRuntimeBloomFilter<TYPE_INT>* build_partitioned_filter(const RuntimeFilterLayout& layout, const Column* column) {
        const auto partitions = operator_partitions(layout, column);
        const auto& data = down_cast<const Int32Column*>(column)->get_data();

        auto* filter = _pool.add(new TRuntimeBloomFilter<TYPE_INT>());
        filter->set_global();
        for (uint32_t p = 0; p < kBucketSeqToInstance.size(); ++p) {
            auto* part = _pool.add(new TRuntimeBloomFilter<TYPE_INT>());
            part->init(1024);
            for (size_t i = 0; i < data.size(); ++i) {
                if (partitions[i] == p) {
                    part->insert(data[i]);
                }
            }
            filter->concat(part);
        }
        // After the concats: RuntimeMembershipFilter::concat() copies the join mode off the
        // filter being absorbed, which would otherwise reset this back to NONE.
        filter->set_join_mode(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
        return filter;
    }

    ObjectPool _pool;
    RuntimeState _runtime_state;
};

// The invariant the whole bucket-aware pushdown rests on: the storage-layer predicate must
// resolve the same sub-filter as the operator-level probe. A bloom filter has no false
// negatives, so if every row was inserted into the sub-filter its own bucket maps to, every
// row must survive. Resolving a different sub-filter tests the row against a filter that
// never saw it, and it is dropped -- silently, with no error anywhere.
TEST_F(RuntimeFilterPredicateTest, BucketAwareFilterKeepsEveryRowItWasBuiltFrom) {
    auto* desc = make_desc(1);
    ASSERT_FALSE(desc->layout().bucket_properties().empty());

    auto column = make_probe_column(256);
    auto* filter = build_partitioned_filter(desc->layout(), column.get());
    ASSERT_EQ(filter->num_hash_partitions(), kBucketSeqToInstance.size());
    desc->set_runtime_filter(filter);

    RuntimeFilterPredicate pred(desc, kProbeSlotId);
    ASSERT_TRUE(pred.init(0 /*driver_sequence*/));

    // Keyed by ColumnId, matching how the connector path builds the probe chunk:
    // ConnectorPredicateParser::column_id() returns SlotDescriptor::id(), so the
    // predicate's ColumnId is the probe slot id.
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(column, static_cast<ColumnId>(kProbeSlotId), true);

    std::vector<uint8_t> selection(column->size(), 1);
    ASSERT_OK(pred.evaluate(chunk.get(), selection.data(), 0, static_cast<uint16_t>(column->size())));

    // The direct form of the invariant (these tests build with -fno-access-control): the
    // indices the predicate resolved are exactly the operator's.
    EXPECT_EQ(pred._running_ctx.hash_values, operator_partitions(desc->layout(), column.get()));

    for (size_t i = 0; i < selection.size(); ++i) {
        EXPECT_EQ(selection[i], 1) << "row " << i << " was dropped: wrong sub-filter selected";
    }

    // Same invariant through the selective (sel/sel_size) entry point.
    std::vector<uint16_t> sel(column->size());
    for (size_t i = 0; i < sel.size(); ++i) {
        sel[i] = static_cast<uint16_t>(i);
    }
    std::vector<uint16_t> target_sel(column->size());
    uint16_t kept = 0;
    ASSIGN_OR_ABORT(kept, pred.evaluate(chunk.get(), sel.data(), static_cast<uint16_t>(sel.size()), target_sel.data()));
    EXPECT_EQ(kept, column->size());
}

// Documents *why* RuntimeFilterPredicate has to route bucket-aware filters through the
// RunningContext overload: the selection-aware overloads have no bucket_properties branch,
// so they hash with CRC32 and index bucketseq_to_instance[hash % size] instead of
// bucketseq_to_instance[bucket_id]. If this ever starts failing, those overloads have
// gained bucket handling and the routing in RuntimeFilterPredicate can be simplified.
TEST_F(RuntimeFilterPredicateTest, SelectionAwareOverloadsIgnoreBucketProperties) {
    auto* desc = make_desc(2);
    auto column = make_probe_column(256);
    auto* filter = build_partitioned_filter(desc->layout(), column.get());

    RuntimeFilter::RunningContext operator_ctx;
    operator_ctx.use_merged_selection = false;
    operator_ctx.compatibility = false;
    filter->compute_partition_index(desc->layout(), {column.get()}, &operator_ctx);
    ASSERT_EQ(operator_ctx.hash_values, operator_partitions(desc->layout(), column.get()));

    RuntimeFilter::RunningContext selection_ctx;
    selection_ctx.use_merged_selection = false;
    std::vector<uint8_t> selection(column->size(), 1);
    std::vector<uint32_t> selection_hash_values(column->size());
    filter->compute_partition_index(desc->layout(), {column.get()}, selection.data(), 0,
                                    static_cast<uint16_t>(column->size()), selection_hash_values, &selection_ctx);

    EXPECT_NE(operator_ctx.hash_values, selection_hash_values);
}

} // namespace starrocks
