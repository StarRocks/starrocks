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

#include "exec/pipeline/scan/priority_morsel_queue.h"

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include "exec/pipeline/scan/scan_morsel.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"

namespace starrocks::pipeline {

namespace {
constexpr int32_t kSlotId = 5;

// A morsel whose scan range carries a per-file [min,max] bound on kSlotId.
MorselPtr bound_morsel(int64_t min_v, int64_t max_v, bool has_null = false, bool all_null = false) {
    TScanRange scan_range;
    THdfsScanRange hdfs;
    TExprMinMaxValue v;
    v.__set_type(TExprNodeType::INT_LITERAL);
    v.__set_has_null(has_null);
    v.__set_all_null(all_null);
    v.__set_min_int_value(min_v);
    v.__set_max_int_value(max_v);
    hdfs.min_max_values.emplace(kSlotId, v);
    hdfs.__isset.min_max_values = true;
    scan_range.__set_hdfs_scan_range(hdfs);
    return std::make_unique<ScanMorsel>(1, scan_range);
}

// A morsel with no shipped bound (empty scan range).
MorselPtr no_bound_morsel() {
    return std::make_unique<ScanMorsel>(1, TScanRange());
}

bool has_bound(const MorselPtr& m) {
    auto* sm = down_cast<ScanMorsel*>(m.get());
    auto* sr = sm->get_scan_range();
    return sr->__isset.hdfs_scan_range && sr->hdfs_scan_range.__isset.min_max_values &&
           sr->hdfs_scan_range.min_max_values.count(kSlotId) > 0;
}

int64_t min_of(const MorselPtr& m) {
    return down_cast<ScanMorsel*>(m.get())->get_scan_range()->hdfs_scan_range.min_max_values.at(kSlotId).min_int_value;
}

int64_t max_of(const MorselPtr& m) {
    return down_cast<ScanMorsel*>(m.get())->get_scan_range()->hdfs_scan_range.min_max_values.at(kSlotId).max_int_value;
}

Morsels morsels_of(std::vector<MorselPtr> v) {
    Morsels out;
    for (auto& m : v) out.emplace_back(std::move(m));
    return out;
}

// Drain the whole queue front-to-back.
std::vector<MorselPtr> drain(PriorityMorselQueue& q) {
    std::vector<MorselPtr> out;
    while (!q.empty()) {
        auto m = q.try_get();
        EXPECT_TRUE(m.ok()) << m.status().to_string();
        if (!m.value()) break;
        out.emplace_back(std::move(m.value()));
    }
    return out;
}
} // namespace

// ASC: morsels are served by ascending per-file min.
TEST(PriorityMorselQueueTest, asc_orders_by_min) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(30, 35));
                              v.emplace_back(bound_morsel(10, 15));
                              v.emplace_back(bound_morsel(20, 25));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    auto out = drain(q);
    ASSERT_EQ(3u, out.size());
    EXPECT_EQ(10, min_of(out[0]));
    EXPECT_EQ(20, min_of(out[1]));
    EXPECT_EQ(30, min_of(out[2]));
}

// DESC: morsels are served by descending per-file max.
TEST(PriorityMorselQueueTest, desc_orders_by_max) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(0, 10));
                              v.emplace_back(bound_morsel(0, 30));
                              v.emplace_back(bound_morsel(0, 20));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/true, /*nulls_first=*/false);
    auto out = drain(q);
    ASSERT_EQ(3u, out.size());
    EXPECT_EQ(30, max_of(out[0]));
    EXPECT_EQ(20, max_of(out[1]));
    EXPECT_EQ(10, max_of(out[2]));
}

// No-bound morsels (nulls trailing) sort last, after every bounded morsel.
TEST(PriorityMorselQueueTest, no_bound_served_last) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(10, 15));
                              v.emplace_back(no_bound_morsel());
                              v.emplace_back(bound_morsel(5, 8));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    auto out = drain(q);
    ASSERT_EQ(3u, out.size());
    ASSERT_TRUE(has_bound(out[0]));
    ASSERT_TRUE(has_bound(out[1]));
    EXPECT_EQ(5, min_of(out[0]));
    EXPECT_EQ(10, min_of(out[1]));
    EXPECT_FALSE(has_bound(out[2]));
}

// NULLS FIRST: null-containing files lead, before any numeric bound.
TEST(PriorityMorselQueueTest, nulls_first_served_first) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(10, 15));
                              v.emplace_back(bound_morsel(/*min*/ 0, /*max*/ 0, /*has_null=*/false, /*all_null=*/true));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/true);
    auto out = drain(q);
    ASSERT_EQ(2u, out.size());
    EXPECT_TRUE(out[0] != nullptr);
    // The all-null file leads; the bounded (min=10) file follows.
    EXPECT_TRUE(has_bound(out[1]));
    EXPECT_EQ(10, min_of(out[1]));
}

// Incrementally appended batches preserve the global ascending order.
TEST(PriorityMorselQueueTest, incremental_append_keeps_order) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(20, 25));
                              return v;
                          }()),
                          /*has_more=*/true, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    std::vector<MorselPtr> batch;
    batch.emplace_back(bound_morsel(10, 15));
    batch.emplace_back(bound_morsel(30, 35));
    ASSERT_TRUE(q.append_morsels(morsels_of(std::move(batch))).ok());

    auto out = drain(q);
    ASSERT_EQ(3u, out.size());
    EXPECT_EQ(10, min_of(out[0]));
    EXPECT_EQ(20, min_of(out[1]));
    EXPECT_EQ(30, min_of(out[2]));
}

// Equal keys keep insertion order (stable) — split pieces of one file cluster adjacently.
TEST(PriorityMorselQueueTest, equal_keys_stable_then_higher) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(10, 11)); // first 10
                              v.emplace_back(bound_morsel(10, 12)); // second 10
                              v.emplace_back(bound_morsel(20, 21));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    auto out = drain(q);
    ASSERT_EQ(3u, out.size());
    EXPECT_EQ(10, min_of(out[0]));
    EXPECT_EQ(11, max_of(out[0])); // first-inserted 10 comes first
    EXPECT_EQ(10, min_of(out[1]));
    EXPECT_EQ(12, max_of(out[1]));
    EXPECT_EQ(20, min_of(out[2]));
}

// unget reinserts a morsel at its priority position.
TEST(PriorityMorselQueueTest, unget_reinserts_in_order) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(10, 15));
                              v.emplace_back(bound_morsel(20, 25));
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    auto first = q.try_get();
    ASSERT_TRUE(first.ok());
    ASSERT_EQ(10, min_of(first.value()));
    q.unget(std::move(first.value()));
    auto out = drain(q);
    ASSERT_EQ(2u, out.size());
    EXPECT_EQ(10, min_of(out[0]));
    EXPECT_EQ(20, min_of(out[1]));
}

// Diagnostics: eligible (with bound) vs no-bound morsels are counted.
TEST(PriorityMorselQueueTest, reorder_metrics_counted) {
    PriorityMorselQueue q(morsels_of([] {
                              std::vector<MorselPtr> v;
                              v.emplace_back(bound_morsel(10, 15));
                              v.emplace_back(bound_morsel(20, 25));
                              v.emplace_back(no_bound_morsel());
                              return v;
                          }()),
                          /*has_more=*/false, kSlotId, /*desc=*/false, /*nulls_first=*/false);
    EXPECT_EQ(2, q.reorder_eligible_morsels());
    EXPECT_EQ(1, q.reorder_no_bound_morsels());
}

// The builder forces a single shared work-stealing queue (no uniform per-driver distribution).
TEST(PriorityMorselQueueTest, builder_forces_shared_queue) {
    auto builder = make_priority_morsel_queue_builder(morsels_of([] {
                                                          std::vector<MorselPtr> v;
                                                          v.emplace_back(bound_morsel(10, 15));
                                                          return v;
                                                      }()),
                                                      /*has_more_scan_ranges=*/false, /*max_dop=*/8, kSlotId,
                                                      /*desc=*/false, /*nulls_first=*/false);
    EXPECT_FALSE(builder->can_uniform_distribute());
    EXPECT_EQ(1u, builder->num_original_morsels());
    EXPECT_EQ(8u, builder->max_degree_of_parallelism());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    EXPECT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    EXPECT_EQ(8u, queue->max_degree_of_parallelism());
}

} // namespace starrocks::pipeline
