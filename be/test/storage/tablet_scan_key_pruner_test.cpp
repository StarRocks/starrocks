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

#include "storage/tablet_scan_key_pruner.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/field.h"
#include "column/schema.h"
#include "column/fixed_length_column.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"

namespace starrocks {

namespace {

TabletSchemaCSPtr make_schema(const std::vector<std::pair<std::string, std::string>>& key_columns) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(static_cast<int>(key_columns.size()));
    schema_pb.set_num_rows_per_row_block(1024);
    int32_t uid = 1;
    for (const auto& [name, type] : key_columns) {
        auto* column = schema_pb.add_column();
        column->set_unique_id(uid++);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(true);
        column->set_is_nullable(false);
        column->set_aggregation("NONE");
        if (type == "VARCHAR" || type == "CHAR") {
            column->set_length(64);
        }
    }
    // A value column so the schema is not key-only, mirroring a real table.
    auto* value = schema_pb.add_column();
    value->set_unique_id(uid);
    value->set_name("v1");
    value->set_type("INT");
    value->set_is_key(false);
    value->set_is_nullable(true);
    value->set_aggregation("NONE");
    return TabletSchema::create(schema_pb);
}

std::unique_ptr<OlapScanRange> point_range(const std::vector<std::string>& values) {
    auto range = std::make_unique<OlapScanRange>();
    range->begin_include = true;
    range->end_include = true;
    range->begin_scan_range = OlapTuple(values);
    range->end_scan_range = OlapTuple(values);
    return range;
}

std::vector<OlapScanRange*> borrow(const std::vector<std::unique_ptr<OlapScanRange>>& owned) {
    std::vector<OlapScanRange*> out;
    out.reserve(owned.size());
    for (const auto& range : owned) {
        out.emplace_back(range.get());
    }
    return out;
}

TabletHashBucketConstraint make_constraint(std::vector<int32_t> positions, int32_t bucket_id, int32_t bucket_num) {
    TabletHashBucketConstraint constraint;
    constraint.distribution_key_positions = std::move(positions);
    constraint.bucket_id = bucket_id;
    constraint.bucket_num = bucket_num;
    constraint.hash_version = TabletScanKeyPruner::kSupportedHashVersion;
    constraint.pruning_was_exact = true;
    return constraint;
}

// Computes the bucket the LOAD path would assign, independently of the pruner: build the same
// execution-layer column type the sink hashes and accumulate crc32 into a zero seed, exactly as
// OlapTablePartitionParam::_compute_hashes() does.
uint32_t load_path_bucket_for_int(int32_t value, int32_t bucket_num) {
    uint32_t seed = 0;
    auto column = Int32Column::create();
    column->append(value);
    column->crc32_hash(&seed, 0, 1);
    return seed % static_cast<uint32_t>(bucket_num);
}

uint32_t load_path_bucket_for_string(const std::string& value, int32_t bucket_num) {
    uint32_t seed = 0;
    auto column = BinaryColumn::create();
    column->append(Slice(value));
    column->crc32_hash(&seed, 0, 1);
    return seed % static_cast<uint32_t>(bucket_num);
}

} // namespace

// The load path decides where a row lives; the pruner must route the same value to the same bucket or
// it will drop scan keys that do have data. This is the contract the whole optimization rests on.
TEST(TabletScanKeyPrunerTest, RoutingAgreesWithLoadPathForInt) {
    auto schema = make_schema({{"k1", "INT"}});
    constexpr int32_t kBucketNum = 16;

    for (int32_t value : {0, 1, 7, 42, 1000, -1, -37, 2147483647}) {
        auto expected_bucket = static_cast<int32_t>(load_path_bucket_for_int(value, kBucketNum));

        std::vector<std::unique_ptr<OlapScanRange>> owned;
        owned.emplace_back(point_range({std::to_string(value)}));

        // Kept by the bucket the load path picked...
        auto kept = TabletScanKeyPruner::prune_hash(make_constraint({0}, expected_bucket, kBucketNum), *schema,
                                                    borrow(owned));
        EXPECT_FALSE(kept.fallback) << "value=" << value;
        EXPECT_FALSE(kept.exact_empty) << "value=" << value;
        EXPECT_EQ(1, kept.ranges.size()) << "value=" << value;
        EXPECT_EQ(0, kept.pruned) << "value=" << value;

        // ...and dropped by every other bucket.
        int32_t other_bucket = (expected_bucket + 1) % kBucketNum;
        auto dropped =
                TabletScanKeyPruner::prune_hash(make_constraint({0}, other_bucket, kBucketNum), *schema, borrow(owned));
        EXPECT_FALSE(dropped.fallback) << "value=" << value;
        EXPECT_TRUE(dropped.exact_empty) << "value=" << value;
        EXPECT_EQ(1, dropped.pruned) << "value=" << value;
    }
}

TEST(TabletScanKeyPrunerTest, RoutingAgreesWithLoadPathForVarchar) {
    auto schema = make_schema({{"k1", "VARCHAR"}});
    constexpr int32_t kBucketNum = 96;

    for (const std::string& value : {std::string("DC0027fa3dff407f3e73cf346eKMP"), std::string(""),
                                     std::string("a"), std::string("zh-Hans")}) {
        auto expected_bucket = static_cast<int32_t>(load_path_bucket_for_string(value, kBucketNum));

        std::vector<std::unique_ptr<OlapScanRange>> owned;
        owned.emplace_back(point_range({value}));

        auto kept = TabletScanKeyPruner::prune_hash(make_constraint({0}, expected_bucket, kBucketNum), *schema,
                                                    borrow(owned));
        EXPECT_FALSE(kept.fallback) << "value=" << value;
        EXPECT_EQ(1, kept.ranges.size()) << "value=" << value;
    }
}

// Only the keys of this bucket survive; the rest are structurally impossible here.
TEST(TabletScanKeyPrunerTest, KeepsOnlyOwnBucketAcrossManyKeys) {
    auto schema = make_schema({{"k1", "INT"}});
    constexpr int32_t kBucketNum = 8;
    constexpr int32_t kBucketId = 3;

    std::vector<std::unique_ptr<OlapScanRange>> owned;
    size_t expected_kept = 0;
    for (int32_t value = 0; value < 100; ++value) {
        owned.emplace_back(point_range({std::to_string(value)}));
        if (static_cast<int32_t>(load_path_bucket_for_int(value, kBucketNum)) == kBucketId) {
            ++expected_kept;
        }
    }
    ASSERT_GT(expected_kept, 0u);

    auto result =
            TabletScanKeyPruner::prune_hash(make_constraint({0}, kBucketId, kBucketNum), *schema, borrow(owned));
    EXPECT_FALSE(result.fallback);
    EXPECT_FALSE(result.exact_empty);
    EXPECT_EQ(expected_kept, result.ranges.size());
    EXPECT_EQ(static_cast<int64_t>(100 - expected_kept), result.pruned);
}

// An empty input carries no scan-key predicate at all. Reporting exact_empty here would make the
// caller skip a tablet that should be scanned in full.
TEST(TabletScanKeyPrunerTest, EmptyInputIsNotExactEmpty) {
    auto schema = make_schema({{"k1", "INT"}});
    std::vector<OlapScanRange*> none;

    auto result = TabletScanKeyPruner::prune_hash(make_constraint({0}, 0, 8), *schema, none);
    EXPECT_FALSE(result.exact_empty);
    EXPECT_FALSE(result.fallback);
    EXPECT_TRUE(result.ranges.empty());
    EXPECT_EQ(0, result.pruned);
}

// A distribution column left open by the predicate cannot be routed, so the range stays. This keeps
// the optimization correct for `k1 > 10` and for the default all-infinity range.
TEST(TabletScanKeyPrunerTest, KeepsRangeWhenDistributionColumnIsNotFixed) {
    auto schema = make_schema({{"k1", "INT"}});
    std::vector<std::unique_ptr<OlapScanRange>> owned;

    // Open range: begin != end.
    auto open = std::make_unique<OlapScanRange>();
    open->begin_include = true;
    open->end_include = true;
    open->begin_scan_range = OlapTuple(std::vector<std::string>{"10"});
    open->end_scan_range = OlapTuple(std::vector<std::string>{"20"});
    owned.emplace_back(std::move(open));

    // Default range: -oo / +oo sentinels.
    owned.emplace_back(std::make_unique<OlapScanRange>());

    // Point value but exclusive bound, which describes an empty range rather than a routable key.
    auto exclusive = point_range({"7"});
    exclusive->begin_include = false;
    owned.emplace_back(std::move(exclusive));

    // NULL bound carries no hashable value.
    auto null_range = std::make_unique<OlapScanRange>();
    null_range->begin_include = true;
    null_range->end_include = true;
    null_range->begin_scan_range.add_null();
    null_range->end_scan_range.add_null();
    owned.emplace_back(std::move(null_range));

    auto result = TabletScanKeyPruner::prune_hash(make_constraint({0}, 0, 8), *schema, borrow(owned));
    EXPECT_FALSE(result.fallback);
    EXPECT_FALSE(result.exact_empty);
    EXPECT_EQ(owned.size(), result.ranges.size());
    EXPECT_EQ(0, result.pruned);
}

// A multi-column distribution key routes only when every column is pinned, and hashes in DDL order.
TEST(TabletScanKeyPrunerTest, MultiColumnDistributionKey) {
    auto schema = make_schema({{"k1", "INT"}, {"k2", "VARCHAR"}});
    constexpr int32_t kBucketNum = 12;

    uint32_t seed = 0;
    auto k1 = Int32Column::create();
    k1->append(5);
    k1->crc32_hash(&seed, 0, 1);
    auto k2 = BinaryColumn::create();
    k2->append(Slice("abc"));
    k2->crc32_hash(&seed, 0, 1);
    auto expected_bucket = static_cast<int32_t>(seed % kBucketNum);

    std::vector<std::unique_ptr<OlapScanRange>> owned;
    owned.emplace_back(point_range({"5", "abc"}));
    auto result = TabletScanKeyPruner::prune_hash(make_constraint({0, 1}, expected_bucket, kBucketNum), *schema,
                                                 borrow(owned));
    EXPECT_FALSE(result.fallback);
    EXPECT_EQ(1, result.ranges.size());

    // Only the leading column pinned -> not routable, keep it.
    std::vector<std::unique_ptr<OlapScanRange>> partial;
    auto prefix = std::make_unique<OlapScanRange>();
    prefix->begin_include = true;
    prefix->end_include = true;
    prefix->begin_scan_range = OlapTuple(std::vector<std::string>{"5", "abc"});
    prefix->end_scan_range = OlapTuple(std::vector<std::string>{"5", "xyz"});
    partial.emplace_back(std::move(prefix));
    auto partial_result = TabletScanKeyPruner::prune_hash(make_constraint({0, 1}, expected_bucket, kBucketNum),
                                                         *schema, borrow(partial));
    EXPECT_FALSE(partial_result.fallback);
    EXPECT_EQ(1, partial_result.ranges.size());
    EXPECT_EQ(0, partial_result.pruned);
}

// Anything the pruner cannot justify degrades the whole tablet, never a subset.
TEST(TabletScanKeyPrunerTest, MalformedTopologyFallsBack) {
    auto schema = make_schema({{"k1", "INT"}});
    std::vector<std::unique_ptr<OlapScanRange>> owned;
    owned.emplace_back(point_range({"1"}));
    owned.emplace_back(point_range({"2"}));

    struct Case {
        const char* name;
        TabletHashBucketConstraint constraint;
    };
    std::vector<Case> cases;

    cases.push_back({"unknown hash version", make_constraint({0}, 0, 8)});
    cases.back().constraint.hash_version = TabletScanKeyPruner::kSupportedHashVersion + 1;

    cases.push_back({"bucket_id out of range", make_constraint({0}, 8, 8)});
    cases.push_back({"negative bucket_id", make_constraint({0}, -1, 8)});
    cases.push_back({"zero bucket_num", make_constraint({0}, 0, 0)});
    cases.push_back({"no positions", make_constraint({}, 0, 8)});
    cases.push_back({"position beyond sort key", make_constraint({9}, 0, 8)});

    for (const auto& c : cases) {
        auto result = TabletScanKeyPruner::prune_hash(c.constraint, *schema, borrow(owned));
        EXPECT_TRUE(result.fallback) << c.name;
        EXPECT_FALSE(result.exact_empty) << c.name;
        EXPECT_EQ(owned.size(), result.ranges.size()) << c.name;
        EXPECT_EQ(0, result.pruned) << c.name;
    }
}

// DECIMAL columns carry precision/scale that a bare LogicalType loses, so they are excluded until the
// FE/BE/load type matrix covers them. Excluded types must degrade, never mis-route.
TEST(TabletScanKeyPrunerTest, UnsupportedDistributionTypeFallsBack) {
    EXPECT_FALSE(TabletScanKeyPruner::is_routable_type(TYPE_DECIMAL32));
    EXPECT_FALSE(TabletScanKeyPruner::is_routable_type(TYPE_DECIMAL64));
    EXPECT_FALSE(TabletScanKeyPruner::is_routable_type(TYPE_DECIMAL128));
    EXPECT_TRUE(TabletScanKeyPruner::is_routable_type(TYPE_INT));
    EXPECT_TRUE(TabletScanKeyPruner::is_routable_type(TYPE_VARCHAR));
    EXPECT_TRUE(TabletScanKeyPruner::is_routable_type(TYPE_CHAR));
    EXPECT_TRUE(TabletScanKeyPruner::is_routable_type(TYPE_DATE));

    auto schema = make_schema({{"k1", "DECIMAL64"}});
    std::vector<std::unique_ptr<OlapScanRange>> owned;
    owned.emplace_back(point_range({"1.50"}));

    auto result = TabletScanKeyPruner::prune_hash(make_constraint({0}, 0, 8), *schema, borrow(owned));
    EXPECT_TRUE(result.fallback);
    EXPECT_EQ(1, result.ranges.size());
    EXPECT_EQ(0, result.pruned);
}

// The owning-container overload must behave identically to the borrowing one.
TEST(TabletScanKeyPrunerTest, OwningOverloadMatchesBorrowingOverload) {
    auto schema = make_schema({{"k1", "INT"}});
    constexpr int32_t kBucketNum = 8;
    constexpr int32_t kBucketId = 2;

    std::vector<std::unique_ptr<OlapScanRange>> owned;
    for (int32_t value = 0; value < 32; ++value) {
        owned.emplace_back(point_range({std::to_string(value)}));
    }
    auto constraint = make_constraint({0}, kBucketId, kBucketNum);

    auto from_owning = TabletScanKeyPruner::prune_hash(constraint, *schema, owned);
    auto from_borrowing = TabletScanKeyPruner::prune_hash(constraint, *schema, borrow(owned));
    EXPECT_EQ(from_borrowing.ranges.size(), from_owning.ranges.size());
    EXPECT_EQ(from_borrowing.pruned, from_owning.pruned);
    EXPECT_EQ(from_borrowing.exact_empty, from_owning.exact_empty);
    EXPECT_EQ(from_borrowing.fallback, from_owning.fallback);
}

// ---------------------------------------------------------------------------------------------
// RANGE distribution: a scan key is dropped only when it provably cannot intersect the tablet range.
// A wrong "disjoint" verdict silently loses rows, so every case below pins the direction of the error.
// ---------------------------------------------------------------------------------------------

namespace {

Schema int_schema(int num_cols) {
    Schema s;
    for (int i = 0; i < num_cols; i++) {
        auto f = std::make_shared<Field>(static_cast<ColumnId>(i), "k" + std::to_string(i),
                                         get_type_info(TYPE_INT), false);
        f->set_is_key(true);
        s.append(f);
    }
    return s;
}

SeekTuple int_tuple(const std::vector<int32_t>& vals, int num_cols) {
    std::vector<Datum> ds;
    ds.reserve(vals.size());
    for (int32_t v : vals) {
        ds.emplace_back(v);
    }
    return SeekTuple(int_schema(num_cols), std::move(ds));
}

// Builds [lo, hi] over `num_cols` columns; an empty vector means unbounded on that side.
SeekRange int_range(const std::vector<int32_t>& lo, bool inc_lo, const std::vector<int32_t>& hi, bool inc_hi,
                    int num_cols) {
    SeekRange r(lo.empty() ? SeekTuple() : int_tuple(lo, num_cols),
                hi.empty() ? SeekTuple() : int_tuple(hi, num_cols));
    r.set_inclusive_lower(inc_lo);
    r.set_inclusive_upper(inc_hi);
    return r;
}

bool disjoint(const SeekRange& q, const SeekRange& t, int num_cols) {
    return seek_range_disjoint_from_tablet_range(q, t, static_cast<size_t>(num_cols));
}

} // namespace

TEST(TabletScanKeyPrunerTest, RangeDisjointSingleColumn) {
    // tablet owns [100, 200)
    auto tablet = int_range({100}, true, {200}, false, 1);

    // Point keys below, inside and above.
    EXPECT_TRUE(disjoint(int_range({42}, true, {42}, true, 1), tablet, 1));
    EXPECT_FALSE(disjoint(int_range({150}, true, {150}, true, 1), tablet, 1));
    EXPECT_TRUE(disjoint(int_range({260}, true, {260}, true, 1), tablet, 1));

    // Exactly on the closed lower bound: overlaps.
    EXPECT_FALSE(disjoint(int_range({100}, true, {100}, true, 1), tablet, 1));
    // Exactly on the open upper bound: the tablet excludes it.
    EXPECT_TRUE(disjoint(int_range({200}, true, {200}, true, 1), tablet, 1));

    // Overlapping spans stay.
    EXPECT_FALSE(disjoint(int_range({50}, true, {120}, true, 1), tablet, 1));
    EXPECT_FALSE(disjoint(int_range({180}, true, {900}, true, 1), tablet, 1));
    EXPECT_FALSE(disjoint(int_range({0}, true, {999}, true, 1), tablet, 1));
}

TEST(TabletScanKeyPrunerTest, RangeUnboundedSidesNeverProveDisjoint) {
    auto tablet = int_range({100}, true, {200}, false, 1);
    EXPECT_TRUE(disjoint(int_range({}, false, {50}, true, 1), tablet, 1));   // upper 50 < tablet lower 100
    EXPECT_FALSE(disjoint(int_range({}, false, {150}, true, 1), tablet, 1)); // overlaps
    EXPECT_TRUE(disjoint(int_range({260}, true, {}, false, 1), tablet, 1));  // lower 260 >= tablet upper
    EXPECT_FALSE(disjoint(int_range({150}, true, {}, false, 1), tablet, 1)); // overlaps

    // A fully unbounded tablet range or query range proves nothing.
    EXPECT_FALSE(disjoint(int_range({42}, true, {42}, true, 1), int_range({}, false, {}, false, 1), 1));
    EXPECT_FALSE(disjoint(int_range({}, false, {}, false, 1), tablet, 1));
}

// A prefix bound must cover every extension of that prefix. Getting the padding backwards here is the
// classic way to silently drop rows, so both directions are pinned.
TEST(TabletScanKeyPrunerTest, RangePrefixBoundsCoverExtensions) {
    // tablet owns [(100), (200)) over two sort key columns -- bounds are 1-column prefixes.
    auto tablet = int_range({100}, true, {200}, false, 1);

    // (100, 5) is inside: the closed prefix lower bound covers all extensions of 100.
    EXPECT_FALSE(disjoint(int_range({100, 5}, true, {100, 5}, true, 2), tablet, 2));
    // (99, 999) is below every extension of 100.
    EXPECT_TRUE(disjoint(int_range({99, 999}, true, {99, 999}, true, 2), tablet, 2));
    // (200, 0) is excluded: the open prefix upper bound excludes all extensions of 200.
    EXPECT_TRUE(disjoint(int_range({200, 0}, true, {200, 0}, true, 2), tablet, 2));
    // (199, 999) is the last key the tablet owns.
    EXPECT_FALSE(disjoint(int_range({199, 999}, true, {199, 999}, true, 2), tablet, 2));

    // Mirror: a prefix QUERY bound against a full-arity tablet range.
    auto tablet2 = int_range({100, 50}, true, {100, 80}, false, 2);
    // Query prefix 100 spans (100,*) which overlaps [ (100,50), (100,80) ).
    EXPECT_FALSE(disjoint(int_range({100}, true, {100}, true, 2), tablet2, 2));
    // Query prefix 99 cannot reach it.
    EXPECT_TRUE(disjoint(int_range({99}, true, {99}, true, 2), tablet2, 2));
}

TEST(TabletScanKeyPrunerTest, RangeOpenBoundsTouchingExactly) {
    // Query (,50] vs tablet [50,): closed/closed touch at 50 -> overlap.
    EXPECT_FALSE(disjoint(int_range({}, false, {50}, true, 1), int_range({50}, true, {}, false, 1), 1));
    // Query (,50) vs tablet [50,): query excludes 50 -> disjoint.
    EXPECT_TRUE(disjoint(int_range({}, false, {50}, false, 1), int_range({50}, true, {}, false, 1), 1));
    // Query (,50] vs tablet (50,): tablet excludes 50 -> disjoint.
    EXPECT_TRUE(disjoint(int_range({}, false, {50}, true, 1), int_range({50}, false, {}, false, 1), 1));
}

// Regression: the caller used to take the arity from Segment::num_sort_key_columns(), which reads a
// decoder that is only published once the full sort key index is loaded and is null otherwise -- a
// release build has no DCHECK to catch it, so a segment without that index crashed the scan
// (SIGSEGV in _get_row_ranges_by_key_ranges). The arity must come from the tablet schema instead.
// Here we pin the property that makes the schema a valid source: a bound shorter than the arity is
// padded, so passing the schema arity for tuples of any length is well-defined.
TEST(TabletScanKeyPrunerTest, RangeArityLargerThanBoundsIsWellDefined) {
    // Bounds carry 1 value each while the sort key has 3 columns.
    auto tablet = int_range({100}, true, {200}, false, 1);
    for (size_t arity : {size_t{1}, size_t{2}, size_t{3}, size_t{8}}) {
        EXPECT_TRUE(seek_range_disjoint_from_tablet_range(int_range({42}, true, {42}, true, 1), tablet, arity))
                << "arity=" << arity;
        EXPECT_FALSE(seek_range_disjoint_from_tablet_range(int_range({150}, true, {150}, true, 1), tablet, arity))
                << "arity=" << arity;
    }
}

TEST(TabletScanKeyPrunerTest, RangeZeroSortKeyColumnsProvesNothing) {
    auto tablet = int_range({100}, true, {200}, false, 1);
    EXPECT_FALSE(disjoint(int_range({42}, true, {42}, true, 1), tablet, 0));
}

} // namespace starrocks
