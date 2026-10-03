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

#include "common/flexible_partial_update.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "base/utility/defer_op.h"

namespace starrocks {

namespace {

using Names = std::vector<std::string>;

// txn_ids that no other test in the binary uses; the registry is process-wide.
constexpr int64_t kRetainTxnId = 0x5DC6'0000'0000'0001LL;
constexpr int64_t kReleaseTxnId = 0x5DC6'0000'0000'0002LL;

} // namespace

// A sender's snapshot is loaded by position: index == set-id, each entry canonicalized exactly like
// intern() so the same set resolves to the sender's id no matter how it ordered its keys.
PARALLEL_TEST(FlexiblePartialUpdateTest, merge_assigns_ids_by_position) {
    ColumnSetDict dict;
    // Entries arrive neither sorted nor de-duplicated; an empty set is a legal column-set too.
    ASSERT_OK(dict.merge(0, {{"v2", "v1"}, {"v3", "v1", "v3"}, {}}));
    ASSERT_EQ(3u, dict.size());

    auto sets = dict.snapshot();
    ASSERT_EQ(3u, sets.size());
    EXPECT_EQ((Names{"v1", "v2"}), sets[0]);
    EXPECT_EQ((Names{"v1", "v3"}), sets[1]);
    EXPECT_TRUE(sets[2].empty());

    // The canonical-key index was built alongside the sets, so intern() resolves to the positional ids
    // regardless of the incoming key order or duplicates ...
    EXPECT_EQ(0, dict.intern({"v2", "v1"}));
    EXPECT_EQ(0, dict.intern({"v1", "v2", "v2"}));
    EXPECT_EQ(1, dict.intern({"v1", "v3"}));
    EXPECT_EQ(2, dict.intern({}));
    // ... and a set the sender never saw is appended after the snapshot's ids.
    EXPECT_EQ(3, dict.intern({"v9"}));
    EXPECT_EQ(4u, dict.size());
}

// A sender only ever appends to its dictionary, so the entries it ships extend one another; they may
// arrive in any order and more than once.
PARALLEL_TEST(FlexiblePartialUpdateTest, merge_extends_a_prefix) {
    {
        // The same snapshot twice changes nothing.
        ColumnSetDict dict;
        ASSERT_OK(dict.merge(0, {{"v1"}}));
        ASSERT_OK(dict.merge(0, {{"v1"}}));
        EXPECT_EQ(1u, dict.size());
        EXPECT_EQ(0, dict.intern({"v1"}));
    }
    {
        // A longer snapshot appends what the shorter one did not have; the shorter one arriving last is a
        // no-op.
        ColumnSetDict dict;
        ASSERT_OK(dict.merge(0, {{"v1"}}));
        ASSERT_OK(dict.merge(0, {{"v1"}, {"v2", "v3"}}));
        ASSERT_OK(dict.merge(0, {{"v1"}}));
        ASSERT_EQ(2u, dict.size());
        EXPECT_EQ(1, dict.intern({"v3", "v2"}));
    }
    {
        // The scanner's own dictionary (sender and writer in one process) receives its own snapshot back.
        ColumnSetDict dict;
        EXPECT_EQ(0, dict.intern({"v1"}));
        EXPECT_EQ(1, dict.intern({"v2"}));
        ASSERT_OK(dict.merge(0, dict.snapshot()));
        EXPECT_EQ(2u, dict.size());
    }
    {
        // An empty snapshot leaves an empty dictionary empty, and a later non-empty one still populates it.
        ColumnSetDict dict;
        ASSERT_OK(dict.merge(0, std::vector<std::vector<std::string>>{}));
        EXPECT_EQ(0u, dict.size());
        ASSERT_OK(dict.merge(0, {{"v1"}}));
        EXPECT_EQ(1u, dict.size());
    }
}

// A sender ships every entry once, with the first request whose rows use it, so each of its ranges starts
// where its previous one ended; the ranges of several senders overlap and arrive in any order.
PARALLEL_TEST(FlexiblePartialUpdateTest, merge_extends_by_ranges) {
    ColumnSetDict dict;
    ASSERT_OK(dict.merge(0, {{"v1"}, {"v2"}}));
    ASSERT_OK(dict.merge(2, {{"v3"}}));
    // Another sender, overlapping.
    ASSERT_OK(dict.merge(1, {{"v2"}, {"v3"}, {"v4"}}));
    ASSERT_EQ(4u, dict.size());
    EXPECT_EQ((Names{"v4"}), dict.snapshot(3)[0]);
    EXPECT_EQ(2u, dict.snapshot(2).size());
    EXPECT_TRUE(dict.snapshot(4).empty());
    // A range that starts beyond the end would leave set-ids without entries.
    EXPECT_FALSE(dict.merge(6, {{"v7"}}).ok());
    // A range that disagrees with the entries it overlaps comes from another set-id space.
    EXPECT_FALSE(dict.merge(3, {{"v9"}}).ok());
    EXPECT_EQ(4u, dict.size());
}

// Two dictionaries with different set-id spaces must never be combined: a row's set-id would then name
// another row's columns.
PARALLEL_TEST(FlexiblePartialUpdateTest, merge_rejects_a_different_id_space) {
    {
        ColumnSetDict dict;
        EXPECT_EQ(0, dict.intern({"local"}));
        EXPECT_FALSE(dict.merge(0, {{"v1"}, {"v2"}}).ok());
        // The dictionary is unchanged.
        ASSERT_EQ(1u, dict.size());
        EXPECT_EQ((Names{"local"}), dict.snapshot()[0]);
    }
    {
        // The same set at two positions cannot come from one sender.
        ColumnSetDict dict;
        EXPECT_FALSE(dict.merge(0, {{"v1"}, {"v1"}}).ok());
    }
    {
        // More sets than a set-id can address.
        ColumnSetDict dict;
        std::vector<std::vector<std::string>> sets(kMaxColumnSets + 1);
        for (size_t i = 0; i < sets.size(); ++i) {
            sets[i] = {"c" + std::to_string(i)};
        }
        EXPECT_FALSE(dict.merge(0, sets).ok());
        EXPECT_EQ(0u, dict.size());
    }
}

// The set-id must fit a signed int16 slot, so the dictionary stops minting at kMaxColumnSets and answers
// the invalid id for a NEW set beyond that, while sets it already holds still resolve.
PARALLEL_TEST(FlexiblePartialUpdateTest, intern_returns_invalid_id_when_set_space_is_exhausted) {
    ColumnSetDict dict;
    for (size_t i = 0; i < kMaxColumnSets; ++i) {
        ASSERT_EQ(static_cast<ColumnSetId>(i), dict.intern({"c" + std::to_string(i)}));
    }
    ASSERT_EQ(kMaxColumnSets, dict.size());
    EXPECT_EQ(kInvalidColumnSetId, dict.intern({"one_too_many"}));
    EXPECT_EQ(kMaxColumnSets, dict.size());
    // An existing set is still found; only minting is refused.
    EXPECT_EQ(0, dict.intern({"c0"}));
    EXPECT_EQ(static_cast<ColumnSetId>(kMaxColumnSets - 1), dict.intern({"c" + std::to_string(kMaxColumnSets - 1)}));
}

// Every holder of a load (json scanners, the sink, tablets channels) retains the same dictionary; the
// first one creates it, and an entry made without a reference by get_or_create() is reused, not replaced.
PARALLEL_TEST(FlexiblePartialUpdateTest, registry_retain_shares_one_dict_per_txn) {
    auto* registry = FlexiblePartialUpdateRegistry::instance();
    const int64_t txn_id = kRetainTxnId;
    const int64_t other_txn_id = kRetainTxnId + 1;
    DeferOp cleanup([&]() {
        registry->erase(txn_id);
        registry->erase(other_txn_id);
    });
    ASSERT_TRUE(registry->get(txn_id) == nullptr);

    ColumnSetDictPtr first = registry->retain(txn_id);
    ASSERT_TRUE(first != nullptr);
    ColumnSetDictPtr second = registry->retain(txn_id);
    EXPECT_EQ(first, second);
    EXPECT_EQ(first, registry->get(txn_id));
    EXPECT_EQ(first, registry->get_or_create(txn_id));
    // Distinct loads get distinct dictionaries.
    EXPECT_NE(first, registry->retain(other_txn_id));

    // The interned contents are visible to every holder through the shared object.
    EXPECT_EQ(0, first->intern({"v1"}));
    EXPECT_EQ(1u, second->size());
    EXPECT_EQ(1u, registry->get(txn_id)->size());

    // Balance the retains; the entry is dropped with the last one and erase() in the guard is then a no-op.
    registry->release(txn_id);
    registry->release(txn_id);
    registry->release(other_txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);
    EXPECT_TRUE(registry->get(other_txn_id) == nullptr);

    // retain() on an entry that get_or_create() made without a reference adopts that dictionary.
    ColumnSetDictPtr unreferenced = registry->get_or_create(txn_id);
    ASSERT_TRUE(unreferenced != nullptr);
    EXPECT_EQ(unreferenced, registry->retain(txn_id));
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);
}

// release() drops the entry exactly when the last holder lets go, tolerates releases without a matching
// retain, and never resurrects a dropped dictionary.
PARALLEL_TEST(FlexiblePartialUpdateTest, registry_release_drops_entry_with_last_holder) {
    auto* registry = FlexiblePartialUpdateRegistry::instance();
    const int64_t txn_id = kReleaseTxnId;
    DeferOp cleanup([&]() { registry->erase(txn_id); });
    ASSERT_TRUE(registry->get(txn_id) == nullptr);

    ColumnSetDictPtr dict = registry->retain(txn_id);
    ASSERT_TRUE(dict != nullptr);
    (void)registry->retain(txn_id);
    ASSERT_EQ(0, dict->intern({"v1"}));

    // Two holders: the first release keeps the entry, and what was interned, alive for the other one.
    registry->release(txn_id);
    ASSERT_EQ(dict, registry->get(txn_id));
    EXPECT_EQ(1u, registry->get(txn_id)->size());

    // The last holder is gone: the entry is dropped, while the shared_ptr a holder still owns stays valid.
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);
    EXPECT_EQ(1u, dict->size());

    // Releasing a txn with no entry is a no-op (the entry may already be gone).
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);

    // A new retain() after the drop starts a fresh, empty dictionary rather than resurrecting the old one.
    ColumnSetDictPtr fresh = registry->retain(txn_id);
    ASSERT_TRUE(fresh != nullptr);
    EXPECT_NE(dict, fresh);
    EXPECT_EQ(0u, fresh->size());
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);

    // An entry created by get_or_create() has no holder: release() must not underflow the count, and it
    // drops the unreferenced entry.
    ColumnSetDictPtr unreferenced = registry->get_or_create(txn_id);
    ASSERT_TRUE(unreferenced != nullptr);
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);

    // erase() ignores outstanding references (the last-resort / test path).
    (void)registry->retain(txn_id);
    (void)registry->retain(txn_id);
    registry->erase(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);
    registry->release(txn_id);
    EXPECT_TRUE(registry->get(txn_id) == nullptr);
}

} // namespace starrocks
