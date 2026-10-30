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

#include "storage_primitive/range.h"

#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <sstream>

namespace starrocks {

inline std::string to_bitmap_string(const uint8_t* bitmap, size_t n) {
    std::string s;
    for (size_t i = 0; i < n; i++) {
        s.push_back(bitmap[i] ? '1' : '0');
    }
    return s;
}

TEST(SparseRangeTest, range_union) {
    SparseRange<> range;
    ASSERT_TRUE(range.empty());
    EXPECT_EQ(0, range.span_size());

    // Add an empty range: ignored
    range.add({10, 0});
    ASSERT_TRUE(range.empty());
    EXPECT_EQ(0u, range.span_size());

    // Add [10, 20)
    range.add({10, 20});
    ASSERT_FALSE(range.empty());
    EXPECT_EQ(10u, range.span_size());
    EXPECT_EQ("([10,20))", range.to_string());

    // Add [30, 40)
    range.add({30, 40});
    EXPECT_EQ(20u, range.span_size());
    EXPECT_EQ("([10,20), [30,40))", range.to_string());

    // Add [40, 50)
    range.add({40, 50});
    EXPECT_EQ("([10,20), [30,50))", range.to_string());

    // Add [8, 9)
    range.add({8, 9});
    EXPECT_EQ("([8,9), [10,20), [30,50))", range.to_string());

    // Add [25, 27)
    range.add({25, 27});
    EXPECT_EQ("([8,9), [10,20), [25,27), [30,50))", range.to_string());

    // Add [9, 21)
    range.add({9, 21});
    EXPECT_EQ("([8,21), [25,27), [30,50))", range.to_string());

    // Add [28, 60)
    range.add({28, 60});
    EXPECT_EQ("([8,21), [25,27), [28,60))", range.to_string());

    // Add [27, 28)
    range.add({27, 28});
    EXPECT_EQ("([8,21), [25,60))", range.to_string());

    // Add [6, 70)
    range.add({6, 70});
    EXPECT_EQ("([6,70))", range.to_string());
}

TEST(SparseRangeTest, range_intersection) {
    SparseRange<> r1({{1, 10}, {20, 40}, {50, 70}});
    SparseRange<> r2{{0, 100}};
    SparseRange<> r3{};
    SparseRange<> r4{{2, 30}};
    SparseRange<> r5{{1, 20}, {25, 26}, {30, 65}};

    auto r = r1.intersection(r1);
    EXPECT_EQ(r1, r);

    r = r1.intersection(r2);
    EXPECT_EQ(r1, r);

    r = r1.intersection(r3);
    EXPECT_EQ(SparseRange(), r);

    r = r1.intersection(r4);
    EXPECT_EQ(SparseRange({{2, 10}, {20, 30}}), r);

    r = r1.intersection(r5);
    EXPECT_EQ(SparseRange({{1, 10}, {25, 26}, {30, 40}, {50, 65}}), r);
}

TEST(SparseRangeIteratorTest, covered_ranges) {
    SparseRange<> r1({{0, 10}, {20, 40}, {50, 70}});
    SparseRangeIterator<> iter = r1.new_iterator();
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 20; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 21; i <= 50; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 51; i <= 100; i++) {
        EXPECT_EQ(3u, iter.covered_ranges(i)) << "i=" << i;
    }

    (void)iter.next(5);
    // [5, 10), [20, 40), [50, 70)
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 15; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 16; i <= 45; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 46; i <= 100; i++) {
        EXPECT_EQ(3u, iter.covered_ranges(i)) << "i=" << i;
    }

    (void)iter.next(10);
    // [20, 40), [50, 70)
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 30; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 31; i <= 55; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
}

std::string dump_range_iter(const SparseRangeIterator<>& iter) {
    std::stringstream ss;
    if (!iter.has_more()) {
        ss << "[]";
        return ss.str();
    }
    bool fst = true;
    for (size_t i = iter._index; i < iter._range->_ranges.size(); ++i) {
        if (!fst) {
            ss << ",";
        }
        ss << "[";
        if (fst) {
            ss << iter._next_rowid;
        } else {
            ss << iter._range->_ranges[i].begin();
        }
        ss << ",";
        ss << iter._range->_ranges[i].end();
        ss << "]";
        fst = false;
    }
    return ss.str();
}

TEST(SparseRangeIteratorTest, intersect_test) {
    // test intersection
    {
        SparseRange<> r1(0, 4096);
        SparseRangeIterator<> iter = r1.new_iterator();
        iter.skip(1000);
        SparseRange<> r2({{0, 10}, {20, 40}, {50, 70}});
        SparseRange<> r3;
        auto iter2 = iter.intersection(r2, &r3);
        EXPECT_STREQ(dump_range_iter(iter2).data(), "[]");
    }
    {
        SparseRange<> r1(0, 4096);
        SparseRangeIterator<> iter = r1.new_iterator();
        iter.skip(30);
        SparseRange<> r2({{0, 10}, {20, 40}, {50, 7000}});
        SparseRange<> r3;
        auto iter2 = iter.intersection(r2, &r3);
        EXPECT_STREQ(dump_range_iter(iter2).data(), "[30,40],[50,4096]");
    }
    {
        SparseRange<> r1(0, 4096);
        SparseRangeIterator<> iter = r1.new_iterator();
        iter.skip(30);
        SparseRange<> r2({{1000, 1500}, {2000, 25000}, {3000, 7000}});
        SparseRange<> r3;
        auto iter2 = iter.intersection(r2, &r3);
        EXPECT_STREQ(dump_range_iter(iter2).data(), "[1000,1500],[2000,4096]");
    }
    {
        SparseRange<> r1(0, 100);
        SparseRangeIterator<> iter = r1.new_iterator();
        iter.skip(30);
        SparseRange<> r2({{10, 200}, {2000, 25000}, {3000, 7000}});
        SparseRange<> r3;
        auto iter2 = iter.intersection(r2, &r3);
        EXPECT_STREQ(dump_range_iter(iter2).data(), "[30,100]");
    }
}

TEST(SparseRangeIteratorTest, remaining_rows) {
    // Three disjoint ranges, 10 + 20 + 20 = 50 rows in total.
    SparseRange<> r1({{0, 10}, {20, 40}, {50, 70}});
    SparseRangeIterator<> iter = r1.new_iterator();

    // Fresh iterator: every row remains.
    EXPECT_EQ(50u, iter.remaining_rows());

    // Consume 5 rows inside the first range (still _index == 0).
    (void)iter.next(5);
    EXPECT_EQ(45u, iter.remaining_rows());

    // Consume the remaining 5 rows of the first range; the iterator now sits at the
    // start of the second range (_index == 1, _next_rowid == 20). This is the case the
    // fix targets: the un-consumed part of the *current* range must be measured against
    // _ranges[_index].end(), not _ranges[0].end(). The buggy version treated the current
    // partial range as empty and returned 20 (dropping [20,40)'s 20 rows).
    (void)iter.next(5);
    EXPECT_EQ(40u, iter.remaining_rows());

    // Advance 5 rows into the second range (_index == 1, _next_rowid == 25).
    (void)iter.next(5);
    EXPECT_EQ(35u, iter.remaining_rows());

    // Finish the second range; now at the third range (_index == 2, _next_rowid == 50).
    (void)iter.next(15);
    EXPECT_EQ(20u, iter.remaining_rows());

    // Drain the last range.
    (void)iter.next(20);
    EXPECT_FALSE(iter.has_more());
    EXPECT_EQ(0u, iter.remaining_rows());
}

// Regression test for issue #75203: CN SIGSEGV @0x0 in
// PhysicalSplitMorselQueue::_try_get_split_from_single_tablet().
//
// The physical-split loop evaluates `_segment_range_iter.has_more()` in its while
// condition. On empty/edge tablets the iterator can still be default-constructed
// (its _range pointer is null) when has_more() is reached. Before the fix has_more()
// did an unconditional `_index < _range->_ranges.size()`, dereferencing the null
// _range -> std::vector::size() read at address 0 -> SIGSEGV @0x0 (exactly the crash
// stack in the issue: has_more() inlined into _try_get_split_from_single_tablet()).
//
// A default-constructed iterator has no ranges, so has_more() must report false
// rather than dereference null. Without the fix this line is an ASAN
// null/heap-buffer-overflow read; with it, it returns false.
TEST(SparseRangeIteratorTest, has_more_on_default_constructed_is_null_safe) {
    SparseRangeIterator<> iter; // default-constructed: _range == nullptr
    EXPECT_FALSE(iter.has_more());
}

// The O(n * m) nested loop SparseRange::intersection() used before it became a linear merge. Every
// pair of sub-ranges is checked and the pieces are appended without merging, in |lhs| order.
static std::vector<Range<>> legacy_intersection(const SparseRange<>& lhs, const SparseRange<>& rhs) {
    std::vector<Range<>> result;
    for (size_t i = 0; i < lhs.size(); i++) {
        for (size_t j = 0; j < rhs.size(); j++) {
            if (lhs[i].has_intersection(rhs[j])) {
                result.emplace_back(lhs[i].intersection(rhs[j]));
            }
        }
    }
    return result;
}

// What SparseRange::operator|= did before it became a linear merge: add() every range of |rhs| in turn.
static std::vector<Range<>> legacy_union(const SparseRange<>& lhs, const SparseRange<>& rhs) {
    SparseRange<> result = lhs;
    for (size_t i = 0; i < rhs.size(); i++) {
        result.add(rhs[i]);
    }
    std::vector<Range<>> ranges;
    for (size_t i = 0; i < result.size(); i++) {
        ranges.emplace_back(result[i]);
    }
    return ranges;
}

static std::vector<Range<>> ranges_of(const SparseRange<>& r) {
    std::vector<Range<>> ranges;
    for (size_t i = 0; i < r.size(); i++) {
        ranges.emplace_back(r[i]);
    }
    return ranges;
}

static void check_against_legacy(const SparseRange<>& lhs, const SparseRange<>& rhs) {
    SCOPED_TRACE("lhs=" + lhs.to_string() + " rhs=" + rhs.to_string());
    EXPECT_EQ(legacy_intersection(lhs, rhs), ranges_of(lhs.intersection(rhs)));
    EXPECT_EQ(legacy_intersection(rhs, lhs), ranges_of(rhs.intersection(lhs)));
    SparseRange<> and_result = lhs;
    and_result &= rhs;
    EXPECT_EQ(legacy_intersection(lhs, rhs), ranges_of(and_result));

    SparseRange<> or_result = lhs;
    or_result |= rhs;
    EXPECT_EQ(legacy_union(lhs, rhs), ranges_of(or_result));
    or_result = rhs;
    or_result |= lhs;
    EXPECT_EQ(legacy_union(rhs, lhs), ranges_of(or_result));
}

// A sorted range built by add(), so abutting sub-ranges are merged.
static SparseRange<> random_range(std::mt19937& rng, rowid_t max_gap, rowid_t max_len, size_t n) {
    SparseRange<> r;
    rowid_t pos = 0;
    for (size_t i = 0; i < n; i++) {
        pos += rng() % (max_gap + 1);
        const rowid_t len = 1 + rng() % max_len;
        r.add({pos, pos + len});
        pos += len;
    }
    return r;
}

// Cut every sub-range of |r| at random points into abutting pieces, which add() would have merged back.
// intersection() of such ranges keeps the cuts, so they reach |= in practice.
static SparseRange<> split_into_abutting_pieces(std::mt19937& rng, const SparseRange<>& r) {
    SparseRange<> result;
    for (size_t i = 0; i < r.size(); i++) {
        rowid_t begin = r[i].begin();
        while (begin < r[i].end()) {
            const rowid_t end = std::min<rowid_t>(r[i].end(), begin + 1 + rng() % 4);
            result._add_uncheck({begin, end});
            begin = end;
        }
    }
    return result;
}

TEST(SparseRangeTest, intersection_and_union_edge_cases) {
    const SparseRange<> empty;
    const SparseRange<> r1({{10, 20}, {30, 40}});
    // empty
    check_against_legacy(empty, empty);
    check_against_legacy(empty, r1);
    EXPECT_TRUE(r1.intersection(empty).empty());
    // disjoint
    check_against_legacy(r1, SparseRange<>({{0, 5}, {20, 30}, {45, 50}}));
    EXPECT_TRUE(r1.intersection(SparseRange<>({{0, 5}, {20, 30}, {45, 50}})).empty());
    // abutting: [20, 30) touches both ranges of |r1| but intersects neither
    check_against_legacy(r1, SparseRange<>({{0, 10}, {20, 30}, {40, 50}}));
    check_against_legacy(r1, SparseRange<>(20, 30));
    // containment, both ways
    check_against_legacy(r1, SparseRange<>(0, 100));
    check_against_legacy(r1, SparseRange<>({{12, 15}, {16, 18}, {31, 39}}));
    EXPECT_EQ(SparseRange<>({{12, 15}, {16, 18}, {31, 39}}),
              r1.intersection(SparseRange<>({{12, 15}, {16, 18}, {31, 39}})));
    // one range spanning several
    check_against_legacy(r1, SparseRange<>({{15, 35}, {38, 60}}));
    EXPECT_EQ(SparseRange<>({{15, 20}, {30, 35}, {38, 40}}), r1.intersection(SparseRange<>({{15, 35}, {38, 60}})));
    // equal ends advance either side
    check_against_legacy(r1, SparseRange<>({{5, 20}, {25, 40}}));
}

// A range may hold abutting sub-ranges, e.g. ([0,3), [3,5)). add() never merges two of them unless an
// added range touches both, and |= must keep that.
TEST(SparseRangeTest, union_keeps_unmerged_abutting_ranges) {
    SparseRange<> abutting;
    abutting._add_uncheck({0, 3});
    abutting._add_uncheck({3, 5});
    ASSERT_EQ(2u, abutting.size());
    // intersection() keeps the cut
    EXPECT_EQ(2u, SparseRange<>(0, 5).intersection(abutting).size());
    check_against_legacy(abutting, SparseRange<>(10, 12));
    check_against_legacy(abutting, SparseRange<>(1, 2));
    check_against_legacy(abutting, SparseRange<>(3, 4));
    check_against_legacy(abutting, SparseRange<>(5, 6));
    check_against_legacy(abutting, SparseRange<>(2, 4));
    check_against_legacy(abutting, abutting);
}

TEST(SparseRangeTest, intersection_and_union_random_against_legacy) {
    std::mt19937 rng(20261003);
    for (int round = 0; round < 2000; round++) {
        const size_t n = rng() % 40;
        const size_t m = rng() % 40;
        const SparseRange<> lhs = random_range(rng, 1 + rng() % 8, 1 + rng() % 8, n);
        const SparseRange<> rhs = random_range(rng, 1 + rng() % 8, 1 + rng() % 8, m);
        check_against_legacy(lhs, rhs);
        const SparseRange<> lhs_pieces = split_into_abutting_pieces(rng, lhs);
        const SparseRange<> rhs_pieces = split_into_abutting_pieces(rng, rhs);
        check_against_legacy(lhs_pieces, rhs);
        check_against_legacy(lhs, rhs_pieces);
        check_against_legacy(lhs_pieces, rhs_pieces);
    }
}

// A reversed range (split_and_reverse()) still goes through the original nested loop.
TEST(SparseRangeTest, intersection_of_unsorted_range) {
    std::mt19937 rng(7);
    for (int round = 0; round < 200; round++) {
        SparseRange<> lhs = random_range(rng, 5, 5, 30);
        lhs.split_and_reverse(64, 4);
        ASSERT_FALSE(lhs.is_sorted());
        const SparseRange<> rhs = random_range(rng, 5, 5, 30);
        EXPECT_EQ(legacy_intersection(lhs, rhs), ranges_of(lhs.intersection(rhs)));
        EXPECT_EQ(legacy_intersection(rhs, lhs), ranges_of(rhs.intersection(lhs)));
    }
}

static SparseRange<> from_ranges(const std::vector<Range<>>& ranges) {
    SparseRange<> r;
    for (const auto& range : ranges) {
        r._add_uncheck(range);
    }
    return r;
}

// intersection() of a reversed range returns its pieces out of order but still flagged sorted, as it always
// did. Intersecting or unioning that result again must not take the linear merge, which would drop rows.
TEST(SparseRangeTest, chained_operations_on_reversed_range) {
    SparseRange<> reversed({{0, 2}, {5, 7}});
    reversed.split_and_reverse(1, 1);
    ASSERT_FALSE(reversed.is_sorted());
    const SparseRange<> first = reversed.intersection(SparseRange<>(0, 7));
    EXPECT_EQ(SparseRange<>(0, 2), first.intersection(SparseRange<>(0, 2)));
    EXPECT_EQ(legacy_intersection(from_ranges(legacy_intersection(reversed, SparseRange<>(0, 7))), SparseRange<>(0, 2)),
              ranges_of(first.intersection(SparseRange<>(0, 2))));

    std::mt19937 rng(11);
    for (int round = 0; round < 500; round++) {
        SparseRange<> lhs = random_range(rng, 5, 5, 30);
        lhs.split_and_reverse(64, 4);
        const SparseRange<> rhs = random_range(rng, 5, 5, 30);
        const SparseRange<> rhs2 = random_range(rng, 5, 5, 30);
        const SparseRange<> chained = lhs.intersection(rhs);
        const SparseRange<> legacy_chained = from_ranges(legacy_intersection(lhs, rhs));
        ASSERT_EQ(ranges_of(legacy_chained), ranges_of(chained));
        check_against_legacy(chained, rhs2);
        check_against_legacy(rhs2, chained);

#ifdef NDEBUG
        // The iterator path, with the out-of-order range as the iterated one. Its DCHECKs reject such a range,
        // so this only runs with them compiled out, where it must keep the old nested loop's result.
        SparseRange<> iter_result;
        chained.new_iterator().intersection(rhs2, &iter_result);
        std::vector<Range<>> expected;
        for (size_t i = 0; i < chained.size(); i++) {
            for (size_t j = 0; j < rhs2.size(); j++) {
                if (chained[i].end() < rhs2[j].begin()) break;
                if (chained[i].has_intersection(rhs2[j])) expected.emplace_back(chained[i].intersection(rhs2[j]));
            }
        }
        ASSERT_EQ(expected, ranges_of(iter_result));
#endif
    }
}

// The primary key table case that made prepared split scans dozens of times slower: a delvec that
// deletes every 10th row leaves the segment scan range in hundreds of thousands of pieces, and the
// precomputed morsel range is as fragmented. With the nested loop this was ~10^11 comparisons.
TEST(SparseRangeTest, intersection_and_union_of_fragmented_ranges_is_linear) {
    constexpr rowid_t kPieces = 500000;
    SparseRange<> scan_range;   // rows k % 10 != 0
    SparseRange<> morsel_range; // rows k % 7 != 0, shifted so the boundaries interleave
    for (rowid_t k = 0; k < kPieces; k++) {
        scan_range.add({k * 10 + 1, k * 10 + 10});
        morsel_range.add({k * 7 + 1, k * 7 + 7});
    }
    ASSERT_EQ(kPieces, scan_range.size());
    ASSERT_EQ(kPieces, morsel_range.size());

    const auto start = std::chrono::steady_clock::now();
    SparseRange<> intersected = scan_range;
    intersected &= morsel_range;
    SparseRange<> unioned = scan_range;
    unioned |= morsel_range;
    const auto elapsed = std::chrono::steady_clock::now() - start;
    // Linear is a few milliseconds even in an instrumented build; the old code ran for hours.
    EXPECT_LT(elapsed, std::chrono::seconds(10));

    // Check against a per-row oracle.
    const rowid_t rows = kPieces * 10;
    std::vector<uint8_t> in_scan(rows, 0);
    std::vector<uint8_t> in_morsel(rows, 0);
    for (rowid_t k = 0; k < rows; k++) {
        in_scan[k] = k % 10 != 0;
        in_morsel[k] = k < kPieces * 7 && k % 7 != 0;
    }
    std::vector<uint8_t> got_and(rows, 0);
    std::vector<uint8_t> got_or(rows, 0);
    for (size_t i = 0; i < intersected.size(); i++) {
        if (i > 0) {
            ASSERT_LE(intersected[i - 1].end(), intersected[i].begin());
        }
        for (rowid_t k = intersected[i].begin(); k < intersected[i].end(); k++) got_and[k] = 1;
    }
    for (size_t i = 0; i < unioned.size(); i++) {
        if (i > 0) {
            ASSERT_LT(unioned[i - 1].end(), unioned[i].begin());
        }
        for (rowid_t k = unioned[i].begin(); k < unioned[i].end(); k++) got_or[k] = 1;
    }
    size_t expected_and_rows = 0;
    for (rowid_t k = 0; k < rows; k++) {
        ASSERT_EQ(in_scan[k] && in_morsel[k], got_and[k]) << "row " << k;
        ASSERT_EQ(in_scan[k] || in_morsel[k], got_or[k]) << "row " << k;
        expected_and_rows += in_scan[k] && in_morsel[k];
    }
    EXPECT_EQ(expected_and_rows, intersected.span_size());

    // The same through the iterator, which the runtime filter path uses.
    SparseRange<> iter_result;
    scan_range.new_iterator().intersection(morsel_range, &iter_result);
    EXPECT_EQ(ranges_of(intersected), ranges_of(iter_result));
}

} // namespace starrocks
