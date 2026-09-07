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

#include "storage/seek_tuple.h"

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace starrocks {

namespace {

// Build a Schema whose i-th field is a key column of |types[i]|, with field id == position (satisfying
// SeekTuple's "column ids are continuous, starting from zero" invariant for the compact code path).
Schema make_key_schema(const std::vector<LogicalType>& types) {
    Fields fields;
    fields.reserve(types.size());
    for (size_t i = 0; i < types.size(); i++) {
        auto field = std::make_shared<Field>(static_cast<ColumnId>(i), "c" + std::to_string(i), types[i], true);
        field->set_is_key(true);
        fields.push_back(field);
    }
    return Schema(fields);
}

} // namespace

class SeekTupleTest : public testing::Test {};

// Multi-column varchar ordering, including the classic middle-column collision case: without a
// terminator, ("ab","c") and ("a","bc") would both encode to "abc...". full_sort_key_encode must keep
// them distinguishable and correctly ordered.
TEST_F(SeekTupleTest, FullSortKeyEncodeVarcharOrdering) {
    Schema schema = make_key_schema({TYPE_VARCHAR, TYPE_VARCHAR, TYPE_VARCHAR});

    std::string a0 = "ab", a1 = "c", a2 = "x";
    std::string b0 = "a", b1 = "bc", b2 = "x";
    SeekTuple ta(schema, {Datum(Slice(a0)), Datum(Slice(a1)), Datum(Slice(a2))});
    SeekTuple tb(schema, {Datum(Slice(b0)), Datum(Slice(b1)), Datum(Slice(b2))});

    // ("ab","c",*) > ("a","bc",*) because the first column ("ab" vs "a") already decides the order.
    ASSERT_GT(ta.full_sort_key_encode(3, 0), tb.full_sort_key_encode(3, 0));

    // Ascending order across several tuples, including a change in the middle column only.
    std::vector<std::vector<std::string>> rows = {{"a", "a", "a"}, {"a", "a", "b"}, {"a", "b", "a"}, {"b", "a", "a"}};
    std::vector<std::string> encoded;
    for (auto& r : rows) {
        SeekTuple t(schema, {Datum(Slice(r[0])), Datum(Slice(r[1])), Datum(Slice(r[2]))});
        encoded.push_back(t.full_sort_key_encode(3, 0));
    }
    for (size_t i = 1; i < encoded.size(); i++) {
        ASSERT_LT(encoded[i - 1], encoded[i]);
    }
}

// Physical overload: a middle CHAR column shorter than its declared width must encode identically
// whether the source datum carries writer-side NUL padding or not (write/query consistency).
TEST_F(SeekTupleTest, FullSortKeyEncodePhysicalCharPaddingInsensitive) {
    Schema schema = make_key_schema({TYPE_VARCHAR, TYPE_CHAR, TYPE_INT});

    std::string c0 = "xx";
    std::string padded_char(5, '\0');
    std::memcpy(padded_char.data(), "ab", 2); // "ab\0\0\0", simulates writer-side CHAR(5) padding.
    std::string unpadded_char = "ab";

    SeekTuple padded(schema, {Datum(Slice(c0)), Datum(Slice(padded_char)), Datum(int32_t(7))});
    SeekTuple unpadded(schema, {Datum(Slice(c0)), Datum(Slice(unpadded_char)), Datum(int32_t(7))});

    std::vector<uint32_t> idxes = {0, 1, 2};
    ASSERT_EQ(padded.full_sort_key_encode(idxes, 0), unpadded.full_sort_key_encode(idxes, 0));
}

// Compact overload: an embedded-NUL CHAR query key and an over-width CHAR query key must both stay
// byte-greater than the truncated stored form, since the compact overload escapes the raw slice with no
// NUL-truncation (asymmetric vs. the physical overload).
TEST_F(SeekTupleTest, FullSortKeyEncodeCompactCharAsymmetricOrdering) {
    Schema schema = make_key_schema({TYPE_CHAR, TYPE_INT});

    // Embedded NUL: query "a\0b" (raw, 3 bytes) vs. the truncated stored entry "a".
    {
        std::string query_char("a\0b", 3);
        SeekTuple query(schema, {Datum(Slice(query_char))});
        std::string query_encoded = query.full_sort_key_encode(2, KEY_MINIMAL_MARKER);

        std::string stored_char = "a";
        SeekTuple stored(schema, {Datum(Slice(stored_char)), Datum(int32_t(5))});
        std::string stored_encoded = stored.full_sort_key_encode(std::vector<uint32_t>{0, 1}, 0);

        ASSERT_GT(query_encoded, stored_encoded);
    }

    // Over-width literal: query "abc" (raw, no truncation) vs. the truncated stored entry "ab".
    {
        std::string query_char = "abc";
        SeekTuple query(schema, {Datum(Slice(query_char))});
        std::string query_encoded = query.full_sort_key_encode(2, KEY_MINIMAL_MARKER);

        std::string stored_char = "ab";
        SeekTuple stored(schema, {Datum(Slice(stored_char)), Datum(int32_t(5))});
        std::string stored_encoded = stored.full_sort_key_encode(std::vector<uint32_t>{0, 1}, 0);

        ASSERT_GT(query_encoded, stored_encoded);
    }
}

// Prefix sentinel: a partial key with KEY_MINIMAL_MARKER padding sorts below any full tuple sharing the
// same prefix, and KEY_MAXIMAL_MARKER padding sorts above it.
TEST_F(SeekTupleTest, FullSortKeyEncodePrefixSentinel) {
    Schema schema = make_key_schema({TYPE_VARCHAR, TYPE_VARCHAR, TYPE_INT});

    std::string ab = "ab";
    SeekTuple prefix(schema, {Datum(Slice(ab))});
    std::string prefix_min = prefix.full_sort_key_encode(3, KEY_MINIMAL_MARKER);
    std::string prefix_max = prefix.full_sort_key_encode(3, KEY_MAXIMAL_MARKER);

    std::string mid = "zzzz";
    SeekTuple full(schema, {Datum(Slice(ab)), Datum(Slice(mid)), Datum(int32_t(123))});
    std::string full_encoded = full.full_sort_key_encode(3, 0);

    ASSERT_LT(prefix_min, full_encoded);
    ASSERT_GT(prefix_max, full_encoded);
}

// A custom sort key order (idxes [2,0]) on the physical overload must produce exactly the same bytes as
// manually reordering the columns and encoding them with the compact overload -- proving that the
// physical overload indexes _values[sort_key_idxes[i]] rather than positionally.
TEST_F(SeekTupleTest, FullSortKeyEncodePhysicalCustomOrderMatchesReorderedCompact) {
    Schema schema = make_key_schema({TYPE_INT, TYPE_VARCHAR, TYPE_VARCHAR});
    std::string s1 = "hello";
    std::string s2 = "world";
    SeekTuple tuple(schema, {Datum(int32_t(42)), Datum(Slice(s1)), Datum(Slice(s2))});

    std::string physical_encoded = tuple.full_sort_key_encode(std::vector<uint32_t>{2, 0}, 0);

    Schema reordered_schema = make_key_schema({TYPE_VARCHAR, TYPE_INT});
    SeekTuple reordered_tuple(reordered_schema, {Datum(Slice(s2)), Datum(int32_t(42))});
    std::string compact_encoded = reordered_tuple.full_sort_key_encode(2, 0);

    ASSERT_EQ(physical_encoded, compact_encoded);
}

} // namespace starrocks
