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

#include "storage/rowset/default_value_column_iterator.h"

#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_rowset_fwd.h"
#include "gtest/gtest.h"
#include "storage/rowset/column_iterator.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage_primitive/column_or_predicate.h"
#include "storage_primitive/column_predicate_factory.h"

namespace starrocks {
class DefaultValueColumnIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// NOLINTNEXTLINE
TEST_F(DefaultValueColumnIteratorTest, delete_after_column) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    DefaultValueColumnIterator iter(false, "", true, type_info, 0, 10);

    ColumnIteratorOptions opts;
    Status st = iter.init(opts);
    ASSERT_TRUE(st.ok());

    std::vector<const ColumnPredicate*> preds;
    std::unique_ptr<ColumnPredicate> del_pred(new_column_null_predicate(type_info, 1, true));
    SparseRange<> row_ranges;
    st = iter.get_row_ranges_by_zone_map(preds, del_pred.get(), &row_ranges, CompoundNodeType::AND);
    ASSERT_TRUE(st.ok());
    // An empty predicate list must keep every row: the delete predicate alone never prunes.
    ASSERT_EQ(1u, row_ranges.size());
    ASSERT_EQ(10u, row_ranges.span_size());

    TypeDescriptor type_desc(LogicalType::TYPE_INT);
    MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

    size_t num_rows = 10;
    st = iter.next_batch(&num_rows, column.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(column->delete_state(), DEL_PARTIAL_SATISFIED);
    ASSERT_EQ(num_rows, 10);
    ASSERT_EQ(column->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_TRUE(column->is_null(i));
    }
}

// Test that DefaultValueColumnIterator properly destroys placement-new'd Datum
// for complex types (ARRAY/MAP/STRUCT), preventing memory leaks.
// This test is designed to be run under ASAN to detect leaks.
TEST_F(DefaultValueColumnIteratorTest, no_leak_for_array_default_value) {
    // Build an ARRAY<INT> TypeInfo via TabletColumn
    TabletColumn array_col;
    array_col.set_unique_id(0);
    array_col.set_name("c_array");
    array_col.set_type(TYPE_ARRAY);
    array_col.set_is_nullable(true);
    array_col.set_length(24);

    TabletColumn int_col;
    int_col.set_unique_id(1);
    int_col.set_name("element");
    int_col.set_type(TYPE_INT);
    int_col.set_is_nullable(false);
    int_col.set_length(4);
    array_col.add_sub_column(int_col);

    TypeInfoPtr type_info = get_type_info(array_col);
    ASSERT_NE(type_info, nullptr);

    // JSON representation of [1, 2, 3]
    std::string default_value = "[1, 2, 3]";

    {
        // Scope the iterator so its destructor runs before test ends.
        // Under ASAN, a missing Datum destructor call would be reported as a leak.
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);

        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        // Read a batch to ensure the default value is actually used
        TypeDescriptor type_desc(LogicalType::TYPE_ARRAY);
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 5;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 5);
    }
    // If the destructor doesn't call Datum::~Datum(), ASAN will report a leak here.
}

TEST_F(DefaultValueColumnIteratorTest, no_leak_for_map_default_value) {
    TabletColumn map_col;
    map_col.set_unique_id(0);
    map_col.set_name("c_map");
    map_col.set_type(TYPE_MAP);
    map_col.set_is_nullable(true);
    map_col.set_length(24);

    TabletColumn key_col;
    key_col.set_unique_id(1);
    key_col.set_name("key");
    key_col.set_type(TYPE_VARCHAR);
    key_col.set_is_nullable(false);
    key_col.set_length(128);
    map_col.add_sub_column(key_col);

    TabletColumn val_col;
    val_col.set_unique_id(2);
    val_col.set_name("value");
    val_col.set_type(TYPE_INT);
    val_col.set_is_nullable(true);
    val_col.set_length(4);
    map_col.add_sub_column(val_col);

    TypeInfoPtr type_info = get_type_info(map_col);
    ASSERT_NE(type_info, nullptr);

    std::string default_value = R"({"a": 1, "b": 2})";

    {
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);
        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        TypeDescriptor type_desc(LogicalType::TYPE_MAP);
        type_desc.children.emplace_back(LogicalType::TYPE_VARCHAR);
        type_desc.children.back().len = 128;
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 3;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 3);
    }
}

TEST_F(DefaultValueColumnIteratorTest, no_leak_for_struct_default_value) {
    TabletColumn struct_col;
    struct_col.set_unique_id(0);
    struct_col.set_name("c_struct");
    struct_col.set_type(TYPE_STRUCT);
    struct_col.set_is_nullable(true);
    struct_col.set_length(24);

    TabletColumn field1;
    field1.set_unique_id(1);
    field1.set_name("f1");
    field1.set_type(TYPE_INT);
    field1.set_is_nullable(true);
    field1.set_length(4);
    struct_col.add_sub_column(field1);

    TabletColumn field2;
    field2.set_unique_id(2);
    field2.set_name("f2");
    field2.set_type(TYPE_VARCHAR);
    field2.set_is_nullable(true);
    field2.set_length(128);
    struct_col.add_sub_column(field2);

    TypeInfoPtr type_info = get_type_info(struct_col);
    ASSERT_NE(type_info, nullptr);

    std::string default_value = R"([42, "hello"])";

    {
        DefaultValueColumnIterator iter(true, default_value, true, type_info, 24, 10);
        ColumnIteratorOptions opts;
        ASSERT_TRUE(iter.init(opts).ok());

        TypeDescriptor type_desc(LogicalType::TYPE_STRUCT);
        type_desc.children.emplace_back(LogicalType::TYPE_INT);
        type_desc.children.emplace_back(LogicalType::TYPE_VARCHAR);
        type_desc.children.back().len = 128;
        type_desc.field_names.emplace_back("f1");
        type_desc.field_names.emplace_back("f2");
        MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);

        size_t num_rows = 3;
        ASSERT_TRUE(iter.next_batch(&num_rows, column.get()).ok());
        ASSERT_EQ(column->size(), 3);
    }
}

// ---------------------------------------------------------------------------
// Constant folding of the zone map for a column that is physically absent from
// the segment. Every row of such a column holds the same value, so the zone map
// degenerates to min == max and predicates can be evaluated against it exactly.
// ---------------------------------------------------------------------------

namespace {

constexpr ordinal_t kNumRows = 10;

// Runs the folding entry point and returns the resulting row range.
SparseRange<> zone_map_ranges(DefaultValueColumnIterator* iter, const std::vector<const ColumnPredicate*>& preds,
                              const ColumnPredicate* del_pred = nullptr,
                              CompoundNodeType relation = CompoundNodeType::AND, const Range<>* src_range = nullptr) {
    SparseRange<> ranges;
    Status st = iter->get_row_ranges_by_zone_map(preds, del_pred, &ranges, relation, src_range);
    EXPECT_TRUE(st.ok()) << st;
    return ranges;
}

// The iterator must never hand a non-OK Status back to the query, so "cannot fold" always shows up
// as "kept everything" rather than as an error.
void expect_full_range(const SparseRange<>& ranges) {
    ASSERT_EQ(1u, ranges.size());
    ASSERT_EQ(kNumRows, ranges.span_size());
}

void expect_pruned(const SparseRange<>& ranges) {
    // Do not probe begin()/end() here: they index _ranges[0]/_ranges.back(), which is out of bounds
    // on an empty SparseRange.
    ASSERT_TRUE(ranges.empty());
    ASSERT_EQ(0u, ranges.span_size());
}

std::unique_ptr<DefaultValueColumnIterator> make_iter(bool has_default_value, const std::string& default_value,
                                                      bool is_nullable, const TypeInfoPtr& type_info,
                                                      size_t schema_length = 0) {
    auto iter = std::make_unique<DefaultValueColumnIterator>(has_default_value, default_value, is_nullable, type_info,
                                                             schema_length, kNumRows);
    ColumnIteratorOptions opts;
    EXPECT_TRUE(iter->init(opts).ok());
    return iter;
}

DelCondSatisfied read_delete_state(DefaultValueColumnIterator* iter, LogicalType lt) {
    TypeDescriptor type_desc(lt);
    MutableColumnPtr column = ColumnHelper::create_column(type_desc, true);
    size_t n = kNumRows;
    EXPECT_TRUE(iter->next_batch(&n, column.get()).ok());
    return column->delete_state();
}

} // namespace

TEST_F(DefaultValueColumnIteratorTest, fold_int_matching_and_non_matching) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);

    auto iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_full_range(zone_map_ranges(iter.get(), {hit.get()}));

    auto iter2 = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_pruned(zone_map_ranges(iter2.get(), {miss.get()}));

    // Range predicates fold just as exactly: min == max means no false positives at all.
    auto iter3 = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> gt(new_column_gt_predicate(type_info, 0, Slice("41")));
    expect_full_range(zone_map_ranges(iter3.get(), {gt.get()}));

    auto iter4 = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> ge(new_column_ge_predicate(type_info, 0, Slice("43")));
    expect_pruned(zone_map_ranges(iter4.get(), {ge.get()}));
}

TEST_F(DefaultValueColumnIteratorTest, fold_respects_src_range) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    const Range<> src(3, 7);

    auto iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    SparseRange<> kept = zone_map_ranges(iter.get(), {hit.get()}, nullptr, CompoundNodeType::AND, &src);
    ASSERT_EQ(1u, kept.size());
    ASSERT_EQ(3u, kept.begin());
    ASSERT_EQ(7u, kept.end());

    auto iter2 = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_pruned(zone_map_ranges(iter2.get(), {miss.get()}, nullptr, CompoundNodeType::AND, &src));
}

TEST_F(DefaultValueColumnIteratorTest, fold_empty_predicates_keeps_everything) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    const Range<> src(3, 7);

    // Callers pass an empty predicate list for delete-only columns; both relations must keep
    // everything, exactly like ColumnReader::_zone_map_filter.
    for (auto relation : {CompoundNodeType::AND, CompoundNodeType::OR}) {
        auto iter = make_iter(true, "42", false, type_info);
        expect_full_range(zone_map_ranges(iter.get(), {}, nullptr, relation));

        auto iter2 = make_iter(true, "42", false, type_info);
        SparseRange<> bounded = zone_map_ranges(iter2.get(), {}, nullptr, relation, &src);
        ASSERT_EQ(1u, bounded.size());
        ASSERT_EQ(4u, bounded.span_size());
    }
}

TEST_F(DefaultValueColumnIteratorTest, fold_honours_pred_relation) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    std::unique_ptr<ColumnPredicate> hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    std::unique_ptr<ColumnPredicate> miss2(new_column_eq_predicate(type_info, 0, Slice("8")));

    // OR: one satisfied disjunct is enough.
    auto or_hit = make_iter(true, "42", false, type_info);
    expect_full_range(zone_map_ranges(or_hit.get(), {miss.get(), hit.get()}, nullptr, CompoundNodeType::OR));

    auto or_miss = make_iter(true, "42", false, type_info);
    expect_pruned(zone_map_ranges(or_miss.get(), {miss.get(), miss2.get()}, nullptr, CompoundNodeType::OR));

    // AND: one unsatisfied conjunct prunes the whole segment.
    auto and_miss = make_iter(true, "42", false, type_info);
    expect_pruned(zone_map_ranges(and_miss.get(), {hit.get(), miss.get()}, nullptr, CompoundNodeType::AND));

    auto and_hit = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> ge(new_column_ge_predicate(type_info, 0, Slice("40")));
    expect_full_range(zone_map_ranges(and_hit.get(), {hit.get(), ge.get()}, nullptr, CompoundNodeType::AND));
}

TEST_F(DefaultValueColumnIteratorTest, fold_all_null_column) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);

    // A nullable column with no declared default is NULL on every row.
    auto is_null_iter = make_iter(false, "", true, type_info);
    std::unique_ptr<ColumnPredicate> is_null(new_column_null_predicate(type_info, 0, true));
    expect_full_range(zone_map_ranges(is_null_iter.get(), {is_null.get()}));

    auto is_not_null_iter = make_iter(false, "", true, type_info);
    std::unique_ptr<ColumnPredicate> is_not_null(new_column_null_predicate(type_info, 0, false));
    expect_pruned(zone_map_ranges(is_not_null_iter.get(), {is_not_null.get()}));

    // A comparison can never match NULL either.
    auto eq_iter = make_iter(false, "", true, type_info);
    std::unique_ptr<ColumnPredicate> eq(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_pruned(zone_map_ranges(eq_iter.get(), {eq.get()}));

    // The literal "NULL" default takes the same path.
    auto literal_null_iter = make_iter(true, "NULL", true, type_info);
    std::unique_ptr<ColumnPredicate> eq2(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_pruned(zone_map_ranges(literal_null_iter.get(), {eq2.get()}));

    // ... and the mirror image: a non-NULL default is never NULL.
    auto valued = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> is_null2(new_column_null_predicate(type_info, 0, true));
    expect_pruned(zone_map_ranges(valued.get(), {is_null2.get()}));

    auto valued2 = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> is_not_null2(new_column_null_predicate(type_info, 0, false));
    expect_full_range(zone_map_ranges(valued2.get(), {is_not_null2.get()}));
}

// The folded CHAR value must be byte-identical to what next_batch() materialises, which for CHAR is
// the NUL-padded form. Folding the unpadded literal instead would prune rows that the row-level
// predicate keeps.
TEST_F(DefaultValueColumnIteratorTest, fold_char_uses_padded_value) {
    TypeInfoPtr type_info = get_type_info(TYPE_CHAR);
    constexpr size_t kCharLen = 10;

    auto gt_iter = make_iter(true, "abc", false, type_info, kCharLen);
    std::unique_ptr<ColumnPredicate> gt(new_column_gt_predicate(type_info, 0, Slice("abc")));
    ASSERT_TRUE(gt->padding_zeros(kCharLen));
    // "abc\0\0\0\0\0\0\0" > "abc", so every row survives -- the same answer the row-level predicate
    // gives. An unpadded fold would report min == max == "abc" and prune all ten rows.
    expect_full_range(zone_map_ranges(gt_iter.get(), {gt.get()}));

    auto eq_iter = make_iter(true, "abc", false, type_info, kCharLen);
    std::unique_ptr<ColumnPredicate> eq(new_column_eq_predicate(type_info, 0, Slice("abc")));
    ASSERT_TRUE(eq->padding_zeros(kCharLen));
    expect_pruned(zone_map_ranges(eq_iter.get(), {eq.get()}));

    // This one holds under either fold (a strictly shorter prefix compares less than both "abc" and
    // the padded form); it pins prefix ordering, not padding.
    auto gt_short_iter = make_iter(true, "abc", false, type_info, kCharLen);
    std::unique_ptr<ColumnPredicate> gt_short(new_column_gt_predicate(type_info, 0, Slice("ab")));
    ASSERT_TRUE(gt_short->padding_zeros(kCharLen));
    expect_full_range(zone_map_ranges(gt_short_iter.get(), {gt_short.get()}));
}

TEST_F(DefaultValueColumnIteratorTest, fold_varchar) {
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);

    auto hit_iter = make_iter(true, "hello", false, type_info);
    std::unique_ptr<ColumnPredicate> hit(new_column_eq_predicate(type_info, 0, Slice("hello")));
    expect_full_range(zone_map_ranges(hit_iter.get(), {hit.get()}));

    auto miss_iter = make_iter(true, "hello", false, type_info);
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("world")));
    expect_pruned(zone_map_ranges(miss_iter.get(), {miss.get()}));

    auto empty_iter = make_iter(true, "hello", false, type_info);
    std::unique_ptr<ColumnPredicate> empty(new_column_eq_predicate(type_info, 0, Slice("")));
    expect_pruned(zone_map_ranges(empty_iter.get(), {empty.get()}));
}

// Decimal defaults are human literals ("1.23"), not the scaled integer a persisted zone map holds,
// so they must be parsed with the column's own DecimalTypeInfo. Parsing them with the delegate
// integer type would silently compare 1 against the predicate's 123.
TEST_F(DefaultValueColumnIteratorTest, fold_decimal_respects_scale) {
    for (auto lt : {TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128}) {
        TypeInfoPtr type_info = get_type_info(lt, 10, 2);
        ASSERT_NE(nullptr, type_info);

        auto hit_iter = make_iter(true, "1.23", false, type_info);
        std::unique_ptr<ColumnPredicate> hit(new_column_eq_predicate(type_info, 0, Slice("1.23")));
        expect_full_range(zone_map_ranges(hit_iter.get(), {hit.get()}));

        auto miss_iter = make_iter(true, "1.23", false, type_info);
        std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("1.24")));
        expect_pruned(zone_map_ranges(miss_iter.get(), {miss.get()}));

        // The discriminating case: a delegate-typed fold would produce 1 instead of 123 and wrongly
        // decide that 1 > 100 is false.
        auto gt_iter = make_iter(true, "1.23", false, type_info);
        std::unique_ptr<ColumnPredicate> gt(new_column_gt_predicate(type_info, 0, Slice("1.00")));
        expect_full_range(zone_map_ranges(gt_iter.get(), {gt.get()}));
    }
}

TEST_F(DefaultValueColumnIteratorTest, fold_date_and_datetime) {
    TypeInfoPtr date_type = get_type_info(TYPE_DATE);
    auto date_iter = make_iter(true, "2024-01-02", false, date_type);
    std::unique_ptr<ColumnPredicate> date_miss(new_column_eq_predicate(date_type, 0, Slice("2024-01-03")));
    expect_pruned(zone_map_ranges(date_iter.get(), {date_miss.get()}));

    auto date_iter2 = make_iter(true, "2024-01-02", false, date_type);
    std::unique_ptr<ColumnPredicate> date_hit(new_column_eq_predicate(date_type, 0, Slice("2024-01-02")));
    expect_full_range(zone_map_ranges(date_iter2.get(), {date_hit.get()}));

    TypeInfoPtr dt_type = get_type_info(TYPE_DATETIME);
    auto dt_iter = make_iter(true, "2024-01-02 03:04:05", false, dt_type);
    std::unique_ptr<ColumnPredicate> dt_miss(new_column_lt_predicate(dt_type, 0, Slice("2024-01-01 00:00:00")));
    expect_pruned(zone_map_ranges(dt_iter.get(), {dt_miss.get()}));
}

// A predicate reads the zone-map Datum through its own TypeInfo, so a type or scale mismatch must
// disable folding entirely rather than risk a bad_variant_access or a silent wrong answer.
TEST_F(DefaultValueColumnIteratorTest, fold_rejects_mismatched_predicate_type) {
    TypeInfoPtr int_type = get_type_info(TYPE_INT);
    auto iter = make_iter(true, "42", false, int_type);
    std::unique_ptr<ColumnPredicate> bigint_pred(new_column_eq_predicate(get_type_info(TYPE_BIGINT), 0, Slice("7")));
    expect_full_range(zone_map_ranges(iter.get(), {bigint_pred.get()}));

    TypeInfoPtr dec_type = get_type_info(TYPE_DECIMAL64, 10, 2);
    auto dec_iter = make_iter(true, "1.23", false, dec_type);
    std::unique_ptr<ColumnPredicate> other_scale(
            new_column_eq_predicate(get_type_info(TYPE_DECIMAL64, 10, 3), 0, Slice("9.99")));
    expect_full_range(zone_map_ranges(dec_iter.get(), {other_scale.get()}));

    // All-or-nothing: one mismatched predicate disables folding for the whole call, because dropping
    // it from an OR disjunction would shrink the kept set.
    auto mixed_iter = make_iter(true, "42", false, int_type);
    std::unique_ptr<ColumnPredicate> int_miss(new_column_eq_predicate(int_type, 0, Slice("7")));
    expect_full_range(
            zone_map_ranges(mixed_iter.get(), {int_miss.get(), bigint_pred.get()}, nullptr, CompoundNodeType::AND));

    // Same for a mismatched delete predicate.
    auto del_iter = make_iter(true, "42", false, int_type);
    std::unique_ptr<ColumnPredicate> bigint_del(new_column_eq_predicate(get_type_info(TYPE_BIGINT), 0, Slice("7")));
    expect_full_range(zone_map_ranges(del_iter.get(), {int_miss.get()}, bigint_del.get()));
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(del_iter.get(), TYPE_INT));
}

TEST_F(DefaultValueColumnIteratorTest, unsupported_types_fall_back_to_full_range) {
    // JSON: init() stores the raw default as a Slice, but a JSON Datum holds a JsonValue*.
    TabletColumn json_col;
    json_col.set_unique_id(0);
    json_col.set_name("c_json");
    json_col.set_type(TYPE_JSON);
    json_col.set_is_nullable(true);
    json_col.set_length(16);
    TypeInfoPtr json_type = get_type_info(json_col);

    auto json_iter = make_iter(true, R"({"a":1})", true, json_type, 16);
    std::unique_ptr<ColumnPredicate> json_not_null(new_column_null_predicate(json_type, 0, false));
    expect_full_range(zone_map_ranges(json_iter.get(), {json_not_null.get()}));

    auto json_iter2 = make_iter(true, R"({"a":1})", true, json_type, 16);
    std::unique_ptr<ColumnPredicate> json_is_null(new_column_null_predicate(json_type, 0, true));
    expect_full_range(zone_map_ranges(json_iter2.get(), {json_is_null.get()}));

    // An unparsable JSON default is downgraded to NULL by init(); the type gate still rejects it, so
    // it must not be folded into an "all NULL" zone map either.
    auto broken_iter = make_iter(true, "{not json", true, json_type, 16);
    std::unique_ptr<ColumnPredicate> broken_not_null(new_column_null_predicate(json_type, 0, false));
    expect_full_range(zone_map_ranges(broken_iter.get(), {broken_not_null.get()}));

    // ARRAY: init() placement-news a Datum into _mem_value instead of a scalar.
    TabletColumn array_col;
    array_col.set_unique_id(0);
    array_col.set_name("c_array");
    array_col.set_type(TYPE_ARRAY);
    array_col.set_is_nullable(true);
    array_col.set_length(24);
    TabletColumn element_col;
    element_col.set_unique_id(1);
    element_col.set_name("element");
    element_col.set_type(TYPE_INT);
    element_col.set_is_nullable(false);
    element_col.set_length(4);
    array_col.add_sub_column(element_col);
    TypeInfoPtr array_type = get_type_info(array_col);

    auto array_iter = make_iter(true, "[1, 2, 3]", true, array_type, 24);
    std::unique_ptr<ColumnPredicate> array_not_null(new_column_null_predicate(array_type, 0, false));
    expect_full_range(zone_map_ranges(array_iter.get(), {array_not_null.get()}));
}

// The delete predicate never prunes the row range; it only decides whether the batch has to be
// re-checked against the delete condition row by row.
TEST_F(DefaultValueColumnIteratorTest, delete_predicate_is_exact_on_a_constant_column) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);

    // The delete condition cannot match the constant default: no row is deleted, so the batch does
    // not need the per-row delete check.
    auto clean = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> del_miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_full_range(zone_map_ranges(clean.get(), {}, del_miss.get()));
    ASSERT_EQ(DEL_NOT_SATISFIED, read_delete_state(clean.get(), TYPE_INT));

    // The delete condition matches the constant default: stay conservative.
    auto dirty = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> del_hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_full_range(zone_map_ranges(dirty.get(), {}, del_hit.get()));
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(dirty.get(), TYPE_INT));

    // A delete predicate is never allowed to shrink the row range, not even when it cannot match.
    auto with_query_pred = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> query_hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    std::unique_ptr<ColumnPredicate> del_miss2(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_full_range(zone_map_ranges(with_query_pred.get(), {query_hit.get()}, del_miss2.get()));
}

// A call that carries no delete predicate must not be read as "no delete condition touches this
// column": there are paths (zone-map config off, read-state-cache replay) where _del_predicates is
// empty even though the tablet has delete predicates, and the runtime filter pass still calls in
// with a null argument. Naively writing `_may_contain_deleted_row = del != nullptr && filter()`
// would clear the flag here and let already-deleted rows through.
TEST_F(DefaultValueColumnIteratorTest, delete_flag_survives_a_null_delete_predicate_call) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    auto iter = make_iter(true, "42", false, type_info);

    std::unique_ptr<ColumnPredicate> del_hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_full_range(zone_map_ranges(iter.get(), {}, del_hit.get()));

    // SegmentIterator::_apply_del_predicate() shape: the delete group arrives in |predicates| and the
    // delete predicate argument is null.
    std::unique_ptr<ColumnPredicate> child(new_column_eq_predicate(type_info, 0, Slice("42")));
    ColumnOrPredicate or_pred(type_info, 0);
    or_pred.add_child(child.get());
    const Range<> bound(0, kNumRows);
    expect_full_range(zone_map_ranges(iter.get(), {&or_pred}, nullptr, CompoundNodeType::OR, &bound));

    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(iter.get(), TYPE_INT));

    // A null delete predicate is conservative on its own too, even when nothing proved a hit first.
    auto fresh = make_iter(true, "42", false, type_info);
    expect_full_range(zone_map_ranges(fresh.get(), {}, nullptr));
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(fresh.get(), TYPE_INT));
}

// Pins the OR-accumulate itself: a second call carrying a delete predicate that cannot match must
// not clear a hit the first call proved. No current SegmentIterator call site produces this ordering
// (they all resolve the same _del_predicates[cid] entry, so repeated calls agree), so this asserts
// the function's contract rather than a reachable sequence -- but it is what makes replacing `|=`
// with `=` fail instead of shipping silently.
TEST_F(DefaultValueColumnIteratorTest, delete_flag_accumulates_across_differing_predicates) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    auto iter = make_iter(true, "42", false, type_info);

    std::unique_ptr<ColumnPredicate> del_hit(new_column_eq_predicate(type_info, 0, Slice("42")));
    expect_full_range(zone_map_ranges(iter.get(), {}, del_hit.get()));

    std::unique_ptr<ColumnPredicate> del_miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_full_range(zone_map_ranges(iter.get(), {}, del_miss.get()));

    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(iter.get(), TYPE_INT));
}

// _apply_del_predicate's shape: the delete group arrives in |predicates|, so an empty range there
// means "no row of this column can be deleted", not "no row survives".
TEST_F(DefaultValueColumnIteratorTest, delete_group_in_predicates_collapses_range) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    const Range<> bound(0, kNumRows);

    auto iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> child(new_column_eq_predicate(type_info, 0, Slice("7")));
    ColumnOrPredicate or_pred(type_info, 0);
    or_pred.add_child(child.get());
    expect_pruned(zone_map_ranges(iter.get(), {&or_pred}, nullptr, CompoundNodeType::OR, &bound));
}

// A runtime-filter placeholder never reads the zone-map Datum, so it must not disable folding for
// the real predicates beside it. On a DECIMAL column the placeholder's TypeInfo carries no
// precision/scale, which is exactly where a strict type gate would silently give up.
TEST_F(DefaultValueColumnIteratorTest, placeholder_predicate_does_not_disable_folding) {
    TypeInfoPtr type_info = get_type_info(TYPE_DECIMAL64, 10, 2);
    auto iter = make_iter(true, "1.23", false, type_info);

    std::unique_ptr<ColumnPredicate> placeholder(new_column_placeholder_predicate(get_type_info(TYPE_DECIMAL64), 0));
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("9.99")));
    expect_pruned(zone_map_ranges(iter.get(), {placeholder.get(), miss.get()}, nullptr, CompoundNodeType::AND));

    // Under OR the placeholder answers "keep", so nothing may be pruned.
    auto iter2 = make_iter(true, "1.23", false, type_info);
    std::unique_ptr<ColumnPredicate> miss2(new_column_eq_predicate(type_info, 0, Slice("9.99")));
    expect_full_range(zone_map_ranges(iter2.get(), {placeholder.get(), miss2.get()}, nullptr, CompoundNodeType::OR));
}

TEST_F(DefaultValueColumnIteratorTest, config_off_restores_legacy_behaviour) {
    TypeInfoPtr type_info = get_type_info(TYPE_INT);
    const bool saved = config::enable_default_value_column_zonemap_filter;
    config::enable_default_value_column_zonemap_filter = false;
    DeferOp reset([&]() { config::enable_default_value_column_zonemap_filter = saved; });

    auto iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_full_range(zone_map_ranges(iter.get(), {miss.get()}));

    auto del_iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> del_miss(new_column_eq_predicate(type_info, 0, Slice("7")));
    expect_full_range(zone_map_ranges(del_iter.get(), {}, del_miss.get()));
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, read_delete_state(del_iter.get(), TYPE_INT));

    // The fallback must honour src_range too, not widen it back to [0, _num_rows).
    const Range<> src(3, 7);
    auto bounded_iter = make_iter(true, "42", false, type_info);
    std::unique_ptr<ColumnPredicate> miss2(new_column_eq_predicate(type_info, 0, Slice("7")));
    SparseRange<> bounded = zone_map_ranges(bounded_iter.get(), {miss2.get()}, nullptr, CompoundNodeType::AND, &src);
    ASSERT_EQ(1u, bounded.size());
    ASSERT_EQ(4u, bounded.span_size());
}

} // namespace starrocks