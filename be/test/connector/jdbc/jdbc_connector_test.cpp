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

#include "connector/jdbc/jdbc_connector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <utility>

#include "base/string/utf8_check.h"
#include "common/config_exec_fwd.h"
#include "common/config_metrics_fwd.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/in_const_predicate.hpp"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/column_test_helper.h"
#include "types/date_value.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/type_descriptor.h"

namespace starrocks::connector {

class JDBCConnectorTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<const QueryExecutionServices*>(nullptr), nullptr);
        TUniqueId id;
        _runtime_state->init_mem_trackers(id);
        _pool = _runtime_state->obj_pool();
    }

protected:
    void _create_desc_tbl() {
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(TYPE_INT).nullable(true).column_name("c1");
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, desc_tbl_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        _runtime_state->set_desc_tbl(tbl);
    }

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ObjectPool* _pool = nullptr;
};

TEST_F(JDBCConnectorTest, ConnectorCreatesProviderForJDBCScan) {
    _create_desc_tbl();

    TJDBCScanNode scan_node;
    scan_node.__set_tuple_id(0);
    scan_node.__set_table_name("jdbc_table");
    scan_node.__set_columns({"c1"});
    scan_node.__set_filters({});

    TPlanNode plan_node;
    plan_node.__set_jdbc_scan_node(scan_node);

    JDBCConnector connector;
    ASSERT_EQ(ConnectorType::JDBC, connector.connector_type());

    auto provider = connector.create_data_source_provider(nullptr, plan_node);
    ASSERT_NE(provider, nullptr);
    ASSERT_TRUE(provider->insert_local_exchange_operator());
    ASSERT_FALSE(provider->accept_empty_scan_ranges());
    ASSERT_EQ(_runtime_state->desc_tbl().get_tuple_descriptor(0), provider->tuple_descriptor(_runtime_state.get()));

    TScanRange scan_range;
    auto data_source = provider->create_data_source(scan_range);
    ASSERT_NE(data_source, nullptr);
    ASSERT_EQ("JDBCDataSource", data_source->name());
    ASSERT_EQ(0, data_source->raw_rows_read());
    ASSERT_EQ(0, data_source->num_rows_read());
    ASSERT_EQ(0, data_source->num_bytes_read());
    ASSERT_EQ(0, data_source->cpu_time_spent());
}

// ================== join runtime filter pushdown ==================

// Slot ids are pinned so the FE-supplied column map in each case is readable.
static constexpr SlotId kIntSlot = 1;
static constexpr SlotId kStrSlot = 2;
static constexpr SlotId kDoubleSlot = 3;
static constexpr SlotId kCharSlot = 4;
static constexpr SlotId kBigintSlot = 5;
static constexpr SlotId kFloatSlot = 6;
static constexpr SlotId kDateSlot = 7;
static constexpr SlotId kDatetimeSlot = 8;
static constexpr SlotId kTimeSlot = 9;

// ColumnTestHelper::build_column only knows numbers and slices, and a date is neither.
template <LogicalType Type, class T>
static MutableColumnPtr build_temporal_column(const std::vector<T>& values) {
    auto column = RunTimeColumnType<Type>::create();
    for (const T& value : values) {
        column->append(value);
    }
    return column;
}

// The rendered IN list, as the half-open range between the `(` that follows ` IN ` and its closing
// `)`. A value is a SQL literal that may hold either bracket or a comma, so the scan tracks
// quoting; `''` inside a literal is an escaped quote, which this sees as a close immediately
// followed by an open and therefore needs no case of its own.
static std::pair<size_t, size_t> in_list_range(const std::string& predicate) {
    const size_t open = predicate.find(" IN (");
    CHECK(open != std::string::npos) << "no IN list in: " << predicate;
    const size_t first = open + 5;
    bool quoted = false;
    for (size_t i = first; i < predicate.size(); i++) {
        if (predicate[i] == '\'') {
            quoted = !quoted;
        } else if (!quoted && predicate[i] == ')') {
            return {first, i};
        }
    }
    CHECK(false) << "unterminated IN list in: " << predicate;
    return {first, first};
}

// The values of a rendered IN list, sorted. The values come out of the predicate's hash set, whose
// iteration order is undefined, so a case holding more than one of them asserts the list as a set
// -- but still on the exact rendered text of every value, quoting and escaping included, which is
// the half of this that the remote engine's behaviour turns on.
static std::vector<std::string> in_list_values(const std::string& predicate) {
    const auto [first, last] = in_list_range(predicate);
    std::vector<std::string> values;
    std::string current;
    bool quoted = false;
    for (size_t i = first; i < last; i++) {
        const char c = predicate[i];
        if (c == '\'') {
            quoted = !quoted;
        } else if (c == ',' && !quoted) {
            values.emplace_back(std::move(current));
            current.clear();
            continue;
        }
        current.push_back(c);
    }
    values.emplace_back(std::move(current));
    std::sort(values.begin(), values.end());
    return values;
}

// The same fragment with its IN list replaced by `...`, so a case can assert the shape around the
// list -- the column reference, and R2's `OR ... IS NULL` arm -- without depending on the order of
// the values inside it.
static std::string in_list_elided(const std::string& predicate) {
    const auto [first, last] = in_list_range(predicate);
    return predicate.substr(0, first) + "..." + predicate.substr(last);
}

class JDBCRuntimeFilterPushdownTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;
        // date::to_date_with_cache answers from a table that daemon.cpp fills at BE startup, and
        // this binary links gtest_main rather than a test main that fills it (the other connector
        // suites have one of their own). Left empty, the table is all zeros, and every julian day
        // it covers -- UNIX_EPOCH_JULIAN plus 200*366 days, so 1970 through about 2170 -- decodes
        // as 0000-00-00 while every date outside it takes the arithmetic path and is correct. A
        // date case would then silently test the wrong value: 2024-01-15 arrives as year 0 and is
        // refused by the year gate, while 9999-12-31 beside it in the same filter is fine.
        date::init_date_cache();

        TUniqueId fragment_id;
        TQueryOptions query_options;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals,
                                                        static_cast<const QueryExecutionServices*>(nullptr), nullptr);
        TUniqueId id;
        _runtime_state->init_mem_trackers(id);
        _pool = _runtime_state->obj_pool();
        _build_tuple();
    }

protected:
    void _build_tuple() {
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("c_int").id(kIntSlot).build());
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().string_type(32).nullable(true).column_name("c_str").id(kStrSlot).build());
        tuple_desc_builder.add_slot(TSlotDescriptorBuilder()
                                            .type(TYPE_DOUBLE)
                                            .nullable(true)
                                            .column_name("c_double")
                                            .id(kDoubleSlot)
                                            .build());
        tuple_desc_builder.add_slot(TSlotDescriptorBuilder()
                                            .type(TypeDescriptor::create_char_type(8))
                                            .nullable(true)
                                            .column_name("c_char")
                                            .id(kCharSlot)
                                            .build());
        tuple_desc_builder.add_slot(TSlotDescriptorBuilder()
                                            .type(TYPE_BIGINT)
                                            .nullable(true)
                                            .column_name("c_bigint")
                                            .id(kBigintSlot)
                                            .build());
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_FLOAT).nullable(true).column_name("c_float").id(kFloatSlot).build());
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_DATE).nullable(true).column_name("c_date").id(kDateSlot).build());
        tuple_desc_builder.add_slot(TSlotDescriptorBuilder()
                                            .type(TYPE_DATETIME)
                                            .nullable(true)
                                            .column_name("c_ts")
                                            .id(kDatetimeSlot)
                                            .build());
        tuple_desc_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_TIME).nullable(true).column_name("c_time").id(kTimeSlot).build());
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _pool, desc_tbl_builder.desc_tbl(), &tbl,
                                    config::vector_chunk_size)
                      .ok());
        _runtime_state->set_desc_tbl(tbl);
        _tuple_desc = tbl->get_tuple_descriptor(0);
        CHECK(_tuple_desc != nullptr);
    }

    // Builds the same in-const predicate a hash join builds and hands the scan.
    ExprContext* _make_in_filter(const TypeDescriptor& type, SlotId slot_id, const ColumnPtr& values,
                                 bool join_runtime_filter = true, bool eq_null = false) {
        auto* col_ref = _pool->add(new ColumnRef(type, slot_id));
        VectorizedInConstPredicateBuilder builder(_runtime_state.get(), _pool, col_ref);
        if (join_runtime_filter) {
            builder.use_as_join_runtime_filter();
        }
        builder.set_eq_null(eq_null);
        CHECK(builder.create().ok());
        if (values != nullptr) {
            // For string types add_values keeps only the slices, so the column has to outlive it.
            _value_columns.emplace_back(values);
            builder.add_values(values, 0);
        }
        return builder.get_in_const_predicate();
    }

    JDBCRuntimeFilterPushdown _run(const std::map<SlotId, std::string>& columns,
                                   const std::vector<ExprContext*>& conjuncts, bool scan_has_limit = false,
                                   size_t max_values = 1024) {
        JDBCRuntimeFilterPushdown pushdown;
        build_jdbc_runtime_filter_pushdown(columns, scan_has_limit, conjuncts, *_tuple_desc, max_values, &pushdown);
        return pushdown;
    }

    static std::vector<std::string> _sorted(std::vector<std::string> values) {
        std::sort(values.begin(), values.end());
        return values;
    }

    static size_t _count_placeholders(const std::string& predicate) {
        return std::count(predicate.begin(), predicate.end(), '?');
    }

    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ObjectPool* _pool = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    std::vector<ColumnPtr> _value_columns;
};

TEST_F(JDBCRuntimeFilterPushdownTest, IntegerFilterRendersEveryValueIntoTheStatement) {
    std::vector<int32_t> values{1, 3, 5, 7, 9};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ(5, pushdown.value_count);
    ASSERT_EQ(1, pushdown.predicates.size());
    ASSERT_EQ("\"c_int\" IN (...)", in_list_elided(pushdown.predicates[0]));
    // The set has no defined iteration order, but every value must be carried across: dropping one
    // would make the remote IN list a strict subset of the build keys and lose rows.
    ASSERT_EQ((std::vector<std::string>{"1", "3", "5", "7", "9"}), in_list_values(pushdown.predicates[0]));
    // The fragment is finished SQL: nothing rides beside it and nothing is bound to it.
    ASSERT_EQ(0, _count_placeholders(pushdown.predicates[0]));
    ASSERT_EQ("c_int:5", pushdown.pushed_columns);
    ASSERT_EQ("", pushdown.skip_reasons);
}

// Regression guard, and the first half of the quoting asymmetry. An integer goes in bare, and
// quoting it is not the harmless tidy-up it looks like: it makes the remote engine read the
// operand as text, which matters wherever the StarRocks type and the remote column's type
// disagree. A PostgreSQL `text` column that a hand-written schema declared BIGINT answers
// `c IN ('42')` with the single row whose text is exactly "42" where the local filter keeps six --
// a silent subset, which is the one failure R3 exists to prevent. Bare, the same query is
// `operator does not exist: text = integer`, which fails loudly instead. A single value keeps the
// whole fragment deterministic, so this asserts the text itself rather than a set.
TEST_F(JDBCRuntimeFilterPushdownTest, IntegerLiteralIsNotQuoted) {
    std::vector<int64_t> values{42};
    auto* ctx = _make_in_filter(TYPE_BIGINT_DESC, kBigintSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kBigintSlot, "\"c_bigint\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_bigint\" IN (42)", pushdown.predicates[0]);
    ASSERT_EQ(std::string::npos, pushdown.predicates[0].find('\''));
}

// R2: a null-safe join whose build side held a NULL must let probe-side NULLs through. Folding the
// NULL into the IN list would silently drop them, because `NULL IN (...)` is UNKNOWN -- so the
// list has to hold the two real values and nothing else.
TEST_F(JDBCRuntimeFilterPushdownTest, NullInSetRendersOrIsNullAndKeepsNullOutOfTheList) {
    std::vector<int32_t> values{1, 2};
    auto column = ColumnTestHelper::build_nullable_column<int32_t>({1, 2, 0}, {0, 0, 1});
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, std::move(column), true, /*eq_null=*/true);

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_int\" IN (...) OR \"c_int\" IS NULL", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"1", "2"}), in_list_values(pushdown.predicates[0]));
    ASSERT_EQ(2, pushdown.value_count);
}

// A build side of nothing but NULLs still matches probe-side NULLs under a null-safe join.
TEST_F(JDBCRuntimeFilterPushdownTest, OnlyNullInSetRendersIsNullAlone) {
    auto column = ColumnTestHelper::build_nullable_column<int32_t>({0}, {1});
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, std::move(column), true, /*eq_null=*/true);

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_int\" IS NULL", pushdown.predicates[0]);
    ASSERT_EQ(0, pushdown.value_count);
    ASSERT_EQ("c_int:0", pushdown.pushed_columns);
}

// An empty build side means the join yields nothing, so nothing should travel back.
TEST_F(JDBCRuntimeFilterPushdownTest, EmptyBuildSideRendersConstantFalse) {
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, nullptr);

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("1 = 0", pushdown.predicates[0]);
    ASSERT_EQ(0, pushdown.value_count);
}

// R1: the WHERE and the row limit end up in the same SELECT, so adding a predicate under a limit
// turns limit-then-filter into filter-then-limit and changes which rows come back.
TEST_F(JDBCRuntimeFilterPushdownTest, ScanWithLimitPushesNothing) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx}, /*scan_has_limit=*/true);

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_TRUE(pushdown.predicates.empty());
    ASSERT_EQ(0, pushdown.value_count);
    ASSERT_EQ("scan_has_limit", pushdown.skip_reasons);
}

// A question mark in FE-supplied text used to refuse the whole push down, because the driver
// numbers placeholders by position and an earlier `?` would have shifted every bound value onto
// the wrong column. Nothing is bound now, so there is no numbering to get wrong, and the gate is
// gone. Measured on pgJDBC 42.7.12's own parser: a `?` inside a string literal or a quoted
// identifier is not counted at all, so even the shapes the gate named were never the hazard. The
// one shape that *is* counted -- a bare `?` operator, PostgreSQL's jsonb containment -- reaches a
// statement only through a native_query pass-through, and it fails that statement with or without
// an appended predicate, so refusing to append changed no outcome.
TEST_F(JDBCRuntimeFilterPushdownTest, QuestionMarkInFeSuppliedTextNoLongerRefusesThePushDown) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_int\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"1", "2"}), in_list_values(pushdown.predicates[0]));
    ASSERT_TRUE(pushdown.skip_reasons.empty());
}

// An absent or empty column map is the FE withholding permission, not an absence of columns.
TEST_F(JDBCRuntimeFilterPushdownTest, NoColumnMapPushesNothing) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("not_authorized_by_fe", pushdown.skip_reasons);
}

// A slot the FE left out of the map is a deliberate exclusion, and is reported by name.
TEST_F(JDBCRuntimeFilterPushdownTest, SlotMissingFromColumnMapIsReported) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kBigintSlot, "\"c_bigint\""}}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("c_int:column_not_authorized_by_fe", pushdown.skip_reasons);
}

// G6: the in-filter builder happily builds filters on every scalar type, so the whitelist has to
// turn the unsupported ones away here rather than assume everything that arrives can be rendered.
// TIME is the live example: its FE gate would look exactly like DATETIME's, but the bridge reads a
// PostgreSQL `time` as java.sql.Time, whose millisecond resolution has already dropped the
// microseconds -- 10:20:30.123456 comes back as 10:20:30, and asking the remote side for what came
// back returned no row for three of four measured values and the wrong row for the fourth.
TEST_F(JDBCRuntimeFilterPushdownTest, TimeFilterIsRefusedByTypeWhitelist) {
    std::vector<double> values{3723.456, 86399.999};
    auto* ctx = _make_in_filter(TYPE_TIME_DESC, kTimeSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kTimeSlot, "\"c_time\""}}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_TRUE(pushdown.predicates.empty());
    ASSERT_EQ("c_time:unsupported_type", pushdown.skip_reasons);
}

// CHAR is withheld pending its own evaluation. Not for the reason first assumed -- PostgreSQL
// coerces a varchar parameter to bpchar and compares with bpchareq, which ignores trailing blanks,
// so the blank-padded value the driver returns does match the row it came from. See the note at the
// declaration.
TEST_F(JDBCRuntimeFilterPushdownTest, CharFilterIsRefusedByTypeWhitelist) {
    std::string a = "aa";
    std::vector<Slice> values{Slice(a)};
    auto* ctx = _make_in_filter(TypeDescriptor::create_char_type(8), kCharSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kCharSlot, "\"c_char\""}}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("c_char:unsupported_type", pushdown.skip_reasons);
}

// G3: an IN the user wrote is not a join runtime filter. It is left to the FE's own predicate
// pushdown, and is not reported as a missed chance.
TEST_F(JDBCRuntimeFilterPushdownTest, NonJoinInPredicateIsIgnoredSilently) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values),
                                /*join_runtime_filter=*/false);

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("", pushdown.skip_reasons);
}

TEST_F(JDBCRuntimeFilterPushdownTest, TooManyValuesIsRefused) {
    std::vector<int32_t> values{1, 2, 3};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx}, false, /*max_values=*/2);

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("c_int:too_many_values", pushdown.skip_reasons);
}

// Oracle rejects an IN list of more than 1000 expressions outright, so the effective ceiling is
// lower than the value count the join itself was willing to build.
TEST_F(JDBCRuntimeFilterPushdownTest, InListLongerThanTheRemoteCeilingIsRefused) {
    std::vector<int32_t> values(1001);
    std::iota(values.begin(), values.end(), 1);
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}}, {ctx}, false, /*max_values=*/1024);

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_EQ("c_int:too_many_values", pushdown.skip_reasons);
}

// SQL Server caps one statement at 2100 expressions of this kind, so several filters that are each
// acceptable can still be too much together. The one that would cross the line is dropped whole.
TEST_F(JDBCRuntimeFilterPushdownTest, StatementWideValueCeilingDropsTheLastFilter) {
    std::vector<int32_t> ints(1000);
    std::iota(ints.begin(), ints.end(), 1);
    std::vector<int64_t> bigints(1000);
    std::iota(bigints.begin(), bigints.end(), 1);
    std::string s = "alpha";
    std::vector<Slice> strings{Slice(s)};

    auto* int_ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(ints));
    auto* bigint_ctx = _make_in_filter(TYPE_BIGINT_DESC, kBigintSlot, ColumnTestHelper::build_column(bigints));
    auto* str_ctx =
            _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot, ColumnTestHelper::build_column(strings));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}, {kBigintSlot, "\"c_bigint\""}, {kStrSlot, "\"c_str\""}},
                         {int_ctx, bigint_ctx, str_ctx});

    ASSERT_EQ(2, pushdown.filter_count);
    ASSERT_EQ(2000, pushdown.value_count);
    ASSERT_EQ(2, pushdown.predicates.size());
    ASSERT_EQ(1000, in_list_values(pushdown.predicates[0]).size());
    ASSERT_EQ(1000, in_list_values(pushdown.predicates[1]).size());
    ASSERT_EQ("c_int:1000, c_bigint:1000", pushdown.pushed_columns);
    ASSERT_EQ("c_str:too_many_values_in_statement", pushdown.skip_reasons);
}

// A remote identifier may legitimately contain a question mark -- PostgreSQL allows a column
// named `a?b` if it is quoted. That used to refuse the filter, for the same numbering reason as
// above. Measured on pgJDBC's parser, a `?` inside a quoted identifier yields no bind parameter,
// and nothing is bound anyway, so the reference is used as it stands.
TEST_F(JDBCRuntimeFilterPushdownTest, ColumnReferenceContainingAQuestionMarkIsUsedAsIs) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, "\"c?int\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c?int\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"1", "2"}), in_list_values(pushdown.predicates[0]));
    ASSERT_TRUE(pushdown.skip_reasons.empty());
}

// An empty reference would render as ` IN (1,2)`, which no engine parses -- and only for the
// queries where the join happened to build a filter.
TEST_F(JDBCRuntimeFilterPushdownTest, EmptyColumnReferenceIsRefused) {
    std::vector<int32_t> values{1, 2};
    auto* ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kIntSlot, ""}}, {ctx});

    ASSERT_EQ(0, pushdown.filter_count);
    ASSERT_TRUE(pushdown.predicates.empty());
    ASSERT_EQ("c_int:column_ref_empty", pushdown.skip_reasons);
}

TEST_F(JDBCRuntimeFilterPushdownTest, VarcharFilterRendersQuotedLiterals) {
    std::string a = "alpha";
    std::string b = "beta";
    std::vector<Slice> values{Slice(a), Slice(b)};
    auto* ctx =
            _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_str\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"'alpha'", "'beta'"}), in_list_values(pushdown.predicates[0]));
    ASSERT_EQ(0, _count_placeholders(pushdown.predicates[0]));
}

// Regression guard. A value's own single quote is escaped by doubling it, which is what every
// dialect this catalog reaches reads as one quote inside a literal. Left alone it would close the
// literal early and turn the rest of the value into SQL -- a syntax error at best, and the reason
// the backslash gate below exists is that doubling is only correct while a backslash cannot escape
// anything.
TEST_F(JDBCRuntimeFilterPushdownTest, SingleQuoteInAValueIsDoubled) {
    const std::vector<std::pair<std::string, std::string>> cases = {{"O'Brien", "\"c_str\" IN ('O''Brien')"},
                                                                    {"'", "\"c_str\" IN ('''')"},
                                                                    {"a''b", "\"c_str\" IN ('a''''b')"}};
    for (const auto& [raw, expected] : cases) {
        _value_columns.clear();
        std::vector<Slice> values{Slice(raw)};
        auto* ctx = _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot,
                                    ColumnTestHelper::build_column(values));

        auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

        ASSERT_EQ(1, pushdown.filter_count) << raw;
        ASSERT_EQ(expected, pushdown.predicates[0]);
    }
}

// Regression guard, and the gate that came with literal rendering. Doubling the quote above is
// correct on every dialect here, but only while the remote reads a backslash as an ordinary
// character -- and two session settings the BE cannot see decide that: PostgreSQL's
// standard_conforming_strings and MySQL's NO_BACKSLASH_ESCAPES. With the wrong one `'a\b'` matches
// nothing and `'ab\'` does not terminate at all, which swallows the rest of the statement. That
// second half is not only the server's view: measured on pgJDBC 42.7.12's own parser
// (org.postgresql.core.Parser.parseJdbcSql), `... IN ('ab\') AND "d" = ?` yields one bind
// parameter with standard_conforming_strings on and none with it off, because the driver reads
// `\'` as an escaped quote and everything after it is inside the literal. R3 then forces the whole
// filter out rather than the one value: keeping the rest would leave an IN list narrower than the
// build keys and make the remote side answer with too few rows.
TEST_F(JDBCRuntimeFilterPushdownTest, BackslashInAValueDropsTheWholeFilter) {
    std::string good = "alpha";
    for (const std::string& bad : {std::string("back\\slash"), std::string("trailing\\"), std::string("\\")}) {
        _value_columns.clear();
        std::vector<Slice> values{Slice(good), Slice(bad)};
        auto* ctx = _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot,
                                    ColumnTestHelper::build_column(values));

        auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

        ASSERT_EQ(0, pushdown.filter_count) << bad;
        ASSERT_TRUE(pushdown.predicates.empty()) << bad;
        ASSERT_EQ(0, pushdown.value_count) << bad;
        ASSERT_EQ("c_str:value_not_representable", pushdown.skip_reasons) << bad;
    }
}

// R3: one value that cannot be carried across costs the whole filter. Skipping just that value
// would leave an IN list narrower than the build keys, and the remote side would answer with too
// few rows -- a wrong answer that only appears when the runtime filter happens to be built. These
// two guard the statement rather than the value: what survives here is spliced into SQL text, so a
// mangling can cut the text short mid-literal and leave a quote unclosed.
TEST_F(JDBCRuntimeFilterPushdownTest, OneUnrepresentableValueDropsTheWholeFilter) {
    std::string good = "alpha";
    std::string invalid_utf8 = "a";
    invalid_utf8.push_back(static_cast<char>(0xff)); // a lone continuation byte is not valid UTF-8
    invalid_utf8 += "b";
    for (const std::string& bad : {invalid_utf8, std::string("a\0b", 3)}) {
        _value_columns.clear();
        std::vector<Slice> values{Slice(good), Slice(bad)};
        auto* ctx = _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot,
                                    ColumnTestHelper::build_column(values));

        auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

        ASSERT_EQ(0, pushdown.filter_count);
        ASSERT_TRUE(pushdown.predicates.empty());
        ASSERT_EQ("c_str:value_not_representable", pushdown.skip_reasons);
    }
}

// A supplementary character is valid UTF-8, so validate_utf8 lets it through, but it is not
// valid *modified* UTF-8, which is what NewStringUTF on the far side of this handoff decodes.
// Measured on JDK 17.0.12: NewStringUTF("\xf0\x9f\x98\x80") yields a one-char String holding
// U+00F0, and "a\xf0\x9f\x98\x80b" yields three chars with the 'b' swallowed. The statement now
// carries the values itself, so the truncation does not merely change which rows match: it can end
// the string mid-literal and leave a quote unclosed. Every four-byte sequence is affected, not just
// emoji: CJK Extension B lives there too.
TEST_F(JDBCRuntimeFilterPushdownTest, SupplementaryCharacterValueDropsTheWholeFilter) {
    std::string good = "alpha";
    const std::string emoji = "\xf0\x9f\x98\x80"; // U+1F600
    const std::string embedded =
            "a\xf0\x9f\x98\x80"
            "b";                                      // the swallowed-'b' shape
    const std::string cjk_ext_b = "\xf0\xa0\x80\x80"; // U+20000, a name character
    for (const std::string& bad : {emoji, embedded, cjk_ext_b}) {
        ASSERT_TRUE(validate_utf8(bad.data(), bad.size())) << "precondition: still valid UTF-8";
        _value_columns.clear();
        std::vector<Slice> values{Slice(good), Slice(bad)};
        auto* ctx = _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot,
                                    ColumnTestHelper::build_column(values));

        auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

        ASSERT_EQ(0, pushdown.filter_count);
        ASSERT_TRUE(pushdown.predicates.empty());
        ASSERT_EQ("c_str:value_not_representable", pushdown.skip_reasons);
    }
}

// The counterpart: everything inside the BMP encodes identically in both UTF-8 flavours, so a
// three-byte character must still push. Without this the gate above could be satisfied by
// refusing all non-ASCII, which would silently give up most of the feature outside English.
TEST_F(JDBCRuntimeFilterPushdownTest, BmpMultiByteValueStillPushes) {
    std::string ascii = "alpha";
    std::string cjk = "\xe4\xb8\xad\xe6\x96\x87"; // U+4E2D U+6587
    std::vector<Slice> values{Slice(ascii), Slice(cjk)};
    auto* ctx =
            _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kStrSlot, "\"c_str\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_str\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ(_sorted({"'alpha'", "'" + cjk + "'"}), in_list_values(pushdown.predicates[0]));
}

// Each filter becomes one finished fragment, in the order the conjuncts were visited, and the two
// halves of the quoting rule sit side by side here: the integer key is bare and the string key is
// quoted. One value each keeps both fragments deterministic.
TEST_F(JDBCRuntimeFilterPushdownTest, TwoFiltersRenderOneFinishedFragmentEach) {
    std::vector<int32_t> ints{4};
    std::string s = "alpha";
    std::vector<Slice> strings{Slice(s)};
    auto* int_ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(ints));
    auto* str_ctx =
            _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot, ColumnTestHelper::build_column(strings));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}, {kStrSlot, "\"c_str\""}}, {int_ctx, str_ctx});

    ASSERT_EQ(2, pushdown.filter_count);
    ASSERT_EQ((std::vector<std::string>{"\"c_int\" IN (4)", "\"c_str\" IN ('alpha')"}), pushdown.predicates);
    ASSERT_EQ(2, pushdown.value_count);
    ASSERT_EQ("c_int:1, c_str:1", pushdown.pushed_columns);
}

// The profile has to be able to say which column carried how many values, not just a total: a
// filter that reached the remote query and one that reached it carrying a useless number of values
// look identical otherwise.
TEST_F(JDBCRuntimeFilterPushdownTest, PushedColumnsAreReportedPerColumn) {
    std::vector<int32_t> ints{4, 5, 6};
    std::string s = "alpha";
    std::vector<Slice> strings{Slice(s)};
    auto* int_ctx = _make_in_filter(TYPE_INT_DESC, kIntSlot, ColumnTestHelper::build_column(ints));
    auto* str_ctx =
            _make_in_filter(TypeDescriptor::create_varchar_type(32), kStrSlot, ColumnTestHelper::build_column(strings));
    std::vector<double> times{3723.456};
    auto* time_ctx = _make_in_filter(TYPE_TIME_DESC, kTimeSlot, ColumnTestHelper::build_column(times));

    auto pushdown = _run({{kIntSlot, "\"c_int\""}, {kStrSlot, "\"c_str\""}, {kTimeSlot, "\"c_time\""}},
                         {int_ctx, str_ctx, time_ctx});

    ASSERT_EQ("c_int:3, c_str:1", pushdown.pushed_columns);
    ASSERT_EQ("c_time:unsupported_type", pushdown.skip_reasons);
}

// Nothing pushed means an empty report, not a stale one: the profile renders "none" for it.
TEST_F(JDBCRuntimeFilterPushdownTest, PushedColumnsIsEmptyWhenNothingIsPushed) {
    std::vector<double> times{3723.456};
    auto* ctx = _make_in_filter(TYPE_TIME_DESC, kTimeSlot, ColumnTestHelper::build_column(times));

    auto pushdown = _run({{kTimeSlot, "\"c_time\""}}, {ctx});

    ASSERT_EQ("", pushdown.pushed_columns);
}

// A FLOAT value is rendered as the shortest decimal that reads back as the same 32-bit value, and
// it is quoted. The text matters because a `real` column compares against what the literal parses
// to: the shortest form is the only one guaranteed to parse back to the build key, and PostgreSQL
// resolves an untyped quoted literal against the column's own type rather than widening the column
// to meet a `numeric`.
TEST_F(JDBCRuntimeFilterPushdownTest, FloatFilterRendersQuotedShortestRoundTripText) {
    std::vector<float> values{0.1f,
                              std::numeric_limits<float>::max(),
                              std::numeric_limits<float>::min(),
                              std::numeric_limits<float>::denorm_min(),
                              1.0000001f,
                              -0.0f};
    auto* ctx = _make_in_filter(TYPE_FLOAT_DESC, kFloatSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kFloatSlot, "\"c_float\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_float\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((_sorted({"'-0'", "'0.1'", "'1.0000001'", "'1.1754944e-38'", "'1e-45'", "'3.4028235e+38'"})),
              in_list_values(pushdown.predicates[0]));
}

// Regression guard, and the second half of the quoting asymmetry -- the half that only reproduces
// at one length of IN list. A bare floating-point literal is read as `numeric`, and PostgreSQL
// widens the column to meet it, so `r IN (0.1)` against a `real` column holding 0.1 matches
// nothing. Two values hide it: `r IN (0.1,0.2)` is rewritten as `= ANY('{...}'::real[])`, the array
// takes the column's own type, and both match. The defect therefore appears and disappears with the
// length of the list, which is why a single-value case has to exist for both widths.
TEST_F(JDBCRuntimeFilterPushdownTest, FloatAndDoubleLiteralsAreQuotedEvenAsTheOnlyValue) {
    std::vector<float> floats{0.1f};
    auto* float_ctx = _make_in_filter(TYPE_FLOAT_DESC, kFloatSlot, ColumnTestHelper::build_column(floats));
    auto float_pushdown = _run({{kFloatSlot, "\"c_float\""}}, {float_ctx});
    ASSERT_EQ(1, float_pushdown.filter_count);
    ASSERT_EQ("\"c_float\" IN ('0.1')", float_pushdown.predicates[0]);

    std::vector<double> doubles{0.1};
    auto* double_ctx = _make_in_filter(TYPE_DOUBLE_DESC, kDoubleSlot, ColumnTestHelper::build_column(doubles));
    auto double_pushdown = _run({{kDoubleSlot, "\"c_double\""}}, {double_ctx});
    ASSERT_EQ(1, double_pushdown.filter_count);
    ASSERT_EQ("\"c_double\" IN ('0.1')", double_pushdown.predicates[0]);
}

// The negative half of the case above, and the reason the text is not written with std::to_string:
// that is `%f` with six fraction digits, so the smallest normal and the smallest subnormal would
// both come out as "0.000000" -- two different build keys collapsing onto a third value that
// matches rows neither of them should. Asserting only the expected string would still pass if the
// implementation switched back, because the two happen to look the same at 0.1.
TEST_F(JDBCRuntimeFilterPushdownTest, FloatTextIsNotStdToString) {
    std::vector<float> values{std::numeric_limits<float>::min()};
    auto* ctx = _make_in_filter(TYPE_FLOAT_DESC, kFloatSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kFloatSlot, "\"c_float\""}}, {ctx});

    ASSERT_EQ("0.000000", std::to_string(std::numeric_limits<float>::min()));
    ASSERT_EQ("\"c_float\" IN ('1.1754944e-38')", pushdown.predicates[0]);
}

// fmt writes the non-finite values as "nan" / "inf" / "-inf", and PostgreSQL parses none of the
// three: bare they are read as column references (`column "nan" does not exist`) and quoted they
// are not valid input for `real`. They are respelled rather than dropped because PostgreSQL's
// 'NaN' = 'NaN' is true, so carrying them across returns a superset.
//
// NaN is asserted on the formatter directly, because it cannot be put into a filter at all: the
// in-filter's value set is a phmap flat_hash_set, and inserting a NaN trips its "constructed value
// does not match the lookup key" assertion, since NaN compares unequal to itself. That is a
// property of the existing in-filter machinery rather than of this rendering, so it is not worked
// around here -- which does mean the NaN spelling is covered here and its quoting only by the
// single fmt::format("'{}'", ...) that renders every float, exercised by the infinities below.
TEST_F(JDBCRuntimeFilterPushdownTest, NonFiniteFloatsUseTheJavaSpelling) {
    ASSERT_EQ("NaN", jdbc_ieee754_to_java_text(std::numeric_limits<float>::quiet_NaN()));
    ASSERT_EQ("NaN", jdbc_ieee754_to_java_text(-std::numeric_limits<float>::quiet_NaN()));
    ASSERT_EQ("Infinity", jdbc_ieee754_to_java_text(std::numeric_limits<float>::infinity()));
    ASSERT_EQ("-Infinity", jdbc_ieee754_to_java_text(-std::numeric_limits<float>::infinity()));
    ASSERT_EQ("NaN", jdbc_ieee754_to_java_text(std::numeric_limits<double>::quiet_NaN()));
    ASSERT_EQ("Infinity", jdbc_ieee754_to_java_text(std::numeric_limits<double>::infinity()));
    ASSERT_EQ("-Infinity", jdbc_ieee754_to_java_text(-std::numeric_limits<double>::infinity()));
}

// Regression guard. The infinities are ordinary hash-set members (inf == inf), so they do travel
// through a real filter, and they are the case where losing the quotes is not a subset but an
// outright failure: bare, `Infinity` is an identifier and PostgreSQL answers `column "infinity"
// does not exist`, failing the whole remote query on exactly the executions where a join happened
// to build this filter.
TEST_F(JDBCRuntimeFilterPushdownTest, NonFiniteFloatsAreQuotedInTheInList) {
    std::vector<float> values{std::numeric_limits<float>::infinity(), -std::numeric_limits<float>::infinity()};
    auto* ctx = _make_in_filter(TYPE_FLOAT_DESC, kFloatSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kFloatSlot, "\"c_float\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_float\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"'-Infinity'", "'Infinity'"}), in_list_values(pushdown.predicates[0]));
    ASSERT_EQ(std::string::npos, pushdown.predicates[0].find("(Infinity"));
    ASSERT_EQ(std::string::npos, pushdown.predicates[0].find(",Infinity"));
}

TEST_F(JDBCRuntimeFilterPushdownTest, DoubleFilterRendersQuotedShortestRoundTripText) {
    std::vector<double> values{0.1, std::numeric_limits<double>::max(), std::numeric_limits<double>::min(),
                               std::numeric_limits<double>::denorm_min(), 1.0000000000000002};
    auto* ctx = _make_in_filter(TYPE_DOUBLE_DESC, kDoubleSlot, ColumnTestHelper::build_column(values));

    auto pushdown = _run({{kDoubleSlot, "\"c_double\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_double\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((_sorted({"'0.1'", "'1.0000000000000002'", "'1.7976931348623157e+308'", "'2.2250738585072014e-308'",
                        "'5e-324'"})),
              in_list_values(pushdown.predicates[0]));
}

// A DATE is rendered as the same ten characters the bridge's reader produced for the column, quoted
// so PostgreSQL parses them itself. Quoted text never reaches the JVM, which is the point:
// 1582-10-10 is the value that proved it, because java.sql.Date.valueOf moves it to 1582-10-20
// through the hybrid Julian/Gregorian calendar java.util uses.
TEST_F(JDBCRuntimeFilterPushdownTest, DateFilterRendersQuotedText) {
    std::vector<DateValue> values{DateValue::create(1582, 10, 10), DateValue::create(9999, 12, 31),
                                  DateValue::create(1, 1, 1)};
    auto* ctx = _make_in_filter(TYPE_DATE_DESC, kDateSlot, build_temporal_column<TYPE_DATE>(values));

    auto pushdown = _run({{kDateSlot, "\"c_date\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count);
    ASSERT_EQ("\"c_date\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"'0001-01-01'", "'1582-10-10'", "'9999-12-31'"}),
              in_list_values(pushdown.predicates[0]));
}

// TimestampValue::to_string() omits the fraction when the microsecond field is zero, so one filter
// carries both a 19-character and a 26-character value and PostgreSQL's own parser has to accept
// either. The microseconds are not optional: a value truncated to the second matched a different
// row rather than none. Quoted text also keeps setTimestamp's default-zone resolution out of it --
// a wall clock inside a DST gap would be moved an hour.
TEST_F(JDBCRuntimeFilterPushdownTest, DatetimeFilterRendersQuotedTextAtBothLengths) {
    std::vector<TimestampValue> values{TimestampValue::create(2024, 1, 15, 10, 20, 30, 0),
                                       TimestampValue::create(2024, 1, 15, 10, 20, 30, 123456),
                                       TimestampValue::create(9999, 12, 31, 23, 59, 59, 999999)};
    auto* ctx = _make_in_filter(TYPE_DATETIME_DESC, kDatetimeSlot, build_temporal_column<TYPE_DATETIME>(values));

    auto pushdown = _run({{kDatetimeSlot, "\"c_ts\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count) << "skipped: [" << pushdown.skip_reasons << "]";
    ASSERT_EQ("\"c_ts\" IN (...)", in_list_elided(pushdown.predicates[0]));
    ASSERT_EQ((std::vector<std::string>{"'2024-01-15 10:20:30'", "'2024-01-15 10:20:30.123456'",
                                        "'9999-12-31 23:59:59.999999'"}),
              in_list_values(pushdown.predicates[0]));
}

// R3: StarRocks accepts year 0000 and PostgreSQL has no year 0 -- it reads 0000-01-01 as
// 0001-01-01 BC, a different date. One such value costs the whole filter, the same way an
// unrepresentable string does, and for the same reason: keeping the rest would leave an IN list
// narrower than the build keys.
TEST_F(JDBCRuntimeFilterPushdownTest, YearOutsideTheRemoteRangeDropsTheWholeFilter) {
    std::vector<DateValue> dates{DateValue::create(2024, 5, 17), DateValue::create(0, 1, 1)};
    auto* date_ctx = _make_in_filter(TYPE_DATE_DESC, kDateSlot, build_temporal_column<TYPE_DATE>(dates));
    auto date_pushdown = _run({{kDateSlot, "\"c_date\""}}, {date_ctx});
    ASSERT_EQ(0, date_pushdown.filter_count);
    ASSERT_TRUE(date_pushdown.predicates.empty());
    ASSERT_EQ("c_date:value_not_representable", date_pushdown.skip_reasons);

    std::vector<TimestampValue> timestamps{TimestampValue::create(2024, 5, 17, 0, 0, 0, 0),
                                           TimestampValue::create(0, 1, 1, 0, 0, 0, 0)};
    auto* ts_ctx = _make_in_filter(TYPE_DATETIME_DESC, kDatetimeSlot, build_temporal_column<TYPE_DATETIME>(timestamps));
    auto ts_pushdown = _run({{kDatetimeSlot, "\"c_ts\""}}, {ts_ctx});
    ASSERT_EQ(0, ts_pushdown.filter_count);
    ASSERT_EQ("c_ts:value_not_representable", ts_pushdown.skip_reasons);
}

// R2 is untouched by the wider whitelist: a null-safe join on one of the new types still renders
// the IS NULL arm rather than folding a NULL into the list.
TEST_F(JDBCRuntimeFilterPushdownTest, NullSafeJoinOnDateStillRendersOrIsNull) {
    auto values = build_temporal_column<TYPE_DATE>(std::vector<DateValue>{DateValue::create(2024, 5, 17)});
    auto column = NullableColumn::create(std::move(values), NullColumn::create(1, 0));
    column->append_nulls(1);
    auto* ctx = _make_in_filter(TYPE_DATE_DESC, kDateSlot, std::move(column), true, /*eq_null=*/true);

    auto pushdown = _run({{kDateSlot, "\"c_date\""}}, {ctx});

    ASSERT_EQ(1, pushdown.filter_count) << "skipped: [" << pushdown.skip_reasons << "]";
    ASSERT_EQ("\"c_date\" IN ('2024-05-17') OR \"c_date\" IS NULL", pushdown.predicates[0]);
    ASSERT_EQ(1, pushdown.value_count);
}

} // namespace starrocks::connector
