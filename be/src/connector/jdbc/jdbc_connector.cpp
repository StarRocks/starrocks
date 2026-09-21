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

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/string/slice.h"
#include "base/string/utf8_check.h"
#include "common/config_scan_io_fwd.h"
#include "connector/jdbc/jdbc_driver_manager.h"
#include "connector/jdbc/jdbc_scanner.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/in_const_predicate.hpp"
#include "runtime/descriptors.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"
#include "types/logical_type_infra.h"

namespace starrocks::connector {

// ================================

DataSourceProviderPtr JDBCConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                 const TPlanNode& plan_node) const {
    return std::make_unique<JDBCDataSourceProvider>(scan_node, plan_node);
}

// ================================

JDBCDataSourceProvider::JDBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _jdbc_scan_node(plan_node.jdbc_scan_node) {}

DataSourcePtr JDBCDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<JDBCDataSource>(this, scan_range);
}

const TupleDescriptor* JDBCDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_jdbc_scan_node.tuple_id);
}

// ================================

static std::string get_jdbc_sql(const Slice jdbc_url, const std::string& table, const std::vector<std::string>& columns,
                                const std::vector<std::string>& filters, int64_t limit) {
    std::ostringstream oss;
    oss << "SELECT";
    if (limit != -1 && jdbc_url.starts_with("jdbc:sqlserver")) {
        oss << fmt::format(" TOP({}) ", limit);
        limit = -1;
    }
    for (size_t i = 0; i < columns.size(); i++) {
        oss << (i == 0 ? "" : ",") << " " << columns[i];
    }
    oss << " FROM " << table;
    if (!filters.empty()) {
        oss << " WHERE ";
        for (size_t i = 0; i < filters.size(); i++) {
            oss << (i == 0 ? "" : " AND") << "(" << filters[i] << ")";
        }
    }
    if (limit != -1) {
        if (jdbc_url.starts_with("jdbc:oracle")) {
            // oracle doesn't support limit clause, we should generate a subquery to do this
            // ref: https://stackoverflow.com/questions/470542/how-do-i-limit-the-number-of-rows-returned-by-an-oracle-query-after-ordering
            return fmt::format("SELECT * FROM ({}) WHERE ROWNUM <= {}", oss.str(), limit);
        } else {
            oss << " LIMIT " << limit;
        }
    }
    return oss.str();
}

namespace {

// The years a StarRocks date can hold but a remote engine cannot be asked about. StarRocks accepts
// year 0000 (`cast("0000-01-01" as date)` is not NULL) while PostgreSQL has no year 0 at all: it
// reads 0000-01-01 as 0001-01-01 BC, so a value from that year would be bound as a different date.
// The bridge already refuses to read a year outside this range back (JDBCScanner rejects a
// PostgreSQL temporal value outside 0001-01-01..9999-12-31), so no row StarRocks can see holds one
// and dropping the filter costs nothing -- it just keeps the rendering and the reading halves
// agreeing on one range instead of letting PostgreSQL resolve the era on its own.
constexpr int MIN_BINDABLE_YEAR = 1;
constexpr int MAX_BINDABLE_YEAR = 9999;

// Shortest text that round-trips back to the same IEEE-754 value through the bridge's
// Float.parseFloat / Double.parseDouble.
//
// std::to_string is unusable here and the difference is not cosmetic: it is `%f` with six fraction
// digits, so std::to_string(1.17549435e-38f) is "0.000000" and std::to_string(1.4e-45f) is
// "0.000000" too -- two different build keys collapsing onto a third value that matches rows
// neither of them should. fmt's Dragonbox formatting was checked against std::to_chars on 26
// values, including both subnormal minima, both maxima and the ulp neighbours 1.0000001f and
// 1.0000000000000002, and agreed byte for byte; every one of them parsed back to the same bit
// pattern in Java.
//
// The three non-finite values need a spelling of their own. fmt writes them as "nan" / "inf" /
// "-inf" and Float.parseFloat rejects all three with NumberFormatException, which the bridge turns
// into a SQLException that fails the whole remote query -- again only when a join happened to build
// a filter. They are not dropped instead, because PostgreSQL compares them the way an IN list
// needs: 'NaN' = 'NaN' is true there (unlike IEEE-754, and unlike the BE's own HashSet<float>,
// which uses C++ equality and therefore drops NaN locally), so carrying them across returns a
// superset, never a subset. fmt spells a negative NaN "-nan"; PostgreSQL has a single canonical
// NaN, so normalising both to "NaN" changes nothing about what matches.
template <typename T>
std::string ieee754_to_java_text(T value) {
    if (std::isnan(value)) {
        return "NaN";
    }
    if (std::isinf(value)) {
        return value > 0 ? "Infinity" : "-Infinity";
    }
    return fmt::format("{}", value);
}

// A ceiling the remote engine imposes, not a second opinion on how selective a filter is: Oracle
// raises ORA-01795 for an IN list of more than 1000 expressions, and it counts literals, not just
// parameter markers. Integers reach every dialect (canCarryRuntimeFilter allows them unconditionally),
// so Oracle is reachable. Exceeding it is an outright query failure, and because a runtime filter
// only exists when the join builds one, it would be an intermittent one. It is the smallest limit
// across the engines the JDBC catalog supports, so one conservative number keeps this code free of
// per-dialect branches. In practice it costs nothing: the join stops building an in-filter above
// max_pushdown_conditions_per_column (1024 by default) rows.
constexpr size_t MAX_REMOTE_IN_LIST_VALUES = 1000;
// And a ceiling on one statement's worth of them, across every column that carried a filter. This
// one is about the size of the generated text, which is a concern here and would not have been for
// bound parameters: a value is spliced in as a literal, so a VARCHAR filter at the per-column
// ceiling already writes a megabyte-class IN list, and several columns multiply it. The statement
// crosses JNI as one string and is parsed by the remote engine; neither has a stated limit worth
// relying on, so cap the total rather than discover one in production.
constexpr size_t MAX_REMOTE_IN_LIST_VALUES_PER_STATEMENT = 2000;

// What happened to one conjunct while looking for a pushable join runtime filter.
enum class RuntimeFilterOutcome {
    // Not a join runtime filter at all -- an ordinary conjunct, or the min/max predicate
    // DataSource::parse_runtime_filters prepends. Reported nowhere: it is not a missed chance.
    kNotRuntimeFilter = 0,
    kRendered,
    kSkipped,
};

struct RuntimeFilterRenderResult {
    RuntimeFilterOutcome outcome = RuntimeFilterOutcome::kNotRuntimeFilter;
    // Only set when outcome is kSkipped. A stable code, not prose: it ends up in the scan profile.
    const char* reason = nullptr;
};

struct RenderedRuntimeFilter {
    // Complete SQL fragment, values already rendered as literals; get_jdbc_sql wraps it in its own
    // parentheses. Nothing here is bound: the statement the bridge receives carries no parameters.
    std::string predicate;
    // How many build-side values this fragment stands for. Reported in the scan profile -- the
    // predicate text alone does not say, once an IN list is long.
    size_t value_count = 0;
};

// Renders one conjunct into a remote predicate, if it is a join runtime filter this scan is allowed
// and able to push down. Instantiated for every scalar type so that an unsupported type gets a
// reason code instead of vanishing.
//
// Two rules drive most of what follows.
//
//   R2 (NULL semantics): null_in_set() means a null-safe join whose build side held a NULL, so
//   probe-side NULLs should match -- `(col IN (...) OR col IS NULL)`. Folding the NULL into the
//   list would drop those rows, because `NULL IN (...)` is UNKNOWN. (mysql_scanner.cpp gets this
//   wrong; the authority is ChunkPredicateBuilder::normalize_join_runtime_filter.)
//
//   R3 (superset law): what we render must return a superset. A value we cannot carry safely drops
//   the *whole* filter, never just that value -- a shorter IN list is a strict subset of the build
//   keys and answers with too few rows. When equivalence is uncertain, drop: that costs
//   performance, guessing wrong costs correctness and only reproduces when a filter gets built.
//
// Two types are out of scope for reasons that are not about the whitelist:
//
//   TIME -- the read path already lost the value. pgJDBC hands a PostgreSQL `time` back as
//   java.sql.Time (millisecond resolution), so 10:20:30.123456 arrives as 10:20:30. Measured: no
//   row for three of four values and the wrong row for the fourth. Fix the reader first.
//
//   DECIMAL -- cost, not safety. Precision and scale live on root->get_child(0)->type() rather
//   than on the predicate, and the stored value is an unscaled integer needing
//   DecimalV3Cast::to_string. An unconstrained PostgreSQL `numeric` stays excluded regardless,
//   since it is read strictly as DECIMAL128(38,18).
struct RuntimeFilterRenderer {
    template <LogicalType Type>
    RuntimeFilterRenderResult operator()(const Expr* root, const SlotDescriptor* slot, const std::string& column_ref,
                                         size_t max_values, RenderedRuntimeFilter* out) const {
        if (typeid(*root) != typeid(VectorizedInConstPredicate<Type>)) {
            return {};
        }
        const auto* pred = down_cast<const VectorizedInConstPredicate<Type>*>(root);
        // G3: only the exact value set a join builds. A user-written IN is left to the FE's own
        // predicate pushdown, which renders literals and owns its own correctness rules.
        if (!pred->is_join_runtime_filter()) {
            return {};
        }
        // G2: the values are in the domain of the predicate's probe expression. Only when that
        // expression is the bare column can they be compared against the remote column; a cast or
        // any other expression would put them in a different domain (e.g. `CAST(s AS BIGINT) IN
        // (1)` says nothing about which strings `s` holds).
        const Expr* probe_expr = root->get_num_children() > 0 ? root->get_child(0) : nullptr;
        if (probe_expr == nullptr || !probe_expr->is_slotref() ||
            down_cast<const ColumnRef*>(probe_expr)->slot_id() != slot->id()) {
            return {RuntimeFilterOutcome::kSkipped, "probe_expr_not_slot_ref"};
        }
        // G8: NOT IN from a join runtime filter is not a thing today, but if one ever appears its
        // complement semantics are not what the rendering below produces.
        if (pred->is_not_in()) {
            return {RuntimeFilterOutcome::kSkipped, "not_in"};
        }

        constexpr bool kIsInteger =
                (Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT || Type == TYPE_BIGINT);
        // CHAR is excluded pending its own evaluation, not because padding breaks it: measured on
        // PostgreSQL 16.15, there is no `bpchar = varchar` operator at all -- the comparison
        // coerces to bpchar and ignores trailing blanks, so the direction is a superset. What is
        // still unmeasured is what StarRocks stores in a CHAR slot read from a remote bpchar.
        constexpr bool kIsString = (Type == TYPE_VARCHAR);
        // Single and double precision stay apart even though both render as quoted text: the
        // StarRocks type *is* the filter's domain, because G2 above established the probe
        // expression is this scan's own slot ref, and a width mismatch would have put a cast there
        // that PushDownProjectToJDBCScanRule refuses to push.
        constexpr bool kIsFloat = (Type == TYPE_FLOAT);
        constexpr bool kIsDouble = (Type == TYPE_DOUBLE);
        // `timestamp with time zone` is a different remote type and is refused by the FE, which
        // is the only side that can tell the two apart.
        constexpr bool kIsDate = (Type == TYPE_DATE);
        constexpr bool kIsDatetime = (Type == TYPE_DATETIME);
        if constexpr (!kIsInteger && !kIsString && !kIsFloat && !kIsDouble && !kIsDate && !kIsDatetime) {
            return {RuntimeFilterOutcome::kSkipped, "unsupported_type"};
        } else {
            // The FE only ever puts a real, quoted column reference here, so an empty one is a
            // broken contract rather than an expected case. It would render as ` IN (?)`, which no
            // engine parses, so the whole query would fail -- and only for the queries where the
            // join happened to build a filter. Refuse the filter instead.
            if (column_ref.empty()) {
                return {RuntimeFilterOutcome::kSkipped, "column_ref_empty"};
            }
            // R3: an array-backed set keeps its values in _array_buffer and leaves hash_set() empty,
            // so reading hash_set() would render an empty IN list and drop every row. Join runtime
            // filters do not use it today (hash_joiner.cpp never calls use_array_set), but this must
            // fail closed rather than silently lose values if that ever changes.
            if (pred->is_use_array()) {
                return {RuntimeFilterOutcome::kSkipped, "array_backed_set"};
            }
            // G4: `max_values` is resolved by the caller exactly the way
            // HashJoiner::_create_runtime_in_filters resolves it (session variable first, BE config
            // otherwise), so this adds no third notion of how selective a filter has to be -- it is
            // the same ceiling that decided whether the filter was built at all. The second term is
            // a different question, what the remote engine will accept in one IN list.
            if (pred->hash_set().size() > std::min(max_values, MAX_REMOTE_IN_LIST_VALUES)) {
                return {RuntimeFilterOutcome::kSkipped, "too_many_values"};
            }

            // Each value becomes the SQL text that stands for it inside the IN list. Quoting is not
            // cosmetic and is not uniform -- both halves of the asymmetry below were measured, and
            // both fail silently in the direction R3 forbids if they are "tidied up" into one rule.
            //
            //   An integer is written bare. Quoting it makes the remote engine read the operand as
            //   text, which matters when the StarRocks type and the remote column's type disagree:
            //   a PostgreSQL text column a hand-written schema declared BIGINT answered `c IN
            //   ('42')` with 1 row where the local filter kept 6 -- a silent subset. Bare, the same
            //   query is `operator does not exist: text = integer`, which is loud.
            //
            //   Everything else is quoted, floating point included. A bare float literal is read as
            //   `numeric` and the column widens to meet it, so `r IN (0.1)` against a `real` column
            //   holding 0.1 matches nothing -- while `r IN (0.1, 0.2)` matches both, because a
            //   two-element list is rewritten as `= ANY('{...}'::real[])` and the array takes the
            //   column's own type. The defect therefore appears and disappears with the length of
            //   the IN list. Quoted, the literal is untyped and PostgreSQL resolves it from the
            //   column: `r IN ('0.1')` matches, and so do NaN and Infinity, which bare would be
            //   parsed as column references (`column "nan" does not exist`).
            std::vector<std::string> values;
            values.reserve(pred->hash_set().size());
            for (const auto& value : pred->hash_set()) {
                if constexpr (kIsInteger) {
                    values.emplace_back(std::to_string(value));
                } else if constexpr (kIsFloat || kIsDouble) {
                    values.emplace_back(fmt::format("'{}'", ieee754_to_java_text(value)));
                } else if constexpr (kIsDate || kIsDatetime) {
                    int year = 0;
                    int month = 0;
                    int day = 0;
                    if constexpr (kIsDate) {
                        value.to_date(&year, &month, &day);
                    } else {
                        int hour = 0;
                        int minute = 0;
                        int second = 0;
                        int usec = 0;
                        value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
                    }
                    // R3: a year the remote engine cannot be asked about would be bound as some
                    // other date, so the whole filter goes rather than that one value.
                    if (year < MIN_BINDABLE_YEAR || year > MAX_BINDABLE_YEAR) {
                        return {RuntimeFilterOutcome::kSkipped, "value_not_representable"};
                    }
                    // "YYYY-MM-DD" for a date; for a timestamp either 19 characters or, when the
                    // microsecond field is not zero, 26 with exactly six fraction digits. Both
                    // lengths can occur in one filter and PostgreSQL accepts either. Quoted, the
                    // literal is untyped and the column decides how to read it, which is also what
                    // keeps it out of the JVM: a date bound through java.sql.Date would go through
                    // the hybrid Julian/Gregorian calendar (1582-10-10 arrives as 1582-10-20) and
                    // a timestamp through setTimestamp would be resolved in the JVM's default zone
                    // (a wall clock inside a DST gap is moved an hour). Neither applies to text
                    // PostgreSQL parses itself.
                    values.emplace_back(fmt::format("'{}'", value.to_string()));
                } else {
                    std::string text = value.to_string();
                    // G7/R3: the finished statement crosses JNI through NewStringUTF, which speaks
                    // modified UTF-8. Invalid UTF-8 is undefined behaviour there and an embedded
                    // NUL terminates the string early. These guards protect the *statement*, not
                    // one value: a value that survives them intact is spliced into SQL text, so a
                    // mangling here does not merely change which rows match, it can cut the text
                    // short mid-literal and leave a quote unclosed.
                    if (!validate_utf8(text.data(), text.size()) || text.find('\0') != std::string::npos) {
                        return {RuntimeFilterOutcome::kSkipped, "value_not_representable"};
                    }
                    // R3: a backslash. Doubling the single quote below is correct on every dialect
                    // this catalog can reach, but only while the remote reads a backslash as an
                    // ordinary character. Two session-level settings the BE cannot see decide that
                    // -- PostgreSQL's standard_conforming_strings and MySQL's NO_BACKSLASH_ESCAPES
                    // -- and with the wrong one the same literal either matches nothing ('a\b' on
                    // scs=off) or does not terminate ('ab\' is a syntax error). Measured on
                    // PostgreSQL 16.15 both ways: with no backslash in the value, '' doubling is
                    // correct under scs=on and scs=off alike. So refuse the whole filter rather
                    // than guess at a remote GUC. It costs the join keys that contain a backslash.
                    if (text.find('\\') != std::string::npos) {
                        return {RuntimeFilterOutcome::kSkipped, "value_not_representable"};
                    }
                    // Standard UTF-8 is not modified UTF-8 for anything outside the BMP. A
                    // supplementary character is one four-byte sequence in the former and a
                    // six-byte CESU-8 surrogate pair in the latter, and NewStringUTF is only
                    // specified for the latter. Measured on Corretto 17.0.14 and OpenJDK 17.0.20
                    // alike: the conversion budgets one char per non-continuation byte but then
                    // emits one char for each byte of the sequence, so it stops three bytes early
                    // for every supplementary character in the string -- `IN ('a<U+1F600>','z')`
                    // is 16 bytes and arrives as 13 chars, losing `z')` including the closing
                    // quote and the bracket. validate_utf8 accepts those sequences because they
                    // are valid UTF-8, so the lead byte is rejected separately. This is not only
                    // emoji: CJK Extension B (U+20000-U+2A6DF), which holds rare characters used
                    // in personal names, is four-byte throughout.
                    if (std::any_of(text.begin(), text.end(),
                                    [](char byte) { return (static_cast<unsigned char>(byte) & 0xF8) == 0xF0; })) {
                        return {RuntimeFilterOutcome::kSkipped, "value_not_representable"};
                    }
                    // Standard SQL escaping, and the only one needed once a backslash is refused
                    // above. Every dialect this catalog reaches reads '' inside a quoted literal
                    // as one quote.
                    std::string quoted;
                    quoted.reserve(text.size() + 2);
                    quoted.push_back('\'');
                    for (char byte : text) {
                        if (byte == '\'') {
                            quoted.push_back('\'');
                        }
                        quoted.push_back(byte);
                    }
                    quoted.push_back('\'');
                    values.emplace_back(std::move(quoted));
                }
            }

            std::string predicate;
            if (values.empty()) {
                if (pred->null_in_set()) {
                    // R2: null-safe join whose build side held only NULL.
                    predicate = fmt::format("{} IS NULL", column_ref);
                } else {
                    // Empty build side: the join produces nothing, so the remote query should not
                    // ship a single row back. Same constant-false the MySQL external table uses.
                    predicate = "1 = 0";
                }
            } else {
                std::string list;
                for (size_t i = 0; i < values.size(); i++) {
                    if (i != 0) {
                        list += ',';
                    }
                    list += values[i];
                }
                if (pred->null_in_set()) {
                    // R2.
                    predicate = fmt::format("{} IN ({}) OR {} IS NULL", column_ref, list, column_ref);
                } else {
                    predicate = fmt::format("{} IN ({})", column_ref, list);
                }
            }

            out->predicate = std::move(predicate);
            out->value_count = values.size();
            return {RuntimeFilterOutcome::kRendered, nullptr};
        }
    }
};

} // namespace

std::string jdbc_ieee754_to_java_text(float value) {
    return ieee754_to_java_text(value);
}

std::string jdbc_ieee754_to_java_text(double value) {
    return ieee754_to_java_text(value);
}

// The predicates this produces land in the outermost WHERE that get_jdbc_sql builds. That is the
// only correct injection point and there is no need for another: every pushed-down shape the FE
// produces (aggregation, join, TopN, projection) arrives as `(<body>) sr_inline` in the FROM clause,
// so a WHERE added here sits outside the remote GROUP BY / ORDER BY / LIMIT -- the same place the
// filter occupies in the local plan.
void build_jdbc_runtime_filter_pushdown(const std::map<SlotId, std::string>& runtime_filter_columns,
                                        bool scan_has_limit, const std::vector<ExprContext*>& conjunct_ctxs,
                                        const TupleDescriptor& tuple_desc, size_t max_values,
                                        JDBCRuntimeFilterPushdown* out) {
    std::vector<std::string> skip_reasons;
    std::vector<std::string> pushed_columns;

    // R1, BE half of the gate (the FE withholds runtime_filter_columns for the same reason, and
    // either half alone is enough). get_jdbc_sql puts the WHERE and the row limit in one SELECT with
    // the WHERE first -- and Oracle's `SELECT * FROM (...) WHERE ROWNUM <= n` and SQL Server's
    // `TOP(n)` are filter-then-limit too. Adding a predicate under a limit therefore turns
    // "limit then filter" into "filter then limit" and changes which rows come back: on PostgreSQL a
    // scan that returned ids 1..10 returns 150, 160, 170 instead.
    if (scan_has_limit) {
        skip_reasons.emplace_back("scan_has_limit");
    } else if (runtime_filter_columns.empty()) {
        // An absent or empty map is the FE saying "not allowed here", not "no columns".
        skip_reasons.emplace_back("not_authorized_by_fe");
    } else {
        std::unordered_map<SlotId, SlotDescriptor*> slot_by_id;
        for (SlotDescriptor* slot : tuple_desc.slots()) {
            slot_by_id[slot->id()] = slot;
        }
        static const std::string kNoColumn;

        for (auto* ctx : conjunct_ctxs) {
            const Expr* root = ctx == nullptr ? nullptr : ctx->root();
            if (root == nullptr) {
                continue;
            }
            // An in-filter always references exactly one slot; anything else cannot be one.
            std::vector<SlotId> slot_ids;
            if (root->get_slot_ids(&slot_ids) != 1) {
                continue;
            }
            auto slot_iter = slot_by_id.find(slot_ids[0]);
            if (slot_iter == slot_by_id.end()) {
                continue;
            }
            const SlotDescriptor* slot = slot_iter->second;

            auto column_iter = runtime_filter_columns.find(slot_ids[0]);
            // The FE lists a column here only when it can name it in the remote dialect and is
            // willing to have a filter on it, so a slot that is missing is a deliberate exclusion.
            const std::string* column_ref =
                    column_iter == runtime_filter_columns.end() ? nullptr : &column_iter->second;

            RenderedRuntimeFilter rendered;
            auto result = type_dispatch_predicate<RuntimeFilterRenderResult>(
                    slot->type().type, false, RuntimeFilterRenderer(), root, slot,
                    column_ref == nullptr ? kNoColumn : *column_ref, max_values, &rendered);

            if (result.outcome == RuntimeFilterOutcome::kNotRuntimeFilter) {
                continue;
            }
            if (column_ref == nullptr) {
                // Decided after the dispatch so that this only reports slots that really carry a
                // runtime filter, instead of every unrelated column of the scan.
                skip_reasons.emplace_back(fmt::format("{}:column_not_authorized_by_fe", slot->col_name()));
                continue;
            }
            if (result.outcome == RuntimeFilterOutcome::kSkipped) {
                skip_reasons.emplace_back(fmt::format("{}:{}", slot->col_name(), result.reason));
                continue;
            }
            // Dropping a whole filter only costs performance (R3 runs the other way: it is keeping
            // a partial one that would be wrong), so the statement-wide parameter ceiling is
            // enforced by leaving this filter out entirely.
            if (out->value_count + static_cast<int64_t>(rendered.value_count) >
                static_cast<int64_t>(MAX_REMOTE_IN_LIST_VALUES_PER_STATEMENT)) {
                skip_reasons.emplace_back(fmt::format("{}:too_many_values_in_statement", slot->col_name()));
                continue;
            }

            out->predicates.emplace_back(std::move(rendered.predicate));
            out->filter_count++;
            out->value_count += static_cast<int64_t>(rendered.value_count);
            pushed_columns.emplace_back(fmt::format("{}:{}", slot->col_name(), rendered.value_count));
        }
    }

    for (size_t i = 0; i < pushed_columns.size(); i++) {
        out->pushed_columns += (i == 0 ? "" : ", ");
        out->pushed_columns += pushed_columns[i];
    }
    for (size_t i = 0; i < skip_reasons.size(); i++) {
        out->skip_reasons += (i == 0 ? "" : ", ");
        out->skip_reasons += skip_reasons[i];
    }
}

// Appends a remote predicate for every join runtime filter this scan may push down, and records in
// `scan_ctx` both what was pushed and why anything else was not.
void JDBCDataSource::_append_runtime_filters(RuntimeState* state, JDBCScanContext* scan_ctx,
                                             std::vector<std::string>* filters) {
    const TJDBCScanNode& jdbc_scan_node = _provider->_jdbc_scan_node;
    static const std::map<SlotId, std::string> kNoColumns;

    // Resolved the same way HashJoiner::_create_runtime_in_filters resolves it (session variable
    // first, BE config otherwise), so this introduces no third notion of "too many values".
    size_t max_values = config::max_pushdown_conditions_per_column;
    if (state->query_options().__isset.max_pushdown_conditions_per_column) {
        max_values = state->query_options().max_pushdown_conditions_per_column;
    }

    JDBCRuntimeFilterPushdown pushdown;
    build_jdbc_runtime_filter_pushdown(
            jdbc_scan_node.__isset.runtime_filter_columns ? jdbc_scan_node.runtime_filter_columns : kNoColumns,
            _read_limit != -1, _conjunct_ctxs, *_tuple_desc, max_values, &pushdown);

    for (auto& predicate : pushdown.predicates) {
        filters->emplace_back(std::move(predicate));
    }
    scan_ctx->pushed_runtime_filter_count = pushdown.filter_count;
    scan_ctx->pushed_runtime_filter_value_count = pushdown.value_count;
    scan_ctx->runtime_filter_pushed_columns = std::move(pushdown.pushed_columns);
    scan_ctx->runtime_filter_skip_reasons = std::move(pushdown.skip_reasons);
}

JDBCDataSource::JDBCDataSource(const JDBCDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {}

std::string JDBCDataSource::name() const {
    return "JDBCDataSource";
}

Status JDBCDataSource::open(RuntimeState* state) {
    const TJDBCScanNode& jdbc_scan_node = _provider->_jdbc_scan_node;
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(jdbc_scan_node.tuple_id);
    RETURN_IF_ERROR(_create_scanner(state));
    return Status::OK();
}

void JDBCDataSource::close(RuntimeState* state) {
    if (_scanner != nullptr) {
        WARN_IF_ERROR(_scanner->close(state), "close jdbc scanner failed");
    }
}

Status JDBCDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    bool eos = false;
    RETURN_IF_ERROR(_init_chunk_if_needed(chunk, 0));
    do {
        RETURN_IF_ERROR(_scanner->get_next(state, chunk, &eos));
    } while (!eos && (*chunk)->num_rows() == 0);
    if (eos) {
        return Status::EndOfFile("");
    }
    _rows_read += (*chunk)->num_rows();
    _bytes_read += (*chunk)->bytes_usage();
    return Status::OK();
}

int64_t JDBCDataSource::raw_rows_read() const {
    return _rows_read;
}
int64_t JDBCDataSource::num_rows_read() const {
    return _rows_read;
}
int64_t JDBCDataSource::num_bytes_read() const {
    return _bytes_read;
}
int64_t JDBCDataSource::cpu_time_spent() const {
    // TODO: calculte the real cputime
    return 0;
}

Status JDBCDataSource::_create_scanner(RuntimeState* state) {
    const TJDBCScanNode& jdbc_scan_node = _provider->_jdbc_scan_node;
    const auto* jdbc_table = down_cast<const JDBCTableDescriptor*>(_tuple_desc->table_desc());

    Status status;
    std::string driver_name(jdbc_table->jdbc_driver_name());
    std::string driver_url(jdbc_table->jdbc_driver_url());
    std::string driver_checksum(jdbc_table->jdbc_driver_checksum());
    std::string driver_class(jdbc_table->jdbc_driver_class());
    std::string driver_location;

    status = JDBCDriverManager::getInstance()->get_driver_location(driver_name, driver_url, driver_checksum,
                                                                   &driver_location);
    if (!status.ok()) {
        LOG(ERROR) << fmt::format("Get JDBC Driver[{}] error, error is {}", driver_name, status.to_string());
        return status;
    }

    JDBCScanContext scan_ctx;
    if (jdbc_scan_node.__isset.strict_numeric_columns) {
        scan_ctx.strict_numeric_columns = jdbc_scan_node.strict_numeric_columns;
    }
    scan_ctx.driver_path = driver_location;
    scan_ctx.driver_class_name = driver_class;
    scan_ctx.jdbc_url = jdbc_table->jdbc_url();
    scan_ctx.user = jdbc_table->jdbc_user();
    scan_ctx.passwd = jdbc_table->jdbc_passwd();
    // The FE-rendered predicates stay exactly as they are, and the runtime filters are appended to
    // a copy (_jdbc_scan_node is const) with their values already written in as literals, so the
    // statement the bridge receives is complete and binds nothing.
    std::vector<std::string> filters = jdbc_scan_node.filters;
    _append_runtime_filters(state, &scan_ctx, &filters);
    scan_ctx.sql =
            get_jdbc_sql(scan_ctx.jdbc_url, jdbc_scan_node.table_name, jdbc_scan_node.columns, filters, _read_limit);
    _scanner = _pool->add(new JDBCScanner(scan_ctx, _tuple_desc, _runtime_profile));

    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

} // namespace starrocks::connector
