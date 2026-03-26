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

#include "connector/jdbc_connector.h"

#include <algorithm>
#include <cctype>
#include <regex>
#include <sstream>
#include <unordered_map>

#include "base/string/slice.h"
#include "exec/jdbc_scanner.h"
#include "exprs/expr.h"
#include "runtime/descriptors_ext.h"
#include "runtime/jdbc_driver_manager.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::connector {

namespace {

using TemporalColumnMap = std::unordered_map<std::string, LogicalType>;

std::string to_lower_ascii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) { return std::tolower(ch); });
    return value;
}

std::string normalize_column_name(std::string column_name) {
    if (column_name.size() >= 2 && ((column_name.front() == '"' && column_name.back() == '"') ||
                                    (column_name.front() == '`' && column_name.back() == '`'))) {
        column_name = column_name.substr(1, column_name.size() - 2);
    }
    return to_lower_ascii(std::move(column_name));
}

std::vector<std::string> split_identifier_tokens(const std::string& identifier) {
    std::vector<std::string> tokens;
    std::string current;
    current.reserve(identifier.size());
    for (unsigned char ch : identifier) {
        if (std::isalnum(ch)) {
            current.push_back(static_cast<char>(std::tolower(ch)));
        } else if (!current.empty()) {
            tokens.emplace_back(std::move(current));
            current.clear();
        }
    }
    if (!current.empty()) {
        tokens.emplace_back(std::move(current));
    }
    return tokens;
}

LogicalType infer_temporal_kind_from_name(const std::string& normalized_column_name) {
    const auto tokens = split_identifier_tokens(normalized_column_name);
    bool has_date_like_token = false;
    bool has_time_like_token = false;

    for (const std::string& token : tokens) {
        if (token == "date" || token == "dt") {
            has_date_like_token = true;
            continue;
        }
        if (token == "time" || token == "datetime" || token == "timestamp" || token == "ts" || token == "tstz" ||
            token == "tsltz" || token == "timetz" || token == "timestamptz" || token == "timestampltz" ||
            token.rfind("timestamp", 0) == 0) {
            has_time_like_token = true;
            continue;
        }
    }

    if (has_time_like_token) {
        return TYPE_DATETIME;
    }
    if (has_date_like_token) {
        return TYPE_DATE;
    }
    return TYPE_UNKNOWN;
}

TemporalColumnMap collect_oracle_temporal_columns(const TupleDescriptor* tuple_desc) {
    TemporalColumnMap temporal_columns;
    if (tuple_desc == nullptr) {
        return temporal_columns;
    }

    for (SlotDescriptor* slot_desc : tuple_desc->slots()) {
        const auto slot_type = slot_desc->type().type;
        const std::string normalized_col_name = normalize_column_name(slot_desc->col_name());
        if (slot_type == TYPE_DATETIME || slot_type == TYPE_DATETIME_V1 || slot_type == TYPE_DATE) {
            temporal_columns[normalized_col_name] = slot_type;
            continue;
        }

        if (slot_type == TYPE_VARCHAR || slot_type == TYPE_CHAR) {
            const auto inferred_temporal_type = infer_temporal_kind_from_name(normalized_col_name);
            if (inferred_temporal_type != TYPE_UNKNOWN) {
                temporal_columns[normalized_col_name] = inferred_temporal_type;
            }
        }
    }
    return temporal_columns;
}

bool literal_contains_time_component(const std::string& literal) {
    return literal.find(':') != std::string::npos || literal.find(' ') != std::string::npos ||
           literal.find('.') != std::string::npos;
}

std::string build_oracle_temporal_conversion_expr(const std::string& literal, LogicalType slot_type) {
    static const std::regex kDatetimeLiteralRegex(
            R"(^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?)?$)");
    std::smatch match;
    if (!std::regex_match(literal, match, kDatetimeLiteralRegex)) {
        // Fallback to Oracle NLS-based conversion for non-canonical literals,
        // such as '2022-01-1'.
        if (slot_type == TYPE_DATE || !literal_contains_time_component(literal)) {
            return fmt::format("TO_DATE('{}')", literal);
        }
        return fmt::format("TO_TIMESTAMP('{}')", literal);
    }

    if (!match[4].matched) {
        if (slot_type == TYPE_DATE) {
            return fmt::format("TO_DATE('{}', 'YYYY-MM-DD')", literal);
        }
        return fmt::format("TO_TIMESTAMP('{}', 'YYYY-MM-DD')", literal);
    }

    if (!match[7].matched) {
        if (slot_type == TYPE_DATE) {
            return fmt::format("TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')", literal);
        }
        return fmt::format("TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS')", literal);
    }

    return fmt::format("TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF{}')", literal, match.str(7).size());
}

std::string rewrite_oracle_column_literal_predicates(const std::string& filter,
                                                     const TemporalColumnMap& temporal_columns) {
    static const std::regex kColumnLiteralPredicateRegex(
            R"((^|[^[:alnum:]_$#])("?[[:alpha:]_][[:alnum:]_$#]*"?)\s*(=|!=|<>|<=|>=|<|>)\s*'([^']*)')",
            std::regex_constants::icase);

    std::string rewritten;
    std::smatch match;
    std::string::const_iterator begin = filter.begin();
    while (std::regex_search(begin, filter.end(), match, kColumnLiteralPredicateRegex)) {
        rewritten.append(begin, match[0].first);
        std::string replacement = match.str(0);

        auto it = temporal_columns.find(normalize_column_name(match.str(2)));
        if (it != temporal_columns.end()) {
            const std::string datetime_expr = build_oracle_temporal_conversion_expr(match.str(4), it->second);
            if (!datetime_expr.empty()) {
                replacement = match.str(1) + match.str(2) + " " + match.str(3) + " " + datetime_expr;
            }
        }

        rewritten.append(replacement);
        begin = match[0].second;
    }
    rewritten.append(begin, filter.end());
    return rewritten;
}

std::string rewrite_oracle_literal_column_predicates(const std::string& filter,
                                                     const TemporalColumnMap& temporal_columns) {
    static const std::regex kLiteralColumnPredicateRegex(
            R"((^|[^[:alnum:]_$#])'([^']*)'\s*(=|!=|<>|<=|>=|<|>)\s*("?[[:alpha:]_][[:alnum:]_$#]*"?))",
            std::regex_constants::icase);

    std::string rewritten;
    std::smatch match;
    std::string::const_iterator begin = filter.begin();
    while (std::regex_search(begin, filter.end(), match, kLiteralColumnPredicateRegex)) {
        rewritten.append(begin, match[0].first);
        std::string replacement = match.str(0);

        auto it = temporal_columns.find(normalize_column_name(match.str(4)));
        if (it != temporal_columns.end()) {
            const std::string datetime_expr = build_oracle_temporal_conversion_expr(match.str(2), it->second);
            if (!datetime_expr.empty()) {
                replacement = match.str(1) + datetime_expr + " " + match.str(3) + " " + match.str(4);
            }
        }

        rewritten.append(replacement);
        begin = match[0].second;
    }
    rewritten.append(begin, filter.end());
    return rewritten;
}

std::string rewrite_oracle_between_predicates(const std::string& filter, const TemporalColumnMap& temporal_columns) {
    static const std::regex kBetweenPredicateRegex(
            R"((^|[^[:alnum:]_$#])("?[[:alpha:]_][[:alnum:]_$#]*"?)\s+BETWEEN\s+'([^']*)'\s+AND\s+'([^']*)')",
            std::regex_constants::icase);

    std::string rewritten;
    std::smatch match;
    std::string::const_iterator begin = filter.begin();
    while (std::regex_search(begin, filter.end(), match, kBetweenPredicateRegex)) {
        rewritten.append(begin, match[0].first);
        std::string replacement = match.str(0);

        auto it = temporal_columns.find(normalize_column_name(match.str(2)));
        if (it != temporal_columns.end()) {
            const std::string low_expr = build_oracle_temporal_conversion_expr(match.str(3), it->second);
            const std::string high_expr = build_oracle_temporal_conversion_expr(match.str(4), it->second);
            if (!low_expr.empty() && !high_expr.empty()) {
                replacement = match.str(1) + match.str(2) + " BETWEEN " + low_expr + " AND " + high_expr;
            }
        }

        rewritten.append(replacement);
        begin = match[0].second;
    }
    rewritten.append(begin, filter.end());
    return rewritten;
}

std::vector<std::string> rewrite_oracle_datetime_filters(const std::vector<std::string>& filters,
                                                         const TupleDescriptor* tuple_desc) {
    TemporalColumnMap temporal_columns = collect_oracle_temporal_columns(tuple_desc);
    if (temporal_columns.empty()) {
        return filters;
    }

    std::vector<std::string> rewritten_filters;
    rewritten_filters.reserve(filters.size());
    for (const std::string& filter : filters) {
        std::string rewritten = rewrite_oracle_between_predicates(filter, temporal_columns);
        rewritten = rewrite_oracle_column_literal_predicates(rewritten, temporal_columns);
        rewritten = rewrite_oracle_literal_column_predicates(rewritten, temporal_columns);
        rewritten_filters.emplace_back(std::move(rewritten));
    }
    return rewritten_filters;
}

} // namespace

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
            // ref:
            // https://stackoverflow.com/questions/470542/how-do-i-limit-the-number-of-rows-returned-by-an-oracle-query-after-ordering
            return fmt::format("SELECT * FROM ({}) WHERE ROWNUM <= {}", oss.str(), limit);
        } else {
            oss << " LIMIT " << limit;
        }
    }
    return oss.str();
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
    std::string driver_name = jdbc_table->jdbc_driver_name();
    std::string driver_url = jdbc_table->jdbc_driver_url();
    std::string driver_checksum = jdbc_table->jdbc_driver_checksum();
    std::string driver_class = jdbc_table->jdbc_driver_class();
    std::string driver_location;

    status = JDBCDriverManager::getInstance()->get_driver_location(driver_name, driver_url, driver_checksum,
                                                                   &driver_location);
    if (!status.ok()) {
        LOG(ERROR) << fmt::format("Get JDBC Driver[{}] error, error is {}", driver_name, status.to_string());
        return status;
    }

    JDBCScanContext scan_ctx;
    scan_ctx.driver_path = driver_location;
    scan_ctx.driver_class_name = driver_class;
    scan_ctx.jdbc_url = jdbc_table->jdbc_url();
    scan_ctx.user = jdbc_table->jdbc_user();
    scan_ctx.passwd = jdbc_table->jdbc_passwd();

    const std::vector<std::string>& original_filters = jdbc_scan_node.filters;
    std::vector<std::string> rewritten_filters = original_filters;
    if (scan_ctx.jdbc_url.starts_with("jdbc:oracle")) {
        rewritten_filters = rewrite_oracle_datetime_filters(original_filters, _tuple_desc);
    }
    scan_ctx.sql = get_jdbc_sql(scan_ctx.jdbc_url, jdbc_scan_node.table_name, jdbc_scan_node.columns, rewritten_filters,
                                _read_limit);
    _scanner = _pool->add(new JDBCScanner(scan_ctx, _tuple_desc, _runtime_profile));

    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

} // namespace starrocks::connector
