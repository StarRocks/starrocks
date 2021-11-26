// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_table_writer.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <mariadb/mysql.h>

#include <memory>
#include <string_view>
#include <type_traits>
#include <variant>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "fmt/compile.h"
#include "fmt/core.h"
#include "storage/null_predicate.h"
#include "util/radix_sort.h"
#include "util/slice.h"

#define __StarRocksMysql MYSQL
#include <sstream>

#include "exprs/expr.h"
#include "runtime/mysql_table_writer.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/types.h"

namespace starrocks {

std::string MysqlConnInfo::debug_string() const {
    std::stringstream ss;

    ss << "(host=" << host << ",port=" << port << ",user=" << user << ",db=" << db << ",passwd=" << passwd << ")";
    return ss.str();
}

MysqlTableWriter::MysqlTableWriter(const std::vector<ExprContext*>& output_expr_ctxs, int batch_size)
        : _output_expr_ctxs(output_expr_ctxs), _batch_size(batch_size) {}

MysqlTableWriter::~MysqlTableWriter() {
    if (_mysql_conn) {
        mysql_close(_mysql_conn);
    }
}

Status MysqlTableWriter::open(const MysqlConnInfo& conn_info, const std::string& tbl) {
    // Init for mysql connecter
    _mysql_conn = mysql_init(nullptr);
    if (_mysql_conn == nullptr) {
        return Status::InternalError("Call mysql_init failed.");
    }

    MYSQL* res = mysql_real_connect(_mysql_conn, conn_info.host.c_str(), conn_info.user.c_str(),
                                    conn_info.passwd.c_str(), conn_info.db.c_str(), conn_info.port,
                                    nullptr, // unix socket
                                    0);      // flags
    if (res == nullptr) {
        return Status::InternalError(fmt::format("mysql_real_connect failed because {}", mysql_error(_mysql_conn)));
    }

    // set character
    if (mysql_set_character_set(_mysql_conn, "utf8")) {
        return Status::InternalError(
                fmt::format("mysql_set_character_set failed because {}", mysql_error(_mysql_conn)));
    }

    _mysql_tbl = tbl;
    // Init FormatConverter
    _viewers.reserve(_output_expr_ctxs.size());

    return Status::OK();
}

Status MysqlTableWriter::_build_viewers(vectorized::Columns& columns) {
    _viewers.clear();
    DCHECK_EQ(columns.size(), _output_expr_ctxs.size());

    int num_cols = columns.size();

    for (int i = 0; i < num_cols; ++i) {
        auto* ctx = _output_expr_ctxs[i];
        const auto& type = ctx->root()->type();
        if (!is_scalar_primitive_type(type.type)) {
            return Status::InternalError(fmt::format("unsupport type in mysql sink:{}", type.type));
        }

        switch (type.type) {
#define M(NAME)                                                                           \
    case PrimitiveType::NAME: {                                                           \
        _viewers.emplace_back(vectorized::ColumnViewer<PrimitiveType::NAME>(columns[i])); \
        break;                                                                            \
    }
            APPLY_FOR_ALL_PRIMITIVE_TYPE(M)
            APPLY_FOR_ALL_NULL_TYPE(M)
#undef M
        case PrimitiveType::TYPE_TIME: {
            columns[i] = vectorized::ColumnHelper::convert_time_column_from_double_to_str(columns[i]);
            _viewers.emplace_back(vectorized::ColumnViewer<TYPE_VARCHAR>(columns[i]));
            break;
        }

        default:
            return Status::InternalError(fmt::format("unsupport type in mysql sink:{}", type.type));
        }
    }

    return Status::OK();
}

Status MysqlTableWriter::_build_insert_sql(int from, int to, std::string_view* sql) {
    _stmt_buffer.clear();
    int num_cols = _viewers.size();
    fmt::format_to(_stmt_buffer, "INSERT INTO {} VALUES ", _mysql_tbl);

    for (int i = from; i < to; ++i) {
        if (i != from) {
            _stmt_buffer.push_back(',');
        }
        _stmt_buffer.push_back('(');
        for (size_t col = 0; col < num_cols; col++) {
            std::visit(
                    [&](auto&& viewer) {
                        using ViewerType = decay_t<decltype(viewer)>;
                        constexpr PrimitiveType type = ViewerType::TYPE;

                        if (viewer.is_null(i)) {
                            fmt::format_to(_stmt_buffer, "NULL");
                            return;
                        }

                        if constexpr (type == TYPE_DECIMALV2) {
                            fmt::format_to(_stmt_buffer, "{}", viewer.value(i).to_string());
                        } else if constexpr (pt_is_decimal<type>) {
                            const auto& data_type = _output_expr_ctxs[col]->root()->type();
                            using CppType = vectorized::RunTimeCppType<type>;
                            fmt::format_to(_stmt_buffer, "{}",
                                           DecimalV3Cast::to_string<CppType>(viewer.value(i), data_type.precision,
                                                                             data_type.scale));
                        } else if constexpr (pt_is_date<type>) {
                            int y, m, d;
                            viewer.value(i).to_date(&y, &m, &d);
                            fmt::format_to(_stmt_buffer, "'{}'", vectorized::date::to_string(y, m, d));
                        } else if constexpr (pt_is_datetime<type>) {
                            fmt::format_to(_stmt_buffer, "'{}'", viewer.value(i).to_string());
                        } else if constexpr (pt_is_binary<type>) {
                            auto slice = viewer.value(i);
                            _escape_buffer.resize(slice.size * 2 + 1);

                            int sz = mysql_real_escape_string(_mysql_conn, _escape_buffer.data(), slice.data,
                                                              slice.size);
                            _stmt_buffer.push_back('"');
                            _stmt_buffer.append(_escape_buffer.data(), _escape_buffer.data() + sz);
                            _stmt_buffer.push_back('"');
                        } else if constexpr (type == TYPE_TINYINT || type == TYPE_BOOLEAN) {
                            fmt::format_to(_stmt_buffer, "{}", (int32_t)viewer.value(i));
                        } else {
                            fmt::format_to(_stmt_buffer, "{}", viewer.value(i));
                        }
                    },
                    _viewers[col]);

            if (col != num_cols - 1) {
                _stmt_buffer.push_back(',');
            }
        }
        _stmt_buffer.push_back(')');
    }

    *sql = std::string_view(_stmt_buffer.data(), _stmt_buffer.size());
    return Status::OK();
}

Status MysqlTableWriter::append(vectorized::Chunk* chunk) {
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }

    // eval output expr
    vectorized::Columns result_columns(_output_expr_ctxs.size());
    for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
        result_columns[i] = _output_expr_ctxs[i]->evaluate(chunk);
    }

    RETURN_IF_ERROR(_build_viewers(result_columns));

    int num_rows = chunk->num_rows();
    int i = 0;
    while (i + _batch_size < num_rows) {
        std::string_view insert_stmt;
        RETURN_IF_ERROR(_build_insert_sql(i, i + _batch_size, &insert_stmt));
        if (mysql_real_query(_mysql_conn, insert_stmt.data(), insert_stmt.length())) {
            return Status::InternalError(fmt::format("Insert to mysql server({}) failed, err:{}",
                                                     mysql_get_host_info(_mysql_conn), mysql_error(_mysql_conn)));
        }
        i += _batch_size;
    }

    std::string_view insert_stmt;
    RETURN_IF_ERROR(_build_insert_sql(i, num_rows, &insert_stmt));

    if (mysql_real_query(_mysql_conn, insert_stmt.data(), insert_stmt.length())) {
        return Status::InternalError(fmt::format("Insert to mysql server({}) failed, err:{}",
                                                 mysql_get_host_info(_mysql_conn), mysql_error(_mysql_conn)));
    }

    return Status::OK();
}

} // namespace starrocks
