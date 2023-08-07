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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/mysql_scanner.cpp

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

#define __StarRocksMysql MYSQL
#define __StarRocksMysqlRes MYSQL_RES
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "mysql_scanner.h"

namespace starrocks {

MysqlScanner::MysqlScanner(const MysqlScannerParam& param)
        : _my_param(param), _my_conn(nullptr), _my_result(nullptr), _opened(false), _field_num(0) {}

MysqlScanner::~MysqlScanner() {
    if (_my_result) {
        // In some large data queries (such as select*), executing free_result directly
        // will cause a blocking until mysql server returns all the data. Also the rpc thread
        // of BE can get stuck under unknown reasons.
        // So we need to execute a cancel to avoid this blocking.
        mariadb_cancel(_my_conn);
        mysql_free_result(_my_result);
        _my_result = nullptr;
    }

    if (_my_conn) {
        mysql_close(_my_conn);
        _my_conn = nullptr;
    }
}

Status MysqlScanner::open() {
    if (_opened) {
        LOG(INFO) << "this scanner already opened";
        return Status::OK();
    }

    _my_conn = mysql_init(nullptr);

    if (nullptr == _my_conn) {
        return Status::InternalError("mysql init failed.");
    }

    VLOG(1) << "MysqlScanner::Connect";

    if (nullptr == mysql_real_connect(_my_conn, _my_param.host.c_str(), _my_param.user.c_str(),
                                      _my_param.passwd.c_str(), _my_param.db.c_str(), atoi(_my_param.port.c_str()),
                                      nullptr, _my_param.client_flag)) {
        LOG(WARNING) << "connect Mysql: "
                     << "Host: " << _my_param.host << " user: " << _my_param.user << " passwd: " << _my_param.passwd
                     << " db: " << _my_param.db << " port: " << _my_param.port;

        return _error_status("mysql real connect failed.");
    }

    if (mysql_set_character_set(_my_conn, "utf8")) {
        return Status::InternalError("mysql set character set failed.");
    }

    _opened = true;

    return Status::OK();
}

Status MysqlScanner::query(const std::string& query) {
    if (!_opened) {
        return Status::InternalError("Query before open.");
    }

    int sql_result = mysql_query(_my_conn, query.c_str());

    if (0 != sql_result) {
        LOG(WARNING) << "mysql query failed. query =" << query;
        return _error_status("mysql query failed.");
    } else {
        LOG(INFO) << "mysql query success. query =" << query;
    }

    // clean the last query result
    if (_my_result) {
        mysql_free_result(_my_result);
    }

    // NOTE(zc): Result set may be very large, such as 100GB, which can not be stored in memory.
    // So we use mysql_use_result here to read result set in streaming. But this may hurt the
    // performance of small result sets. This need to be more investigation.
    _my_result = mysql_use_result(_my_conn);
    if (_my_result == nullptr) {
        return _error_status("mysql store result failed.");
    }

    _field_num = mysql_num_fields(_my_result);

    return Status::OK();
}

Status MysqlScanner::query(const std::string& table, const std::vector<std::string>& fields,
                           const std::vector<std::string>& filters,
                           const std::unordered_map<std::string, std::vector<std::string>>& filters_in,
                           std::unordered_map<std::string, bool>& filters_null_in_set, int64_t limit,
                           const std::string& temporal_clause) {
    if (!_opened) {
        return Status::InternalError("Query before open.");
    }

    _sql_str = "SELECT";

    for (int i = 0; i < fields.size(); ++i) {
        if (0 != i) {
            _sql_str += ",";
        }

        _sql_str += " " + fields[i];
    }

    _sql_str += " FROM " + table;

    bool is_filter_initial = false;
    if (!filters.empty()) {
        is_filter_initial = true;
        _sql_str += " WHERE ";

        for (int i = 0; i < filters.size(); ++i) {
            if (0 != i) {
                _sql_str += " AND";
            }

            _sql_str += " (" + filters[i] + ") ";
        }
    }

    // In Filter part.
    if (filters_in.size() > 0) {
        if (!is_filter_initial) {
            is_filter_initial = true;
            _sql_str += " WHERE (";
        } else {
            _sql_str += " AND (";
        }

        bool is_first_conjunct = true;
        for (auto& iter : filters_in) {
            if (!is_first_conjunct) {
                _sql_str += " AND (";
            }
            is_first_conjunct = false;
            if (iter.second.size() > 0) {
                auto curr = iter.second.begin();
                auto end = iter.second.end();
                _sql_str += iter.first + " in (";
                _sql_str += *curr;
                ++curr;

                // collect optional values.
                while (curr != end) {
                    _sql_str += ", " + *curr;
                    ++curr;
                }
                if (filters_null_in_set[iter.first]) {
                    _sql_str += ", null";
                }
                _sql_str += ")) ";
            }
        }
    }

    if (!temporal_clause.empty()) {
        _sql_str += " " + temporal_clause;
    }

    if (limit != -1) {
        _sql_str += " limit " + std::to_string(limit) + " ";
    }

    return query(_sql_str);
}

Slice MysqlScanner::escape(const std::string& value) {
    _escape_buffer.resize(value.size() * 2 + 1 + 2);
    _escape_buffer[0] = '\'';
    auto sz = mysql_real_escape_string(_my_conn, _escape_buffer.data() + 1, value.data(), value.size());
    _escape_buffer[sz + 1] = '\'';
    return {_escape_buffer.data(), sz + 2};
}

Status MysqlScanner::get_next_row(char*** buf, unsigned long** lengths, bool* eos) {
    if (!_opened) {
        return Status::InternalError("GetNextRow before open.");
    }

    if (nullptr == buf || nullptr == lengths || nullptr == eos) {
        return Status::InternalError("input parameter invalid.");
    }

    if (nullptr == _my_result) {
        return Status::InternalError("get next row before query.");
    }

    *buf = mysql_fetch_row(_my_result);
    if (*buf == nullptr) {
        // because we use mysql_use_result, we should check mysql error to see what error happend
        // https://dev.mysql.com/doc/c-api/8.0/en/mysql-fetch-row.html
        if (mysql_errno(_my_conn) != 0) {
            return Status::InternalError(
                    strings::Substitute("fail to read MySQL result, msg=$0", mysql_error(_my_conn)));
        } else {
            // if mysql_errno is 0, it means query finish normally.
            *eos = true;
            return Status::OK();
        }
    }

    *lengths = mysql_fetch_lengths(_my_result);

    if (nullptr == *lengths) {
        return _error_status("mysql fetch row failed.");
    }

    *eos = false;

    return Status::OK();
}

Status MysqlScanner::_error_status(const std::string& prefix) {
    std::stringstream msg;
    msg << prefix << " Err: " << mysql_error(_my_conn);
    LOG(INFO) << msg.str();
    return Status::InternalError(msg.str());
}

} // namespace starrocks
