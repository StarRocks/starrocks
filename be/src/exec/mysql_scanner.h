// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/mysql_scanner.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_MYSQL_SCANNER_H
#define STARROCKS_BE_SRC_QUERY_EXEC_MYSQL_SCANNER_H

#include <cstdlib>
#include <string>
#include <vector>

#include "common/status.h"

#ifndef __StarRocksMysql
#define __StarRocksMysql void
#endif

#ifndef __StarRocksMysqlRes
#define __StarRocksMysqlRes void
#endif

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

struct MysqlScannerParam {
    std::string host;
    std::string port;
    std::string user;
    std::string passwd;
    std::string db;
    unsigned long client_flag{0};
    MysqlScannerParam() {}
};

// Mysql Scanner for scan data from mysql
class MysqlScanner {
public:
    MysqlScanner(const MysqlScannerParam& param);
    ~MysqlScanner();

    Status open();
    Status query(const std::string& query);

    // query for STARROCKS
    Status query(const std::string& table, const std::vector<std::string>& fields,
                 const std::vector<std::string>& filters);
    Status get_next_row(char*** buf, unsigned long** lengths, bool* eos);

    int field_num() const { return _field_num; }

private:
    Status _error_status(const std::string& prefix);

    const MysqlScannerParam& _my_param;
    __StarRocksMysql* _my_conn;
    __StarRocksMysqlRes* _my_result;
    std::string _sql_str;
    bool _is_open;
    int _field_num;
};

} // namespace starrocks

#endif
