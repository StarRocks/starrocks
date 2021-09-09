// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scanner/schema_variables_scanner.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_SCHEMA_VARIABLES_SCANNER_H
#define STARROCKS_BE_SRC_QUERY_EXEC_SCHEMA_SCANNER_SCHEMA_VARIABLES_SCANNER_H

#include <map>
#include <string>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaVariablesScanner : public SchemaScanner {
public:
    SchemaVariablesScanner(TVarType::type type);
    virtual ~SchemaVariablesScanner();

    virtual Status start(RuntimeState* state);
    virtual Status get_next_row(Tuple* tuple, MemPool* pool, bool* eos);

private:
    Status fill_one_row(Tuple* tuple, MemPool* pool);

    int _index = 0;
    static SchemaScanner::ColumnDesc _s_vars_columns[];

    TShowVariableResult _var_result;
    TVarType::type _type;
    std::map<std::string, std::string>::iterator _begin;
};

} // namespace starrocks
#endif
