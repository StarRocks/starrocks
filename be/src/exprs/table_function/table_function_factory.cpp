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

#include "table_function_factory.h"

#include <unordered_map>

#include "column/column.h"
#include "column/type_traits.h"
#include "exprs/table_function/json_each.h"
#include "exprs/table_function/multi_unnest.h"
#include "exprs/table_function/table_function.h"
#include "exprs/table_function/unnest.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

struct TableFunctionMapHash {
    size_t operator()(
            const std::tuple<std::string, std::vector<LogicalType>, std::vector<LogicalType>>& quadruple) const {
        std::hash<std::string> hasher;

        size_t fn_hash = hasher(std::get<0>(quadruple));
        for (auto i : std::get<1>(quadruple)) {
            fn_hash = fn_hash ^ i;
        }

        for (auto i : std::get<2>(quadruple)) {
            fn_hash = fn_hash ^ i;
        }

        return fn_hash;
    }
};

class TableFunctionResolver {
    DECLARE_SINGLETON(TableFunctionResolver);

public:
    const TableFunction* get_table_function(const std::string& name, const std::vector<LogicalType>& arg_type,
                                            const std::vector<LogicalType>& return_type) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second.get();
    }

    void add_function_mapping(std::string&& name, const std::vector<LogicalType>& arg_type,
                              const std::vector<LogicalType>& return_type, const TableFunctionPtr& table_func) {
        _infos_mapping.emplace(std::make_tuple(name, arg_type, return_type), table_func);
    }

private:
    std::unordered_map<std::tuple<std::string, std::vector<LogicalType>, std::vector<LogicalType>>, TableFunctionPtr,
                       TableFunctionMapHash>
            _infos_mapping;
    TableFunctionResolver(const TableFunctionResolver&) = delete;
    const TableFunctionResolver& operator=(const TableFunctionResolver&) = delete;
};

TableFunctionResolver::TableFunctionResolver() {
    TableFunctionPtr func_unnest = std::make_shared<Unnest>();
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_TINYINT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_SMALLINT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_INT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_BIGINT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_LARGEINT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_FLOAT}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DOUBLE}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMALV2}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL32}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL64}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL128}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_CHAR}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_VARCHAR}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DATE}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DATETIME}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_BOOLEAN}, func_unnest);
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_ARRAY}, func_unnest);

    // Use special invalid as the parameter of multi_unnest, because multi_unnest is a variable parameter function,
    // and there is no special treatment for different types of input parameters,
    // this is just for compatibility and find the corresponding function
    TableFunctionPtr multi_unnest = std::make_shared<MultiUnnest>();
    add_function_mapping("unnest", {}, {}, multi_unnest);

    TableFunctionPtr func_json_each = std::make_shared<JsonEach>();
    add_function_mapping("json_each", {TYPE_JSON}, {TYPE_VARCHAR, TYPE_JSON}, func_json_each);
}

TableFunctionResolver::~TableFunctionResolver() = default;

const TableFunction* get_table_function(const std::string& name, const std::vector<LogicalType>& arg_type,
                                        const std::vector<LogicalType>& return_type,
                                        TFunctionBinaryType::type binary_type) {
    if (binary_type == TFunctionBinaryType::BUILTIN) {
        return TableFunctionResolver::instance()->get_table_function(name, arg_type, return_type);
    } else if (binary_type == TFunctionBinaryType::SRJAR) {
        return getJavaUDTFFunction();
    }
    return nullptr;
}
} // namespace starrocks
