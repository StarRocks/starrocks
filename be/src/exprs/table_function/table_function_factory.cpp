// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "table_function_factory.h"

#include <unordered_map>

#include "column/column.h"
#include "column/type_traits.h"
#include "exprs/table_function/unnest.h"

namespace starrocks::vectorized {

TableFunctionPtr TableFunctionFactory::MakeUnnest() {
    return std::make_shared<Unnest>();
}

struct TableFunctionMapHash {
    size_t operator()(
            const std::tuple<std::string, std::vector<PrimitiveType>, std::vector<PrimitiveType>>& quadruple) const {
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
    const TableFunction* get_table_function(const std::string& name, const std::vector<PrimitiveType>& arg_type,
                                            const std::vector<PrimitiveType>& return_type) const {
        auto pair = _infos_mapping.find(std::make_tuple(name, arg_type, return_type));
        if (pair == _infos_mapping.end()) {
            return nullptr;
        }
        return pair->second.get();
    }

    void add_function_mapping(std::string&& name, const std::vector<PrimitiveType>& arg_type,
                              const std::vector<PrimitiveType>& return_type) {
        _infos_mapping.emplace(std::make_tuple(name, arg_type, return_type), TableFunctionFactory::MakeUnnest());
    }

private:
    std::unordered_map<std::tuple<std::string, std::vector<PrimitiveType>, std::vector<PrimitiveType>>,
                       TableFunctionPtr, TableFunctionMapHash>
            _infos_mapping;
    TableFunctionResolver(const TableFunctionResolver&) = delete;
    const TableFunctionResolver& operator=(const TableFunctionResolver&) = delete;
};

TableFunctionResolver::TableFunctionResolver() {
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_TINYINT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_SMALLINT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_INT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_BIGINT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_LARGEINT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_FLOAT});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DOUBLE});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMALV2});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL32});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL64});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DECIMAL128});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_CHAR});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_VARCHAR});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DATE});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_DATETIME});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_BOOLEAN});
    add_function_mapping("unnest", {TYPE_ARRAY}, {TYPE_ARRAY});
}

TableFunctionResolver::~TableFunctionResolver() = default;

const TableFunction* get_table_function(const std::string& name, const std::vector<PrimitiveType>& arg_type,
                                        const std::vector<PrimitiveType>& return_type) {
    return TableFunctionResolver::instance()->get_table_function(name, arg_type, return_type);
}
} // namespace starrocks::vectorized