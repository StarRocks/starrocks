// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "column/column.h"
#include "common/status.h"
#include "numeric_column.h"
#include "binary_column.h"
#include "boolean_column.h"
#include "runtime/types.h"
#include "simdjson.h"

namespace starrocks::vectorized {

template <typename T>
Status add_nullable_numeric_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null) {

    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    if (value.is_null()) {
        null_column->append(1);
        return Status::OK();
    }

    RETURN_IF_ERROR(add_numeric_column<T>(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

Status add_nullable_binary_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null) {
    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    if (value.is_null()) {
        null_column->append(1);
        return Status::OK();
    }

    RETURN_IF_ERROR(add_binary_column(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

Status add_nullable_boolean_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null) {
    auto nullable_column = down_cast<NullableColumn*>(column);

    auto& null_column = nullable_column->null_column();
    auto& data_column = nullable_column->data_column();

    if (value.is_null()) {
        null_column->append(1);
        return Status::OK();
    }

    RETURN_IF_ERROR(add_boolean_column(data_column.get(), type_desc, name, value));
    null_column->append(0);
    return Status::OK();
}

Status add_nullable_array_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                                   simdjson::ondemand::value& value, bool invalid_as_null) {
    try {
        if (value.type() == simdjson::ondemand::json_type::array) {
            auto nullable_column = down_cast<NullableColumn*>(column);
            auto array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());

            auto& null_column_data = nullable_column->null_column_data();
            null_column_data.push_back(0);

            auto& elems_column = array_column->elements_column();

            for (auto elem : value.get_array()) {
                add_nullable_array_column(elems_column.get(), type_desc.children[0], name, elem.value(),
                                          invalid_as_null);
            }
            auto &offsets = array_column->offsets_column();
            auto sz = offsets->get_data().back() + value.count_elements();
            offsets->append_numbers(&sz, sizeof(sz));
            return Status::OK();
        } else {
            std::string_view sv = value.raw_json_token();
            column->append_strings(std::vector<Slice>{Slice{sv.data(), sv.size()}});
            return Status::OK();
        }
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as array, column=$0", name);
        return Status::DataQualityError(err_msg);
    }
}

} // namespace starrocks::vectorized
