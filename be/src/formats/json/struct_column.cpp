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

#include "formats/json/struct_column.h"

#include "column/struct_column.h"
#include "formats/json/nullable_column.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

Status add_struct_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                         simdjson::ondemand::value* value) {
    auto struct_column = down_cast<StructColumn*>(column);

    try {
        if (value->type() != simdjson::ondemand::json_type::object) {
            std::ostringstream ss;
            ss << "Expected value type [object], got [" << value->type() << "]";
            return Status::DataQualityError(ss.str());
        }
        simdjson::ondemand::object obj = value->get_object();

        for (size_t i = 0; i < type_desc.children.size(); i++) {
            const auto& field_name = type_desc.field_names[i];
            const auto& field_type_desc = type_desc.children[i];

            auto field_column = struct_column->field_column(field_name);
            simdjson::ondemand::value field_value;
            auto err = obj.find_field_unordered(field_name).get(field_value);
            simdjson::ondemand::value* field_value_ptr = nullptr;
            if (err == simdjson::SUCCESS) {
                field_value_ptr = &field_value;
            } else if (err != simdjson::NO_SUCH_FIELD) {
                // if returns error, the struct field columns may be inconsistent.
                // so fill null if error.
                auto err_msg = strings::Substitute("Failed to parse value, field=$0.$1, error=$2", name, field_name,
                                                   simdjson::error_message(err));
                LOG(WARNING) << err_msg;
            }
            RETURN_IF_ERROR(add_nullable_column(field_column.get(), field_type_desc, name, field_value_ptr, true));
        }
        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as object, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}
} // namespace starrocks