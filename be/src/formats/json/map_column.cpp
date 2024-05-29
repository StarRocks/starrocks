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

#include "column/map_column.h"

#include "fmt/format.h"
#include "formats/json/map_column.h"
#include "formats/json/nullable_column.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

Status add_map_column(Column* column, const TypeDescriptor& type_desc, const std::string& name,
                      simdjson::ondemand::value* value) {
    auto map_column = down_cast<MapColumn*>(column);

    try {
        if (value->type() != simdjson::ondemand::json_type::object) {
            std::ostringstream ss;
            ss << "Expected value type [object], got [" << value->type() << "]";
            return Status::DataQualityError(ss.str());
        }
        simdjson::ondemand::object obj = value->get_object();
        simdjson::ondemand::parser parser;
        size_t field_count = 0;
        for (auto field : obj) {
            {
                // This is a tricky way to transform a std::string to simdjson:ondemand:value
                std::string_view field_name_str = field.unescaped_key();
                auto dummy_json = simdjson::padded_string(R"({"dummy_key": ")" + std::string(field_name_str) + R"("})");
                simdjson::ondemand::document doc = parser.iterate(dummy_json);
                simdjson::ondemand::object obj = doc.get_object();
                simdjson::ondemand::value field_key = obj.find_field("dummy_key");

                RETURN_IF_ERROR(add_nullable_column(map_column->keys_column().get(), type_desc.children[0], name,
                                                    &field_key, true));
            }

            {
                simdjson::ondemand::value field_value = field.value();
                RETURN_IF_ERROR(add_nullable_column(map_column->values_column().get(), type_desc.children[1], name,
                                                    &field_value, true));
            }
            ++field_count;
        }
        map_column->offsets_column()->append(map_column->offsets_column()->get_data().back() + field_count);

        return Status::OK();
    } catch (simdjson::simdjson_error& e) {
        auto err_msg = strings::Substitute("Failed to parse value as object, column=$0, error=$1", name,
                                           simdjson::error_message(e.error()));
        return Status::DataQualityError(err_msg);
    }
}
} // namespace starrocks