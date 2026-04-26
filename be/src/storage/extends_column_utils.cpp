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

#include "storage/extends_column_utils.h"

#include "exprs/jsonpath.h"
#include "types/json_value.h"

namespace starrocks {

// Inherit default value from JSON parent column for extended subcolumn.
// This method extracts the default value of a JSON subfield based on the access path
// and sets it to the column if extraction succeeds.

void inherit_default_value_from_json(TabletColumn* column, const TabletColumn& root_column,
                                     const ColumnAccessPath* path) {
    if (!root_column.has_default_value() || root_column.type() != TYPE_JSON) {
        return;
    }

    const std::string& json_default = root_column.default_value();
    auto json_value_or = JsonValue::parse_json_or_string(Slice(json_default));
    if (!json_value_or.ok()) {
        LOG(WARNING) << "Failed to parse JSON default value: " << json_value_or.status();
        return;
    }

    // Extract the sub path from linear path, e.g. "profile.level" -> "$.level"
    const std::string& linear = path->linear_path();
    const std::string& parent = path->path();
    std::string json_path_str;
    if (linear.size() > parent.size() && linear.compare(0, parent.size(), parent) == 0) {
        // linear = "profile.level", parent = "profile" -> sub = ".level" -> "$level"
        json_path_str = "$" + linear.substr(parent.size());
    } else {
        json_path_str = "$";
    }

    auto json_path_or = JsonPath::parse(Slice(json_path_str));
    if (!json_path_or.ok()) {
        LOG(WARNING) << "Failed to parse JSON path: " << json_path_str;
        return;
    }

    vpack::Builder builder;
    vpack::Slice extracted = JsonPath::extract(&json_value_or.value(), json_path_or.value(), &builder);
    if (extracted.isNone() || extracted.isNull()) {
        return;
    }

    const LogicalType value_type = column->type();
    std::string default_value_str;

    if (value_type == TYPE_VARCHAR || value_type == TYPE_CHAR) {
        if (extracted.isString()) {
            default_value_str = extracted.copyString();
        } else {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            default_value_str = extracted.toJson(&options);
        }
        column->set_default_value(default_value_str);
        return;
    }

    if (value_type == TYPE_BOOLEAN) {
        if (extracted.isString()) {
            vpack::ValueLength len;
            const char* str = extracted.getStringUnchecked(len);
            StringParser::ParseResult parse_result;
            auto as_int = StringParser::string_to_int<int32_t>(str, len, &parse_result);
            if (parse_result == StringParser::PARSE_SUCCESS) {
                default_value_str = (as_int != 0) ? "1" : "0";
            } else {
                bool b = StringParser::string_to_bool(str, len, &parse_result);
                if (parse_result != StringParser::PARSE_SUCCESS) {
                    return;
                }
                default_value_str = b ? "1" : "0";
            }
        } else if (extracted.isBool()) {
            default_value_str = extracted.getBool() ? "1" : "0";
        } else if (extracted.isNumber()) {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            default_value_str = extracted.toJson(&options);
        } else {
            return;
        }
        column->set_default_value(default_value_str);
        return;
    }

    if (extracted.isString()) {
        default_value_str = extracted.copyString();
    } else if (extracted.isBool()) {
        default_value_str = extracted.getBool() ? "1" : "0";
    } else if (extracted.isNumber()) {
        vpack::Options options = vpack::Options::Defaults;
        options.singleLinePrettyPrint = true;
        default_value_str = extracted.toJson(&options);
    } else {
        return;
    }

    column->set_default_value(default_value_str);
}

StatusOr<TabletSchemaCSPtr> extend_schema_by_access_paths(const TabletSchemaCSPtr& tablet_schema, size_t next_unique_id,
                                                          const std::vector<ColumnAccessPathPtr>& access_paths) {
    bool need_extend = std::ranges::any_of(access_paths, [](const auto& path) { return path->is_extended(); });
    if (!need_extend) {
        return tablet_schema;
    }

    auto tmp_schema = TabletSchema::copy(*tablet_schema);
    for (auto& path : access_paths) {
        if (!path->is_extended()) {
            continue;
        }
        int root_column_index = tablet_schema->field_index(path->path());
        RETURN_IF(root_column_index < 0, Status::RuntimeError("unknown access path: " + path->path()));

        LogicalType value_type = path->value_type().type;
        TabletColumn column;
        column.set_name(path->linear_path());
        column.set_unique_id(++next_unique_id);
        column.set_type(value_type);
        column.set_length(path->value_type().len);
        column.set_is_nullable(true);
        // Record root column unique id to make it robust across schema changes
        int32_t root_uid = tablet_schema->column(static_cast<size_t>(root_column_index)).unique_id();
        column.set_extended_info(std::make_unique<ExtendedColumnInfo>(path.get(), root_uid));

        // Inherit default value from parent column if exists
        const auto& root_column = tablet_schema->column(static_cast<size_t>(root_column_index));
        inherit_default_value_from_json(&column, root_column, path.get());

        // For UNIQUE/AGG tables, extended flat JSON subcolumns act as value columns and
        // must have a valid aggregation method for pre-aggregation. Use REPLACE, which is
        // consistent with value-column semantics in these models.
        auto keys_type = tablet_schema->keys_type();
        if (keys_type == KeysType::UNIQUE_KEYS || keys_type == KeysType::AGG_KEYS) {
            column.set_aggregation(StorageAggregateType::STORAGE_AGGREGATE_REPLACE);
        }

        tmp_schema->append_column(column);
        VLOG(2) << "extend the access path column: " << path->linear_path();
    }
    return tmp_schema;
}

} // namespace starrocks