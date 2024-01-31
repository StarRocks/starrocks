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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/json_util.cpp

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

#include "util/json_util.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <vector>

#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "util/json.h"

namespace starrocks {

std::string to_json(const Status& status) {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // status
    writer.Key("status");
    if (status.ok()) {
        writer.String("Success");
    } else {
        writer.String("Fail");
    }
    // msg
    writer.Key("msg");
    if (status.ok()) {
        writer.String("OK");
    } else {
        std::string_view msg = status.message();
        writer.String(msg.data(), msg.size());
    }
    writer.EndObject();
    return s.GetString();
}

std::string to_json(const std::map<std::string, std::map<std::string, std::string>>& value) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer writer(buffer);

    writer.StartObject();

    for (auto& iter : value) {
        writer.Key(iter.first.c_str());
        writer.StartObject();
        for (auto innerIt = iter.second.begin(); innerIt != iter.second.end(); ++innerIt) {
            writer.Key(innerIt->first.c_str());
            writer.String(innerIt->second.c_str());
        }
        writer.EndObject();
    }
    writer.EndObject();
    return buffer.GetString();
}

Status from_json(const std::string& json_value, std::map<std::string, std::map<std::string, std::string>>* map_result) {
    rapidjson::Document document;
    document.Parse(json_value.c_str());

    if (!document.HasParseError()) {
        rapidjson::Document::AllocatorType allocator;
        for (rapidjson::Value::ConstMemberIterator itr = document.MemberBegin(); itr != document.MemberEnd(); itr++) {
            rapidjson::Value key;
            rapidjson::Value value;
            key.CopyFrom(itr->name, allocator);
            value.CopyFrom(itr->value, allocator);

            RETURN_IF(!key.IsString(), Status::JsonFormatError("Parse json (" + json_value + ") error!"));
            std::string name = key.GetString();

            RETURN_IF(!value.IsObject(), Status::JsonFormatError("Parse json (" + json_value + ") error!"));
            std::map<std::string, std::string> properties;
            for (rapidjson::Value::ConstMemberIterator v_itr = value.MemberBegin(); v_itr != value.MemberEnd();
                 v_itr++) {
                properties.emplace(v_itr->name.GetString(), v_itr->value.GetString());
            }
            map_result->emplace(name, properties);
        }
    } else {
        return Status::JsonFormatError("Parse json(" + json_value + ") error!");
    }
    return Status::OK();
}

void JsonFlater::flatten(const Column* json_column, std::vector<ColumnPtr>* result) {
    DCHECK(result->size() == _flat_paths.size());

    // input
    const JsonColumn* json_data = nullptr;
    if (json_column->is_nullable()) {
        // append null column
        auto* nullable_column = down_cast<const NullableColumn*>(json_column);
        json_data = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    } else {
        json_data = down_cast<const JsonColumn*>(json_column);
    }

    // output
    std::vector<JsonColumn*> flat_jsons;
    std::vector<NullColumn*> flat_nulls;
    for (auto& col : *result) {
        auto* flat_nullable = down_cast<NullableColumn*>(col.get());
        flat_jsons.emplace_back(down_cast<JsonColumn*>(flat_nullable->data_column().get()));
        flat_nulls.emplace_back(down_cast<NullColumn*>(flat_nullable->null_column().get()));
    }

    for (size_t i = 0; i < json_column->size(); i++) {
        if (json_column->is_null(i)) {
            for (size_t k = 0; k < result->size(); k++) {
                (*result)[k]->append_nulls(1);
            }
            continue;
        }

        auto* obj = json_data->get_object(i);
        auto vslice = obj->to_vslice();
        if (UNLIKELY(!vslice.isObject() || vslice.isNull())) {
            for (size_t k = 0; k < result->size(); k++) {
                (*result)[k]->append_nulls(1);
            }
            continue;
        } else if (vslice.isEmptyObject() || vslice.isNone()) {
            for (size_t k = 0; k < result->size(); k++) {
                // push none
                flat_jsons[k]->append(JsonValue::from_none());
                flat_nulls[k]->append(0);
            }
            continue;
        }
        for (size_t k = 0; k < _flat_paths.size(); k++) {
            auto st = obj->get_obj(_flat_paths[k]);
            if (st.ok() && !st.value().to_vslice().isNull()) {
                flat_jsons[k]->append(st.value());
                flat_nulls[k]->append(0);
            } else {
                (*result)[k]->append_nulls(1);
            }
        }
    }
    for (auto& col : *result) {
        down_cast<NullableColumn*>(col.get())->update_has_null();
    }
}

} // namespace starrocks
