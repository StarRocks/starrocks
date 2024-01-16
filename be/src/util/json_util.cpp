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

} // namespace starrocks
