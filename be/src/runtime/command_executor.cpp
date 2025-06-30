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

#include "command_executor.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "http/action/update_config_action.h"
#include "script/script.h"

namespace starrocks {

void get_be_config_map(std::vector<std::pair<std::string, std::string>>* configs, const std::string& pattern = "") {
    std::vector<config::ConfigInfo> config_infos = config::list_configs();
    
    for (const auto& info : config_infos) {
        const std::string& name = info.name;

        if (!pattern.empty()) {
            if (name.find(pattern) == std::string::npos) {
                continue;
            }
        }

        configs->emplace_back(name, info.value);
    }
}

Status handle_set_config(const string& params_str) {
    rapidjson::Document params;
    params.Parse(params_str.c_str());
    auto update_config = UpdateConfigAction::instance();
    if (update_config == nullptr) {
        LOG(WARNING) << "write_be_configs_table ignored: UpdateConfigAction is not inited";
        return Status::OK();
    }
    auto name_itr = params.FindMember("name");
    if (name_itr == params.MemberEnd() || !name_itr->value.IsString()) {
        return Status::InvalidArgument("invalid param name");
    }
    auto value_itr = params.FindMember("value");
    if (value_itr == params.MemberEnd() || !value_itr->value.IsString()) {
        return Status::InvalidArgument("invalid param value");
    }
    return update_config->update_config(name_itr->value.GetString(), value_itr->value.GetString());
}

Status handle_show_config(const string& params_str, std::string* result) {
    rapidjson::Document params;
    params.Parse(params_str.c_str());
    std::string pattern;
    auto pattern_itr = params.FindMember("pattern");
    if (pattern_itr != params.MemberEnd() && pattern_itr->value.IsString()) {
        pattern = pattern_itr->value.GetString();
    }

    rapidjson::Document doc;
    doc.SetObject();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
    
    rapidjson::Value configs_array(rapidjson::kArrayType);

    std::vector<std::pair<std::string, std::string>> configs;
    get_be_config_map(&configs, pattern);

    std::vector<config::ConfigInfo> config_infos = config::list_configs();
    std::map<std::string, config::ConfigInfo> config_info_map;
    for (const auto& info : config_infos) {
        config_info_map[info.name] = info;
    }
    
    for (const auto& config_item : configs) {
        rapidjson::Value config_obj(rapidjson::kObjectType);
        
        const std::string& name = config_item.first;
        const std::string& value = config_item.second;

        config_obj.AddMember("key", rapidjson::Value(name.c_str(), allocator), allocator);

        std::string type_name = "string";
        bool is_mutable = true;

        auto it = config_info_map.find(name);
        if (it != config_info_map.end()) {
            const auto& info = it->second;
            type_name = info.type;
            is_mutable = info.valmutable;
        }
        
        config_obj.AddMember("value", rapidjson::Value(value.c_str(), allocator), allocator);
        config_obj.AddMember("type", rapidjson::Value(type_name.c_str(), allocator), allocator);
        config_obj.AddMember("is_mutable", is_mutable, allocator);
        
        configs_array.PushBack(config_obj, allocator);
    }
    
    doc.AddMember("configs", configs_array, allocator);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    *result = buffer.GetString();
    return Status::OK();
}

Status execute_command(const std::string& command, const std::string& params, std::string* result) {
    LOG(INFO) << "execute command: " << command << " params: " << params.substr(0, 2000);
    if (command == "set_config") {
        return handle_set_config(params);
    } else if (command == "show_config") {
        return handle_show_config(params, result);
    } else if (command == "execute_script") {
        return execute_script(params, *result);
    }
    return Status::NotSupported(strings::Substitute("command $0 not supported", command));
}

} // namespace starrocks
