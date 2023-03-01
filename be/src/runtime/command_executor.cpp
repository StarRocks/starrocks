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

#include "common/configbase.h"
#include "gutil/strings/substitute.h"
#include "http/action/update_config_action.h"

namespace starrocks {

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

Status execute_command(const std::string& command, const std::string& params) {
    LOG(INFO) << "execute command: " << command << " params: " << params;
    if (command == "set_config") {
        return handle_set_config(params);
    }
    return Status::NotSupported(strings::Substitute("command $0 not supported", command));
}

} // namespace starrocks
