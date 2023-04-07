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

#include "common/configbase.h"
#include "gutil/strings/substitute.h"
#include "script/script.h"

namespace starrocks {

Status execute_command(const std::string& command, const std::string& params, std::string* result) {
    LOG(INFO) << "execute command: " << command << " params: " << params.substr(0, 2000);
    if (command == "execute_script") {
        return execute_script(params, *result);
    }
    return Status::NotSupported(strings::Substitute("command $0 not supported", command));
}

} // namespace starrocks
