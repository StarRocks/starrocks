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

#pragma once

#include <glog/logging.h>

#include <string>

#include "common/config.h"
#include "gutil/macros.h"

namespace starrocks {
class VLogCntl {
public:
    static VLogCntl& getInstance() {
        static VLogCntl log_module;
        return log_module;
    }

    DISALLOW_COPY_AND_MOVE(VLogCntl);

    void setLogLevel(const std::string& module, int level) { google::SetVLOGLevel(module.c_str(), level); }

    void enable(const std::string& module) {
        int32_t vlog_level = config::sys_log_verbose_level;
        google::SetVLOGLevel(module.c_str(), vlog_level);
    }

    void disable(const std::string& module) { google::SetVLOGLevel(module.c_str(), 0); }

private:
    VLogCntl() = default;
};
} // namespace starrocks