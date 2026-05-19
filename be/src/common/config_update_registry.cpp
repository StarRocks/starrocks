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

#include "common/config_update_registry.h"

#include <mutex>

#include "common/configbase.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

ConfigUpdateRegistry* ConfigUpdateRegistry::instance() {
    static ConfigUpdateRegistry registry;
    return &registry;
}

void ConfigUpdateRegistry::register_callback(const std::string& name, Callback callback) {
    std::unique_lock lock(_mutex);
    _callbacks.emplace(name, std::move(callback));
}

Status ConfigUpdateRegistry::update_config(const std::string& name, const std::string& value) {
    Callback callback;
    {
        std::shared_lock lock(_mutex);
        if (!_ready) {
            LOG(WARNING) << "update_config ignored: ConfigUpdateRegistry is not ready";
            return Status::OK();
        }
        if (auto it = _callbacks.find(name); it != _callbacks.end()) {
            callback = it->second;
        }
    }

    Status s = config::set_config(name, value);
    if (s.ok() && callback != nullptr) {
        s = callback();
        if (!s.ok()) {
            Status rollback_status = config::rollback_config(name);
            if (!rollback_status.ok()) {
                LOG(WARNING) << strings::Substitute("Failed to rollback config: $0.", name);
            }
        } else {
            LOG(INFO) << "set_config " << name << "=" << value << " success";
        }
    }
    return s;
}

void ConfigUpdateRegistry::set_ready() {
    std::unique_lock lock(_mutex);
    _ready = true;
}

void ConfigUpdateRegistry::TEST_reset() {
    std::unique_lock lock(_mutex);
    _ready = false;
    _callbacks.clear();
}

} // namespace starrocks
