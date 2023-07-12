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

#include "runtime/health_check/disk_checker.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "storage/storage_engine.h"

namespace starrocks {

void DiskChecker::debug(std::stringstream& ss) {
    auto stores = StorageEngine::instance()->get_stores();
    rapidjson::Document root;
    root.SetObject();
    for (auto& it : stores) {
        rapidjson::Value path;
        path.SetString(it->path().c_str(), it->path().size(), root.GetAllocator());
        root.AddMember(path, it->is_used(), root.GetAllocator());
    }

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    ss << std::string(strbuf.GetString());
}

DiskChecker::DiskChecker() : BaseMonitor("disk_checker") {
    _callback_function = nullptr;
}

DiskChecker::~DiskChecker() {}

} // namespace starrocks
