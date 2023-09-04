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

#include "dummy_load_path_mgr.h"

namespace starrocks {

Status DummyLoadPathMgr::init() {
    LOG(INFO) << "Load path configured to []";
    return Status::OK();
}

Status DummyLoadPathMgr::allocate_dir(const std::string& db, const std::string& label, std::string* prefix) {
    return Status::InternalError("No load path configed.");
}

void DummyLoadPathMgr::get_load_data_path(std::vector<std::string>* data_paths) {}

Status DummyLoadPathMgr::get_load_error_file_name(const TUniqueId& fragment_instance_id, std::string* error_path) {
    *error_path = "";
    return Status::OK();
}

std::string DummyLoadPathMgr::get_load_error_absolute_path(const std::string& file_path) {
    return "";
}

std::string DummyLoadPathMgr::get_load_rejected_record_absolute_path(const std::string& rejected_record_dir,
                                                                     const std::string& db, const std::string& label,
                                                                     const int64_t id,
                                                                     const TUniqueId& fragment_instance_id) {
    return "";
}
} // namespace starrocks