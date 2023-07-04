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

#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks {

class TUniqueId;

class BaseLoadPathMgr {
public:
    virtual ~BaseLoadPathMgr() = default;

    virtual Status init() = 0;

    virtual Status allocate_dir(const std::string& db, const std::string& label, std::string* prefix) = 0;

    virtual void get_load_data_path(std::vector<std::string>* data_paths) = 0;

    virtual Status get_load_error_file_name(const TUniqueId& fragment_instance_id, std::string* error_path) = 0;
    virtual std::string get_load_error_absolute_path(const std::string& file_path) = 0;
    const std::string& get_load_error_file_dir() const { return _error_log_dir; }

    virtual std::string get_load_rejected_record_absolute_path(const std::string& rejected_record_dir,
                                                               const std::string& db, const std::string& label,
                                                               const int64_t id,
                                                               const TUniqueId& fragment_instance_id) = 0;

protected:
    std::string _error_log_dir;
};

} // namespace starrocks
