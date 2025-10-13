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

#include <sstream>
#include <string>
#include <vector>

#include "http/http_handler.h"

namespace starrocks {

class ExecEnv;

// Display BE proc profile files from BRPC profiling
class ProcProfileAction : public HttpHandler {
public:
    explicit ProcProfileAction(ExecEnv* exec_env);

    ~ProcProfileAction() override = default;

    void handle(HttpRequest* req) override;

private:
    struct ProfileFileInfo {
        std::string type;
        std::string timestamp;
        long file_size;
        std::string filename;
        std::string format; // "flame" or "pprof"
    };

    void get_page_header(HttpRequest* req, std::stringstream* output);
    void get_page_footer(std::stringstream* output);
    void add_profile_list_info(std::stringstream* output);
    std::vector<ProfileFileInfo> get_profile_files();
    std::string get_profile_type(const std::string& filename);
    std::string get_time_part(const std::string& filename);
    std::string get_format_part(const std::string& filename);
    void append_profile_table_header(std::stringstream* output);
    void append_profile_table_body(std::stringstream* output, const std::vector<ProfileFileInfo>& profile_files);
    void append_table_footer(std::stringstream* output);

    [[maybe_unused]] ExecEnv* _exec_env;
};

} // end namespace starrocks
