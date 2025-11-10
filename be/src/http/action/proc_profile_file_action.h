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

#include "common/status.h"
#include "http/http_handler.h"

namespace starrocks {

class ExecEnv;

// Serve individual proc profile files from tar.gz archives
class ProcProfileFileAction : public HttpHandler {
public:
    explicit ProcProfileFileAction(ExecEnv* exec_env);

    ~ProcProfileFileAction() override = default;

    void handle(HttpRequest* req) override;

private:
    bool is_valid_filename(const std::string& filename);
    void serve_gzipped_html(HttpRequest* req, const std::string& file_path);
    void serve_gz_file(HttpRequest* req, const std::string& file_path);
    void serve_pprof_as_flame(HttpRequest* req, const std::string& file_path);
    void serve_flame_as_html_gz(HttpRequest* req, const std::string& file_path);
    Status convert_pprof_to_flame(const std::string& pprof_file_path, std::string& flame_svg_content);

    [[maybe_unused]] ExecEnv* _exec_env;
};

} // end namespace starrocks
