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
    std::string extract_html_from_tar_gz(const std::string& file_path);
    void serve_tar_gz_file(HttpRequest* req, const std::string& file_path);

    [[maybe_unused]] ExecEnv* _exec_env;
};

} // end namespace starrocks
