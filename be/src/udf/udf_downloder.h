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

#include <memory>
#include <mutex>
#include <string>
#include <fs/fs.h>

#include "common/status.h"

namespace starrocks
{
    class udf_downloder
    {
    public:
        static Status download_remote_file_2_local(const std::string& remotePath, std::string& localPath,
                                                   const FSOptions& options);

    private:
        Status setup_local_file_path(const std::string& local_path);

        Status do_download(const std::string& remotePath, std::string& localPath, const FSOptions& options);

        static std::unordered_map<std::string, std::shared_ptr<std::mutex>> _path_mutexes;

        static std::mutex _download_mutex;

        static std::shared_ptr<std::mutex> get_mutex_for_path(const std::string& localPath);
    };
}
