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
#include <fs/fs.h>

#include "common/status.h"

namespace starrocks
{
    class DownloadUtil
    {
    public:
        static Status download(const std::string& url, const std::string& target_file,
                               const std::string& expected_checksum, const TCloudConfiguration& cloud_configuration);

    private:
        static Status get_real_url(const std::string& url, std::string* real_url, const FSOptions& options);

        static Status get_java_udf_url(const std::string& url, std::string* real_url, const FSOptions& options);
    };
} // namespace starrocks
