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

#include "fs/fs_options.h"

namespace starrocks {

class FSOptionsHelper {
public:
    static const THdfsProperties* hdfs_properties(const FSOptions& options);
    static const TCloudConfiguration* cloud_configuration(const FSOptions& options);
    static bool azure_use_native_sdk(const FSOptions& options);
};

} // namespace starrocks
