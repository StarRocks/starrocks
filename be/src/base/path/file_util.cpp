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

#include "file_util.h"

#include <fstream>
#include <sstream>
namespace starrocks {

bool FileUtil::read_whole_content(const std::string& path, std::string& dest) {
    std::ifstream ifs;
    ifs.open(path);
    if (!ifs.good()) {
        return false;
    }
    std::stringstream ss;
    ss << ifs.rdbuf();
    ifs.close();

    dest = ss.str();
    return true;
}
}; // namespace starrocks
