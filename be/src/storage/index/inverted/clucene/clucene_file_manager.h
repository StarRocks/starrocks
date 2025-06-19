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

#include <common/statusor.h>

#include <boost/core/noncopyable.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace starrocks {

class CLuceneFileWriter;
class CLuceneFileReader;

class CLuceneFileManager : public boost::noncopyable {
public:
    static CLuceneFileManager& getInstance();

    StatusOr<std::shared_ptr<CLuceneFileWriter>> get_or_create_clucene_file_writer(const std::string& path);
    Status remove_clucene_file_writer(const std::string& path);

    StatusOr<std::shared_ptr<CLuceneFileReader>> get_or_create_clucene_file_reader(const std::string& path);

private:
    CLuceneFileManager() = default;

    std::mutex _write_mutex;
    std::unordered_map<std::string, std::shared_ptr<CLuceneFileWriter>> _path_to_file_writers;
};

} // namespace starrocks