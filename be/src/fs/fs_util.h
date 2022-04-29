// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/env/env_util.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <memory>
#include <string>

#include "common/status.h"

namespace starrocks {

class FileSystem;
class SequentialFile;
class RandomAccessFile;
class WritableFile;
struct WritableFileOptions;

namespace fs_util {

Status open_file_for_write(FileSystem* fs, const std::string& path, std::shared_ptr<WritableFile>* file);

Status open_file_for_write(const WritableFileOptions& opts, FileSystem* fs, const std::string& path,
                           std::shared_ptr<WritableFile>* file);

Status open_file_for_sequential(FileSystem* fs, const std::string& path, std::shared_ptr<SequentialFile>* file);

Status open_file_for_random(FileSystem* fs, const std::string& path, std::shared_ptr<RandomAccessFile>* file);

} // namespace fs_util
} // namespace starrocks
