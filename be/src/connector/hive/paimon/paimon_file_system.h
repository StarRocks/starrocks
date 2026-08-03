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
#include <string>

#include "paimon/fs/file_system.h"

namespace starrocks {

class FileSystem;
class RandomAccessFile;
struct FormatScannerStats;

// Adapts the FileSystem instance already created for an HDFS scan range to the
// read-only file-system API used by paimon-cpp. This keeps cloud credentials and
// scheme-specific behavior in StarRocks instead of configuring a second client.
class PaimonFileSystem final : public paimon::FileSystem {
public:
    PaimonFileSystem(FileSystem* file_system, FormatScannerStats* fs_stats, FormatScannerStats* app_stats)
            : _file_system(file_system), _fs_stats(fs_stats), _app_stats(app_stats) {}
    ~PaimonFileSystem() override = default;

    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path) const override;
    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path,
                                                              int64_t file_size) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override;
    paimon::Status Mkdirs(const std::string& path) const override;
    paimon::Status Rename(const std::string& src, const std::string& dst) const override;
    paimon::Status Delete(const std::string& path, bool recursive) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string& path) const override;
    paimon::Status ListDir(const std::string& directory,
                           std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const override;
    paimon::Status ListFileStatus(const std::string& path,
                                  std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const override;
    paimon::Result<bool> Exists(const std::string& path) const override;

private:
    FileSystem* _file_system;
    FormatScannerStats* _fs_stats;
    FormatScannerStats* _app_stats;
};

} // namespace starrocks
