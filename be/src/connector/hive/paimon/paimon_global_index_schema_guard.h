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

#include <paimon/fs/file_system.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

namespace starrocks {

// paimon-cpp 0.3 reads the latest schema even for a snapshot-bound GlobalIndexScan.
// Validate the exact bytes it consumes against the FE schema whose BTree encoding
// history was checked. Other reads retain the existing credentials/cache/IO path.
// Used only by distributed scalar index scans, not ordinary Paimon data readers.
class PaimonGlobalIndexSchemaGuard final : public paimon::FileSystem {
public:
    PaimonGlobalIndexSchemaGuard(std::shared_ptr<paimon::FileSystem> delegate, std::string table_path,
                                 int64_t schema_id, int64_t schema_time_millis)
            : _delegate(std::move(delegate)), _schema_id(schema_id), _schema_time_millis(schema_time_millis) {
        while (!table_path.empty() && table_path.back() == '/') {
            table_path.pop_back();
        }
        _schema_prefix = table_path + "/schema/schema-";
    }

    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path) const override;
    bool schema_validated() const { return _schema_validated.load(); }

    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override {
        return _delegate->Create(path, overwrite);
    }
    paimon::Status Mkdirs(const std::string& path) const override { return _delegate->Mkdirs(path); }
    paimon::Status Rename(const std::string& src, const std::string& dst) const override {
        return _delegate->Rename(src, dst);
    }
    paimon::Status Delete(const std::string& path, bool recursive) const override {
        return _delegate->Delete(path, recursive);
    }
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string& path) const override {
        return _delegate->GetFileStatus(path);
    }
    paimon::Status ListDir(const std::string& path,
                           std::vector<std::unique_ptr<paimon::BasicFileStatus>>* entries) const override {
        return _delegate->ListDir(path, entries);
    }
    paimon::Status ListFileStatus(const std::string& path,
                                  std::vector<std::unique_ptr<paimon::FileStatus>>* entries) const override {
        return _delegate->ListFileStatus(path, entries);
    }
    paimon::Result<bool> Exists(const std::string& path) const override { return _delegate->Exists(path); }

private:
    std::shared_ptr<paimon::FileSystem> _delegate;
    std::string _schema_prefix;
    int64_t _schema_id;
    int64_t _schema_time_millis;
    mutable std::atomic<bool> _schema_validated{false};
};

} // namespace starrocks
