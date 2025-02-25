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

#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"

namespace starrocks {

class Status;
class FileSystem;
class WritableFile;
class RandomAccessFile;
struct FSOptions;

class PaimonInputStream : public paimon::InputStream {
public:
    PaimonInputStream(std::unique_ptr<RandomAccessFile> file);
    ~PaimonInputStream() override;
    paimon::Status Close() override;
    paimon::Status Seek(int64_t offset, paimon::SeekOrigin origin) override;
    paimon::Result<int64_t> GetPos() const override;
    paimon::Result<int32_t> Read(char* buffer, uint32_t size) override;
    paimon::Result<int32_t> Read(char* buffer, uint32_t size, uint64_t offset) override;
    void ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                   std::function<void(paimon::Status)>&& callback) override;
    std::string GetUri() const override;
    paimon::Result<uint64_t> Length() const override;

private:
    std::unique_ptr<RandomAccessFile> _file;
};

class PaimonOutputStream : public paimon::OutputStream {
public:
    PaimonOutputStream(std::unique_ptr<WritableFile> file);
    ~PaimonOutputStream() override;
    paimon::Status Close() override;
    paimon::Result<int32_t> Write(const char* buffer, uint32_t size) override;
    paimon::Result<int32_t> Write(const char* buffer, uint32_t size, uint64_t crc32c) override;
    paimon::Result<uint64_t> Flush() override;
    paimon::Result<int64_t> GetPos() const override;
    std::string GetUri() const override;

private:
    std::unique_ptr<WritableFile> _file;
};

class PaimonFileStatus : public paimon::FileStatus {
public:
    PaimonFileStatus(uint64_t len, int64_t last_modification_time, bool is_dir, std::string path);
    ~PaimonFileStatus() override;
    uint64_t GetLen() const override;
    bool IsDir() const override;
    std::string GetPath() const override;
    int64_t GetModificationTime() const override;

private:
    uint64_t _len;
    int64_t _last_mod_time;
    bool _is_dir;
    std::string _path;
};

class PaimonFileSystem : public paimon::FileSystem {
public:
    PaimonFileSystem(const std::map<std::string, std::string>& options);
    ~PaimonFileSystem() override;
    paimon::Result<std::unique_ptr<paimon::InputStream>> Open(const std::string& path) const override;
    paimon::Result<std::unique_ptr<paimon::OutputStream>> Create(const std::string& path,
                                                                 bool overwrite) const override;
    paimon::Status Mkdirs(const std::string& path) const override;
    paimon::Status Rename(const std::string& src, const std::string& dst) const override;
    paimon::Status Delete(const std::string& path, bool recursive) const override;
    paimon::Result<std::unique_ptr<paimon::FileStatus>> GetFileStatus(const std::string& path) const override;
    paimon::Status ListFileStatus(const std::string& directory, std::vector<std::string>* files,
                                  std::vector<std::string>* subdirs,
                                  std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const override;
    paimon::Result<bool> Exists(const std::string& path) const override;

private:
    Status delete_internal(const std::string& path, bool is_dir, bool recursive) const;
    FSOptions from_map();
    std::map<std::string, std::string> _options;
    std::unique_ptr<starrocks::FileSystem> _fs;
};

class PaimonFileSystemFactory : public paimon::FileSystemFactory {
public:
    static const std::string IDENTIFIER;
    const std::string& Identifier() const override;
    std::unique_ptr<paimon::FileSystem> Create(const std::map<std::string, std::string>& options) const override;
};

} // namespace starrocks
