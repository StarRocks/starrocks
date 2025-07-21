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

#include "paimon_file_system.h"

#include "fmt/format.h"
#include "fs/fs.h"

namespace starrocks {

const std::string PaimonFileSystemFactory::IDENTIFIER = "paimon";
const std::string PaimonOptions::ROOT_PATH = "path";

const char* PaimonFileSystemFactory::Identifier() const {
    return IDENTIFIER.c_str();
}

paimon::Result<std::unique_ptr<paimon::FileSystem>> PaimonFileSystemFactory::Create(
        const std::map<std::string, std::string>& options) const {
    return std::make_unique<PaimonFileSystem>(options);
}

uint64_t PaimonFileStatus::GetLen() const {
    return _len;
}

std::string PaimonFileStatus::GetPath() const {
    return _path;
}

bool PaimonFileStatus::IsDir() const {
    return _is_dir;
}

int64_t PaimonFileStatus::GetModificationTime() const {
    return _last_mod_time;
}

PaimonBasicFileStatus::PaimonBasicFileStatus(const std::string& path, bool is_dir)
        : path_(std::move(path)), is_dir_(is_dir) {}

PaimonBasicFileStatus::~PaimonBasicFileStatus() = default;

std::string PaimonBasicFileStatus::GetPath() const {
    return path_;
}

bool PaimonBasicFileStatus::IsDir() const {
    return is_dir_;
}

PaimonFileStatus::PaimonFileStatus(uint64_t len, int64_t last_modification_time, bool is_dir, std::string path)
        : _len(len), _last_mod_time(last_modification_time), _is_dir(is_dir), _path(std::move(path)) {}

PaimonFileStatus::~PaimonFileStatus() = default;

PaimonFileSystem::PaimonFileSystem(const std::map<std::string, std::string>& options) : _options(options) {
    std::string path = _options[PaimonOptions::ROOT_PATH];
    FSOptions fs_options = std::move(from_map());
    auto st = starrocks::FileSystem::CreateUniqueFromString(path, fs_options);
    if (!st.ok()) {
        // It looks like no scenario can reach this code path, but just in case.
        LOG(ERROR) << "Failed to create delegate file system, reason: " << st.status().detailed_message();
        throw std::runtime_error("Failed to create delegate file system");
    }
    _fs = std::move(st).value();
}

PaimonFileSystem::~PaimonFileSystem() = default;

FSOptions PaimonFileSystem::from_map() {
    FSOptions fs_options;
    return fs_options;
}

paimon::Result<std::unique_ptr<paimon::OutputStream>> PaimonFileSystem::Create(const std::string& path,
                                                                               bool overwrite) const {
    VLOG(10) << "Creating path " << path;
    WritableFileOptions options;
    options.mode = starrocks::FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    auto st = _fs->new_writable_file(options, path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to create file {}, reason: {}", path, st.status().detailed_message()));
    }
    return std::make_unique<PaimonOutputStream>(std::move(st).value());
}

paimon::Result<std::unique_ptr<paimon::InputStream>> PaimonFileSystem::Open(const std::string& path) const {
    VLOG(10) << "Open path " << path;
    RandomAccessFileOptions options;
    auto st = _fs->new_random_access_file(options, path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to open file {}, reason: {}", path, st.status().detailed_message()));
    }
    return std::make_unique<PaimonInputStream>(std::move(st).value());
}

paimon::Status PaimonFileSystem::Delete(const std::string& path, bool recursive) const {
    VLOG(10) << "Deleting path " << path << ", recursive: " << recursive;
    auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check whether {} is directory or not from delete, reason: {}", path,
                            st.status().detailed_message()));
    }
    Status status = delete_internal(path, st.value(), recursive);
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to delete path {}, reason: {}", path, status.detailed_message()));
    }
    return paimon::Status::OK();
}

Status PaimonFileSystem::delete_internal(const std::string& path, bool is_dir, bool recursive) const {
    VLOG(10) << "Deleting path " << path << ", is dir: " << is_dir << ", recursive: " << recursive;
    if (is_dir) {
        if (recursive) {
            return _fs->delete_dir_recursive(path);
        }
        return _fs->delete_dir(path);
    }
    return _fs->delete_file(path);
}

paimon::Result<bool> PaimonFileSystem::Exists(const std::string& path) const {
    VLOG(10) << "Checking path " << path << " exists or not.";
    auto st = _fs->path_exists(path);
    if (st.ok()) {
        return true;
    }
    if (st.is_not_found()) {
        return false;
    }
    return paimon::Status::IOError(
            fmt::format("Error occurs while checking path {} exist, reason: {}", path, st.detailed_message()));
}

paimon::Status PaimonFileSystem::Mkdirs(const std::string& path) const {
    VLOG(10) << "Creating directory " << path;
    auto st = _fs->create_dir_recursive(path);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to mkdirs for {}, reason: {}", path, st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::Rename(const std::string& src, const std::string& dst) const {
    VLOG(10) << "Renaming path " << src << " to " << dst;
    auto st = _fs->is_directory(src);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check whether source path {} is directory or not from rename, reason: {}", src,
                            st.status().detailed_message()));
    }
    // todo: should we asure src is exists while processing?
    if (st.value()) {
        // src is directory
        return paimon::Status::IOError("Not support rename directory currently.");
    }
    auto status = _fs->rename_file(src, dst);
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to rename source {} to dst {}, reason: {}", src, dst, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<std::unique_ptr<paimon::FileStatus>> PaimonFileSystem::GetFileStatus(const std::string& path) const {
    VLOG(10) << "Get path status for " << path;
    if (const auto st = _fs->path_exists(path); !st.ok()) {
        if (st.is_not_found()) {
            return paimon::Status::NotExist(fmt::format("Path {} is not exist.", path));
        }
        return paimon::Status::IOError(
                fmt::format("Get file status for {} failed, reason: {}", path, st.detailed_message()));
    }
    auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format(
                "Get file status but failed to check whether path {} is directory or not from get status, reason: {}",
                path, st.status().detailed_message()));
    }
    auto st1 = _fs->get_file_size(path);
    if (!st1.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to get file size for {}, reason: {}", path, st1.status().detailed_message()));
    }
    auto st2 = _fs->get_file_modified_time(path);
    if (!st2.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get file modified time for {}, reason: {}", path,
                                                   st2.status().detailed_message()));
    }
    return std::make_unique<PaimonFileStatus>(st1.value(), st2.value(), st.value(), path);
}

paimon::Status PaimonFileSystem::ListDir(
        const std::string& dir, std::vector<std::unique_ptr<paimon::BasicFileStatus>>* file_status_list) const {
    if (dir.empty()) {
        return paimon::Status::IOError("dir is empty.");
    }
    VLOG(10) << "List Dir status for " << dir;
    const auto st = _fs->is_directory(dir);
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to check {} is directory or not from list dir, reason: {}",
                                                   dir, st.status().detailed_message()));
    }
    if (!st.value()) {
        return paimon::Status::IOError(fmt::format("Cannot get status for {}, because it is not a directory.", dir));
    }

    if (file_status_list == nullptr) {
        file_status_list = new std::vector<std::unique_ptr<paimon::BasicFileStatus>>();
    }
    const auto status = _fs->iterate_dir2(dir, [this, &file_status_list](const DirEntry& dir_entry) -> bool {
        const std::string filename(dir_entry.name.begin(), dir_entry.name.end());
        if (filename.size() != dir_entry.name.size()) {
            // that means file name contains delimiter character which will cause a failure cast.
            return false;
        }
        file_status_list->emplace_back(
                std::make_unique<PaimonBasicFileStatus>(filename, dir_entry.is_dir.value_or(false)));
        return true;
    });
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to get status for {}, reason: {}", dir, status.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Status PaimonFileSystem::ListFileStatus(
        const std::string& path, std::vector<std::unique_ptr<paimon::FileStatus>>* file_status_list) const {
    if (path.empty()) {
        return paimon::Status::IOError("path is empty.");
    }
    VLOG(10) << "List file status for " << path;
    const auto st = _fs->is_directory(path);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to check {} is directory or not from list status, reason: {}", path,
                            st.status().detailed_message()));
    }

    if (file_status_list == nullptr) {
        file_status_list = new std::vector<std::unique_ptr<paimon::FileStatus>>();
    }
    if (st.value()) {
        // path is directory
        const auto status = _fs->iterate_dir2(path, [this, &file_status_list](const DirEntry& dir_entry) -> bool {
            const std::string filename(dir_entry.name.begin(), dir_entry.name.end());
            if (filename.size() != dir_entry.name.size()) {
                // that means file name contains delimiter character which will cause a failure cast.
                return false;
            }
            if (auto res = GetFileStatus(filename); res.ok()) {
                file_status_list->emplace_back(std::move(res).value());
            }
            return true;
        });
        if (!status.ok()) {
            return paimon::Status::IOError(
                    fmt::format("Failed to list file status for {}, reason: {}", path, status.detailed_message()));
        }
    } else {
        auto res = GetFileStatus(path);
        if (!res.ok() && !res.status().IsNotExist()) {
            return paimon::Status::IOError(fmt::format("Failed to get file status for {}, reason: {}", path,
                                                       res.status().detail()->ToString()));
        }
        file_status_list->emplace_back(std::move(res).value());
    }
    return paimon::Status::OK();
}

PaimonInputStream::PaimonInputStream(std::unique_ptr<RandomAccessFile> file) : _file(std::move(file)) {}

PaimonInputStream::~PaimonInputStream() = default;

paimon::Result<int32_t> PaimonInputStream::Read(char* buffer, uint32_t size) {
    auto st = _file->read(buffer, size);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to read file {}, reason: {}", _file->filename(), st.status().detailed_message()));
    }
    return static_cast<int32_t>(st.value());
}

paimon::Status PaimonInputStream::Close() {
    // todo: close
    return paimon::Status::OK();
}

paimon::Result<uint64_t> PaimonInputStream::Length() const {
    auto st = _file->get_size();
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get length for file {}, reason: {}", _file->filename(),
                                                   st.status().detailed_message()));
    }
    return static_cast<uint64_t>(st.value());
}

paimon::Result<int32_t> PaimonInputStream::Read(char* buffer, uint32_t size, uint64_t offset) {
    auto status = _file->read_at_fully(offset, buffer, size);
    if (!status.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to read file {}, reason: {}", _file->filename(), status.detailed_message()));
    }
    return static_cast<int32_t>(size);
}

paimon::Status PaimonInputStream::Seek(int64_t offset, paimon::SeekOrigin origin) {
    int64_t new_pos = offset;
    if (origin == paimon::SeekOrigin::FS_SEEK_CUR) {
        /* set file offset to current plus offset */
        auto res = GetPos();
        if (!res.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}",
                                                       _file->filename(), res.status().ToString()));
        }
        new_pos = res.value() + offset;
    } else if (origin == paimon::SeekOrigin::FS_SEEK_END) {
        /* set file offset to EOF plus offset */
        auto res = Length();
        if (!res.ok()) {
            return paimon::Status::IOError(fmt::format("Failed to get length for file {}, reason: {}",
                                                       _file->filename(), res.status().ToString()));
        }
        new_pos = res.value() + offset;
    }
    auto st = _file->seek(new_pos);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to seek file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<int64_t> PaimonInputStream::GetPos() const {
    auto st = _file->position();
    if (!st.ok()) {
        return paimon::Status::IOError(fmt::format("Failed to get position for file {}, reason: {}", _file->filename(),
                                                   st.status().detailed_message()));
    }
    return st.value();
}

paimon::Result<std::string> PaimonInputStream::GetUri() const {
    return _file->filename();
}

void PaimonInputStream::ReadAsync(char* buffer, uint32_t size, uint64_t offset,
                                  std::function<void(paimon::Status)>&& callback) {
    paimon::Result<int32_t> read_size = Read(buffer, size, offset);
    paimon::Status status = paimon::Status::OK();
    if (read_size.ok() && (uint32_t)read_size.value() != size) {
        status = paimon::Status::IOError(
                fmt::format("file '{}' async read size {} != expected {}", _file->filename(), read_size.value(), size));
    } else if (!read_size.ok()) {
        status = read_size.status();
    }
    callback(status);
}

PaimonOutputStream::PaimonOutputStream(std::unique_ptr<WritableFile> file) : _file(std::move(file)) {}

PaimonOutputStream::~PaimonOutputStream() = default;

paimon::Result<int32_t> PaimonOutputStream::Write(const char* buffer, uint32_t size) {
    if (const auto st = _file->append(Slice(buffer, size)); !st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to write file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Result(static_cast<int32_t>(size));
}

paimon::Result<int32_t> PaimonOutputStream::Write(const char* buffer, uint32_t size, uint64_t crc32c) {
    return Write(buffer, size);
}

paimon::Status PaimonOutputStream::Close() {
    const auto st = _file->close();
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to close file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Status::OK();
}

paimon::Result<uint64_t> PaimonOutputStream::Flush() {
    WritableFile::FlushMode mode = WritableFile::FLUSH_SYNC;
    auto st = _file->flush(mode);
    if (!st.ok()) {
        return paimon::Status::IOError(
                fmt::format("Failed to flush file {}, reason: {}", _file->filename(), st.detailed_message()));
    }
    return paimon::Result(_file->size());
}

paimon::Result<int64_t> PaimonOutputStream::GetPos() const {
    return _file->size();
}

paimon::Result<std::string> PaimonOutputStream::GetUri() const {
    return _file->filename();
}

} // namespace starrocks
