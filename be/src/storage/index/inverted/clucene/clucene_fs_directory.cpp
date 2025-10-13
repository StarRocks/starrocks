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

#include "clucene_fs_directory.h"

#include <utility>

#include "CLucene.h"
#include "CLucene/SharedHeader.h"
#include "column/column_view/column_view.h"
#include "common/config.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "util/stack_util.h"

namespace starrocks {

const char* const StarRocksMergingDirectory::WRITE_LOCK_FILE = "write.lock";
const std::string StarRocksMergingDirectory::CLASS_NAME = "StarRocksMergingDirectory";

const std::string StarRocksIndexInput::CLASS_NAME = "StarRocksIndexInput";

StarRocksIndexInput::StarRocksIndexInput(FileSystem* fs, const std::string& path, const int32_t buffer_size)
        : _fs(fs), _path(path), _buffer_size(buffer_size) {}

StarRocksIndexInput::StarRocksIndexInput(const StarRocksIndexInput& other)
        : BufferedIndexInput(other),
          _fs(other._fs),
          _path(other._path),
          _buffer_size(other._buffer_size),
          _offset(other._offset),
          _length(other._length) {}

StarRocksIndexInput::~StarRocksIndexInput() {
    StarRocksIndexInput::close();
}

const char* StarRocksIndexInput::getDirectoryType() const {
    if (_offset.has_value()) {
        return StarRocksMergedDirectory::CLASS_NAME.c_str();
    } else {
        return StarRocksMergingDirectory::CLASS_NAME.c_str();
    }
}

const char* StarRocksIndexInput::getObjectName() const {
    return fmt::format("{}@{}", CLASS_NAME.c_str(), _path).c_str();
}

const char* StarRocksIndexInput::getClassName() {
    return CLASS_NAME.c_str();
}

lucene::store::IndexInput* StarRocksIndexInput::clone() const {
    return _CLNEW StarRocksIndexInput(*this);
}

void StarRocksIndexInput::close() {
    BufferedIndexInput::close();
}

int64_t StarRocksIndexInput::length() const {
    if (_length.has_value()) {
        return _length.value();
    }
    auto st = _fs->get_file_size(_path);
    if (st.ok()) {
        return st.value();
    }
    _CLTHROWA(CL_ERR_IO,
              fmt::format("Failed get file size for {}, reason: {}", _path, st.status().detailed_message()).c_str());
}

void StarRocksIndexInput::init_if_necessary() {
    if (_reader == nullptr) {
        RandomAccessFileOptions options;
        options.buffer_size = _buffer_size;
        auto st = _fs->new_random_access_file(options, _path);
        if (!st.ok()) {
            _CLTHROWA(
                    CL_ERR_IO,
                    fmt::format("Failed open file for {}, reason: {}", _path, st.status().detailed_message()).c_str());
        }
        _reader = std::move(st).value();
    }
}

void StarRocksIndexInput::seekInternal(const int64_t position) {
    const int64_t start = _offset.has_value() ? _offset.value() : 0;
    const int64_t end = length();
    if (position < start || position > end) {
        _CLTHROWA(CL_ERR_IO, "seek past EOF");
    }
    _pos = position;
}

void StarRocksIndexInput::readInternal(uint8_t* b, const int32_t len) {
    init_if_necessary();

    if (const auto st = _reader->read_at_fully(_pos, b, len); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    _pos += len;
}

StarRocksIndexOutput::StarRocksIndexOutput(std::unique_ptr<WritableFile> writer) : _writer(std::move(writer)) {}

StarRocksIndexOutput::~StarRocksIndexOutput() {
    if (_writer != nullptr) {
        try {
            BufferedIndexOutput::close();
        } catch (CLuceneError& e) {
            LOG(WARNING) << "BufferedIndexOutput close failed: " << e.what();
        }

        auto st = _writer->sync();
        if (!st.ok()) {
            LOG(WARNING) << "[" << _writer->filename() << "] Sync writer failed, reason: " << st.detailed_message();
        }
        if (st = _writer->close(); !st.ok()) {
            LOG(WARNING) << "[" << _writer->filename() << "] Close writer failed, reason: " << st.detailed_message();
        }
        _writer.reset();
    }
}

void StarRocksIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (b != nullptr && size > 0) {
        if (_writer == nullptr) {
            _CLTHROWA(CL_ERR_IO, "File writer is nullptr in StarRocksIndexOutput");
        }
        const Slice data{b, static_cast<size_t>(size)};
        if (const Status& st = _writer->append(data); !st.ok()) {
            _CLTHROWA(
                    CL_ERR_IO,
                    fmt::format("writer append data when flushBuffer error, reason: {}", st.detailed_message()).data());
        }
    } else {
        VLOG(1) << "skip flush buffer, reason: "
                << (b == nullptr ? "buffer is nullptr." : size < 0 ? "negative buffer size." : "zero buffer size.");
    }
}

int64_t StarRocksIndexOutput::length() const {
    CND_PRECONDITION(_writer != nullptr, "file is not open");
    return _writer->size();
}

StarRocksMergingDirectory::StarRocksMergingDirectory(FileSystem* fs, std::string directory,
                                                     std::shared_ptr<WritableFileOptions> opts)
        : _fs(fs), _directory(std::move(directory)), _opts(std::move(opts)) {}

StarRocksMergingDirectory::~StarRocksMergingDirectory() = default;

const char* StarRocksMergingDirectory::getObjectName() const {
    return fmt::format("{}@{}", CLASS_NAME, _directory).c_str();
}

bool StarRocksMergingDirectory::list(std::vector<std::string>* names) const {
    if (names == nullptr) {
        names = new std::vector<std::string>();
    }

    const auto st = _fs->iterate_dir2(_directory, [&](const DirEntry& entry) -> bool {
        std::string filename(entry.name.begin(), entry.name.end());
        if (entry.name != filename) {
            LOG(WARNING) << "List " << _directory << ", get a non valid utf-8 file name: " << entry.name;
            return false;
        }
        names->emplace_back(std::move(filename));
        return true;
    });

    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO,
                  fmt::format("Error occurs while listing directory {}, reason: {}", _directory, st.detailed_message())
                          .c_str());
    }
    return true;
}

bool StarRocksMergingDirectory::fileExists(const char* name) const {
    const std::string& filename = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "checking file " << filename << " existing.";

    const auto st = _fs->path_exists(filename);

    if (st.ok()) return true;
    if (st.is_not_found()) return false;
    _CLTHROWA(CL_ERR_IO,
              fmt::format("Error occurs while checking file {} exists, reason: {}", filename, st.detailed_message())
                      .c_str());
}

int64_t StarRocksMergingDirectory::fileModified(const char* name) const {
    const std::string& filename = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "get file " << filename << " modified time.";
    auto st = _fs->get_file_modified_time(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Get file modified error: {}", st.status().detailed_message()).c_str());
    }
    return st.value();
}

void StarRocksMergingDirectory::touchFile(const char* name) {
    const std::string& filename = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "touch file " << filename;
    const auto st = _fs->new_writable_file(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Touch file error: {}", st.status().detailed_message()).c_str());
    }
    if (const auto& status = st.value()->close(); !status.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Touch file error: {}", st.status().detailed_message()).c_str());
    }
}

int64_t StarRocksMergingDirectory::fileLength(const char* name) const {
    const std::string& filename = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "get file " << filename << " length.";
    auto st = _fs->get_file_size(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Get file size error: {}", st.status().detailed_message()).c_str());
    }
    return st.value();
}

bool StarRocksMergingDirectory::openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                                          const int32_t bufferSize) {
    const std::string& fullname = fmt::format("{}/{}", _directory, name);
    ret = new StarRocksIndexInput(_fs, fullname, bufferSize);
    return true;
}

void StarRocksMergingDirectory::close() {
    // No idea what should we do at here.
}

bool StarRocksMergingDirectory::doDeleteFile(const char* name) {
    const std::string& filename = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "delete file " << filename;
    if (const auto st = _fs->delete_file(filename); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Delete file {} error: {}", filename, st.detailed_message()).c_str());
    }
    return true;
}

void StarRocksMergingDirectory::deleteDirectory() const {
    VLOG(10) << "delete directory " << _directory;
    if (const auto st = _fs->delete_dir_recursive(_directory); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Delete directory error: {}", st.detailed_message()).c_str());
    }
}

void StarRocksMergingDirectory::renameFile(const char* from, const char* to) {
    const std::string& from_name = fmt::format("{}/{}", _directory, from);
    const std::string& to_name = fmt::format("{}/{}", _directory, to);
    VLOG(10) << "rename " << from_name << " to " << to_name;

    if (const auto st = _fs->rename_file(from_name, to_name); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Rename {} to {} IO error: {}", from, to, st.detailed_message()).c_str());
    }
}

lucene::store::IndexOutput* StarRocksMergingDirectory::createOutput(const char* name) {
    const std::string& full_name = fmt::format("{}/{}", _directory, name);
    VLOG(10) << "create index output for " << full_name;

    auto st = _fs->new_writable_file(*_opts, full_name);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Error occurs while creating output for {}, reason: {}", full_name,
                                         st.status().detailed_message())
                                     .c_str());
    }
    return _CLNEW StarRocksIndexOutput(std::move(st).value());
}

std::string StarRocksMergingDirectory::toString() const {
    return getObjectName();
}

const std::string StarRocksMergedDirectory::CLASS_NAME = "StarRocksMergedDirectory";

StarRocksMergedDirectory::StarRocksMergedDirectory(const std::unique_ptr<StarRocksIndexInput>& stream,
                                                   const int64_t index_id,
                                                   const std::map<std::string, FileEntry>& entries)
        : _index_id(index_id), _entries(entries) {
    _stream = std::unique_ptr<StarRocksIndexInput>(static_cast<StarRocksIndexInput*>(stream->clone()));
}

StarRocksMergedDirectory::~StarRocksMergedDirectory() = default;

bool StarRocksMergedDirectory::list(std::vector<std::string>* names) const {
    for (auto& file_name : _entries | std::views::keys) {
        names->emplace_back(file_name);
    }
    return true;
}

bool StarRocksMergedDirectory::fileExists(const char* name) const {
    return _entries.contains(name);
}

int64_t StarRocksMergedDirectory::fileModified(const char* name) const {
    return 0;
}

int64_t StarRocksMergedDirectory::fileLength(const char* name) const {
    if (_entries.contains(name)) {
        return _entries.at(name).length;
    }
    _CLTHROWA(CL_ERR_IO, fmt::format("No file {} found in inverted index {}", name, _index_id).c_str());
}

bool StarRocksMergedDirectory::openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                                         int32_t bufferSize) {
    if (_stream == nullptr) {
        err.set(CL_ERR_IO,
                fmt::format("Input stream for merged directory of inverted index {} is not open.", _index_id).c_str());
        return false;
    }

    if (!_entries.contains(name)) {
        err.set(CL_ERR_IO,
                fmt::format("No file {} found in the merged directory of inverted index {}", name, _index_id).c_str());
        return false;
    }
    const auto& file = _entries.at(name);

    const auto cloned_input = _CLNEW StarRocksIndexInput(*_stream);
    cloned_input->set_offset(file.offset);
    cloned_input->set_length(file.length);
    ret = cloned_input;
    return true;
}

void StarRocksMergedDirectory::renameFile(const char* from, const char* to) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, fmt::format("Rename file is not supported in {}", getObjectName()).c_str());
}

void StarRocksMergedDirectory::touchFile(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, fmt::format("Touch file is not supported in {}", getObjectName()).c_str());
}

lucene::store::IndexOutput* StarRocksMergedDirectory::createOutput(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,
              fmt::format("Create output is not supported in {}", getObjectName()).c_str());
}

void StarRocksMergedDirectory::close() {
    // No idea what should we do at here.
}

std::string StarRocksMergedDirectory::toString() const {
    return fmt::format("{}@{}", CLASS_NAME, _index_id);
}

const char* StarRocksMergedDirectory::getObjectName() const {
    return toString().c_str();
}

bool StarRocksMergedDirectory::doDeleteFile(const char* name) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, fmt::format("Delete file is not supported in {}", getObjectName()).c_str());
}

StatusOr<std::shared_ptr<StarRocksMergingDirectory>> StarRocksFSDirectoryFactory::getDirectory(
        FileSystem* fs, const std::string& file, const std::shared_ptr<WritableFileOptions>& opts) {
    if (file.empty()) {
        return Status::InvalidArgument("Invalid file name");
    }

    if (const auto st = fs->path_exists(file); st.is_not_found()) {
        RETURN_IF_ERROR(fs->create_dir_recursive(file));
    } else if (!st.ok()) {
        return Status::IOError(fmt::format("Get directory exists error: {}", st.detailed_message()));
    }
    return std::make_shared<StarRocksMergingDirectory>(fs, file, opts);
}

} // namespace starrocks
