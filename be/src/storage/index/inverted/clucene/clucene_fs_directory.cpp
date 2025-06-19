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
#include "CLucene/_SharedHeader.h"
#include "CLucene/store/LockFactory.h"
#include "common/config.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "util/stack_util.h"

namespace starrocks {

#define CL_MAX_PATH 4096
#define CL_MAX_DIR CL_MAX_PATH

#define PATH_DELIMITERA "/"

const char* const StarRocksFSDirectory::WRITE_LOCK_FILE = "write.lock";
const std::string StarRocksFSDirectory::CLASS_NAME = "StarRocksFSDirectory";

StatusOr<lucene::store::IndexInput*> StarRocksIndexInput::open(FileSystem* fs, const std::string& path,
                                                               int32_t buffer_size) {
    CND_PRECONDITION(!path.empty(), "path is empty");

    if (buffer_size == -1) {
        buffer_size = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    }
    auto h = std::make_shared<SharedHandle>(path);

    RandomAccessFileOptions options;
    options.buffer_size = buffer_size;
    ASSIGN_OR_RETURN(h->_reader, fs->new_random_access_file(options, path));

    if (h->_reader == nullptr) {
        return Status::InternalError("Opened inverted index file successfully, but returned a null file handle.");
    }

    //Check if a valid handle was retrieved
    ASSIGN_OR_RETURN(const auto file_size, h->_reader->get_size());
    if (file_size == 0) {
        // may be an empty file
        LOG(WARNING) << "Opened inverted index file is empty, file is " << path;
    }
    //Store the file length
    h->_length = file_size;
    h->_fpos = 0;
    return _CLNEW StarRocksIndexInput(std::move(h), buffer_size);
}

StarRocksIndexInput::StarRocksIndexInput(const StarRocksIndexInput& clone) : BufferedIndexInput(clone) {
    if (clone._handle == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "other handle is null");
    }

    std::lock_guard wlock(clone._handle->_shared_lock);
    _handle = clone._handle;
    _pos = clone._handle->_fpos; //note where we are currently...
    // _io_ctx = clone._io_ctx;
}

SharedHandle::SharedHandle(const std::string& path) : _path(std::move(path)) {
    _length = 0;
    _fpos = 0;
}

SharedHandle::~SharedHandle() {
    if (_reader) {
        _reader.reset();
    }
}

StarRocksIndexInput::~StarRocksIndexInput() {
    StarRocksIndexInput::close();
}

lucene::store::IndexInput* StarRocksIndexInput::clone() const {
    return _CLNEW StarRocksIndexInput(*this);
}
void StarRocksIndexInput::close() {
    BufferedIndexInput::close();
}

void StarRocksIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < _handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void StarRocksIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(_handle != nullptr, "shared file handle has closed");
    CND_PRECONDITION(_handle->_reader != nullptr, "file is not open");
    std::lock_guard wlock(_handle->_shared_lock);

    int64_t position = getFilePointer();
    if (_pos != position) {
        _pos = position;
    }

    if (_handle->_fpos != _pos) {
        _handle->_fpos = _pos;
    }

    auto st = _handle->_reader->read_at_fully(_pos, b, len);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }
    bufferLength = len;
    _pos += bufferLength;
    _handle->_fpos = _pos;
}

Status StarRocksIndexOutput::init(FileSystem* fs, const std::string& path) {
    ASSIGN_OR_RETURN(_writer, fs->new_writable_file(*_opts, path));
    return Status::OK();
}

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

void StarRocksIndexOutput::set_file_writer_opts(std::shared_ptr<WritableFileOptions> opts) {
    _opts = std::move(opts);
}

void StarRocksIndexOutputV2::init(WritableFile* file_writer) {
    _index_v2_file_writer = file_writer;
}

StarRocksIndexOutputV2::~StarRocksIndexOutputV2() {
    if (_index_v2_file_writer != nullptr) {
        try {
            // flush buffer if buffer is not empty.
            BufferedIndexOutput::close();
        } catch (CLuceneError& e) {
            LOG(WARNING) << "BufferedIndexOutput close failed: " << e.what();
        }

        if (const auto& st = _index_v2_file_writer->close(); !st.ok()) {
            LOG(WARNING) << "Close writer failed, reason: " << st.detailed_message();
        }
    }
}

void StarRocksIndexOutputV2::flushBuffer(const uint8_t* b, const int32_t size) {
    if (b != nullptr && size > 0) {
        if (_index_v2_file_writer == nullptr) {
            _CLTHROWA(CL_ERR_IO, "File writer is nullptr in StarRocksIndexOutputV2");
        }
        const Slice data{b, static_cast<size_t>(size)};
        if (const auto st = _index_v2_file_writer->append(data); !st.ok()) {
            _CLTHROWA(
                    CL_ERR_IO,
                    fmt::format("writer append data when flushBuffer error, reason: {}", st.detailed_message()).data());
        }
    } else {
        VLOG(1) << "skip flush buffer, reason: "
                << (b == nullptr ? "buffer is nullptr." : size < 0 ? "negative buffer size." : "zero buffer size.");
    }
}

int64_t StarRocksIndexOutputV2::length() const {
    CND_PRECONDITION(_index_v2_file_writer != nullptr, "file is not open");
    return _index_v2_file_writer->size();
}

StarRocksFSDirectory::StarRocksFSDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void StarRocksFSDirectory::init(FileSystem* fs, const char* path, lucene::store::LockFactory* lock_factory) {
    _fs = fs;
    directory = path;
    if (lock_factory == nullptr) {
        lock_factory = _CLNEW lucene::store::NoLockFactory();
    }
    setLockFactory(lock_factory);
}

StarRocksFSDirectory::~StarRocksFSDirectory() {
    VLOG(10) << "destruction StarRocksFSDirectory " << directory;
}

const char* StarRocksFSDirectory::getClassName() {
    return CLASS_NAME.c_str();
}
const char* StarRocksFSDirectory::getObjectName() const {
    return getClassName();
}

bool StarRocksFSDirectory::list(std::vector<std::string>* names) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const auto st = _fs->iterate_dir2(directory, [&](const DirEntry& entry) -> bool {
        VLOG(10) << "list file " << directory << ", get " << entry.name << ", is dir " << entry.is_dir.value_or(false)
                 << ", size is " << entry.size.value_or(0);
        if (!entry.is_dir.value_or(false)) {
            std::string filename(entry.name.begin(), entry.name.end());
            names->emplace_back(std::move(filename));
        }
        return true;
    });
    if (!st.ok()) {
        LOG(WARNING) << "List file IO error: " << st.detailed_message();
        return false;
    }
    return true;
}

bool StarRocksFSDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& filename = fmt::format("{}/{}", directory, name);
    VLOG(10) << "checking file " << filename << " existing.";
    const auto st = _fs->path_exists(filename);
    if (st.ok()) return true;
    if (st.is_not_found()) return false;
    _CLTHROWA(CL_ERR_IO, fmt::format("Get file exists error: {}", st.detailed_message()).data());
}

const std::string& StarRocksFSDirectory::getDirName() const {
    return directory;
}

int64_t StarRocksFSDirectory::fileModified(const char* name) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& filename = fmt::format("{}/{}", directory, name);
    VLOG(10) << "get file " << filename << " modified time.";
    auto st = _fs->get_file_modified_time(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Get file modified error: {}", st.status().detailed_message()).data());
    }
    return st.value();
}

void StarRocksFSDirectory::touchFile(const char* name) {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& filename = fmt::format("{}/{}", directory, name);
    VLOG(10) << "touch file " << filename;
    const auto st = _fs->new_writable_file(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Touch file error: {}", st.status().detailed_message()).data());
    }
    if (const auto& status = st.value()->close(); !status.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Touch file error: {}", st.status().detailed_message()).data());
    }
}

int64_t StarRocksFSDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& filename = fmt::format("{}/{}", directory, name);
    VLOG(10) << "get file " << filename << " length.";
    auto st = _fs->get_file_size(filename);
    if (!st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Get file size error: {}", st.status().detailed_message()).data());
    }
    return st.value();
}

bool StarRocksFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                                     int32_t bufferSize) {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& fullname = fmt::format("{}/{}", directory, name);
    VLOG(10) << "open input for " << name << ", convert to " << fullname;
    auto st = StarRocksIndexInput::open(_fs, fullname.c_str(), bufferSize);
    if (st.ok()) {
        ret = std::move(st).value();
        return true;
    }
    if (st.status().is_not_found()) {
        error.set(CL_ERR_FileNotFound, fmt::format("File does not exist, file is {}", name).data());
    } else {
        error.set(CL_ERR_IO, fmt::format("File open error: {}", name).data());
    }
    return false;
}

void StarRocksFSDirectory::close() {
    // No idea what should we do at here.
}

bool StarRocksFSDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    const std::string& filename = fmt::format("{}/{}", directory, name);
    VLOG(10) << "delete file " << filename;
    if (const auto st = _fs->delete_file(filename); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Delete file {} error: {}", filename, st.detailed_message()).data());
    }
    return true;
}

bool StarRocksFSDirectory::deleteDirectory() {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    VLOG(10) << "delete directory " << directory;
    if (const auto st = _fs->delete_dir_recursive(directory); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Delete directory error: {}", st.detailed_message()).data());
    }
    return true;
}

void StarRocksFSDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    std::lock_guard wlock(_this_lock);
    const std::string& from_name = fmt::format("{}/{}", directory, from);
    const std::string& to_name = fmt::format("{}/{}", directory, to);
    VLOG(10) << "rename " << from_name << " to " << to_name;
    if (const auto st = _fs->rename_file(from_name, to_name); !st.ok()) {
        _CLTHROWA(CL_ERR_IO, fmt::format("Rename {} to {} IO error: {}", from, to, st.detailed_message()).data());
    }
}

lucene::store::IndexOutput* StarRocksFSDirectory::createOutput(const char* name) {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    auto* ret = _CLNEW StarRocksIndexOutput();
    try {
        ret->set_file_writer_opts(_opts);
        const std::string& full_name = fmt::format("{}/{}", directory, name);
        VLOG(10) << "create index output for " << full_name;
        if (const auto st = ret->init(_fs, full_name); !st.ok()) {
            _CLTHROWA(CL_ERR_IO, fmt::format("Init index output error: {}", st.detailed_message()).data());
        }
    } catch (CLuceneError& err) {
        ret->close();
        _CLDELETE(ret);
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

lucene::store::IndexOutput* StarRocksFSDirectory::createOutputV2(WritableFile* file_writer) {
    auto* ret = _CLNEW StarRocksIndexOutputV2();
    try {
        ret->init(file_writer);
    } catch (CLuceneError& err) {
        ret->close();
        _CLDELETE(ret);
        LOG(WARNING) << "FSIndexOutput init error: " << err.what();
        _CLTHROWA(CL_ERR_IO, "FSIndexOutput init error");
    }
    return ret;
}

std::string StarRocksFSDirectory::toString() const {
    return std::string("StarRocksFSDirectory@") + this->directory;
}

void StarRocksFSDirectory::set_file_writer_opts(std::shared_ptr<WritableFileOptions> opts) {
    _opts = std::move(opts);
}

StarRocksRAMFSDirectory::StarRocksRAMFSDirectory() {
    filesMap = _CLNEW FileMap(true, true);
    this->sizeInBytes = 0;
}

StarRocksRAMFSDirectory::~StarRocksRAMFSDirectory() {
    std::lock_guard wlock(_this_lock);
    filesMap->clear();
    _CLDELETE(lockFactory);
    _CLDELETE(filesMap);
}

void StarRocksRAMFSDirectory::init(FileSystem* fs, const char* path, lucene::store::LockFactory* lock_factory) {
    _fs = fs;
    directory = path;

    setLockFactory(_CLNEW lucene::store::SingleInstanceLockFactory());
}

bool StarRocksRAMFSDirectory::list(std::vector<std::string>* names) const {
    std::lock_guard wlock(_this_lock);
    auto itr = filesMap->begin();
    while (itr != filesMap->end()) {
        names->emplace_back(itr->first);
        ++itr;
    }
    return true;
}

bool StarRocksRAMFSDirectory::fileExists(const char* name) const {
    std::lock_guard wlock(_this_lock);
    return filesMap->exists(const_cast<char*>(name));
}

int64_t StarRocksRAMFSDirectory::fileModified(const char* name) const {
    std::lock_guard wlock(_this_lock);
    auto* f = filesMap->get(const_cast<char*>(name));
    if (f == nullptr) {
        _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
    }
    return f->getLastModified();
}

void StarRocksRAMFSDirectory::touchFile(const char* name) {
    lucene::store::RAMFile* file = nullptr;
    {
        std::lock_guard wlock(_this_lock);
        file = filesMap->get(const_cast<char*>(name));
        if (file == nullptr) {
            _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
        }
    }
    const uint64_t ts1 = file->getLastModified();
    uint64_t ts2 = lucene::util::Misc::currentTimeMillis();

    //make sure that the time has actually changed
    while (ts1 == ts2) {
        _LUCENE_SLEEP(1);
        ts2 = lucene::util::Misc::currentTimeMillis();
    };

    file->setLastModified(ts2);
}

int64_t StarRocksRAMFSDirectory::fileLength(const char* name) const {
    std::lock_guard wlock(_this_lock);
    auto* f = filesMap->get(const_cast<char*>(name));
    if (f == nullptr) {
        _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
    }
    return f->getLength();
}

bool StarRocksRAMFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                                        int32_t bufferSize) {
    std::lock_guard wlock(_this_lock);
    auto* file = filesMap->get(const_cast<char*>(name));
    if (file == nullptr) {
        error.set(CL_ERR_IO, "[StarRocksRAMFSDirectory::open] The requested file does not exist.");
        return false;
    }
    ret = _CLNEW lucene::store::RAMInputStream(file);
    return true;
}

void StarRocksRAMFSDirectory::close() {
    StarRocksFSDirectory::close();
}

bool StarRocksRAMFSDirectory::doDeleteFile(const char* name) {
    std::lock_guard wlock(_this_lock);
    auto itr = filesMap->find(const_cast<char*>(name));
    if (itr != filesMap->end()) {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr->second->sizeInBytes;
        filesMap->removeitr(itr);
    }
    return true;
}

bool StarRocksRAMFSDirectory::deleteDirectory() {
    // do nothing, RAM dir do not have actual files
    return true;
}

void StarRocksRAMFSDirectory::renameFile(const char* from, const char* to) {
    std::lock_guard wlock(_this_lock);
    auto itr = filesMap->find(const_cast<char*>(from));

    /* DSR:CL_BUG_LEAK:
    ** If a file named $to already existed, its old value was leaked.
    ** My inclination would be to prevent this implicit deletion with an
    ** exception, but it happens routinely in CLucene's internals (e.g., during
    ** IndexWriter.addIndexes with the file named 'segments'). */
    if (filesMap->exists(const_cast<char*>(to))) {
        auto itr1 = filesMap->find(const_cast<char*>(to));
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr1->second->sizeInBytes;
        filesMap->removeitr(itr1);
    }
    if (itr == filesMap->end()) {
        char tmp[1024];
        snprintf(tmp, 1024, "cannot rename %s, file does not exist", from);
        _CLTHROWT(CL_ERR_IO, tmp);
    }
    DCHECK(itr != filesMap->end());
    auto* file = itr->second;
    filesMap->removeitr(itr, false, true);
    filesMap->put(strdup(to), file);
}

lucene::store::IndexOutput* StarRocksRAMFSDirectory::createOutput(const char* name) {
    /* Check the $filesMap VoidMap to see if there was a previous file named
    ** $name.  If so, delete the old RAMFile object, but reuse the existing
    ** char buffer ($n) that holds the filename.  If not, duplicate the
    ** supplied filename buffer ($name) and pass ownership of that memory ($n)
    ** to $files. */
    std::lock_guard wlock(_this_lock);

    // get the actual pointer to the output name
    char* n = nullptr;
    auto itr = filesMap->find(const_cast<char*>(name));
    if (itr != filesMap->end()) {
        n = itr->first;
        lucene::store::RAMFile* rf = itr->second;
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= rf->sizeInBytes;
        _CLDELETE(rf);
    } else {
        n = STRDUP_AtoA(name);
    }

    auto* file = _CLNEW lucene::store::RAMFile();
    (*filesMap)[n] = file;

    return _CLNEW lucene::store::RAMOutputStream(file);
}

std::string StarRocksRAMFSDirectory::toString() const {
    return std::string("StarRocksRAMFSDirectory@") + this->directory;
}

const char* StarRocksRAMFSDirectory::getClassName() {
    return "StarRocksRAMFSDirectory";
}

const char* StarRocksRAMFSDirectory::getObjectName() const {
    return getClassName();
}

StatusOr<std::shared_ptr<StarRocksFSDirectory>> StarRocksFSDirectoryFactory::getDirectory(
        FileSystem* fs, const std::string& file, const bool& can_use_ram_dir,
        lucene::store::LockFactory* lock_factory) {
    if (file.empty()) {
        return Status::InvalidArgument("Invalid file name");
    }

    std::shared_ptr<StarRocksFSDirectory> dir;
    // Write by RAM directory
    // 1. only write separated index files, which is can_use_ram_dir = true.
    // 2. config::inverted_index_ram_dir_enable = true
    if (config::inverted_index_ram_dir_enable && can_use_ram_dir) {
        dir = std::make_shared<StarRocksRAMFSDirectory>();
    } else {
        if (const auto st = fs->path_exists(file); st.is_not_found()) {
            RETURN_IF_ERROR(fs->create_dir_recursive(file));
        } else if (!st.ok()) {
            return Status::IOError(fmt::format("Get directory exists error: {}", st.detailed_message()));
        }
        dir = std::make_shared<StarRocksFSDirectory>();
    }
    dir->init(fs, file.c_str(), lock_factory);
    return dir;
}

} // namespace starrocks
