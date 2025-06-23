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

#include "clucene_compound_reader.h"

#include "storage/index/inverted/clucene/clucene_fs_directory.h"

namespace starrocks {

#define BUFFER_LENGTH 16384
#define CL_MAX_PATH 4096

#define STRDUP_WtoA(x) CL_NS(util)::Misc::_wideToChar(x)
#define STRDUP_TtoA STRDUP_WtoA

/** Implementation of an IndexInput that reads from a portion of the
 *  compound file.
 */
class CSIndexInput : public lucene::store::BufferedIndexInput {
    CL_NS(store)::IndexInput* base;
    std::string file_name;
    int64_t fileOffset;
    int64_t _length;

protected:
    void readInternal(uint8_t* /*b*/, const int32_t /*len*/) override;
    void seekInternal(const int64_t /*pos*/) override {}

public:
    CSIndexInput(CL_NS(store)::IndexInput* base, const std::string& file_name, const int64_t fileOffset,
                 const int64_t length, const int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    CSIndexInput(const CSIndexInput& clone);
    ~CSIndexInput() override;
    void close() override;
    lucene::store::IndexInput* clone() const override;
    int64_t length() const override { return _length; }
    const char* getDirectoryType() const override { return CLuceneCompoundReader::getClassName(); }
    const char* getObjectName() const override { return getClassName(); }
    static const char* getClassName() { return "CSIndexInput"; }
};

CSIndexInput::CSIndexInput(CL_NS(store)::IndexInput* base, const std::string& file_name, const int64_t fileOffset,
                           const int64_t length, const int32_t read_buffer_size)
        : BufferedIndexInput(read_buffer_size) {
    this->base = base;
    this->file_name = file_name;
    this->fileOffset = fileOffset;
    this->_length = length;
}

void CSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    std::lock_guard wlock(static_cast<StarRocksIndexInput*>(base)->_this_lock);

    const auto start = getFilePointer();
    if (start + len > _length) {
        _CLTHROWA(CL_ERR_IO, "read past EOF");
    }

    base->seek(fileOffset + start);
    base->readBytes(b, len, true);
}

CSIndexInput::~CSIndexInput() = default;

lucene::store::IndexInput* CSIndexInput::clone() const {
    return _CLNEW CSIndexInput(*this);
}

CSIndexInput::CSIndexInput(const CSIndexInput& clone) : BufferedIndexInput(clone) {
    this->base = clone.base;
    this->file_name = clone.file_name;
    this->fileOffset = clone.fileOffset;
    this->_length = clone._length;
}

void CSIndexInput::close() {}

CLuceneCompoundReader::CLuceneCompoundReader(CL_NS(store)::IndexInput* stream, EntriesType* entries_clone,
                                             int32_t read_buffer_size)
        : _stream(stream), _entries(_CLNEW EntriesType(true, true)), _read_buffer_size(read_buffer_size) {
    // After stream clone, the io_ctx needs to be reconfigured.
    initialize();

    for (auto& [origin_aid, origin_entry] : *entries_clone) {
        char* aid = strdup(origin_aid);
        auto* entry = _CLNEW ReaderFileEntry();
        entry->file_name = origin_entry->file_name;
        entry->offset = origin_entry->offset;
        entry->length = origin_entry->length;
        _entries->put(aid, entry);
    }
}

CLuceneCompoundReader::CLuceneCompoundReader(CL_NS(store)::IndexInput* stream, int32_t read_buffer_size)
        : _ram_dir(new lucene::store::RAMDirectory()),
          _stream(stream),
          _entries(_CLNEW EntriesType(true, true)),
          _read_buffer_size(read_buffer_size) {
    // After stream clone, the io_ctx needs to be reconfigured.
    initialize();

    try {
        int32_t count = _stream->readVInt();
        ReaderFileEntry* entry = nullptr;
        TCHAR tid[CL_MAX_PATH];
        uint8_t buffer[BUFFER_LENGTH];
        for (int32_t i = 0; i < count; i++) {
            entry = _CLNEW ReaderFileEntry();
            stream->readString(tid, CL_MAX_PATH);
            char* aid = STRDUP_TtoA(tid);
            entry->file_name = aid;
            entry->offset = stream->readLong();
            entry->length = stream->readLong();
            _entries->put(aid, entry);
            // read header file data
            if (entry->offset < 0) {
                //if offset is -1, it means it's size is lower than DorisFSDirectory::MAX_HEADER_DATA_SIZE, which is 128k.
                _copyFile(entry->file_name.c_str(), static_cast<int32_t>(entry->length), buffer, BUFFER_LENGTH);
            }
        }
    } catch (...) {
        try {
            if (_stream != nullptr) {
                _stream->close();
                _CLDELETE(_stream)
            }
            if (_entries != nullptr) {
                _entries->clear();
                _CLDELETE(_entries);
            }
            if (_ram_dir) {
                _ram_dir->close();
                _CLDELETE(_ram_dir)
            }
        } catch (CLuceneError& err) {
            if (err.number() != CL_ERR_IO) {
                throw err;
            }
        }
        throw;
    }
}

void CLuceneCompoundReader::_copyFile(const char* file, int32_t file_length, uint8_t* buffer, int32_t buffer_length) {
    std::unique_ptr<lucene::store::IndexOutput> output(_ram_dir->createOutput(file));
    int64_t start_ptr = output->getFilePointer();
    auto remainder = file_length;
    auto chunk = buffer_length;
    auto batch_len = file_length < chunk ? file_length : chunk;

    while (remainder > 0) {
        auto len = remainder < batch_len ? remainder : batch_len;
        _stream->readBytes(buffer, len);
        output->writeBytes(buffer, len);
        remainder -= len;
    }
    if (remainder != 0) {
        TCHAR buf[CL_MAX_PATH + 100];
        swprintf(buf, CL_MAX_PATH + 100,
                 _T("Non-zero remainder length after copying")
                 _T(": %d (id: %s, length: %d, buffer size: %d)"),
                 (int)remainder, file, (int)file_length, (int)chunk);
        _CLTHROWT(CL_ERR_IO, buf);
    }

    int64_t end_ptr = output->getFilePointer();
    int64_t diff = end_ptr - start_ptr;
    if (diff != file_length) {
        TCHAR buf[100];
        swprintf(buf, 100,
                 _T("Difference in the output file offsets %d ")
                 _T("does not match the original file length %d"),
                 (int)diff, (int)file_length);
        _CLTHROWA(CL_ERR_IO, buf);
    }
    output->close();
}

CLuceneCompoundReader::~CLuceneCompoundReader() {
    if (!_closed) {
        try {
            close();
        } catch (CLuceneError& err) {
            LOG(ERROR) << "CluceneCompoundReader finalize error:" << err.what();
        }
    }
    _CLDELETE(_entries)
}

const char* CLuceneCompoundReader::getClassName() {
    return "CluceneCompoundReader";
}
const char* CLuceneCompoundReader::getObjectName() const {
    return getClassName();
}

bool CLuceneCompoundReader::list(std::vector<std::string>* names) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "CluceneCompoundReader is already closed");
    }
    for (auto& _entry : *_entries) {
        names->emplace_back(_entry.first);
    }
    return true;
}

bool CLuceneCompoundReader::fileExists(const char* name) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "CluceneCompoundReader is already closed");
    }
    return _entries->exists(const_cast<char*>(name));
}

int64_t CLuceneCompoundReader::fileModified(const char* name) const {
    return 0;
}

int64_t CLuceneCompoundReader::fileLength(const char* name) const {
    if (_closed || _entries == nullptr) {
        _CLTHROWA(CL_ERR_IO, "CluceneCompoundReader is already closed");
    }
    ReaderFileEntry* e = _entries->get((char*)name);
    if (e == nullptr) {
        char buf[CL_MAX_PATH + 30];
        strcpy(buf, "File ");
        strncat(buf, name, CL_MAX_PATH);
        strcat(buf, " does not exist");
        _CLTHROWA(CL_ERR_IO, buf);
    }
    return e->length;
}

bool CLuceneCompoundReader::openInput(const char* name, std::unique_ptr<lucene::store::IndexInput>& ret,
                                      CLuceneError& err, const int32_t bufferSize) {
    if (_closed || _entries == nullptr) {
        err.set(CL_ERR_IO, "CluceneCompoundReader is already closed");
        return false;
    }
    lucene::store::IndexInput* tmp;
    bool success = openInput(name, tmp, err, bufferSize);
    if (success) {
        ret.reset(tmp);
    }
    return success;
}

bool CLuceneCompoundReader::openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& error,
                                      int32_t bufferSize) {
    if (_stream == nullptr) {
        error.set(CL_ERR_IO, "Stream closed");
        return false;
    }

    const ReaderFileEntry* entry = _entries->get((char*)name);
    if (entry == nullptr) {
        char buf[CL_MAX_PATH + 26];
        snprintf(buf, CL_MAX_PATH + 26, "No sub-file with id %s found", name);
        error.set(CL_ERR_IO, buf);
        return false;
    }

    // If file is in RAM, just return.
    if (_ram_dir && _ram_dir->fileExists(name)) {
        return _ram_dir->openInput(name, ret, error, bufferSize);
    }

    if (bufferSize < 1) {
        bufferSize = _read_buffer_size;
    }

    ret = _CLNEW CSIndexInput(_stream, entry->file_name, entry->offset, entry->length, bufferSize);
    return true;
}

void CLuceneCompoundReader::close() {
    std::lock_guard wlock(_this_lock);
    if (_stream != nullptr) {
        _stream->close();
        _CLDELETE(_stream)
    }
    if (_entries != nullptr) {
        // The life cycle of _entries should be consistent with that of the CluceneCompoundReader.
        // DO NOT DELETE _entries here, it will be deleted in the destructor
        // When directory is closed, all _entries are cleared. But the directory may be called in other places.
        // If we delete the _entries object here, it will cause core dump.
        _entries->clear();
    }
    if (_ram_dir) {
        _ram_dir->close();
        _CLDELETE(_ram_dir)
    }
    _closed = true;
}

bool CLuceneCompoundReader::doDeleteFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CluceneCompoundReader::doDeleteFile");
}

void CLuceneCompoundReader::renameFile(const char* /*from*/, const char* /*to*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CluceneCompoundReader::renameFile");
}

void CLuceneCompoundReader::touchFile(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CluceneCompoundReader::touchFile");
}

lucene::store::IndexOutput* CLuceneCompoundReader::createOutput(const char* /*name*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CluceneCompoundReader::createOutput");
}

std::string CLuceneCompoundReader::toString() const {
    return "CluceneCompoundReader@";
}

CL_NS(store)::IndexInput* CLuceneCompoundReader::getStarRocksIndexInput() const {
    return _stream;
}

void CLuceneCompoundReader::initialize() const {
    _stream->setIdxFileCache(true);
}

} // namespace starrocks