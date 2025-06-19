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
#include <mutex>

#include "CLucene.h"
#include "storage/index/inverted/clucene/clucene_common.h"

namespace starrocks {

class CLUCENE_EXPORT CLuceneCompoundReader final : public lucene::store::Directory {
    lucene::store::RAMDirectory* _ram_dir = nullptr;
    CL_NS(store)::IndexInput* _stream = nullptr;
    // The life cycle of _entries should be consistent with that of the CLuceneCompoundReader.
    EntriesType* _entries = nullptr;
    std::mutex _this_lock;
    bool _closed = false;
    int32_t _read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE;
    void _copyFile(const char* file, int32_t file_length, uint8_t* buffer, int32_t buffer_length);

protected:
    /** Removes an existing file in the directory-> */
    bool doDeleteFile(const char* name) override;

public:
    CLuceneCompoundReader(CL_NS(store)::IndexInput* stream, EntriesType* entries_clone,
                          int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    CLuceneCompoundReader(CL_NS(store)::IndexInput* stream,
                          int32_t read_buffer_size = CL_NS(store)::BufferedIndexInput::BUFFER_SIZE);
    ~CLuceneCompoundReader() override;
    bool list(std::vector<std::string>* names) const override;
    bool fileExists(const char* name) const override;
    int64_t fileModified(const char* name) const override;
    int64_t fileLength(const char* name) const override;
    bool openInput(const char* name, lucene::store::IndexInput*& ret, CLuceneError& err,
                   int32_t bufferSize = -1) override;
    bool openInput(const char* name, std::unique_ptr<lucene::store::IndexInput>& ret, CLuceneError& err,
                   int32_t bufferSize = -1);
    void renameFile(const char* from, const char* to) override;
    void touchFile(const char* name) override;
    lucene::store::IndexOutput* createOutput(const char* name) override;
    void close() override;
    std::string toString() const override;
    static const char* getClassName();
    const char* getObjectName() const override;
    CL_NS(store)::IndexInput* getStarRocksIndexInput() const;

private:
    void initialize() const;
};

} // namespace starrocks