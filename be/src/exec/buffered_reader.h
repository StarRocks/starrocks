// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/buffered_reader.h

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

#include <cstdint>

#include "common/status.h"
#include "exec/file_reader.h"
#include "storage/olap_define.h"

namespace starrocks {

// Buffered Reader
// Add a cache layer between the caller and the file reader to reduce the
// times of calls to the read function to speed up.
class BufferedReader : public FileReader {
public:
    // If the reader need the file size, set it when construct FileReader.
    // There is no other way to set the file size.
    BufferedReader(FileReader* reader, int64_t = 1024 * 1024);
    ~BufferedReader() override;

    Status open() override;

    // Read
    Status read(uint8_t* buf, size_t* buf_len, bool* eof) override;
    Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override;
    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length) override;
    int64_t size() override;
    Status seek(int64_t position) override;
    Status tell(int64_t* position) override;
    void close() override;
    bool closed() override;

private:
    Status _fill();
    Status _read_once(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out);

private:
    FileReader* _reader;
    char* _buffer;
    int64_t _buffer_size;
    int64_t _buffer_offset;
    int64_t _buffer_limit;
    int64_t _cur_offset;
};

} // namespace starrocks
