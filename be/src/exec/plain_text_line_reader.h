// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/plain_text_line_reader.h

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

#include "exec/line_reader.h"
#include "util/runtime_profile.h"

namespace starrocks {

class FileReader;
class Decompressor;
class Status;

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

class PlainTextLineReader : public LineReader {
public:
    PlainTextLineReader(RuntimeProfile* profile, FileReader* file_reader, Decompressor* decompressor, size_t length,
                        uint8_t row_delimiter);

    ~PlainTextLineReader() override;

    Status read_line(const uint8_t** ptr, size_t* size, bool* eof) override;

    void close() override;

private:
    bool update_eof();

    inline size_t output_buf_read_remaining() { return _output_buf_limit - _output_buf_pos; }

    inline size_t input_buf_read_remaining() { return _input_buf_limit - _input_buf_pos; }

    inline bool done() { return _file_eof && output_buf_read_remaining() == 0; }

    // find row delimiter from 'start' to 'start' + len,
    // return row delimiter pos if found, otherwise return nullptr.
    // TODO:
    //  save to positions of field separator
    uint8_t* update_field_pos_and_find_row_delimiter(const uint8_t* start, size_t len);

    void extend_output_buf();

private:
    RuntimeProfile* _profile;
    FileReader* _file_reader;
    Decompressor* _decompressor;
    size_t _min_length;
    size_t _total_read_bytes;
    uint8_t _row_delimiter;

    // save the data read from file reader
    std::unique_ptr<uint8_t[]> _input_buf;
    size_t _input_buf_size;
    size_t _input_buf_pos;
    size_t _input_buf_limit;

    // save the data decompressed from decompressor.
    std::unique_ptr<uint8_t[]> _output_buf;
    size_t _output_buf_size;
    size_t _output_buf_pos;
    size_t _output_buf_limit;

    bool _file_eof;
    bool _eof;
    bool _stream_end;

    // Profile counters
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _bytes_decompress_counter;
    RuntimeProfile::Counter* _decompress_timer;
};

} // namespace starrocks
