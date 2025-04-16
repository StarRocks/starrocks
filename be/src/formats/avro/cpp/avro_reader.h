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

#include <cctz/time_zone.h>

#include <avrocpp/DataFile.hh>
#include <avrocpp/Generic.hh>
#include <avrocpp/Stream.hh>

#include "column/chunk.h"

namespace starrocks {

struct ScannerCounter;
class RandomAccessFile;
class RuntimeState;
class SlotDescriptor;

class AvroBufferInputStream final : public avro::SeekableInputStream {
public:
    AvroBufferInputStream(std::shared_ptr<RandomAccessFile> file, size_t buffer_size, ScannerCounter* counter)
            : _file(std::move(file)),
              _buffer_size(buffer_size),
              _buffer(new uint8_t[buffer_size]),
              _byte_count(0),
              _next(_buffer),
              _available(0),
              _counter(counter) {}

    ~AvroBufferInputStream() override { delete[] _buffer; }

    bool next(const uint8_t** data, size_t* len) override;
    void backup(size_t len) override;
    void skip(size_t len) override;
    size_t byteCount() const override { return _byte_count; }
    void seek(int64_t position) override;

private:
    bool fill();

    std::shared_ptr<RandomAccessFile> _file;
    const size_t _buffer_size;
    uint8_t* const _buffer;
    size_t _byte_count;
    uint8_t* _next;
    size_t _available;
    ScannerCounter* _counter = nullptr;
};

class AvroReader {
public:
    AvroReader() = default;
    ~AvroReader();

    Status init(std::unique_ptr<avro::InputStream> input_stream);

    Status get_schema(std::vector<SlotDescriptor>* schema);

private:
    std::unique_ptr<avro::DataFileReader<avro::GenericDatum>> _reader = nullptr;
};

using AvroReaderUniquePtr = std::unique_ptr<AvroReader>;

} // namespace starrocks