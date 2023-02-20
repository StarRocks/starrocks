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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/io/InputStream.cc

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "InputStream.hh"

#include <algorithm>
#include <iomanip>

#include "orc/Exceptions.hh"

namespace orc {

void printBuffer(std::ostream& out, const char* buffer, uint64_t length) {
    const uint64_t width = 24;
    out << std::hex;
    for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
        out << std::setfill('0') << std::setw(7) << (line * width);
        for (uint64_t byte = 0; byte < width && line * width + byte < length; ++byte) {
            out << " " << std::setfill('0') << std::setw(2)
                << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
        }
        out << "\n";
    }
    out << std::dec;
}

PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
    position = posns.begin();
}

uint64_t PositionProvider::next() {
    uint64_t result = *position;
    ++position;
    return result;
}

uint64_t PositionProvider::current() {
    return *position;
}

PositionProvider& PositionProviderMap::at(uint64_t columnId) {
    return providers.at(columnId);
}

SeekableInputStream::~SeekableInputStream() {
    // PASS
}

SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
}

SeekableArrayInputStream::SeekableArrayInputStream(const unsigned char* values, uint64_t size, uint64_t blkSize)
        : data(reinterpret_cast<const char*>(values)) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

SeekableArrayInputStream::SeekableArrayInputStream(const char* values, uint64_t size, uint64_t blkSize) : data(values) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
}

bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
    uint64_t currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
        *buffer = data + position;
        *size = static_cast<int>(currentSize);
        position += currentSize;
        return true;
    }
    *size = 0;
    return false;
}

void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
        auto unsignedCount = static_cast<uint64_t>(count);
        if (unsignedCount <= blockSize && unsignedCount <= position) {
            position -= unsignedCount;
        } else {
            throw std::logic_error("Can't backup that much!");
        }
    }
}

bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
        auto unsignedCount = static_cast<uint64_t>(count);
        if (unsignedCount + position <= length) {
            position += unsignedCount;
            return true;
        } else {
            position = length;
        }
    }
    return false;
}

google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
}

void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
}

std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position << " of " << length;
    return result.str();
}

static const uint64_t DEFAULT_FILE_BLOCK_SIZE = 256 * 1024;
static uint64_t computeBlock(uint64_t request, uint64_t length) {
    return std::min(length, request == 0 ? DEFAULT_FILE_BLOCK_SIZE : request);
}

SeekableFileInputStream::SeekableFileInputStream(InputStream* stream, uint64_t offset, uint64_t byteCount,
                                                 MemoryPool& _pool, uint64_t _blockSize)
        : pool(_pool), input(stream), start(offset), length(byteCount), blockSize(computeBlock(_blockSize, length)) {
    position = 0;
    buffer = nullptr;
    pushBack = 0;
    hasSeek = false;
}

SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
}

bool SeekableFileInputStream::Next(const void** data, int* size) {
    if (buffer == nullptr) {
        buffer.reset(new DataBuffer<char>(pool, blockSize));
    }
    uint64_t bytesRead;
    if (pushBack != 0) {
        *data = buffer->data() + (buffer->size() - pushBack);
        bytesRead = pushBack;
    } else {
        bytesRead = std::min(length - position, blockSize);
        if (hasSeek) {
            hasSeek = false;
            bytesRead = std::min(bytesRead, input->getNaturalReadSizeAfterSeek());
        }
        if (bytesRead > 0) {
            input->read(buffer->data(), bytesRead, start + position);
            *data = static_cast<void*>(buffer->data());
        }
    }
    position += bytesRead;
    pushBack = 0;
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
}

void SeekableFileInputStream::BackUp(int signedCount) {
    if (signedCount < 0) {
        throw std::logic_error("can't backup negative distances");
    }
    auto count = static_cast<uint64_t>(signedCount);
    if (pushBack > 0) {
        throw std::logic_error("can't backup unless we just called Next");
    }
    if (count > blockSize || count > position) {
        throw std::logic_error("can't backup that far");
    }
    pushBack = static_cast<uint64_t>(count);
    position -= pushBack;
}

bool SeekableFileInputStream::Skip(int signedCount) {
    if (signedCount < 0) {
        return false;
    }
    auto count = static_cast<uint64_t>(signedCount);
    position = std::min(position + count, length);
    pushBack = 0;
    return position < length;
}

int64_t SeekableFileInputStream::ByteCount() const {
    return static_cast<int64_t>(position);
}

void SeekableFileInputStream::seek(PositionProvider& location) {
    position = location.next();
    if (position > length) {
        position = length;
        throw std::logic_error("seek too far");
    }
    pushBack = 0;
    hasSeek = true;
}

std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input->getName() << " from " << start << " for " << length;
    return result.str();
}

} // namespace orc
