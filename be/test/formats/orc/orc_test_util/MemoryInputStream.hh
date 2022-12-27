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

#include <iostream>
#include <orc/OrcFile.hh>

namespace starrocks {
class MemoryInputStream : public orc::InputStream {
public:
    MemoryInputStream(const char* _buffer, size_t _size)
            : buffer(_buffer), size(_size), naturalReadSize(1024), name("MemoryInputStream") {}

    virtual ~MemoryInputStream() override;

    virtual uint64_t getLength() const override { return size; }

    virtual uint64_t getNaturalReadSize() const override { return naturalReadSize; }

    virtual void read(void* buf, uint64_t length, uint64_t offset) override { memcpy(buf, buffer + offset, length); }

    virtual const std::string& getName() const override { return name; }

    const char* getData() const { return buffer; }

private:
    const char* buffer;
    uint64_t size, naturalReadSize;
    std::string name;
};
} // namespace starrocks
