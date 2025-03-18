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

// The code in this file was modified from https://github.com/artemkin/z85/blob/master/src/z85.c
// Copyright 2013 Stanislav Artemkin <artemkin@gmail.com>.

#include "base85.h"

typedef unsigned char byte;

static byte base256[] = {0x00, 0x44, 0x00, 0x54, 0x53, 0x52, 0x48, 0x00, 0x4B, 0x4C, 0x46, 0x41, 0x00, 0x3F,
                         0x3E, 0x45, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x40, 0x00,
                         0x49, 0x42, 0x4A, 0x47, 0x51, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C,
                         0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A,
                         0x3B, 0x3C, 0x3D, 0x4D, 0x00, 0x4E, 0x43, 0x00, 0x00, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                         0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C,
                         0x1D, 0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23, 0x4F, 0x00, 0x50, 0x00, 0x00};

namespace starrocks {

char* base85_decode_impl(const char* source, const char* sourceEnd, char* dest) {
    byte* src = (byte*)source;
    byte* end = (byte*)sourceEnd;
    byte* dst = (byte*)dest;
    uint32_t value;

    for (; src != end; src += 5, dst += 4) {
        value = base256[(src[0] - 32) & 127];
        value = value * 85 + base256[(src[1] - 32) & 127];
        value = value * 85 + base256[(src[2] - 32) & 127];
        value = value * 85 + base256[(src[3] - 32) & 127];
        value = value * 85 + base256[(src[4] - 32) & 127];

        // pack big-endian frame
        dst[0] = value >> 24;
        dst[1] = (byte)(value >> 16);
        dst[2] = (byte)(value >> 8);
        dst[3] = (byte)(value);
    }

    return (char*)dst;
}

StatusOr<std::string> base85_decode(const std::string& source) {
    if (source.empty()) {
        return Status::RuntimeError("base85 encoded source is empty");
    }

    size_t input_size = source.size();
    if (input_size % 5) {
        return Status::RuntimeError("base85 encoded source size error");
    }

    std::string buf;
    size_t buf_size = input_size * 4 / 5;
    buf.resize(buf_size);
    char* dest = &buf[0];

    const size_t decodedBytes = base85_decode_impl(source.data(), source.data() + input_size, dest) - dest;
    if (decodedBytes == 0) {
        return Status::RuntimeError("base85 decoded failed");
    }

    return buf;
}
} // namespace starrocks