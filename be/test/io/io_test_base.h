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

#include "io/seekable_input_stream.h"
#include "util/random.h"

namespace starrocks::io {

class TestInputStream : public io::SeekableInputStream {
public:
    explicit TestInputStream(std::string contents, int64_t block_size)
            : _contents(std::move(contents)), _block_size(block_size) {}

    StatusOr<int64_t> read(void* data, int64_t count) override {
        count = std::min(count, _block_size);
        count = std::min(count, (int64_t)_contents.size() - _offset);
        memcpy(data, &_contents[_offset], count);
        _offset += count;
        return count;
    }

    Status seek(int64_t position) override {
        _offset = std::min<int64_t>(position, _contents.size());
        return Status::OK();
    }

    StatusOr<int64_t> position() override { return _offset; }

    StatusOr<int64_t> get_size() override { return _contents.size(); }

private:
    std::string _contents;
    int64_t _block_size;
    int64_t _offset{0};
};

static std::string random_string(int len) {
    static starrocks::Random rand(20200722);
    std::string s;
    s.reserve(len);
    for (int i = 0; i < len; i++) {
        s.push_back('a' + (rand.Next() % ('z' - 'a' + 1)));
    }
    return s;
}

} // namespace starrocks::io
