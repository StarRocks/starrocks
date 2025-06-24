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

#include "fs/fs.h"

namespace starrocks {

class BundleSeekableInputStream final : public io::SeekableInputStream {
public:
    explicit BundleSeekableInputStream(std::shared_ptr<SeekableInputStream> stream, int64_t offset, int64_t size)
            : _stream(std::move(stream)), _offset(offset), _size(size) {}

    Status init();
    Status seek(int64_t position) override;
    StatusOr<int64_t> position() override;
    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override;
    Status read_at_fully(int64_t offset, void* out, int64_t count) override;
    StatusOr<int64_t> get_size() override;
    Status skip(int64_t count) override;
    StatusOr<std::string> read_all() override;
    const std::string& filename() const override;
    StatusOr<int64_t> read(void* data, int64_t count) override;
    Status touch_cache(int64_t offset, size_t length) override;
    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override;

private:
    std::shared_ptr<io::SeekableInputStream> _stream;
    int64_t _offset = 0;
    int64_t _size = 0;
};

} // namespace starrocks
