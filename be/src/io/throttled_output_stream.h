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

#include <chrono>
#include <thread>

#include "io/output_stream.h"

// Sleeping is done before writing the stream.
namespace starrocks::io {
class ThrottledOutputStream : public OutputStreamWrapper {
public:
    explicit ThrottledOutputStream(std::unique_ptr<OutputStream> stream, int64_t wait_per_write)
            : OutputStreamWrapper(std::move(stream)), _wait_per_write(wait_per_write) {}

    ~ThrottledOutputStream() = default;

    Status write(const void* data, int64_t size) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(_wait_per_write));
        return OutputStreamWrapper::write(data, size);
    }

private:
    int64_t _wait_per_write;
};
} // namespace starrocks::io