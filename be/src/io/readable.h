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

#include "common/statusor.h"

namespace starrocks::io {

// A Readable is a source of bytes. Bytes from a Readable are made available
// to callers of the read method via a byte array.
class Readable {
public:
    virtual ~Readable() = default;

    // Read up to |count| bytes into |data|.
    // Returns the number of bytes read, 0 means EOF.
    virtual StatusOr<int64_t> read(void* data, int64_t count) = 0;

    // Read exactly |count| bytes into |data|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    virtual Status read_fully(void* data, int64_t count);
};

} // namespace starrocks::io
