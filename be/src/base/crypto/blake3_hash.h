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

#include <cstddef>
#include <cstdint>

#include "blake3.h"

namespace starrocks {

// Thin C++ wrapper around the BLAKE3 C library.
// BLAKE3 produces a 32-byte (256-bit) digest by default.
class Blake3Hash {
public:
    static constexpr int BLAKE3_HASH_BYTES = BLAKE3_OUT_LEN; // 32

    static void blake3_compute(const unsigned char* message, size_t message_len, uint8_t digest[BLAKE3_OUT_LEN]) {
        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, message, message_len);
        blake3_hasher_finalize(&hasher, digest, BLAKE3_OUT_LEN);
    }
};

} // namespace starrocks
