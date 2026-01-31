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
//   https://github.com/Cyan4973/xxHash/blob/dev/xxhash.h

/*
 * xxHash - Extremely Fast Hash algorithm
 * Header File
 * Copyright (C) 2012-2021 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 */

/*!
 * @mainpage xxHash
 *
 * xxHash is an extremely fast non-cryptographic hash algorithm, working at RAM speed
 * limits.
 *
 * It is proposed in four flavors, in three families:
 * 1. @ref XXH32_family
 *   - Classic 32-bit hash function. Simple, compact, and runs on almost all
 *     32-bit and 64-bit systems.
 * 2. @ref XXH64_family
 *   - Classic 64-bit adaptation of XXH32. Just as simple, and runs well on most
 *     64-bit systems (but _not_ 32-bit systems).
 * 3. @ref XXH3_family
 *   - Modern 64-bit and 128-bit hash function family which features improved
 *     strength and performance across the board, especially on smaller data.
 *     It benefits greatly from SIMD and 64-bit without requiring it.
 *
 * Benchmarks
 * ---
 * The reference system uses an Intel i7-9700K CPU, and runs Ubuntu x64 20.04.
 * The open source benchmark program is compiled with clang v10.0 using -O3 flag.
 *
 * | Hash Name            | ISA ext | Width | Large Data Speed | Small Data Velocity |
 * | -------------------- | ------- | ----: | ---------------: | ------------------: |
 * | XXH3_64bits()        | @b AVX2 |    64 |        59.4 GB/s |               133.1 |
 * | MeowHash             | AES-NI  |   128 |        58.2 GB/s |                52.5 |
 * | XXH3_128bits()       | @b AVX2 |   128 |        57.9 GB/s |               118.1 |
 * | CLHash               | PCLMUL  |    64 |        37.1 GB/s |                58.1 |
 * | XXH3_64bits()        | @b SSE2 |    64 |        31.5 GB/s |               133.1 |
 * | XXH3_128bits()       | @b SSE2 |   128 |        29.6 GB/s |               118.1 |
 * | RAM sequential read  |         |   N/A |        28.0 GB/s |                 N/A |
 * | ahash                | AES-NI  |    64 |        22.5 GB/s |               107.2 |
 * | City64               |         |    64 |        22.0 GB/s |                76.6 |
 * | T1ha2                |         |    64 |        22.0 GB/s |                99.0 |
 * | City128              |         |   128 |        21.7 GB/s |                57.7 |
 * | FarmHash             | AES-NI  |    64 |        21.3 GB/s |                71.9 |
 * | XXH64()              |         |    64 |        19.4 GB/s |                71.0 |
 * | SpookyHash           |         |    64 |        19.3 GB/s |                53.2 |
 * | Mum                  |         |    64 |        18.0 GB/s |                67.0 |
 * | CRC32C               | SSE4.2  |    32 |        13.0 GB/s |                57.9 |
 * | XXH32()              |         |    32 |         9.7 GB/s |                71.9 |
 * | City32               |         |    32 |         9.1 GB/s |                66.0 |
 * | Blake3*              | @b AVX2 |   256 |         4.4 GB/s |                 8.1 |
 * | Murmur3              |         |    32 |         3.9 GB/s |                56.1 |
 * | SipHash*             |         |    64 |         3.0 GB/s |                43.2 |
 * | Blake3*              | @b SSE2 |   256 |         2.4 GB/s |                 8.1 |
 * | HighwayHash          |         |    64 |         1.4 GB/s |                 6.0 |
 * | FNV64                |         |    64 |         1.2 GB/s |                62.7 |
 * | Blake2*              |         |   256 |         1.1 GB/s |                 5.1 |
 * | SHA1*                |         |   160 |         0.8 GB/s |                 5.6 |
 * | MD5*                 |         |   128 |         0.6 GB/s |                 7.8 |
 * @note
 *   - Hashes which require a specific ISA extension are noted. SSE2 is also noted,
 *     even though it is mandatory on x64.
 *   - Hashes with an asterisk are cryptographic. Note that MD5 is non-cryptographic
 *     by modern standards.
 *   - Small data velocity is a rough average of algorithm's efficiency for small
 *     data. For more accurate information, see the wiki.
 *   - More benchmarks and strength tests are found on the wiki:
 *         https://github.com/Cyan4973/xxHash/wiki
 *
 * Usage
 * ------
 * All xxHash variants use a similar API. Changing the algorithm is a trivial
 * substitution.
 *
 * @pre
 *    For functions which take an input and length parameter, the following
 *    requirements are assumed:
 *    - The range from [`input`, `input + length`) is valid, readable memory.
 *      - The only exception is if the `length` is `0`, `input` may be `NULL`.
 *    - For C++, the objects must have the *TriviallyCopyable* property, as the
 *      functions access bytes directly as if it was an array of `unsigned char`.
 *
 * @anchor single_shot_example
 * **Single Shot**
 *
 * These functions are stateless functions which hash a contiguous block of memory,
 * immediately returning the result. They are the easiest and usually the fastest
 * option.
 *
 * XXH32(), XXH64(), XXH3_64bits(), XXH3_128bits()
 *
 * @code{.c}
 *   #include <string.h>
 *   #include "xxhash.h"
 *
 *   // Example for a function which hashes a null terminated string with XXH32().
 *   XXH32_hash_t hash_string(const char* string, XXH32_hash_t seed)
 *   {
 *       // NULL pointers are only valid if the length is zero
 *       size_t length = (string == NULL) ? 0 : strlen(string);
 *       return XXH32(string, length, seed);
 *   }
 * @endcode
 *
 * @anchor streaming_example
 * **Streaming**
 *
 * These groups of functions allow incremental hashing of unknown size, even
 * more than what would fit in a size_t.
 *
 * XXH32_reset(), XXH64_reset(), XXH3_64bits_reset(), XXH3_128bits_reset()
 *
 * @code{.c}
 *   #include <stdio.h>
 *   #include <assert.h>
 *   #include "xxhash.h"
 *   // Example for a function which hashes a FILE incrementally with XXH3_64bits().
 *   XXH64_hash_t hashFile(FILE* f)
 *   {
 *       // Allocate a state struct. Do not just use malloc() or new.
 *       XXH3_state_t* state = XXH3_createState();
 *       assert(state != NULL && "Out of memory!");
 *       // Reset the state to start a new hashing session.
 *       XXH3_64bits_reset(state);
 *       char buffer[4096];
 *       size_t count;
 *       // Read the file in chunks
 *       while ((count = fread(buffer, 1, sizeof(buffer), f)) != 0) {
 *           // Run update() as many times as necessary to process the data
 *           XXH3_64bits_update(state, buffer, count);
 *       }
 *       // Retrieve the finalized hash. This will not change the state.
 *       XXH64_hash_t result = XXH3_64bits_digest(state);
 *       // Free the state. Do not use free().
 *       XXH3_freeState(state);
 *       return result;
 *   }
 * @endcode
 *
 * @file xxhash.h
 * xxHash prototypes and implementation
 */

#pragma once

#include "base/hash/xxhash.h"
