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

#include <cstdint>

namespace starrocks {
enum PhmapSeed { PhmapSeed1, PhmapSeed2 };

struct CRC_HASH_SEEDS {
    // TODO: 0x811C9DC5 is not prime number
    static const uint32_t CRC_HASH_SEED1 = 0x811C9DC5;
    static const uint32_t CRC_HASH_SEED2 = 0x811C9DD7;
};

template <class T, PhmapSeed seed>
class StdHashWithSeed;

template <PhmapSeed seed>
struct Hash128WithSeed;

template <PhmapSeed seed>
struct Hash256WithSeed;

template <PhmapSeed>
class SliceHashWithSeed;

template <class T>
class StdHash;
} // namespace starrocks