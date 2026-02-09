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

#include "column/column_hash/column_hash.h"

namespace starrocks {

void fnv_hash_column(const Column&, uint32_t*, uint32_t, uint32_t) {}
void fnv_hash_column_with_selection(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t) {}
void fnv_hash_column_selective(const Column&, uint32_t*, uint16_t*, uint16_t) {}

void crc32_hash_column(const Column&, uint32_t*, uint32_t, uint32_t) {}
void crc32_hash_column_with_selection(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t) {}
void crc32_hash_column_selective(const Column&, uint32_t*, uint16_t*, uint16_t) {}

void murmur_hash3_x86_32_column(const Column&, uint32_t*, uint32_t, uint32_t) {}
void murmur_hash3_x86_32_column_with_selection(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t) {}
void murmur_hash3_x86_32_column_selective(const Column&, uint32_t*, uint16_t*, uint16_t) {}

void xxh3_64_column(const Column&, uint32_t*, uint32_t, uint32_t) {}
void xxh3_64_column_with_selection(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t) {}
void xxh3_64_column_selective(const Column&, uint32_t*, uint16_t*, uint16_t) {}

} // namespace starrocks
