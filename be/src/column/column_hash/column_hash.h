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

#include "column/column.h"

namespace starrocks {

// FNV Hash
void fnv_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to);
void fnv_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                    uint16_t to);
void fnv_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size);

// CRC32 Hash
void crc32_hash_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to);
void crc32_hash_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection, uint16_t from,
                                      uint16_t to);
void crc32_hash_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size);

// Murmur Hash
void murmur_hash3_x86_32_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to);
void murmur_hash3_x86_32_column_with_selection(const Column& column, uint32_t* hashes, uint8_t* selection,
                                               uint16_t from, uint16_t to);
void murmur_hash3_x86_32_column_selective(const Column& column, uint32_t* hashes, uint16_t* sel, uint16_t sel_size);

// XXH3
void xxh3_64_column(const Column& column, uint32_t* hashes, uint32_t from, uint32_t to);

} // namespace starrocks
