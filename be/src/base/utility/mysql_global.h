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
//   https://github.com/apache/incubator-doris/blob/master/be/src/base/utility/mysql_global.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cfloat>
#include <cstdint>
#include <cstring>

namespace starrocks {

typedef unsigned char uchar;

#define int1store(T, A) *((uint8_t*)(T)) = (uint8_t)(A)
#define int2store(T, A)               \
    do {                              \
        uint16_t _v = (uint16_t)(A);  \
        memcpy((T), &_v, sizeof(_v)); \
    } while (0)
#define int3store(T, A)                           \
    do {                                          \
        *(T) = (uchar)((A));                      \
        *(T + 1) = (uchar)(((uint32_t)(A) >> 8)); \
        *(T + 2) = (uchar)(((A) >> 16));          \
    } while (0)
#define int4store(T, A)               \
    do {                              \
        uint32_t _v = (uint32_t)(A);  \
        memcpy((T), &_v, sizeof(_v)); \
    } while (0)
#define int8store(T, A)               \
    do {                              \
        uint64_t _v = (uint64_t)(A);  \
        memcpy((T), &_v, sizeof(_v)); \
    } while (0)
#define float4store(T, A)             \
    do {                              \
        float _v = (float)(A);        \
        memcpy((T), &_v, sizeof(_v)); \
    } while (0)
#define float8store(T, A)             \
    do {                              \
        double _v = (double)(A);      \
        memcpy((T), &_v, sizeof(_v)); \
    } while (0)
#define MAX_TINYINT_WIDTH 3  /* Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH 5 /* Max width for a SHORT w.o. sign */
#define MAX_INT_WIDTH 10     /* Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH 20  /* Max width for a LONGLONG */

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH 24 // see gutil/strings/numbers.h kFloatToBufferSize
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH 32 // see gutil/strings/numbers.h kDoubleToBufferSize

} // namespace starrocks
