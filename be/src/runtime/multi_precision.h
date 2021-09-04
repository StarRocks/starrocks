// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/multi_precision.h

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

#ifndef STARROCKS_BE_RUNTIME_MULTI_PRECISION_H
#define STARROCKS_BE_RUNTIME_MULTI_PRECISION_H

namespace starrocks {

/// Get the high and low bits of an int128_t
inline uint64_t high_bits(__int128 x) {
    return x >> 64;
}

inline uint64_t low_bits(__int128 x) {
    return x & 0xffffffffffffffff;
}

} // namespace starrocks
#endif
