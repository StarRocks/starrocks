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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/base64.cpp

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

#include "base/base64.h"

#include <cstdint>

#include "simdutf.h"

namespace starrocks {

size_t base64_encode2(const unsigned char* data, size_t length, unsigned char* encoded_data) {
    if (encoded_data == nullptr || data == nullptr) {
        return 0;
    }
    return simdutf::binary_to_base64(reinterpret_cast<const char*>(data), length, reinterpret_cast<char*>(encoded_data),
                                     simdutf::base64_default);
}

int64_t base64_decode2(const char* data, size_t length, char* decoded_data) {
    if (decoded_data == nullptr || data == nullptr) {
        return -1;
    }
    simdutf::result r = simdutf::base64_to_binary(data, length, decoded_data, simdutf::base64_default,
                                                  simdutf::last_chunk_handling_options::loose);
    if (r.error != simdutf::error_code::SUCCESS) {
        return -1;
    }
    return static_cast<int64_t>(r.count);
}

} // namespace starrocks
