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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitshuffle_page.cpp

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

#include "storage/rowset/bitshuffle_page.h"

#include "gutil/strings/substitute.h"
#include "storage/rowset/common.h"

namespace starrocks {

std::string bitshuffle_error_msg(int64_t err) {
    switch (err) {
    case -1:
        return "Failed to allocate memory";
    case -11:
        return "Missing SSE";
    case -12:
        return "Missing AVX";
    case -80:
        return "Input size not a multiple of 8";
    case -81:
        return "block_size not multiple of 8";
    case -91:
        return "Decompression error, wrong number of bytes processed";
    default:
        return strings::Substitute("Error internal to compression routine with error code $0", err);
    }
}

} // namespace starrocks
