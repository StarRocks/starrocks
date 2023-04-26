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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/slice.cpp

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

#include "util/slice.h"

#include "util/faststring.h"

namespace starrocks {

static const uint16_t _SLICE_MAX_LENGTH = 65535;
static char _slice_max_value_data[_SLICE_MAX_LENGTH];
static Slice _slice_max_value;
static Slice _slice_min_value;

class SliceInit {
public:
    SliceInit() { Slice::init(); }
};

static SliceInit _slice_init;

// NOTE(zc): we define this function here to make compile work.
Slice::Slice(const faststring& s)
        : // NOLINT(runtime/explicit)
          data((char*)(s.data())),
          size(s.size()) {}

void Slice::init() {
    memset(_slice_max_value_data, 0xff, sizeof(_slice_max_value_data));
    _slice_max_value = Slice(_slice_max_value_data, sizeof(_slice_max_value_data));
    _slice_min_value = Slice(const_cast<char*>(""), 0);
}

const Slice& Slice::max_value() {
    return _slice_max_value;
}

const Slice& Slice::min_value() {
    return _slice_min_value;
}

} // namespace starrocks
