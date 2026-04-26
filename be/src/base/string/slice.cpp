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

#include "base/string/slice.h"

#include "base/string/faststring.h"

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

int64_t Slice::find(const char& c, const int64_t& offset) const {
    int64_t pos = offset;
    while (0 <= pos && pos < size && data[pos] != c) ++pos;
    if (pos < 0 || pos >= size) return -1;
    return pos;
}

int64_t Slice::find(const Slice& pattern, const std::vector<size_t>& next, const int64_t& offset) const {
    if (pattern.size <= 0 || pattern.size > size) return -1;
    if (offset < 0 || offset > size) return -1;
    if (next.size() != pattern.size) return -1;

    int64_t pos = offset;
    int64_t j = 0;

    while (pos < size) {
        if (data[pos] == pattern[j]) {
            ++pos;
            ++j;
        }

        if (j == pattern.size) {
            return pos - j;
        }

        if (pos < size && data[pos] != pattern[j]) {
            if (j != 0) {
                j = next[j - 1];
            } else {
                ++pos;
            }
        }
    }
    return -1;
}

std::vector<size_t> Slice::build_next() const {
    if (size <= 0) return {};

    std::vector<size_t> next(size);

    next[0] = 0;
    size_t i = 1;
    size_t j = 0;

    while (i < size) {
        if (data[i] == data[j]) {
            next[i++] = ++j;
        } else {
            if (j != 0) {
                j = next[j - 1];
            } else {
                next[i++] = 0;
            }
        }
    }
    return next;
}

} // namespace starrocks
