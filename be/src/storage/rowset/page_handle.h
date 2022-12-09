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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_handle.h

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

#include "gutil/macros.h" // for DISALLOW_COPY
#include "storage/page_cache.h"
#include "util/slice.h"

namespace starrocks {

// When a column page is read into memory, we use this to store it.
// A page's data may be in cache, or may not in cache. We use this
// class to unify these two cases.
// If client use this struct to wrap data not in cache, this class
// will free data's memory when it is destroyed.
class PageHandle {
public:
    PageHandle() = default;

    // This class will take the ownership of input data's memory. It will
    // free it when deconstructs.
    explicit PageHandle(const Slice& data) : _is_data_owner(true), _data(data) {}

    // This class will take the content of cache data, and will make input
    // cache_data to a invalid cache handle.
    explicit PageHandle(PageCacheHandle&& cache_data)
            : _data(static_cast<uint8_t*>(nullptr), 0), _cache_data(std::move(cache_data)) {}

    // Move constructor
    PageHandle(PageHandle&& other) noexcept : _data(other._data), _cache_data(std::move(other._cache_data)) {
        // we can use std::exchange if we switch c++14 on
        std::swap(_is_data_owner, other._is_data_owner);
    }

    PageHandle& operator=(PageHandle&& other) noexcept {
        std::swap(_is_data_owner, other._is_data_owner);
        _data = other._data;
        _cache_data = std::move(other._cache_data);
        return *this;
    }

    ~PageHandle() {
        if (_is_data_owner) {
            delete[] _data.data;
            _data.data = nullptr;
        }
    }

    void reset() {
        if (_is_data_owner) {
            delete[] _data.data;
            _is_data_owner = false;
        }
        _data.clear();
    }

    // the return slice contains uncompressed page body, page footer, and footer size
    Slice data() const {
        if (_is_data_owner) {
            return _data;
        }
        return _cache_data.data();
    }

    int64_t mem_usage() const {
        if (_is_data_owner) {
            return _data.size;
        } else {
            return 0;
        }
    }

private:
    // when this is true, it means this struct own data and _data is valid.
    // otherwise _cache_data is valid, and data is belong to cache.
    bool _is_data_owner = false;
    Slice _data;
    PageCacheHandle _cache_data;

    // Don't allow copy and assign
    PageHandle(const PageHandle&) = delete;
    const PageHandle& operator=(const PageHandle&) = delete;
};

} // namespace starrocks
