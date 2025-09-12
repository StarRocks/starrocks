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

#include "cache/object_cache/page_cache.h"
#include "storage/rowset/page_handle_fwd.h"

namespace starrocks {

// When a column page is read into memory, we use this to store it.
// A page's data may be in cache, or may not in cache. We use this
// class to unify these two cases.
// If client use this struct to wrap data not in cache, this class
// will free data's memory when it is destroyed.
template <class PageType>
class PageHandleTmpl {
public:
    PageHandleTmpl() = default;

    // This class will take the ownership of input data's memory. It will
    // free it when deconstructs.
    explicit PageHandleTmpl(PageType* data) : _is_data_owner(true), _data(data) {}

    // This class will take the content of cache data, and will make input
    // cache_data to an invalid cache handle.
    explicit PageHandleTmpl(PageCacheHandle&& cache_data) : _cache_data(std::move(cache_data)) {}

    // Move constructor
    PageHandleTmpl(PageHandleTmpl&& other) noexcept : _data(other._data), _cache_data(std::move(other._cache_data)) {
        // we can use std::exchange if we switch c++14 on
        std::swap(_is_data_owner, other._is_data_owner);
    }

    PageHandleTmpl& operator=(PageHandleTmpl&& other) noexcept {
        std::swap(_is_data_owner, other._is_data_owner);
        std::swap(_data, other._data);
        std::swap(_cache_data, other._cache_data);
        return *this;
    }

    ~PageHandleTmpl() {
        if (_is_data_owner) {
            delete _data;
            _data = nullptr;
        }
    }

    void reset() {
        if (_is_data_owner) {
            delete _data;
            _is_data_owner = false;
        }
        _data = nullptr;
    }

    // the return slice contains uncompressed page body, page footer, and footer size
    const PageType* data() const {
        if (_is_data_owner) {
            return _data;
        } else {
            return reinterpret_cast<const PageType*>(_cache_data.data());
        }
    }

    int64_t mem_usage() const {
        if (_is_data_owner) {
            return _data->size();
        } else {
            return 0;
        }
    }

    // Don't allow copy and assign
    PageHandleTmpl(const PageHandleTmpl&) = delete;
    const PageHandleTmpl& operator=(const PageHandleTmpl&) = delete;

private:
    // when this is true, it means this struct own data and _data is valid.
    // otherwise _cache_data is valid, and data is belonged to cache.
    bool _is_data_owner = false;
    PageType* _data = nullptr;
    PageCacheHandle _cache_data;
};

} // namespace starrocks
