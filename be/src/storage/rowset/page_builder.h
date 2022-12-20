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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_builder.h

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

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "gutil/macros.h"
#include "runtime/global_dict/types.h"
#include "storage/rowset/common.h"
#include "util/faststring.h"

namespace starrocks {

// PageBuilder is used to build page
// Page is a data management unit, including:
// 1. Data Page: store encoded and compressed data
// 2. BloomFilter Page: store bloom filter of data
// 3. Ordinal Index Page: store ordinal index of data
// 4. Short Key Index Page: store short key index of data
// 5. Bitmap Index Page: store bitmap index of data
class PageBuilder {
public:
    PageBuilder() = default;

    virtual ~PageBuilder() = default;

    // Reserve |head_size| bytes at the head of the buffer returned by `finish` for external usage.
    // The first |head_size| bytes of the buffer may be overwritten after `finish`, so the
    // `PageBuilder` should not write content here.
    // This method can only be called on an *empty* page *once*, and `reset` on the `PageBuilder`
    // does not reset this value.
    virtual void reserve_head(uint8_t head_size) { CHECK(false) << "reserve_head() not supported"; }

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // Return the number of elements actually added, which may less than |count| if the page is full.
    //
    // vals size should be decided according to the page build type
    virtual uint32_t add(const uint8_t* vals, uint32_t count) = 0;

    // Finish building the current page and return the encoded data.
    // NOTE: the returned buffer will be invalidated by `reset`, so
    // do NOT call `reset` until you finished access the returned buffer.
    virtual faststring* finish() = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual faststring* get_dictionary_page() { return nullptr; }

    // check global dict valid for dictionary encoding mode column.
    virtual bool is_valid_global_dict(const GlobalDictMap* global_dict) const { return true; }

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    // The reserved head size keep unchanged.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual uint32_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual uint64_t size() const = 0;

    // Return the first value in this page.
    // This method could only be called between finish() and reset().
    // Status::NotFound if no values have been added.
    virtual Status get_first_value(void* value) const = 0;

    // Return the last value in this page.
    // This method could only be called between finish() and reset().
    // Status::NotFound if no values have been added.
    virtual Status get_last_value(void* value) const = 0;

    // Return true iff all data pages so far are encode by dict encoding.
    // only `BinaryDictPageBuilder` needed to overload this method.
    // this information is used for doing low-cardinality string column read optimization.
    virtual bool all_dict_encoded() const { return false; }

private:
    PageBuilder(const PageBuilder&) = delete;
    const PageBuilder& operator=(const PageBuilder&) = delete;
};

} // namespace starrocks
