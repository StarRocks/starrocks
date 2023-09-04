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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/ordinal_page_index.h

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
#include <memory>
#include <string>

#include "common/status.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/common.h"
#include "storage/rowset/index_page.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_pointer.h"
#include "util/coding.h"
#include "util/once.h"
#include "util/slice.h"

namespace starrocks {

class FileSystem;
class WritableFile;

// Ordinal index is implemented by one IndexPage that stores the first value ordinal
// and file pointer for each data page.
// But if there is only one data page, there is no need for index page. So we store
// the file pointer to that data page directly in index meta (OrdinalIndexPB).
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() : _page_builder(new IndexPageBuilder(0, true)) {}
    OrdinalIndexWriter(const OrdinalIndexWriter&) = delete;
    const OrdinalIndexWriter& operator=(const OrdinalIndexWriter&) = delete;

    void append_entry(ordinal_t ordinal, const PagePointer& data_pp);

    uint64_t size() { return _page_builder->size(); }

    Status finish(WritableFile* wfile, ColumnIndexMetaPB* meta);

private:
    std::unique_ptr<IndexPageBuilder> _page_builder;
    PagePointer _last_pp;
};

class OrdinalPageIndexIterator;

class OrdinalIndexReader {
public:
    OrdinalIndexReader();
    ~OrdinalIndexReader();

    // Multiple callers may call this method concurrently, but only the first one
    // can load the data, the others will wait until the first one finished loading
    // data.
    //
    // Return true if the index data was successfully loaded by the caller, false if
    // the data was loaded by another caller.
    StatusOr<bool> load(const IndexReadOptions& opts, const OrdinalIndexPB& meta, ordinal_t num_values);

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    OrdinalPageIndexIterator seek_at_or_before(ordinal_t ordinal);

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    OrdinalPageIndexIterator begin();

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    OrdinalPageIndexIterator end();

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    ordinal_t get_first_ordinal(int page_index) const { return _ordinals[page_index]; }

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    ordinal_t get_last_ordinal(int page_index) const { return get_first_ordinal(page_index + 1) - 1; }

    // for test
    // REQUIRES: the index data has been successfully `load()`ed into memory.
    int32_t num_data_pages() const { return _num_pages; }

    bool loaded() const { return invoked(_load_once); }

private:
    friend OrdinalPageIndexIterator;

    void _reset();

    size_t _mem_usage() const {
        if (_num_pages == 0) {
            return sizeof(OrdinalIndexReader);
        } else {
            return sizeof(OrdinalIndexReader) + (_num_pages + 1) * sizeof(ordinal_t) +
                   (_num_pages + 1) * sizeof(uint64_t);
        }
    }

    Status _do_load(const IndexReadOptions& opts, const OrdinalIndexPB& meta, ordinal_t num_values);

    OnceFlag _load_once;
    // valid after load
    int _num_pages = 0;
    // _ordinals[i] = first ordinal of the i-th data page,
    std::unique_ptr<ordinal_t[]> _ordinals;
    // _pages[i] = page pointer to offset of the i-th data page
    std::unique_ptr<uint64_t[]> _pages;
};

class OrdinalPageIndexIterator {
public:
    OrdinalPageIndexIterator() = default;
    explicit OrdinalPageIndexIterator(OrdinalIndexReader* index) : _index(index), _cur_idx(0) {}
    OrdinalPageIndexIterator(OrdinalIndexReader* index, int cur_idx) : _index(index), _cur_idx(cur_idx) {}
    bool valid() const { return _cur_idx < _index->_num_pages; }
    void next() {
        DCHECK_LT(_cur_idx, _index->_num_pages);
        _cur_idx++;
    }
    int32_t page_index() const { return _cur_idx; };
    PagePointer page() const {
        return {_index->_pages[_cur_idx],
                static_cast<uint32_t>(_index->_pages[_cur_idx + 1] - _index->_pages[_cur_idx])};
    };
    ordinal_t first_ordinal() const { return _index->get_first_ordinal(_cur_idx); }
    ordinal_t last_ordinal() const { return _index->get_last_ordinal(_cur_idx); }

private:
    OrdinalIndexReader* _index{nullptr};
    int32_t _cur_idx{-1};
};

inline OrdinalPageIndexIterator OrdinalIndexReader::begin() {
    return OrdinalPageIndexIterator(this);
}

inline OrdinalPageIndexIterator OrdinalIndexReader::end() {
    return {this, _num_pages};
}

} // namespace starrocks
