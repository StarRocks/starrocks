// This file is made available under Elastic License 2.0.
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
#include "storage/rowset/page_pointer.h"
#include "util/coding.h"
#include "util/once.h"
#include "util/slice.h"

namespace starrocks {

namespace fs {
class BlockManager;
class WritableBlock;
} // namespace fs

// Ordinal index is implemented by one IndexPage that stores the first value ordinal
// and file pointer for each data page.
// But if there is only one data page, there is no need for index page. So we store
// the file pointer to that data page directly in index meta (OrdinalIndexPB).
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() : _page_builder(new IndexPageBuilder(0, true)) {}

    void append_entry(ordinal_t ordinal, const PagePointer& data_pp);

    uint64_t size() { return _page_builder->size(); }

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* meta);

private:
    OrdinalIndexWriter(const OrdinalIndexWriter&) = delete;
    const OrdinalIndexWriter& operator=(const OrdinalIndexWriter&) = delete;
    std::unique_ptr<IndexPageBuilder> _page_builder;
    PagePointer _last_pp;
};

class OrdinalPageIndexIterator;

class OrdinalIndexReader {
public:
    OrdinalIndexReader() : _load_once(), _num_pages(0) {}

    // Multiple callers may call this method concurrently, but only the first one
    // can load the data, the others will wait until the first one finished loading
    // data.
    //
    // Return true if the index data was successfully loaded by the caller, false if
    // the data was loaded by another caller.
    StatusOr<bool> load(fs::BlockManager* fs, const std::string& filename, const OrdinalIndexPB& meta,
                        ordinal_t num_values, bool use_page_cache, bool kept_in_memory, MemTracker* mem_tracker);

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

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    size_t num_rows() const { return _ordinals.back() - _ordinals.front(); }

    size_t mem_usage() const {
        return sizeof(OrdinalIndexReader) + _ordinals.size() * sizeof(ordinal_t) + _pages.size() * sizeof(PagePointer);
    }

    bool loaded() const { return invoked(_load_once); }

private:
    friend OrdinalPageIndexIterator;

    enum State : int {
        kUnloaded = 0, // data has not been loaded into memory
        kLoading = 1,  // loading in process
        kLoaded = 2,   // data was successfully loaded in memory
    };

    Status do_load(fs::BlockManager* fs, const std::string& filename, const OrdinalIndexPB& meta, ordinal_t num_values,
                   bool use_page_cache, bool kept_in_memory, MemTracker* mem_tracker);

    OnceFlag _load_once;
    // valid after load
    int _num_pages;
    // _ordinals[i] = first ordinal of the i-th data page,
    std::vector<ordinal_t> _ordinals;
    // _pages[i] = page pointer to the i-th data page
    std::vector<PagePointer> _pages;
};

class OrdinalPageIndexIterator {
public:
    OrdinalPageIndexIterator() {}
    OrdinalPageIndexIterator(OrdinalIndexReader* index) : _index(index), _cur_idx(0) {}
    OrdinalPageIndexIterator(OrdinalIndexReader* index, int cur_idx) : _index(index), _cur_idx(cur_idx) {}
    bool valid() const { return _cur_idx < _index->_num_pages; }
    void next() {
        DCHECK_LT(_cur_idx, _index->_num_pages);
        _cur_idx++;
    }
    int32_t page_index() const { return _cur_idx; };
    const PagePointer& page() const { return _index->_pages[_cur_idx]; };
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
    return OrdinalPageIndexIterator(this, _num_pages);
}

} // namespace starrocks
