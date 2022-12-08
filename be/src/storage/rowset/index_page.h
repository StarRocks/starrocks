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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/index_page.h

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

#include <cstddef>
#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/macros.h"
#include "storage/rowset/page_pointer.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks {

// IndexPage is the building block for IndexedColumn's ordinal index and value index.
// It is used to guide searching for a particular key to the data page containing it.
// We use the same general format for all index pages, regardless of the data type and node type (leaf or internal)
//   IndexPageBody := IndexEntry^NumEntry
//   IndexEntry := KeyLength(vint), Byte^KeyLength, PageOffset(vlong), PageSize(vint)
//
// IndexPageFooterPB records NumEntry and type (leaf/internal) of the index page.
// For leaf, IndexKey records the first/smallest key of the data page PagePointer points to.
// For internal, IndexKey records the first/smallest key of the next-level index page PagePointer points to.
//
// All keys are treated as binary string and compared with memcpy. Keys of other data type are encoded first by
// KeyCoder, e.g., ordinal index's original key type is uint64_t but is encoded to binary string.
class IndexPageBuilder {
public:
    explicit IndexPageBuilder(size_t index_page_size, bool is_leaf)
            : _index_page_size(index_page_size), _is_leaf(is_leaf) {}
    IndexPageBuilder(const IndexPageBuilder&) = delete;
    const IndexPageBuilder& operator=(const IndexPageBuilder&) = delete;

    void add(const Slice& key, const PagePointer& ptr);

    bool is_full() const;

    size_t count() const { return _count; }

    void finish(OwnedSlice* body, PageFooterPB* footer);

    uint64_t size() { return _buffer.size(); }

    void reset() {
        _finished = false;
        _buffer.clear();
        _count = 0;
    }

private:
    const size_t _index_page_size;
    const bool _is_leaf;
    bool _finished = false;
    faststring _buffer;
    uint32_t _count = 0;
};

class IndexPageIterator;
class IndexPageReader {
public:
    IndexPageReader() = default;

    Status parse(const Slice& body, const IndexPageFooterPB& footer);

    size_t count() const {
        DCHECK(_parsed);
        return _num_entries;
    }

    const Slice& get_key(int idx) const {
        DCHECK(_parsed);
        DCHECK(idx >= 0 && idx < _num_entries);
        return _keys[idx];
    }

    const PagePointer& get_value(int idx) const {
        DCHECK(_parsed);
        DCHECK(idx >= 0 && idx < _num_entries);
        return _values[idx];
    }

    size_t mem_usage() const {
        size_t size = sizeof(IndexPageReader);
        size += _keys.size() * sizeof(Slice) + _values.size() * sizeof(PagePointer);
        return size;
    }

    const std::vector<Slice>& get_keys() const { return _keys; }

private:
    void _reset();
    Status _parse(const Slice& body, const IndexPageFooterPB& footer);

    bool _parsed{false};

    uint32_t _num_entries = 0;

    std::vector<Slice> _keys;
    std::vector<PagePointer> _values;
};

class IndexPageIterator {
public:
    explicit IndexPageIterator(const IndexPageReader* reader) : _reader(reader) {}

    // Find the largest index entry whose key is <= search_key.
    // Return OK status when such entry exists.
    // Return NotFound when no such entry is found (all keys > search_key).
    // Index entry is the start key of page
    // All the key in page is unique
    Status seek_at_or_before(const Slice& search_key);

    // Find the smallest index entry whose key is >= search_key.
    // Index entry is the start key of page
    // All the key in page is unique
    Status seek_at_or_after(const Slice& search_key);

    // Move to the next index entry.
    // Return true on success, false when no more entries can be read.
    bool move_next() {
        _pos++;
        if (_pos >= _reader->count()) {
            return false;
        }
        return true;
    }

    const PagePointer& current_page_pointer() const { return _reader->get_value(_pos); }

private:
    const IndexPageReader* _reader;

    uint32_t _pos = 0;
};

} // namespace starrocks
