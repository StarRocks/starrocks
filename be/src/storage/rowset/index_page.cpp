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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/index_page.cpp

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

#include "storage/rowset/index_page.h"

#include <string>

#include "common/logging.h"
#include "util/coding.h"

namespace starrocks {

void IndexPageBuilder::add(const Slice& key, const PagePointer& ptr) {
    DCHECK(!_finished) << "must reset() after finish() to add new entry";
    put_length_prefixed_slice(&_buffer, key);
    ptr.encode_to(&_buffer);
    _count++;
}

bool IndexPageBuilder::is_full() const {
    // estimate size of IndexPageFooterPB to be 16
    return _buffer.size() + 16 > _index_page_size;
}

void IndexPageBuilder::finish(OwnedSlice* body, PageFooterPB* footer) {
    DCHECK(!_finished) << "already called finish()";
    *body = _buffer.build();

    footer->set_type(INDEX_PAGE);
    footer->set_uncompressed_size(body->slice().get_size());
    footer->mutable_index_page_footer()->set_num_entries(_count);
    footer->mutable_index_page_footer()->set_type(_is_leaf ? IndexPageFooterPB::LEAF : IndexPageFooterPB::INTERNAL);
}

///////////////////////////////////////////////////////////////////////////////

Status IndexPageReader::_parse(const Slice& body, const IndexPageFooterPB& footer) {
    _num_entries = footer.num_entries();

    Slice input(body);
    for (int i = 0; i < _num_entries; ++i) {
        Slice key;
        PagePointer value;
        if (!get_length_prefixed_slice(&input, &key)) {
            return Status::InternalError("Data corruption");
        }
        if (!value.decode_from(&input)) {
            return Status::InternalError("Data corruption");
        }
        _keys.push_back(key);
        _values.push_back(value);
    }

    return Status::OK();
}

Status IndexPageReader::parse(const Slice& body, const IndexPageFooterPB& footer) {
    Status st = _parse(body, footer);
    if (!st.ok()) {
        _reset();
    } else {
        _parsed = true;
    }
    return st;
}

void IndexPageReader::_reset() {
    _parsed = false;
    _num_entries = 0;
    std::vector<Slice>().swap(_keys);
    std::vector<PagePointer>().swap(_values);
}

///////////////////////////////////////////////////////////////////////////////

// This function has the meaning of interval, in fact, it is to find first possible Page which <=search key
Status IndexPageIterator::seek_at_or_before(const Slice& search_key) {
    const auto& keys = _reader->get_keys();
    auto iter = std::upper_bound(keys.begin(), keys.end(), search_key);
    if (iter == keys.begin()) {
        return Status::NotFound("no page contains the given key");
    } else {
        // upper_bound is search the first key > search key, so should return last entry
        _pos = iter - keys.begin() - 1;
        return Status::OK();
    }
}

// This function has the meaning of interval, in fact, it is to find first possible Page which >=search key
Status IndexPageIterator::seek_at_or_after(const Slice& search_key) {
    const auto& keys = _reader->get_keys();
    auto iter = std::upper_bound(keys.begin(), keys.end(), search_key);
    if (iter == keys.begin()) {
        // all the key is larger then search key, so shoud the first entry
        _pos = 0;
    } else {
        // upper_bound is search the first key > search key, so should return last entry
        _pos = iter - keys.begin() - 1;
    }
    return Status::OK();
}

} // namespace starrocks
