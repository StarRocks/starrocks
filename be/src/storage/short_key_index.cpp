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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/short_key_index.cpp

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

#include "storage/short_key_index.h"

#include <string>

#include "gutil/strings/substitute.h"
#include "util/coding.h"

using strings::Substitute;

namespace starrocks {

Status ShortKeyIndexBuilder::add_item(const Slice& key) {
    put_varint32(&_offset_buf, _key_buf.size());
    _key_buf.append(key.data, key.size);
    _num_items++;
    return Status::OK();
}

Status ShortKeyIndexBuilder::finalize(uint32_t num_segment_rows, std::vector<Slice>* body, PageFooterPB* page_footer) {
    page_footer->set_type(SHORT_KEY_PAGE);
    page_footer->set_uncompressed_size(_key_buf.size() + _offset_buf.size());

    ShortKeyFooterPB* footer = page_footer->mutable_short_key_page_footer();
    footer->set_num_items(_num_items);
    footer->set_key_bytes(_key_buf.size());
    footer->set_offset_bytes(_offset_buf.size());
    footer->set_segment_id(_segment_id);
    footer->set_num_rows_per_block(_num_rows_per_block);
    footer->set_num_segment_rows(num_segment_rows);

    body->emplace_back(_key_buf);
    body->emplace_back(_offset_buf);
    return Status::OK();
}

Status ShortKeyIndexDecoder::parse(const Slice& body, const ShortKeyFooterPB& footer) {
    _footer = footer;

    // num_items, key_bytes and offset_bytes come straight from a persisted page footer, so an
    // adversarial or corrupt page can hand us values whose sum overflows uint32_t. Do the geometry
    // arithmetic in 64-bit and validate it before it can wrap, rather than after.
    const uint64_t total_bytes =
            static_cast<uint64_t>(_footer.key_bytes()) + static_cast<uint64_t>(_footer.offset_bytes());
    if (total_bytes != body.size) {
        return Status::Corruption(
                strings::Substitute("Index size not match, need=$0, real=$1", total_bytes, body.size));
    }
    // Each offset is encoded as a varint of at least one byte, so num_items varints can never fit in
    // offset_bytes when num_items > offset_bytes. Reject that shape up front, before it can make
    // num_items + 1 wrap and shrink _offsets below what the loop below writes into.
    if (_footer.num_items() > _footer.offset_bytes()) {
        return Status::Corruption(
                strings::Substitute("Short key index num_items exceeds offset_bytes capacity, num_items=$0, "
                                    "offset_bytes=$1",
                                    _footer.num_items(), _footer.offset_bytes()));
    }

    // set index buffer
    _key_data = Slice(body.data, _footer.key_bytes());

    // parse offset information
    Slice offset_slice(body.data + _footer.key_bytes(), _footer.offset_bytes());
    // +1 for record total length. num_items <= offset_bytes <= UINT32_MAX here, so this cannot wrap.
    _offsets.resize(static_cast<size_t>(_footer.num_items()) + 1);
    uint32_t prev_offset = 0;
    for (uint32_t i = 0; i < _footer.num_items(); ++i) {
        uint32_t offset = 0;
        if (!get_varint32(&offset_slice, &offset)) {
            return Status::Corruption("Fail to get varint from index offset buffer");
        }
        // Validate at RUNTIME, not just under DCHECK. A page that passes its CRC but carries a
        // decreasing or out-of-range offset would otherwise make key(i) read out of bounds in a
        // release build, where the DCHECK this replaces compiles away entirely.
        if (offset < prev_offset || offset > _footer.key_bytes()) {
            return Status::Corruption(strings::Substitute(
                    "Short key index offset out of order or out of range, offset=$0, prev=$1, key_bytes=$2", offset,
                    prev_offset, _footer.key_bytes()));
        }
        _offsets[i] = offset;
        prev_offset = offset;
    }
    _offsets[_footer.num_items()] = _footer.key_bytes();

    if (offset_slice.size != 0) {
        return Status::Corruption("Still has data after parse all key offset");
    }
    _parsed = true;
    return Status::OK();
}

} // namespace starrocks
