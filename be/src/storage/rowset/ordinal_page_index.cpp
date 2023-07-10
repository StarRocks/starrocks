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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/ordinal_page_index.cpp

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

#include "storage/rowset/ordinal_page_index.h"

#include <memory>

#include "common/logging.h"
#include "fs/fs.h"
#include "storage/key_coder.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_io.h"

namespace starrocks {

void OrdinalIndexWriter::append_entry(ordinal_t ordinal, const PagePointer& data_pp) {
    std::string key;
    KeyCoderTraits<TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&ordinal, &key);
    _page_builder->add(key, data_pp);
    _last_pp = data_pp;
}

Status OrdinalIndexWriter::finish(WritableFile* wfile, ColumnIndexMetaPB* meta) {
    meta->set_type(ORDINAL_INDEX);
    BTreeMetaPB* root_page_meta = meta->mutable_ordinal_index()->mutable_root_page();

    // NOTE: It is possible that the count is zero.
    if (_page_builder->count() <= 1) {
        // only one data page, no need to write index page
        root_page_meta->set_is_root_data_page(true);
        _last_pp.to_proto(root_page_meta->mutable_root_page());
    } else {
        OwnedSlice page_body;
        PageFooterPB page_footer;
        _page_builder->finish(&page_body, &page_footer);

        // write index page (currently it's not compressed)
        PagePointer pp;
        RETURN_IF_ERROR(PageIO::write_page(wfile, {page_body.slice()}, page_footer, &pp));

        root_page_meta->set_is_root_data_page(false);
        pp.to_proto(root_page_meta->mutable_root_page());
    }
    return Status::OK();
}

OrdinalIndexReader::OrdinalIndexReader() {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->ordinal_index_mem_tracker(), sizeof(OrdinalIndexReader));
}

OrdinalIndexReader::~OrdinalIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->ordinal_index_mem_tracker(), _mem_usage());
}

StatusOr<bool> OrdinalIndexReader::load(const IndexReadOptions& opts, const OrdinalIndexPB& meta,
                                        ordinal_t num_values) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(opts, meta, num_values);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->ordinal_index_mem_tracker(),
                                     _mem_usage() - sizeof(OrdinalIndexReader))
        } else {
            _reset();
        }
        return st;
    });
}

Status OrdinalIndexReader::_do_load(const IndexReadOptions& opts, const OrdinalIndexPB& meta, ordinal_t num_values) {
    if (meta.root_page().is_root_data_page()) {
        // only one data page, no index page
        _num_pages = 1;

        _ordinals = std::make_unique<ordinal_t[]>(2);
        _ordinals[0] = 0;
        _ordinals[1] = num_values;

        _pages = std::make_unique<uint64_t[]>(2);
        _pages[0] = meta.root_page().root_page().offset();
        _pages[1] = meta.root_page().root_page().offset() + meta.root_page().root_page().size();
        return Status::OK();
    }

    PageReadOptions page_opts;
    page_opts.read_file = opts.read_file;
    page_opts.page_pointer = PagePointer(meta.root_page().root_page());
    page_opts.codec = nullptr; // ordinal index page uses NO_COMPRESSION right now
    page_opts.stats = opts.stats;
    page_opts.use_page_cache = opts.use_page_cache;
    page_opts.kept_in_memory = opts.kept_in_memory;

    // read index page
    PageHandle page_handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(PageIO::read_and_decompress_page(page_opts, &page_handle, &body, &footer));

    // parse and save all (ordinal, pp) from index page
    IndexPageReader reader;
    RETURN_IF_ERROR(reader.parse(body, footer.index_page_footer()));

    _num_pages = reader.count();
    _ordinals = std::make_unique<ordinal_t[]>(_num_pages + 1);
    _pages = std::make_unique<uint64_t[]>(_num_pages + 1);
    for (int i = 0; i < _num_pages; i++) {
        Slice key = reader.get_key(i);
        ordinal_t ordinal = 0;
        RETURN_IF_ERROR(KeyCoderTraits<TYPE_UNSIGNED_BIGINT>::decode_ascending(&key, sizeof(ordinal_t),
                                                                               (uint8_t*)&ordinal, nullptr));

        _ordinals[i] = ordinal;
        _pages[i] = reader.get_value(i).offset;
    }
    _ordinals[_num_pages] = num_values;
    _pages[_num_pages] = reader.get_value(_num_pages - 1).offset + reader.get_value(_num_pages - 1).size;
    return Status::OK();
}

void OrdinalIndexReader::_reset() {
    _num_pages = 0;
    _ordinals.reset();
    _pages.reset();
}

OrdinalPageIndexIterator OrdinalIndexReader::seek_at_or_before(ordinal_t ordinal) {
    int32_t left = 0;
    int32_t right = _num_pages - 1;
    while (left < right) {
        int32_t mid = (left + right + 1) / 2;

        if (_ordinals[mid] < ordinal) {
            left = mid;
        } else if (_ordinals[mid] > ordinal) {
            right = mid - 1;
        } else {
            left = mid;
            break;
        }
    }
    if (_ordinals[left] > ordinal) {
        return {this, _num_pages};
    }
    return {this, left};
}

} // namespace starrocks
