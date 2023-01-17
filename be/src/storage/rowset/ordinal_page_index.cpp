// This file is made available under Elastic License 2.0.
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

#include <bthread/sys_futex.h>

#include "common/logging.h"
#include "env/env.h"
#include "storage/fs/fs_util.h"
#include "storage/key_coder.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_io.h"

namespace starrocks {

void OrdinalIndexWriter::append_entry(ordinal_t ordinal, const PagePointer& data_pp) {
    std::string key;
    KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&ordinal, &key);
    _page_builder->add(key, data_pp);
    _last_pp = data_pp;
}

Status OrdinalIndexWriter::finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* meta) {
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
        RETURN_IF_ERROR(PageIO::write_page(wblock, {page_body.slice()}, page_footer, &pp));

        root_page_meta->set_is_root_data_page(false);
        pp.to_proto(root_page_meta->mutable_root_page());
    }
    return Status::OK();
}

StatusOr<bool> OrdinalIndexReader::load(fs::BlockManager* fs, const std::string& filename, const OrdinalIndexPB& meta,
                                        ordinal_t num_values, bool use_page_cache, bool kept_in_memory,
                                        MemTracker* mem_tracker) {
    return success_once(_load_once, [&]() {
        return do_load(fs, filename, meta, num_values, use_page_cache, kept_in_memory, mem_tracker);
    });
}

Status OrdinalIndexReader::do_load(fs::BlockManager* fs, const std::string& filename, const OrdinalIndexPB& meta,
                                   ordinal_t num_values, bool use_page_cache, bool kept_in_memory,
                                   MemTracker* mem_tracker) {
    const auto old_mem_usage = mem_usage();
    if (meta.root_page().is_root_data_page()) {
        // only one data page, no index page
        _num_pages = 1;
        _ordinals.push_back(0);
        _ordinals.push_back(num_values);
        _pages.emplace_back(meta.root_page().root_page());
        return Status::OK();
    }
    // need to read index page
    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(fs->open_block(filename, &rblock));

    PageReadOptions opts;
    opts.rblock = rblock.get();
    opts.page_pointer = PagePointer(meta.root_page().root_page());
    opts.codec = nullptr; // ordinal index page uses NO_COMPRESSION right now
    OlapReaderStatistics tmp_stats;
    opts.stats = &tmp_stats;
    opts.use_page_cache = use_page_cache;
    opts.kept_in_memory = kept_in_memory;

    // read index page
    PageHandle page_handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &page_handle, &body, &footer));

    // parse and save all (ordinal, pp) from index page
    IndexPageReader reader;
    RETURN_IF_ERROR(reader.parse(body, footer.index_page_footer()));

    _num_pages = reader.count();
    _ordinals.resize(_num_pages + 1);
    _pages.resize(_num_pages);
    for (int i = 0; i < _num_pages; i++) {
        Slice key = reader.get_key(i);
        ordinal_t ordinal = 0;
        RETURN_IF_ERROR(KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::decode_ascending(&key, sizeof(ordinal_t),
                                                                                          (uint8_t*)&ordinal, nullptr));

        _ordinals[i] = ordinal;
        _pages[i] = reader.get_value(i);
    }
    _ordinals[_num_pages] = num_values;
    const auto new_mem_usage = mem_usage();
    mem_tracker->consume(new_mem_usage - old_mem_usage);
    return Status::OK();
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
        return OrdinalPageIndexIterator(this, _num_pages);
    }
    return OrdinalPageIndexIterator(this, left);
}

} // namespace starrocks
