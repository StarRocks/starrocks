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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter_index_reader.cpp

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

#include "storage/rowset/bloom_filter_index_reader.h"

#include <bthread/sys_futex.h>

#include <memory>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/types.h"

namespace starrocks {

BloomFilterIndexReader::BloomFilterIndexReader() {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(), sizeof(BloomFilterIndexReader));
}

BloomFilterIndexReader::~BloomFilterIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(), _mem_usage());
}

StatusOr<bool> BloomFilterIndexReader::load(const IndexReadOptions& opts, const BloomFilterIndexPB& meta) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(opts, meta);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                     _mem_usage() - sizeof(BloomFilterIndexReader));
        } else {
            _reset();
        }
        return st;
    });
}

Status BloomFilterIndexReader::_do_load(const IndexReadOptions& opts, const BloomFilterIndexPB& meta) {
    _typeinfo = get_type_info(TYPE_VARCHAR);
    _algorithm = meta.algorithm();
    _hash_strategy = meta.hash_strategy();
    const IndexedColumnMetaPB& bf_index_meta = meta.bloom_filter();
    _bloom_filter_reader = std::make_unique<IndexedColumnReader>(bf_index_meta);
    RETURN_IF_ERROR(_bloom_filter_reader->load(opts));
    return Status::OK();
}

void BloomFilterIndexReader::_reset() {
    _typeinfo.reset();
    _algorithm = BLOCK_BLOOM_FILTER;
    _hash_strategy = HASH_MURMUR3_X64_64;
    _bloom_filter_reader.reset();
}

Status BloomFilterIndexReader::new_iterator(const IndexReadOptions& opts,
                                            std::unique_ptr<BloomFilterIndexIterator>* iterator) {
    std::unique_ptr<IndexedColumnIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_reader->new_iterator(opts, &bf_iter));
    iterator->reset(new BloomFilterIndexIterator(this, std::move(bf_iter)));
    return Status::OK();
}

Status BloomFilterIndexIterator::read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf) {
    auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    RETURN_IF_ERROR(_bloom_filter_iter->seek_to_ordinal(ordinal));
    size_t num_to_read = 1;
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bloom_filter_iter->next_batch(&num_read, column.get()));
    DCHECK(num_to_read == num_read);

    ColumnViewer<TYPE_VARCHAR> viewer(column);
    auto value = viewer.value(0);
    // construct bloom filter
    BloomFilter::create(_reader->_algorithm, bf);

    RETURN_IF_ERROR((*bf)->init(value.data, value.size, _reader->_hash_strategy));
    return Status::OK();
}

} // namespace starrocks
