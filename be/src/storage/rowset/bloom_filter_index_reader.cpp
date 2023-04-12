// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/bloom_filter.h"
#include "storage/types.h"

namespace starrocks {

BloomFilterIndexReader::BloomFilterIndexReader() {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(), sizeof(BloomFilterIndexReader));
}

BloomFilterIndexReader::~BloomFilterIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(), _mem_usage());
}

StatusOr<bool> BloomFilterIndexReader::load(FileSystem* fs, const std::string& filename, const BloomFilterIndexPB& meta,
                                            bool use_page_cache, bool kept_in_memory) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(fs, filename, meta, use_page_cache, kept_in_memory);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                     _mem_usage() - sizeof(BloomFilterIndexReader));
        } else {
            _reset();
        }
        return st;
    });
}

Status BloomFilterIndexReader::_do_load(FileSystem* fs, const std::string& filename, const BloomFilterIndexPB& meta,
                                        bool use_page_cache, bool kept_in_memory) {
    _typeinfo = get_type_info(OLAP_FIELD_TYPE_VARCHAR);
    _algorithm = meta.algorithm();
    _hash_strategy = meta.hash_strategy();
    const IndexedColumnMetaPB& bf_index_meta = meta.bloom_filter();
    _bloom_filter_reader = std::make_unique<IndexedColumnReader>(fs, filename, bf_index_meta);
    RETURN_IF_ERROR(_bloom_filter_reader->load(use_page_cache, kept_in_memory));
    return Status::OK();
}

void BloomFilterIndexReader::_reset() {
    _typeinfo.reset();
    _algorithm = BLOCK_BLOOM_FILTER;
    _hash_strategy = HASH_MURMUR3_X64_64;
    _bloom_filter_reader.reset();
}

Status BloomFilterIndexReader::new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator) {
    std::unique_ptr<IndexedColumnIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_reader->new_iterator(&bf_iter));
    iterator->reset(new BloomFilterIndexIterator(this, std::move(bf_iter)));
    return Status::OK();
}

Status BloomFilterIndexIterator::read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf) {
    size_t num_to_read = 1;
    std::unique_ptr<ColumnVectorBatch> cvb;
    RETURN_IF_ERROR(ColumnVectorBatch::create(num_to_read, false, _reader->type_info(), nullptr, &cvb));
    ColumnBlock block(cvb.get(), _pool.get());
    ColumnBlockView column_block_view(&block);

    RETURN_IF_ERROR(_bloom_filter_iter->seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bloom_filter_iter->next_batch(&num_read, &column_block_view));
    DCHECK(num_to_read == num_read);
    // construct bloom filter
    BloomFilter::create(_reader->_algorithm, bf);
    const auto* value_ptr = reinterpret_cast<const Slice*>(block.data());
    RETURN_IF_ERROR((*bf)->init(value_ptr->data, value_ptr->size, _reader->_hash_strategy));
    _pool->clear();
    return Status::OK();
}

} // namespace starrocks
