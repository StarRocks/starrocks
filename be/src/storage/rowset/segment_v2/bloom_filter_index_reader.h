// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter_index_reader.h

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

#include <map>
#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "runtime/mem_pool.h"
#include "storage/column_block.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/indexed_column_reader.h"
#include "storage/rowset/segment_v2/row_ranges.h"

namespace starrocks {

class TypeInfo;

namespace fs {
class BlockManager;
}

namespace segment_v2 {

class BloomFilterIndexIterator;
class IndexedColumnReader;
class IndexedColumnIterator;
class BloomFilter;

class BloomFilterIndexReader {
    friend class BloomFilterIndexIterator;

public:
    BloomFilterIndexReader() = default;

    Status load(fs::BlockManager* block_mgr, const std::string& file_name,
                const BloomFilterIndexPB* bloom_filter_index_meta, bool use_page_cache, bool kept_in_memory);

    // create a new column iterator.
    Status new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator);

    const TypeInfoPtr& type_info() const { return _typeinfo; }

    size_t mem_usage() const {
        size_t size = sizeof(BloomFilterIndexReader);
        if (_bloom_filter_reader != nullptr) {
            size += _bloom_filter_reader->mem_usage();
        }
        return size;
    }

private:
    TypeInfoPtr _typeinfo;
    BloomFilterAlgorithmPB _algorithm = BLOCK_BLOOM_FILTER;
    HashStrategyPB _hash_strategy = HASH_MURMUR3_X64_64;
    std::unique_ptr<IndexedColumnReader> _bloom_filter_reader;
};

class BloomFilterIndexIterator {
    friend class BloomFilterIndexReader;

public:
    // Read bloom filter at the given ordinal into `bf`.
    Status read_bloom_filter(rowid_t ordinal, std::unique_ptr<BloomFilter>* bf);

private:
    BloomFilterIndexIterator(BloomFilterIndexReader* reader, std::unique_ptr<IndexedColumnIterator> bf_iter)
            : _reader(reader), _bloom_filter_iter(std::move(bf_iter)), _pool(new MemPool()) {}

    BloomFilterIndexReader* _reader;
    std::unique_ptr<IndexedColumnIterator> _bloom_filter_iter;
    std::unique_ptr<MemPool> _pool;
};

} // namespace segment_v2
} // namespace starrocks
