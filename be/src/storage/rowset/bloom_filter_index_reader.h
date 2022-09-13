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

#include "common/statusor.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/column_block.h"
#include "storage/rowset/common.h"
#include "storage/rowset/indexed_column_reader.h"
#include "util/once.h"

namespace starrocks {

class TypeInfo;

namespace fs {
class BlockManager;
}

class BloomFilterIndexIterator;
class IndexedColumnReader;
class IndexedColumnIterator;
class BloomFilter;

class BloomFilterIndexReader {
    friend class BloomFilterIndexIterator;

public:
    BloomFilterIndexReader() = default;

    // Multiple callers may call this method concurrently, but only the first one
    // can load the data, the others will wait until the first one finished loading
    // data.
    //
    // Return true if the index data was successfully loaded by the caller, false if
    // the data was loaded by another caller.
    StatusOr<bool> load(fs::BlockManager* fs, const std::string& filename, const BloomFilterIndexPB& meta,
                        bool use_page_cache, bool kept_in_memory, MemTracker* mem_tracker);

    // create a new column iterator.
    // REQUIRES: the index data has been successfully `load()`ed into memory.
    Status new_iterator(std::unique_ptr<BloomFilterIndexIterator>* iterator);

    const TypeInfoPtr& type_info() const { return _typeinfo; }

    size_t mem_usage() const {
        size_t size = sizeof(BloomFilterIndexReader);
        if (_bloom_filter_reader != nullptr) {
            size += _bloom_filter_reader->mem_usage();
        }
        return size;
    }

    bool loaded() const { return invoked(_load_once); }

private:
    enum State : int {
        kUnloaded = 0, // data has not been loaded into memory
        kLoading = 1,  // loading in process
        kLoaded = 2,   // data was successfully loaded in memory
    };

    Status _do_load(fs::BlockManager* fs, const std::string& filename, const BloomFilterIndexPB& meta,
                    bool use_page_cache, bool kept_in_memory);

    void _reset();

    OnceFlag _load_once;
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

} // namespace starrocks
