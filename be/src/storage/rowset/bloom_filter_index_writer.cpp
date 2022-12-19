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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bloom_filter_index_writer.cpp

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

#include "storage/rowset/bloom_filter_index_writer.h"

#include <map>
#include <memory>
#include <utility>

#include "fs/fs.h"
#include "runtime/mem_pool.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/bloom_filter.h" // for BloomFilterOptions, BloomFilter
#include "storage/rowset/common.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/slice.h"

namespace starrocks {

namespace {

template <typename CppType>
struct BloomFilterTraits {
    using ValueDict = std::set<CppType>;
};

template <>
struct BloomFilterTraits<Slice> {
    using ValueDict = std::set<Slice, Slice::Comparator>;
};

// supported slice types are: TYPE_CHAR|TYPE_VARCHAR
template <LogicalType type>
constexpr bool is_slice_type() {
    return type == TYPE_VARCHAR || type == TYPE_CHAR;
}

template <LogicalType type>
constexpr bool is_int128() {
    return type == TYPE_LARGEINT || type == TYPE_DECIMALV2;
}

template <LogicalType type>
inline typename CppTypeTraits<type>::CppType get_value(const typename CppTypeTraits<type>::CppType* v,
                                                       const TypeInfoPtr& type_info, MemPool* pool) {
    using CppType = typename CppTypeTraits<type>::CppType;
    if constexpr (is_slice_type<type>()) {
        CppType new_value;
        type_info->deep_copy(&new_value, v, pool);
        return new_value;
    } else {
        return unaligned_load<CppType>(v);
    }
}

template <LogicalType type>
inline void update_bf(BloomFilter* bf, const typename CppTypeTraits<type>::CppType& v) {
    using CppType = typename CppTypeTraits<type>::CppType;
    if constexpr (is_slice_type<type>()) {
        const auto* s = reinterpret_cast<const Slice*>(&v);
        bf->add_bytes(s->data, s->size);
    } else {
        bf->add_bytes(reinterpret_cast<const char*>(&v), sizeof(CppType));
    }
}

// Builder for bloom filter. In starrocks, bloom filter index is used in
// high cardinality key columns and none-agg value columns for high selectivity and storage
// efficiency.
// This builder builds a bloom filter page by every data page, with a page id index.
// Meanswhile, It adds an ordinal index to load bloom filter index according to requirement.
//
template <LogicalType field_type>
class BloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ValueDict = typename BloomFilterTraits<CppType>::ValueDict;

    explicit BloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options, TypeInfoPtr typeinfo)
            : _bf_options(bf_options), _typeinfo(std::move(typeinfo)) {}

    ~BloomFilterIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        const auto* v = (const CppType*)values;
        for (int i = 0; i < count; ++i) {
            if (_values.find(unaligned_load<CppType>(v)) == _values.end()) {
                _values.insert(get_value<field_type>(v, _typeinfo, &_pool));
            }
            ++v;
        }
    }

    void add_nulls(uint32_t count) override { _has_null |= (count > 0); }

    Status flush() override {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
        RETURN_IF_ERROR(bf->init(_values.size(), _bf_options.fpp, _bf_options.strategy));
        bf->set_has_null(_has_null);
        for (auto& v : _values) {
            update_bf<field_type>(bf.get(), v);
        }
        _bf_buffer_size += bf->size();
        _bfs.push_back(std::move(bf));
        _values.clear();
        return Status::OK();
    }

    Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) override {
        if (!_values.empty()) {
            RETURN_IF_ERROR(flush());
        }
        index_meta->set_type(BLOOM_FILTER_INDEX);
        BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
        meta->set_hash_strategy(_bf_options.strategy);
        meta->set_algorithm(BLOCK_BLOOM_FILTER);

        // write bloom filters
        TypeInfoPtr bf_typeinfo = get_type_info(TYPE_VARCHAR);
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING;
        IndexedColumnWriter bf_writer(options, bf_typeinfo, wfile);
        RETURN_IF_ERROR(bf_writer.init());
        for (auto& bf : _bfs) {
            Slice data(bf->data(), bf->size());
            bf_writer.add(&data);
        }
        RETURN_IF_ERROR(bf_writer.finish(meta->mutable_bloom_filter()));
        return Status::OK();
    }

    uint64_t size() override {
        uint64_t total_size = _bf_buffer_size;
        total_size += _pool.total_allocated_bytes();
        return total_size;
    }

private:
    BloomFilterOptions _bf_options;
    TypeInfoPtr _typeinfo;
    MemPool _pool;
    bool _has_null{false};
    uint64_t _bf_buffer_size{0};
    // distinct values
    ValueDict _values;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

} // namespace

struct BloomFilterBuilderFunctor {
    template <LogicalType ftype>
    Status operator()(std::unique_ptr<BloomFilterIndexWriter>* res, const BloomFilterOptions& bf_options,
                      const TypeInfoPtr& typeinfo) {
        *res = std::make_unique<BloomFilterIndexWriterImpl<ftype>>(bf_options, typeinfo);
        return Status::OK();
    }
};

// TODO currently we don't support bloom filter index for tinyint/hll/float/double
Status BloomFilterIndexWriter::create(const BloomFilterOptions& bf_options, const TypeInfoPtr& typeinfo,
                                      std::unique_ptr<BloomFilterIndexWriter>* res) {
    return field_type_dispatch_bloomfilter(typeinfo->type(), BloomFilterBuilderFunctor(), res, bf_options, typeinfo);
}

} // namespace starrocks
