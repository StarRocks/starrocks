// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/segment_v2/bloom_filter_index_writer.h"

#include <map>
#include <memory>
#include <utility>

#include "env/env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/fs/block_manager.h"
#include "storage/rowset/segment_v2/bloom_filter.h" // for BloomFilterOptions, BloomFilter
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/encoding_info.h"
#include "storage/rowset/segment_v2/indexed_column_writer.h"
#include "storage/types.h"
#include "util/slice.h"

namespace starrocks::segment_v2 {

namespace {

template <typename CppType>
struct BloomFilterTraits {
    using ValueDict = std::set<CppType>;
};

template <>
struct BloomFilterTraits<Slice> {
    using ValueDict = std::set<Slice, Slice::Comparator>;
};

// supported slice types are: OLAP_FIELD_TYPE_CHAR|OLAP_FIELD_TYPE_VARCHAR
template <FieldType type>
constexpr bool is_slice_type() {
    return type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_CHAR;
}

template <FieldType type>
constexpr bool is_int128() {
    return type == OLAP_FIELD_TYPE_LARGEINT || type == OLAP_FIELD_TYPE_DECIMAL_V2;
}

template <FieldType type>
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

template <FieldType type>
inline void update_bf(BloomFilter* bf, const typename CppTypeTraits<type>::CppType& v) {
    using CppType = typename CppTypeTraits<type>::CppType;
    if constexpr (is_slice_type<type>()) {
        const Slice* s = reinterpret_cast<const Slice*>(&v);
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
template <FieldType field_type>
class BloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using ValueDict = typename BloomFilterTraits<CppType>::ValueDict;

    explicit BloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options, TypeInfoPtr typeinfo)
            : _bf_options(bf_options),
              _typeinfo(std::move(typeinfo)),

              _pool(&_tracker),
              _has_null(false),
              _bf_buffer_size(0) {}

    ~BloomFilterIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        const CppType* v = (const CppType*)values;
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

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta) override {
        if (!_values.empty()) {
            RETURN_IF_ERROR(flush());
        }
        index_meta->set_type(BLOOM_FILTER_INDEX);
        BloomFilterIndexPB* meta = index_meta->mutable_bloom_filter_index();
        meta->set_hash_strategy(_bf_options.strategy);
        meta->set_algorithm(BLOCK_BLOOM_FILTER);

        // write bloom filters
        TypeInfoPtr bf_typeinfo = get_type_info(OLAP_FIELD_TYPE_VARCHAR);
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING;
        IndexedColumnWriter bf_writer(options, bf_typeinfo, wblock);
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
    MemTracker _tracker;
    MemPool _pool;
    bool _has_null;
    uint64_t _bf_buffer_size;
    // distinct values
    ValueDict _values;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

} // namespace

// TODO currently we don't support bloom filter index for tinyint/hll/float/double
Status BloomFilterIndexWriter::create(const BloomFilterOptions& bf_options, const TypeInfoPtr& typeinfo,
                                      std::unique_ptr<BloomFilterIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
    case OLAP_FIELD_TYPE_SMALLINT:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_SMALLINT>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_INT:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_INT>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_BIGINT:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_BIGINT>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_LARGEINT:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_LARGEINT>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_CHAR:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_CHAR>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_VARCHAR:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_VARCHAR>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DATE:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DATE>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DATE_V2:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DATE_V2>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DATETIME:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DATETIME>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_TIMESTAMP:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_TIMESTAMP>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DECIMAL:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL_V2>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DECIMAL32:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL32>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DECIMAL64:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL64>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_DECIMAL128:
        *res = std::make_unique<BloomFilterIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL128>>(bf_options, typeinfo);
        break;
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
    case OLAP_FIELD_TYPE_FLOAT:
    case OLAP_FIELD_TYPE_DOUBLE:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_PERCENTILE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return Status::NotSupported("unsupported type for bloom filter: " + std::to_string(type));
    }
    return Status::OK();
}

} // namespace starrocks::segment_v2
