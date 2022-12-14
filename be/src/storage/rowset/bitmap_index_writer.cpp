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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitmap_index_writer.cpp

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

#include "storage/rowset/bitmap_index_writer.h"

#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <utility>

#include "fs/fs.h"
#include "runtime/mem_pool.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/common.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks {

namespace {

class BitmapUpdateContext {
    static const size_t estimate_size_threshold = 1024;

public:
    explicit BitmapUpdateContext(rowid_t rid) : _roaring(Roaring::bitmapOf(1, rid)){};

    Roaring* roaring() { return &_roaring; }

    static uint64_t estimate_size(int element_count) {
        // When _element_count is less than estimate_size_threshold, we use
        // (1 + _element_count + 1) * (sizeof(uint32_t)) to approximately estimate true size of roaring bitmap:
        // one bit pre    4 bytes         4 bytes *  _element_count
        // [ 1            cardinality      data ]
        return (1 + sizeof(uint32_t) * (element_count + 1));
    }

    static void init_estimate_size(uint64_t* reverted_index_size) {
        *reverted_index_size += BitmapUpdateContext::estimate_size(1);
    }

    // When _element_count is less than estimate_size_threshold, update the estimate size
    // When _element_count equals to estimate_size_threshold, clear previous estimate size, disable estimation.
    // When _element_count is larger than estimate_size_threshold, use `getSizeInBytes(false)` to get
    // the exact size of roaring bitmap. For efficiency, we will not update the roaring's size each time when size changed.
    // We will save the sized changed roaring bitmap in _late_update_context_vector, and delay calculation of update size
    // each time when `size()` of bitmap is called.
    // Return value in this function indicates whether this BitmapUpdateContext needs to be added to the _late_update_context_vector
    bool update_estimate_size(uint64_t* reverted_index_size) {
        bool need_add = false;
        _element_count++;
        if (_element_count < estimate_size_threshold) {
            *reverted_index_size += sizeof(uint32_t);
        } else if (_element_count == estimate_size_threshold) {
            *reverted_index_size -= BitmapUpdateContext::estimate_size(_element_count);
            _size_changed = true;
            need_add = true;
        } else {
            // Add BitmapUpdateContext to _late_update_context_vector iff
            // it hash not been added to _late_update_context_vector before.
            if (!_size_changed) {
                need_add = true;
            }
            _size_changed = true;
        }
        return need_add;
    }

    void late_update_size(uint64_t* reverted_index_size) {
        uint64_t current_size = _roaring.getSizeInBytes(false);
        *reverted_index_size += (current_size - _previous_size);
        _previous_size = current_size;
        _size_changed = false;
    }

private:
    Roaring _roaring;
    uint64_t _previous_size{0};
    uint32_t _element_count{1};
    bool _size_changed{false};
};

template <typename CppType>
struct BitmapIndexTraits {
    using MemoryIndexType = std::map<CppType, std::unique_ptr<BitmapUpdateContext>>;
};

template <>
struct BitmapIndexTraits<Slice> {
    using MemoryIndexType = std::map<Slice, std::unique_ptr<BitmapUpdateContext>, Slice::Comparator>;
};

// Builder for bitmap index. Bitmap index is comprised of two parts
// - an "ordered dictionary" which contains all distinct values of a column and maps each value to an id.
//   the smallest value mapped to 0, second value mapped to 1, ..
// - a posting list which stores one bitmap for each value in the dictionary. each bitmap is used to represent
//   the list of rowid where a particular value exists.
//
// E.g, if the column contains 10 rows ['x', 'x', 'x', 'b', 'b', 'b', 'x', 'b', 'b', 'b'],
// then the ordered dictionary would be ['b', 'x'] which maps 'b' to 0 and 'x' to 1,
// and the posting list would contain two bitmaps
//   bitmap for ID 0 : [0 0 0 1 1 1 0 1 1 1]
//   bitmap for ID 1 : [1 1 1 0 0 0 1 0 0 0]
//   the n-th bit is set to 1 if the n-th row equals to the corresponding value.
//
template <LogicalType field_type>
class BitmapIndexWriterImpl : public BitmapIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using MemoryIndexType = typename BitmapIndexTraits<CppType>::MemoryIndexType;

    explicit BitmapIndexWriterImpl(TypeInfoPtr type_info) : _typeinfo(std::move(type_info)) {}

    ~BitmapIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            const CppType& value = unaligned_load<CppType>(p);
            auto it = _mem_index.find(value);
            if (it != _mem_index.end()) {
                it->second->roaring()->add(_rid);
                if (it->second->update_estimate_size(&_reverted_index_size)) {
                    _late_update_context_vector.push_back(it->second.get());
                }
            } else {
                // new value, copy value and insert new key->bitmap pair
                CppType new_value;
                _typeinfo->deep_copy(&new_value, &value, &_pool);
                _mem_index.emplace(new_value, std::make_unique<BitmapUpdateContext>(_rid));
                BitmapUpdateContext::init_estimate_size(&_reverted_index_size);
            }
            _rid++;
            p++;
        }
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) override {
        index_meta->set_type(BITMAP_INDEX);
        BitmapIndexPB* meta = index_meta->mutable_bitmap_index();

        meta->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP);
        meta->set_has_null(!_null_bitmap.isEmpty());

        { // write dictionary
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = false;
            options.write_value_index = true;
            options.encoding = EncodingInfo::get_default_encoding(_typeinfo->type(), true);
            options.compression = CompressionTypePB::LZ4;

            IndexedColumnWriter dict_column_writer(options, _typeinfo, wfile);
            RETURN_IF_ERROR(dict_column_writer.init());
            for (auto const& it : _mem_index) {
                RETURN_IF_ERROR(dict_column_writer.add(&(it.first)));
            }
            RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_dict_column()));
        }
        { // write bitmaps
            std::vector<Roaring*> bitmaps;
            for (auto& it : _mem_index) {
                bitmaps.push_back(it.second->roaring());
            }
            if (!_null_bitmap.isEmpty()) {
                bitmaps.push_back(&_null_bitmap);
            }

            uint32_t max_bitmap_size = 0;
            std::vector<uint32_t> bitmap_sizes;
            for (auto& bitmap : bitmaps) {
                bitmap->runOptimize();
                uint32_t bitmap_size = bitmap->getSizeInBytes(false);
                if (max_bitmap_size < bitmap_size) {
                    max_bitmap_size = bitmap_size;
                }
                bitmap_sizes.push_back(bitmap_size);
            }

            TypeInfoPtr bitmap_typeinfo = get_type_info(TYPE_OBJECT);

            IndexedColumnWriterOptions options;
            options.write_ordinal_index = true;
            options.write_value_index = false;
            options.encoding = EncodingInfo::get_default_encoding(bitmap_typeinfo->type(), false);
            // we already store compressed bitmap, use NO_COMPRESSION to save some cpu
            options.compression = NO_COMPRESSION;

            IndexedColumnWriter bitmap_column_writer(options, bitmap_typeinfo, wfile);
            RETURN_IF_ERROR(bitmap_column_writer.init());

            faststring buf;
            buf.reserve(max_bitmap_size);
            for (size_t i = 0; i < bitmaps.size(); ++i) {
                buf.resize(bitmap_sizes[i]); // so that buf[0..size) can be read and written
                bitmaps[i]->write(reinterpret_cast<char*>(buf.data()), false);
                Slice buf_slice(buf);
                RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice));
            }
            RETURN_IF_ERROR(bitmap_column_writer.finish(meta->mutable_bitmap_column()));
        }
        return Status::OK();
    }

    uint64_t size() const override {
        uint64_t size = 0;
        size += _null_bitmap.getSizeInBytes(false);
        for (BitmapUpdateContext* update_context : _late_update_context_vector) {
            update_context->late_update_size(&_reverted_index_size);
        }
        _late_update_context_vector.clear();
        size += _reverted_index_size;
        size += _mem_index.size() * sizeof(CppType);
        size += _pool.total_allocated_bytes();
        return size;
    }

private:
    TypeInfoPtr _typeinfo;
    rowid_t _rid = 0;

    // row id list for null value
    Roaring _null_bitmap;
    // unique value to its row id list
    MemoryIndexType _mem_index;
    MemPool _pool;

    // roaring bitmap size
    mutable uint64_t _reverted_index_size = 0;
    mutable std::vector<BitmapUpdateContext*> _late_update_context_vector;
};

} // namespace

struct BitmapIndexWriterBuilder {
    template <LogicalType ftype>
    std::unique_ptr<BitmapIndexWriter> operator()(const TypeInfoPtr& typeinfo) {
        return std::make_unique<BitmapIndexWriterImpl<ftype>>(typeinfo);
    }
};

Status BitmapIndexWriter::create(const TypeInfoPtr& typeinfo, std::unique_ptr<BitmapIndexWriter>* res) {
    LogicalType type = typeinfo->type();
    *res = field_type_dispatch_bitmap_index(type, BitmapIndexWriterBuilder(), typeinfo);

    return Status::OK();
}

} // namespace starrocks
