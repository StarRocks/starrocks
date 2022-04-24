// This file is made available under Elastic License 2.0.
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

#include "column/column_hash.h"
#include "env/env.h"
#include "exec/vectorized/aggregate/agg_hash_map.h"
#include "runtime/mem_pool.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/common.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/types.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks {

namespace {

static constexpr size_t enable_roaring_threshold = 1024;
static constexpr size_t croaring_serialization_array_uint32 = 1;

// RoaringWrapper is a wrapper for roaring bitmap.
// When the cardinality is less than enable_roaring_threshold, we use _rowid_array to save the bitmap.
// When the BitmapIndexWriter is finished, _rowid_array will be flushed in the form of CROARING_SERIALIZATION_ARRAY_UINT32 in roaring bitmap.
// The form of CROARING_SERIALIZATION_ARRAY_UINT32:
// one bit    4 bytes         4 bytes * cardinality
// [ 1       cardinality      data ....]
// When the cardinality is higher than enable_roaring_threshold, we will transform _rowid_array to `roaring bitmap` _roaring and
// subsequent operations are performed via _roaring.
class RoaringWrapper {
public:
    explicit RoaringWrapper(rowid_t rid)
            : _roaring(nullptr), _old_size(1), _size_changed(true), _enable_roaring(false) {
        _rowid_array.push_back(rid);
    };
    Roaring* roaring() { return _roaring.get(); }
    uint64_t roaring_size() const { return _roaring->getSizeInBytes(false); }
    uint64_t old_size() const { return _old_size; }
    void set_old_size(uint64_t size) const { _old_size = size; }
    bool size_changed() const { return _size_changed; }
    void set_size_changed() const { _size_changed = true; }
    void unset_size_changed() const { _size_changed = false; }
    bool enable_roaring() const { return _enable_roaring; }
    uint32_t rowid_array_size_in_bytes() const {
        DCHECK(_enable_roaring == false);
        return (_rowid_array.size() + 1) * sizeof(uint32_t) + 1;
    }

    void add_rid(const rowid_t rid, uint64_t& reverted_index_size, vector<RoaringWrapper*>& size_changed_roaring_vec) {
        if (_enable_roaring) {
            if (_size_changed == false) {
                size_changed_roaring_vec.push_back(this);
            }
            _size_changed = true;
            _roaring->add(rid);
        } else {
            _old_size++;
            if (LIKELY(_old_size < enable_roaring_threshold)) {
                reverted_index_size += sizeof(uint32_t);
                _rowid_array.push_back(rid);
            } else {
                reverted_index_size -= (enable_roaring_threshold * sizeof(uint32_t) + 1);
                _old_size = 0;
                _size_changed = true;
                _enable_roaring = true;
                size_changed_roaring_vec.push_back(this);
                _roaring = std::make_unique<Roaring>(_rowid_array.size(), _rowid_array.data());
            }
        }
    }

    void flush_rowid_array(char* buf) {
        buf[0] = croaring_serialization_array_uint32;
        int cardinality = _rowid_array.size();
        memcpy(buf + 1, &cardinality, sizeof(uint32_t));

        uint32_t* data = reinterpret_cast<uint32_t*>(buf + 1 + sizeof(uint32_t));
        int outpos = 0;
        for (int i = 0; i < cardinality; ++i) {
            const uint32_t val = _rowid_array[i];
            memcpy(data + outpos, &val, sizeof(uint32_t));
            outpos++;
        }
    }

private:
    std::unique_ptr<Roaring> _roaring;
    vector<rowid_t> _rowid_array;

    mutable uint64_t _old_size;
    mutable bool _size_changed;
    bool _enable_roaring;
};

template <typename CppType>
struct BitmapIndexTraits {
    using MemoryIndexType = std::map<CppType, RoaringWrapper>;
};

template <>
struct BitmapIndexTraits<Slice> {
    using MemoryIndexType = std::map<Slice, RoaringWrapper, Slice::Comparator>;
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
template <FieldType field_type>
class BitmapIndexWriterImpl : public BitmapIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using MemoryIndexType = typename BitmapIndexTraits<CppType>::MemoryIndexType;

    explicit BitmapIndexWriterImpl(TypeInfoPtr type_info) : _typeinfo(std::move(type_info)), _reverted_index_size(0) {}

    ~BitmapIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            const CppType& value = unaligned_load<CppType>(p);
            auto it = _mem_index.find(value);
            if (it != _mem_index.end()) {
                it->second.add_rid(_rid, _reverted_index_size, _size_changed_roaring_vec);
            } else {
                // new value, copy value and insert new key->bitmap pair
                CppType new_value;
                _typeinfo->deep_copy(&new_value, &value, &_pool);
                _mem_index.emplace(new_value, RoaringWrapper(_rid));
                _reverted_index_size += sizeof(uint32_t) * 2 + 1;
            }
            _rid++;
            p++;
        }
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta) override {
        index_meta->set_type(BITMAP_INDEX);
        BitmapIndexPB* meta = index_meta->mutable_bitmap_index();

        meta->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP);
        meta->set_has_null(!_null_bitmap.isEmpty());

        { // write dictionary
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = false;
            options.write_value_index = true;
            options.encoding = EncodingInfo::get_default_encoding(_typeinfo->type(), true);
            options.compression = CompressionTypePB::LZ4_FRAME;

            IndexedColumnWriter dict_column_writer(options, _typeinfo, wblock);
            RETURN_IF_ERROR(dict_column_writer.init());
            for (auto const& it : _mem_index) {
                RETURN_IF_ERROR(dict_column_writer.add(&(it.first)));
            }
            RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_dict_column()));
        }
        { // write bitmaps
            std::vector<RoaringWrapper*> wrappers;
            std::vector<uint32_t> bitmap_sizes;
            bitmap_sizes.resize(_mem_index.size());
            uint32_t max_bitmap_size = 0;

            uint32_t i = 0;
            for (auto& it : _mem_index) {
                if (it.second.enable_roaring() == false) {
                    uint32_t rowid_array_size_in_bytes = it.second.rowid_array_size_in_bytes();
                    if (max_bitmap_size < rowid_array_size_in_bytes) {
                        max_bitmap_size = rowid_array_size_in_bytes;
                    }
                    bitmap_sizes[i] = rowid_array_size_in_bytes;
                }
                wrappers.push_back(&it.second);
                i++;
            }

            for (size_t i = 0; i < wrappers.size(); ++i) {
                if (wrappers[i]->enable_roaring()) {
                    Roaring* roaring = wrappers[i]->roaring();
                    roaring->runOptimize();
                    uint32_t bitmap_size = roaring->getSizeInBytes(false);
                    if (max_bitmap_size < bitmap_size) {
                        max_bitmap_size = bitmap_size;
                    }
                    bitmap_sizes[i] = bitmap_size;
                }
            }

            uint32_t null_bitmap_size = 0;
            if (!_null_bitmap.isEmpty()) {
                _null_bitmap.runOptimize();
                null_bitmap_size = _null_bitmap.getSizeInBytes(false);
                if (max_bitmap_size < null_bitmap_size) {
                    max_bitmap_size = null_bitmap_size;
                }
            }

            TypeInfoPtr bitmap_typeinfo = get_type_info(OLAP_FIELD_TYPE_OBJECT);

            IndexedColumnWriterOptions options;
            options.write_ordinal_index = true;
            options.write_value_index = false;
            options.encoding = EncodingInfo::get_default_encoding(bitmap_typeinfo->type(), false);
            // we already store compressed bitmap, use NO_COMPRESSION to save some cpu
            options.compression = NO_COMPRESSION;

            IndexedColumnWriter bitmap_column_writer(options, bitmap_typeinfo, wblock);
            RETURN_IF_ERROR(bitmap_column_writer.init());

            faststring buf;
            buf.reserve(max_bitmap_size);
            for (size_t i = 0; i < wrappers.size(); ++i) {
                buf.resize(bitmap_sizes[i]); // so that buf[0..size) can be read and written
                if (wrappers[i]->enable_roaring()) {
                    wrappers[i]->roaring()->write(reinterpret_cast<char*>(buf.data()), false);
                } else {
                    wrappers[i]->flush_rowid_array(reinterpret_cast<char*>(buf.data()));
                }
                Slice buf_slice(buf);
                RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice));
            }
            if (!_null_bitmap.isEmpty()) {
                buf.resize(null_bitmap_size);
                _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
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
        for (RoaringWrapper* roaring_wrapper : _size_changed_roaring_vec) {
            uint64_t new_size = roaring_wrapper->roaring_size();
            _reverted_index_size += (new_size - roaring_wrapper->old_size());
            roaring_wrapper->set_old_size(new_size);
            roaring_wrapper->unset_size_changed();
        }
        _size_changed_roaring_vec.clear();
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
    // size changed roaring bitmap in current block
    mutable vector<RoaringWrapper*> _size_changed_roaring_vec;
};

} // namespace

struct BitmapIndexWriterBuilder {
    template <FieldType ftype>
    std::unique_ptr<BitmapIndexWriter> operator()(const TypeInfoPtr& typeinfo) {
        return std::make_unique<BitmapIndexWriterImpl<ftype>>(typeinfo);
    }
};

Status BitmapIndexWriter::create(const TypeInfoPtr& typeinfo, std::unique_ptr<BitmapIndexWriter>* res) {
    FieldType type = typeinfo->type();
    *res = field_type_dispatch_bitmap_index(type, BitmapIndexWriterBuilder(), typeinfo);

    return Status::OK();
}

} // namespace starrocks
