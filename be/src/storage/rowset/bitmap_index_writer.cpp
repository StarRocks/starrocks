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
#include "storage/posting/encoder.h"
#include "storage/posting/posting.h"
#include "storage/rowset/common.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/bitmap_update_context.h"
#include "util/compression/compression_utils.h"
#include "util/faststring.h"
#include "util/phmap/btree.h"
#include "util/phmap/phmap.h"
#include "util/slice.h"
#include "util/utf8.h"
#include "util/xxh3.h"

namespace starrocks {

struct BitmapIndexSliceHash {
    inline size_t operator()(const Slice& v) const { return XXH3_64bits(v.data, v.size); }
};

template <typename CppType>
struct BitmapIndexTraits {
    using UnorderedMemoryIndexType = phmap::flat_hash_map<CppType, BitmapUpdateContextRefOrSingleValue<rowid_t>>;
    using PositionType = phmap::flat_hash_map<CppType, PostingList>;
};

template <>
struct BitmapIndexTraits<Slice> {
    using UnorderedMemoryIndexType = phmap::flat_hash_map<Slice, BitmapUpdateContextRefOrSingleValue<rowid_t>,
                                                          BitmapIndexSliceHash, std::equal_to<Slice>>;
    using PositionType = phmap::flat_hash_map<Slice, PostingList, BitmapIndexSliceHash, std::equal_to<Slice>>;
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
    using UnorderedMemoryIndexType = typename BitmapIndexTraits<CppType>::UnorderedMemoryIndexType;
    using PositionType = typename BitmapIndexTraits<CppType>::PositionType;

    explicit BitmapIndexWriterImpl(TypeInfoPtr type_info, int32_t gram_num, bool position)
            : _gram_num(gram_num), _typeinfo(std::move(type_info)) {
        if (position) {
            _posting_index = PositionType();
        }
    }

    ~BitmapIndexWriterImpl() override = default;

    void add_values(const void* values, size_t count) override {
        auto p = static_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value_with_current_rowid(p);
            incre_rowid();
            ++p;
        }
    }

    inline void add_value_with_current_rowid(const void* vptr) override {
        const CppType& value = *static_cast<const CppType*>(vptr);

        CppType* new_value_ptr = nullptr;

        auto it = _mem_index.find(value);
        if (it != _mem_index.end()) {
            it->second.add(_rid);
            if (it->second.update_estimate_size(&_reverted_index_size)) {
                _late_update_context_vector.push_back(it->second.context());
            }
        } else {
            // new value, copy value and insert new key->bitmap pair
            CppType new_value;
            _typeinfo->deep_copy(&new_value, &value, &_pool);
            _mem_index.emplace(new_value, _rid);

            new_value_ptr = &new_value;
            BitmapUpdateContext<rowid_t>::init_estimate_size(&_reverted_index_size);
        }

        if constexpr (field_type == TYPE_VARCHAR || field_type == TYPE_CHAR) {
            if (_posting_index.has_value()) {
                auto pit = _posting_index->find(value);
                if (pit != _posting_index->end()) {
                    pit->second.add_posting(_rid, _pos);
                } else {
                    auto posting = PostingList();
                    posting.add_posting(_rid, _pos);
                    if (LIKELY(new_value_ptr != nullptr)) {
                        _posting_index->emplace(*new_value_ptr, std::move(posting));
                    } else {
                        CppType new_value;
                        _typeinfo->deep_copy(&new_value, &value, &_pool);
                        _posting_index->emplace(new_value, std::move(posting));
                    }
                }
            }
        }
        ++_pos;
    }

    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) override {
        index_meta->set_type(BITMAP_INDEX);
        BitmapIndexPB* meta = index_meta->mutable_bitmap_index();
        return finish(wfile, meta);
    }

    Status finish(WritableFile* wfile, BitmapIndexPB* meta) override {
        meta->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP);
        meta->set_has_null(!_null_bitmap.isEmpty());

        std::vector<CppType> sorted_dicts;
        sorted_dicts.reserve(_mem_index.size());
        for (auto& p : _mem_index) {
            p.second.flush_pending_adds();
            sorted_dicts.emplace_back(p.first);
        }
        std::sort(sorted_dicts.begin(), sorted_dicts.end());

        // write dictionary
        RETURN_IF_ERROR(_write_dictionary(sorted_dicts, wfile, meta->mutable_dict_column()));
        // write bitmap
        RETURN_IF_ERROR(_write_bitmap(_mem_index, sorted_dicts, wfile, meta->mutable_bitmap_column()));

        if constexpr (field_type == TYPE_VARCHAR || field_type == TYPE_CHAR) {
            if (_gram_num > 0) {
                size_t offset = 0;
                UnorderedMemoryIndexType ngram_index;
                for (const auto& dict : sorted_dicts) {
                    RETURN_IF_ERROR(_build_ngram(ngram_index, &dict, offset++));
                }

                for (auto& it : ngram_index) {
                    it.second.flush_pending_adds();
                }

                std::vector<Slice> sorted_ngram_dicts;
                sorted_ngram_dicts.reserve(ngram_index.size());
                for (const auto& it : ngram_index) {
                    sorted_ngram_dicts.emplace_back(it.first);
                }
                std::ranges::sort(sorted_ngram_dicts);

                RETURN_IF_ERROR(_write_dictionary(sorted_ngram_dicts, wfile, meta->mutable_ngram_dict_column()));
                RETURN_IF_ERROR(
                        _write_bitmap(ngram_index, sorted_ngram_dicts, wfile, meta->mutable_ngram_bitmap_column()));
            }

            if (_posting_index.has_value()) {
                const auto start = wfile->size();
                RETURN_IF_ERROR(_write_posting(sorted_dicts, wfile, meta));
                const auto end = wfile->size();
                LOG(INFO) << "##### writing posting: " << (end - start) << " bytes";
            }
        }
        return Status::OK();
    }

    uint64_t size() const override {
        uint64_t size = 0;
        size += _null_bitmap.getSizeInBytes(false);
        for (BitmapUpdateContext<rowid_t>* update_context : _late_update_context_vector) {
            update_context->flush_pending_adds();
            update_context->late_update_size(&_reverted_index_size);
        }
        _late_update_context_vector.clear();
        size += _reverted_index_size;
        size += _mem_index.size() * sizeof(CppType);
        size += _pool.total_allocated_bytes();
        return size;
    }

    inline void incre_rowid() override {
        ++_rid;
        _pos = 0;
    }

private:
    Status _build_ngram(UnorderedMemoryIndexType& ngram_index, const Slice* cur_slice, const size_t offset) {
        if (_gram_num <= 0) {
            return Status::InvalidArgument(
                    "Invalid gram num while building ngram index for inverted index dictionary.");
        }

        std::vector<size_t> index;
        const size_t slice_gram_num = get_utf8_index(*cur_slice, &index);

        for (size_t j = 0; j + _gram_num <= slice_gram_num; ++j) {
            // find next ngram
            size_t cur_ngram_length =
                    j + _gram_num < slice_gram_num ? index[j + _gram_num] - index[j] : cur_slice->get_size() - index[j];
            Slice cur_ngram(cur_slice->data + index[j], cur_ngram_length);

            // add this ngram into set
            auto it = ngram_index.find(cur_ngram);
            if (it == ngram_index.end()) {
                CppType new_value;
                _typeinfo->deep_copy(&new_value, &cur_ngram, &_pool);
                ngram_index.emplace(new_value, offset);
            } else {
                it->second.add(offset);
            }
        }
        return Status::OK();
    }

    Status _write_dictionary(const std::vector<CppType>& sorted_dicts, WritableFile* wfile, IndexedColumnMetaPB* meta) {
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = true;
        options.encoding = EncodingInfo::get_default_encoding(_typeinfo->type(), true);
        options.compression = _dictionary_compression;

        IndexedColumnWriter dict_column_writer(options, _typeinfo, wfile);
        RETURN_IF_ERROR(dict_column_writer.init());
        for (auto const& dict : sorted_dicts) {
            RETURN_IF_ERROR(dict_column_writer.add(&dict));
        }
        return dict_column_writer.finish(meta);
    }

    Status _write_bitmap(UnorderedMemoryIndexType& ordered_mem_index, const std::vector<CppType>& sorted_dicts,
                         WritableFile* wfile, IndexedColumnMetaPB* meta) {
        std::vector<BitmapUpdateContextRefOrSingleValue<rowid_t>*> bitmaps;
        bitmaps.reserve(sorted_dicts.size());
        for (const auto& dict : sorted_dicts) {
            auto it = ordered_mem_index.find(dict);
            if (it == ordered_mem_index.end()) {
                // should never happen
                return Status::InternalError("No bitmap found for dict");
            }
            bitmaps.push_back(&(it->second));
        }

        uint32_t max_bitmap_size = 0;
        std::vector<uint32_t> bitmap_sizes;
        bitmap_sizes.reserve(bitmaps.size());
        for (auto& bitmap : bitmaps) {
            uint32_t bitmap_size = 0;
            if (bitmap->is_context()) {
                bitmap->context()->roaring()->runOptimize();
                bitmap_size = bitmap->context()->roaring()->getSizeInBytes(false);
                if (max_bitmap_size < bitmap_size) {
                    max_bitmap_size = bitmap_size;
                }
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
            if (bitmaps[i]->is_context()) {
                buf.resize(bitmap_sizes[i]); // so that buf[0..size) can be read and written
                bitmaps[i]->context()->roaring()->write(reinterpret_cast<char*>(buf.data()), false);
            } else {
                roaring::Roaring roar({bitmaps[i]->value()});
                roar.runOptimize();
                auto sz = roar.getSizeInBytes(false);
                buf.resize(sz);
                roar.write(reinterpret_cast<char*>(buf.data()), false);
            }
            Slice buf_slice(buf);
            RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice));
        }
        if (!_null_bitmap.isEmpty()) {
            _null_bitmap.runOptimize();
            buf.resize(_null_bitmap.getSizeInBytes(false)); // so that buf[0..size) can be read and written
            _null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            Slice buf_slice(buf);
            RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice));
        }
        return bitmap_column_writer.finish(meta);
    }

    Status _write_posting(const std::vector<CppType>& sorted_dicts, WritableFile* wfile, BitmapIndexPB* meta) {
        TypeInfoPtr int_typeinfo = get_type_info(TYPE_INT);
        IndexedColumnWriterOptions dict_options;
        dict_options.write_ordinal_index = true;
        dict_options.write_value_index = false;
        dict_options.encoding = EncodingInfo::get_default_encoding(int_typeinfo->type(), true);
        dict_options.compression = _dictionary_compression;
        IndexedColumnWriter dict_column_writer(dict_options, int_typeinfo, wfile);
        RETURN_IF_ERROR(dict_column_writer.init());

        std::vector<PostingList*> posting_lists;
        posting_lists.reserve(sorted_dicts.size());

        uint32_t offset = 0;
        RETURN_IF_ERROR(dict_column_writer.add(&offset));
        for (uint32_t dict_id = 0; dict_id < sorted_dicts.size(); ++dict_id) {
            const auto& dict = sorted_dicts[dict_id];
            auto it = _posting_index->find(dict);
            if (UNLIKELY(it == _posting_index->end())) {
                // should never happen
                return Status::InternalError("No posting found for dict");
            }

            auto& posting_list = it->second;

            posting_lists.emplace_back(&posting_list);
            offset += posting_list.get_num_doc_ids();
            RETURN_IF_ERROR(dict_column_writer.add(&offset));
        }
        RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_posting_index_column()));

        TypeInfoPtr varbinary_typeinfo = get_type_info(TYPE_VARBINARY);
        IndexedColumnWriterOptions value_options;
        value_options.write_ordinal_index = true;
        value_options.write_value_index = false;
        value_options.encoding = EncodingInfo::get_default_encoding(varbinary_typeinfo->type(), false);
        value_options.compression = CompressionUtils::to_compression_pb(config::inverted_index_posting_compression);
        value_options.index_page_size = config::inverted_index_posting_page_size;
        IndexedColumnWriter posting_writer(value_options, varbinary_typeinfo, wfile);
        RETURN_IF_ERROR(posting_writer.init());

        const auto encoder = EncoderFactory::createEncoder(EncodingType::VARINT);
        std::vector<uint8_t> buf;
        for (auto* posting : posting_lists) {
            RETURN_IF_ERROR(posting->for_each_posting([&](rowid_t doc_id, const roaring::Roaring& positions) -> Status {
                VLOG(11) << "Encoding positions for doc " << doc_id << " with " << positions.cardinality()
                         << " positions";
                RETURN_IF_ERROR(encoder->encode(positions, &buf));
                const Slice tmp(buf.data(), buf.size());
                return posting_writer.add(&tmp);
            }));
        }
        return posting_writer.finish(meta->mutable_posting_position_column());
    }

    int32_t _gram_num;

    TypeInfoPtr _typeinfo;
    rowid_t _rid = 0;

    // row id list for null value
    roaring::Roaring _null_bitmap;
    // unique value to its row id list
    // Use UnorderedMemoryIndexType during loading and sort it when finish is more efficient than only
    // use OrderedMemoryIndexType. Especially for the case of built-in inverted index workload.
    UnorderedMemoryIndexType _mem_index;
    MemPool _pool;

    rowid_t _pos = 0;
    std::optional<PositionType> _posting_index = std::nullopt;

    // roaring bitmap size
    mutable uint64_t _reverted_index_size = 0;
    mutable std::vector<BitmapUpdateContext<rowid_t>*> _late_update_context_vector;
};

struct BitmapIndexWriterBuilder {
    template <LogicalType ftype>
    std::unique_ptr<BitmapIndexWriter> operator()(const TypeInfoPtr& typeinfo, int32_t gram_num, bool position) {
        return std::make_unique<BitmapIndexWriterImpl<ftype>>(typeinfo, gram_num, position);
    }
};

Status BitmapIndexWriter::create(const TypeInfoPtr& typeinfo, std::unique_ptr<BitmapIndexWriter>* res, int32_t gram_num,
                                 bool position) {
    LogicalType type = typeinfo->type();
    *res = field_type_dispatch_bitmap_index(type, BitmapIndexWriterBuilder(), typeinfo, gram_num, position);

    return Status::OK();
}

} // namespace starrocks
