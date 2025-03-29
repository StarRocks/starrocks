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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.cpp

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

#include "storage/rowset/column_reader.h"

#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/column_access_path.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/statusor.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/types.h"
#include "storage/column_predicate.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/rowset/array_column_iterator.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter_index_reader.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/json_column_iterator.h"
#include "storage/rowset/map_column_iterator.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/page_pointer.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/struct_column_iterator.h"
#include "storage/rowset/zone_map_index.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/bloom_filter.h"
#include "util/compression/block_compression.h"
#include "util/rle_encoding.h"

namespace starrocks {

StatusOr<std::unique_ptr<ColumnReader>> ColumnReader::create(ColumnMetaPB* meta, Segment* segment,
                                                             const TabletColumn* column) {
    auto r = std::make_unique<ColumnReader>(private_type(0), segment);
    RETURN_IF_ERROR(r->_init(meta, column));
    return std::move(r);
}

ColumnReader::ColumnReader(const private_type&, Segment* segment) : _segment(segment) {
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->column_metadata_mem_tracker(), sizeof(ColumnReader));
}

ColumnReader::~ColumnReader() {
    if (_segment_zone_map != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->segment_zonemap_mem_tracker(),
                                 _segment_zone_map->SpaceUsedLong());
        _segment_zone_map.reset(nullptr);
    }
    if (_ordinal_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->ordinal_index_mem_tracker(),
                                 _ordinal_index_meta->SpaceUsedLong());
        _ordinal_index_meta.reset(nullptr);
    }
    if (_zonemap_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                 _zonemap_index_meta->SpaceUsedLong());
        _zonemap_index_meta.reset(nullptr);
    }
    if (_bitmap_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(),
                                 _bitmap_index_meta->SpaceUsedLong());
        _bitmap_index_meta.reset(nullptr);
    }
    if (_bloom_filter_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                 _bloom_filter_index_meta->SpaceUsedLong());
        _bloom_filter_index_meta.reset(nullptr);
    }
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->column_metadata_mem_tracker(), sizeof(ColumnReader));
}

Status ColumnReader::_init(ColumnMetaPB* meta, const TabletColumn* column) {
    _column_type = static_cast<LogicalType>(meta->type());
    _dict_page_pointer = PagePointer(meta->dict_page());
    _total_mem_footprint = meta->total_mem_footprint();
    if (column == nullptr) {
        _name = meta->has_name() ? meta->name() : "None";
    } else {
        _name = meta->has_name() ? meta->name() : column->name();
    }
    _column_unique_id = meta->unique_id();

    if (meta->is_nullable()) _flags |= kIsNullableMask;
    if (meta->has_all_dict_encoded()) _flags |= kHasAllDictEncodedMask;
    if (meta->all_dict_encoded()) _flags |= kAllDictEncodedMask;

    if (_column_type == TYPE_JSON && meta->has_json_meta()) {
        // TODO(mofei) store format_version in ColumnReader
        const JsonMetaPB& json_meta = meta->json_meta();
        CHECK_EQ(kJsonMetaDefaultFormatVersion, json_meta.format_version()) << "Only format_version=1 is supported";
        _is_flat_json = json_meta.is_flat();
        _has_remain = json_meta.has_remain();

        if (json_meta.has_remain_filter()) {
            DCHECK(_has_remain);
            DCHECK(!json_meta.remain_filter().empty());
            RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &_remain_filter));
            RETURN_IF_ERROR(_remain_filter->init(json_meta.remain_filter().data(), json_meta.remain_filter().size(),
                                                 HASH_MURMUR3_X64_64));
        }
    }
    if (is_scalar_field_type(delegate_type(_column_type))) {
        RETURN_IF_ERROR(EncodingInfo::get(delegate_type(_column_type), meta->encoding(), &_encoding_info));
        RETURN_IF_ERROR(get_block_compression_codec(meta->compression(), &_compress_codec));

        for (int i = 0; i < meta->indexes_size(); i++) {
            auto* index_meta = meta->mutable_indexes(i);
            switch (index_meta->type()) {
            case ORDINAL_INDEX:
                _ordinal_index_meta.reset(index_meta->release_ordinal_index());
                MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->ordinal_index_mem_tracker(),
                                         _ordinal_index_meta->SpaceUsedLong());
                _meta_mem_usage.fetch_add(_ordinal_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
                _ordinal_index = std::make_unique<OrdinalIndexReader>();
                break;
            case ZONE_MAP_INDEX:
                _zonemap_index_meta.reset(index_meta->release_zone_map_index());
                _segment_zone_map.reset(_zonemap_index_meta->release_segment_zone_map());
                // Currently if the column type is varchar(length) and the values is all null,
                // a string of length will be written to the Segment file,
                // causing the loaded metadata to occupy a large amount of memory.
                //
                // The main purpose of this code is to optimize the reading of Segment files
                // generated by the old version.
                if (_segment_zone_map->has_has_not_null() && !_segment_zone_map->has_not_null()) {
                    delete _segment_zone_map->release_min();
                    delete _segment_zone_map->release_max();
                }
                MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                         _zonemap_index_meta->SpaceUsedLong())
                _meta_mem_usage.fetch_add(_zonemap_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
                // the segment zone map will release from zonemap_index_map,
                // so we should calc mem usage after release.
                MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->segment_zonemap_mem_tracker(),
                                         _segment_zone_map->SpaceUsedLong())
                _meta_mem_usage.fetch_add(_segment_zone_map->SpaceUsedLong(), std::memory_order_relaxed);
                _zonemap_index = std::make_unique<ZoneMapIndexReader>();
                break;
            case BITMAP_INDEX:
                _bitmap_index_meta.reset(index_meta->release_bitmap_index());
                MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(),
                                         _bitmap_index_meta->SpaceUsedLong());
                _meta_mem_usage.fetch_add(_bitmap_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
                _bitmap_index = std::make_unique<BitmapIndexReader>();
                break;
            case BLOOM_FILTER_INDEX:
                _bloom_filter_index_meta.reset(index_meta->release_bloom_filter_index());
                MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                         _bloom_filter_index_meta->SpaceUsedLong());
                _meta_mem_usage.fetch_add(_bloom_filter_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
                _bloom_filter_index = std::make_unique<BloomFilterIndexReader>();
                break;
            case UNKNOWN_INDEX_TYPE:
                return Status::Corruption(fmt::format("Bad file {}: unknown index type", file_name()));
            }
        }
        if (_ordinal_index == nullptr) {
            return Status::Corruption(
                    fmt::format("Bad file {}: missing ordinal index for column {}", file_name(), meta->column_id()));
        }

        if (_column_type == LogicalType::TYPE_JSON) {
            _sub_readers = std::make_unique<SubReaderList>();
            for (int i = 0; i < meta->children_columns_size(); ++i) {
                auto sub_column = (column != nullptr) ? column->subcolumn_ptr(i) : nullptr;
                auto res = ColumnReader::create(meta->mutable_children_columns(i), _segment, sub_column);
                RETURN_IF_ERROR(res);
                _sub_readers->emplace_back(std::move(res).value());
            }
            return Status::OK();
        }
        return Status::OK();
    } else if (_column_type == LogicalType::TYPE_ARRAY) {
        _sub_readers = std::make_unique<SubReaderList>();
        if (meta->is_nullable()) {
            if (meta->children_columns_size() != 3) {
                return Status::InvalidArgument("nullable array should have 3 children columns");
            }
            _sub_readers->reserve(3);

            auto sub_column = (column != nullptr) ? column->subcolumn_ptr(0) : nullptr;
            // elements
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment, sub_column);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // null flags
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(2), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        } else {
            if (meta->children_columns_size() != 2) {
                return Status::InvalidArgument("non-nullable array should have 2 children columns");
            }
            _sub_readers->reserve(2);

            auto sub_column = (column != nullptr) ? column->subcolumn_ptr(0) : nullptr;
            // elements
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment, sub_column);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        }
        return Status::OK();
    } else if (_column_type == LogicalType::TYPE_MAP) {
        _sub_readers = std::make_unique<SubReaderList>();
        if (meta->is_nullable()) {
            if (meta->children_columns_size() != 4) {
                return Status::InvalidArgument("nullable array should have 3 children columns");
            }
            _sub_readers->reserve(4);

            // keys
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // values
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // null flags
            res = ColumnReader::create(meta->mutable_children_columns(2), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(3), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        } else {
            if (meta->children_columns_size() != 3) {
                return Status::InvalidArgument("non-nullable array should have 2 children columns");
            }
            _sub_readers->reserve(3);

            // keys
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // values
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(2), _segment, nullptr);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        }
        return Status::OK();
    } else if (_column_type == LogicalType::TYPE_STRUCT) {
        _sub_readers = std::make_unique<SubReaderList>();
        for (int i = 0; i < meta->children_columns_size(); ++i) {
            auto sub_column = (column != nullptr) ? column->subcolumn_ptr(i) : nullptr;
            if (sub_column != nullptr) {
                // the type of unique_id in meta is uint32_t and the default value is -1(4294967295), but the type of
                // unique id in tablet column is int32_t. so cast to int32 to compare
                int32_t uid_in_meta = static_cast<int32_t>(meta->mutable_children_columns(i)->unique_id());
                int32_t uid_in_col = sub_column->unique_id();
                if (uid_in_meta != uid_in_col) {
                    std::string msg =
                            strings::Substitute("sub_column($0) unique id in meta($1) is not equal to schema($2)",
                                                sub_column->name(), uid_in_meta, uid_in_col);
                    LOG(ERROR) << msg;
                    return Status::InternalError(msg);
                }
            }
            auto res = ColumnReader::create(meta->mutable_children_columns(i), _segment, sub_column);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
            _update_sub_reader_pos(sub_column, i);
        }
        return Status::OK();
    } else {
        return Status::NotSupported(fmt::format("unsupported field type {}", (int)_column_type));
    }
}

Status ColumnReader::new_bitmap_index_iterator(const IndexReadOptions& opts, BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_load_bitmap_index(opts));
    RETURN_IF_ERROR(_bitmap_index->new_iterator(opts, iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                               Slice* page_body, PageFooterPB* footer) {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.read_file = iter_opts.read_file;
    opts.page_pointer = pp;
    opts.codec = _compress_codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = true;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.encoding_type = _encoding_info->encoding();
    opts.kept_in_memory = false;

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes, SparseRange<>* row_ranges) {
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        row_ranges->add({static_cast<rowid_t>(page_first_id), static_cast<rowid_t>(page_last_id + 1)});
    }
    return Status::OK();
}

Status ColumnReader::_parse_zone_map(LogicalType type, const ZoneMapPB& zm, ZoneMapDetail* detail) const {
    // DECIMAL32/DECIMAL64/DECIMAL128 stored as INT32/INT64/INT128
    // The DECIMAL type will be delegated to INT type.
    TypeInfoPtr type_info = get_type_info(delegate_type(type));
    detail->set_has_null(zm.has_null());

    if (zm.has_not_null()) {
        RETURN_IF_ERROR(datum_from_string(type_info.get(), &(detail->min_value()), zm.min(), nullptr));
        RETURN_IF_ERROR(datum_from_string(type_info.get(), &(detail->max_value()), zm.max(), nullptr));
    }
    detail->set_num_rows(static_cast<size_t>(num_rows()));
    return Status::OK();
}

template <bool is_original_bf>
Status ColumnReader::bloom_filter(const std::vector<const ColumnPredicate*>& predicates, SparseRange<>* row_ranges,
                                  const IndexReadOptions& opts) {
    RETURN_IF_ERROR(_load_bloom_filter_index(opts));
    SparseRange<> bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(opts, &bf_iter));
    size_t range_size = row_ranges->size();
    // get covered page ids
    std::set<int32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        Range<> r = (*row_ranges)[i];
        int64_t idx = r.begin();
        auto iter = _ordinal_index->seek_at_or_before(r.begin());
        while (idx < r.end()) {
            page_ids.insert(iter.page_index());
            idx = static_cast<int>(iter.last_ordinal() + 1);
            iter.next();
        }
    }

    for (const auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));

        const bool satisfy = std::ranges::any_of(predicates, [&](const ColumnPredicate* pred) {
            if constexpr (is_original_bf) {
                return pred->support_original_bloom_filter() && pred->original_bloom_filter(bf.get());
            } else {
                return pred->support_ngram_bloom_filter() &&
                       pred->ngram_bloom_filter(bf.get(), _get_reader_options_for_ngram());
            }
        });
        if (satisfy) {
            bf_row_ranges.add(
                    Range<>(_ordinal_index->get_first_ordinal(pid), _ordinal_index->get_last_ordinal(pid) + 1));
        }
    }
    *row_ranges = row_ranges->intersection(bf_row_ranges);
    return Status::OK();
}

// prerequisite: at least one predicate in |predicates| support bloom filter.
Status ColumnReader::original_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                           SparseRange<>* row_ranges, const IndexReadOptions& opts) {
    return bloom_filter<true>(predicates, row_ranges, opts);
}

Status ColumnReader::ngram_bloom_filter(const std::vector<const ::starrocks::ColumnPredicate*>& p,
                                        SparseRange<>* ranges, const IndexReadOptions& opts) {
    return bloom_filter<false>(p, ranges, opts);
}

Status ColumnReader::load_ordinal_index(const IndexReadOptions& opts) {
    if (_ordinal_index == nullptr || _ordinal_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto meta = _ordinal_index_meta.get();
    ASSIGN_OR_RETURN(auto first_load, _ordinal_index->load(opts, *meta, num_rows()));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->ordinal_index_mem_tracker(),
                                 _ordinal_index_meta->SpaceUsedLong());
        _meta_mem_usage.fetch_sub(_ordinal_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
        _meta_mem_usage.fetch_add(_ordinal_index->mem_usage(), std::memory_order_relaxed);
        _ordinal_index_meta.reset();
        _segment->update_cache_size();
    }
    return Status::OK();
}

Status ColumnReader::_load_zonemap_index(const IndexReadOptions& opts) {
    if (_zonemap_index == nullptr || _zonemap_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto meta = _zonemap_index_meta.get();
    ASSIGN_OR_RETURN(auto first_load, _zonemap_index->load(opts, *meta));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                 _zonemap_index_meta->SpaceUsedLong());
        _meta_mem_usage.fetch_sub(_zonemap_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
        _meta_mem_usage.fetch_add(_zonemap_index->mem_usage());
        _zonemap_index_meta.reset();
        _segment->update_cache_size();
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index(const IndexReadOptions& opts) {
    if (_bitmap_index == nullptr || _bitmap_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto meta = _bitmap_index_meta.get();
    ASSIGN_OR_RETURN(auto first_load, _bitmap_index->load(opts, *meta));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(),
                                 _bitmap_index_meta->SpaceUsedLong());
        _meta_mem_usage.fetch_sub(_bitmap_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
        _meta_mem_usage.fetch_add(_bitmap_index->mem_usage(), std::memory_order_relaxed);
        _bitmap_index_meta.reset();
        _segment->update_cache_size();
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index(const IndexReadOptions& opts) {
    if (_bloom_filter_index == nullptr || _bloom_filter_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto meta = _bloom_filter_index_meta.get();
    ASSIGN_OR_RETURN(auto first_load, _bloom_filter_index->load(opts, *meta));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                 _bloom_filter_index_meta->SpaceUsedLong());
        _meta_mem_usage.fetch_sub(_bloom_filter_index_meta->SpaceUsedLong(), std::memory_order_relaxed);
        _meta_mem_usage.fetch_add(_bloom_filter_index->mem_usage(), std::memory_order_relaxed);
        _bloom_filter_index_meta.reset();
        _segment->update_cache_size();
    }
    return Status::OK();
}

Status ColumnReader::new_inverted_index_iterator(const std::shared_ptr<TabletIndex>& index_meta,
                                                 InvertedIndexIterator** iterator, const SegmentReadOptions& opts) {
    RETURN_IF_ERROR(_load_inverted_index(index_meta, opts));
    RETURN_IF_ERROR(_inverted_index->new_iterator(index_meta, iterator));
    return Status::OK();
}

Status ColumnReader::_load_inverted_index(const std::shared_ptr<TabletIndex>& index_meta,
                                          const SegmentReadOptions& opts) {
    if (_inverted_index && index_meta && _inverted_index->get_index_id() == index_meta->index_id() &&
        _inverted_index_loaded()) {
        return Status::OK();
    }

    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    return success_once(_inverted_index_load_once,
                        [&]() {
                            LogicalType type;
                            if (_column_type == LogicalType::TYPE_ARRAY) {
                                type = _column_child_type;
                            } else {
                                type = _column_type;
                            }

                            ASSIGN_OR_RETURN(auto imp_type, get_inverted_imp_type(*index_meta))
                            std::string index_path = IndexDescriptor::inverted_index_file_path(
                                    opts.rowset_path, opts.rowsetid.to_string(), _segment->id(),
                                    index_meta->index_id());
                            ASSIGN_OR_RETURN(auto inverted_plugin, InvertedPluginFactory::get_plugin(imp_type));
                            RETURN_IF_ERROR(inverted_plugin->create_inverted_index_reader(index_path, index_meta, type,
                                                                                          &_inverted_index));

                            return Status::OK();
                        })
            .status();
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound(fmt::format("Failed to seek to ordinal {}", ordinal));
    }
    return Status::OK();
}

Status ColumnReader::seek_by_page_index(int page_index, OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->seek_by_page_index(page_index);
    if (!iter->valid()) {
        return Status::NotFound(fmt::format("Failed to seek page_index {}, out of bound", page_index));
    }
    return Status::OK();
}

std::pair<ordinal_t, ordinal_t> ColumnReader::get_page_range(size_t page_index) {
    DCHECK(_ordinal_index);
    return std::make_pair(_ordinal_index->get_first_ordinal(page_index), _ordinal_index->get_last_ordinal(page_index));
}

Status ColumnReader::zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                     const ColumnPredicate* del_predicate,
                                     std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                     SparseRange<>* row_ranges, const IndexReadOptions& opts,
                                     CompoundNodeType pred_relation) {
    RETURN_IF_ERROR(_load_zonemap_index(opts));

    std::vector<uint32_t> page_indexes;
    if (pred_relation == CompoundNodeType::AND) {
        RETURN_IF_ERROR(_zone_map_filter<CompoundNodeType::AND>(predicates, del_predicate, del_partial_filtered_pages,
                                                                &page_indexes));
    } else {
        RETURN_IF_ERROR(_zone_map_filter<CompoundNodeType::OR>(predicates, del_predicate, del_partial_filtered_pages,
                                                               &page_indexes));
    }

    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

StatusOr<std::vector<ZoneMapDetail>> ColumnReader::get_raw_zone_map(const IndexReadOptions& opts) {
    RETURN_IF_ERROR(_load_zonemap_index(opts));
    DCHECK(_zonemap_index);
    DCHECK(_zonemap_index->loaded());

    LogicalType type = _encoding_info->type();
    int32_t num_pages = _zonemap_index->num_pages();
    std::vector<ZoneMapDetail> result(num_pages);

    for (auto& zm : _zonemap_index->page_zone_maps()) {
        ZoneMapDetail detail;
        RETURN_IF_ERROR(_parse_zone_map(type, zm, &detail));
        result.emplace_back(detail);
    }

    return result;
}

template <CompoundNodeType PredRelation>
Status ColumnReader::_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                      const ColumnPredicate* del_predicate,
                                      std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                      std::vector<uint32_t>* pages) {
    // The type of the predicate may be different from the data type in the segment
    // file, e.g., the predicate type may be 'BIGINT' while the data type is 'INT',
    // so it's necessary to use the type of the predicate to parse the zone map string.
    LogicalType lt;
    if (!predicates.empty()) {
        lt = predicates[0]->type_info()->type();
    } else if (del_predicate) {
        lt = del_predicate->type_info()->type();
    } else {
        return Status::OK();
    }

    auto page_satisfies_zone_map_filter = [&](const ZoneMapDetail& detail) {
        if constexpr (PredRelation == CompoundNodeType::AND) {
            return std::ranges::all_of(predicates, [&](const auto* pred) { return pred->zone_map_filter(detail); });
        } else {
            return predicates.empty() ||
                   std::ranges::any_of(predicates, [&](const auto* pred) { return pred->zone_map_filter(detail); });
        }
    };

    const std::vector<ZoneMapPB>& zone_maps = _zonemap_index->page_zone_maps();
    int32_t page_size = _zonemap_index->num_pages();
    for (int32_t i = 0; i < page_size; ++i) {
        const ZoneMapPB& zm = zone_maps[i];
        ZoneMapDetail detail;
        RETURN_IF_ERROR(_parse_zone_map(lt, zm, &detail));

        if (!page_satisfies_zone_map_filter(detail)) {
            continue;
        }
        pages->emplace_back(i);

        if (del_predicate && del_predicate->zone_map_filter(detail)) {
            del_partial_filtered_pages->emplace(i);
        }
    }
    return Status::OK();
}

bool ColumnReader::segment_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates) const {
    if (_segment_zone_map == nullptr || predicates.empty()) {
        return true;
    }
    LogicalType lt = predicates[0]->type_info()->type();
    ZoneMapDetail detail;
    auto st = _parse_zone_map(lt, *_segment_zone_map, &detail);
    CHECK(st.ok()) << st;
    auto filter = [&](const ColumnPredicate* pred) { return pred->zone_map_filter(detail); };
    return std::all_of(predicates.begin(), predicates.end(), filter);
}

void ColumnReader::_update_sub_reader_pos(const TabletColumn* column, int pos) {
    if (column == nullptr) {
        return;
    }
    auto name = column->name();
    int id = column->unique_id();
    _sub_reader_pos[{std::string(name), id}] = pos;
}

StatusOr<std::unique_ptr<ColumnIterator>> ColumnReader::_create_merge_struct_iter(ColumnAccessPath* path,
                                                                                  const TabletColumn* column) {
    DCHECK(_column_type == LogicalType::TYPE_STRUCT);
    DCHECK(column != nullptr);
    auto num_fields = column->subcolumn_count();

    std::unique_ptr<ColumnIterator> null_iter;
    if (is_nullable()) {
        ASSIGN_OR_RETURN(null_iter, (*_sub_readers)[_sub_readers->size() - 1]->new_iterator());
    }

    std::vector<ColumnAccessPath*> child_paths(num_fields, nullptr);
    if (path != nullptr && !path->children().empty()) {
        for (const auto& child : path->children()) {
            child_paths[child->index()] = child.get();
        }
    }

    std::vector<std::unique_ptr<ColumnIterator>> field_iters;
    for (int i = 0; i < num_fields; ++i) {
        auto sub_column = column->subcolumn_ptr(i);
        auto iter = _sub_reader_pos.find({std::string(sub_column->name()), sub_column->unique_id()});
        if (iter != _sub_reader_pos.end()) {
            ASSIGN_OR_RETURN(auto iter, (*_sub_readers)[iter->second]->new_iterator(child_paths[i], sub_column));
            field_iters.emplace_back(std::move(iter));
        } else {
            if (!sub_column->has_default_value() && !sub_column->is_nullable()) {
                return Status::InternalError(
                        fmt::format("invalid nonexistent column({}) without default value.", sub_column->name()));
            } else {
                const TypeInfoPtr& type_info = get_type_info(*sub_column);
                auto default_value_iter = std::make_unique<DefaultValueColumnIterator>(
                        sub_column->has_default_value(), sub_column->default_value(), sub_column->is_nullable(),
                        type_info, sub_column->length(), num_rows());
                ColumnIteratorOptions iter_opts;
                RETURN_IF_ERROR(default_value_iter->init(iter_opts));
                field_iters.emplace_back(std::move(default_value_iter));
            }
        }
    }
    return create_struct_iter(this, std::move(null_iter), std::move(field_iters), path);
}

StatusOr<std::unique_ptr<ColumnIterator>> ColumnReader::new_iterator(ColumnAccessPath* path,
                                                                     const TabletColumn* column) {
    if (_column_type == LogicalType::TYPE_JSON) {
        return _new_json_iterator(path, column);
    } else if (is_scalar_field_type(delegate_type(_column_type))) {
        return std::make_unique<ScalarColumnIterator>(this);
    } else if (_column_type == LogicalType::TYPE_ARRAY) {
        size_t col = 0;

        ColumnAccessPath* value_path = nullptr;
        if (path != nullptr && !path->children().empty()) {
            // must be OFFSET or INDEX or ALL
            if (UNLIKELY(path->children().size() != 1)) {
                LOG(WARNING) << "bad access path on column: " << *path;
            } else {
                auto* p = path->children()[0].get();
                if (p->is_index() || p->is_all()) {
                    value_path = p;
                }
            }
        }

        auto sub_column = (column != nullptr) ? column->subcolumn_ptr(0) : nullptr;
        ASSIGN_OR_RETURN(auto element_iterator, (*_sub_readers)[col++]->new_iterator(value_path, sub_column));

        std::unique_ptr<ColumnIterator> null_iterator;
        if (is_nullable()) {
            ASSIGN_OR_RETURN(null_iterator, (*_sub_readers)[col++]->new_iterator());
        }
        ASSIGN_OR_RETURN(auto array_size_iterator, (*_sub_readers)[col++]->new_iterator());

        return std::make_unique<ArrayColumnIterator>(this, std::move(null_iterator), std::move(array_size_iterator),
                                                     std::move(element_iterator), path);
    } else if (_column_type == LogicalType::TYPE_MAP) {
        size_t col = 0;

        ColumnAccessPath* value_path = nullptr;
        if (path != nullptr && !path->children().empty()) {
            // must be OFFSET or INDEX or ALL or KEY
            if (UNLIKELY(path->children().size() != 1)) {
                LOG(WARNING) << "bad access path on column: " << *path;
            } else {
                auto* p = path->children()[0].get();
                if (p->is_index() || p->is_all()) {
                    value_path = p;
                }
            }
        }

        // key must scalar type now
        ASSIGN_OR_RETURN(auto keys, (*_sub_readers)[col++]->new_iterator());
        ASSIGN_OR_RETURN(auto values, (*_sub_readers)[col++]->new_iterator(value_path, nullptr));
        std::unique_ptr<ColumnIterator> nulls;
        if (is_nullable()) {
            ASSIGN_OR_RETURN(nulls, (*_sub_readers)[col++]->new_iterator());
        }
        ASSIGN_OR_RETURN(auto offsets, (*_sub_readers)[col++]->new_iterator());
        return std::make_unique<MapColumnIterator>(this, std::move(nulls), std::move(offsets), std::move(keys),
                                                   std::move(values), path);
    } else if (_column_type == LogicalType::TYPE_STRUCT) {
        if (column != nullptr) {
            return _create_merge_struct_iter(path, column);
        }
        auto num_fields = _sub_readers->size();

        std::unique_ptr<ColumnIterator> null_iter;
        if (is_nullable()) {
            num_fields -= 1;
            ASSIGN_OR_RETURN(null_iter, (*_sub_readers)[num_fields]->new_iterator());
        }

        std::vector<ColumnAccessPath*> child_paths(num_fields, nullptr);
        if (path != nullptr && !path->children().empty()) {
            for (const auto& child : path->children()) {
                child_paths[child->index()] = child.get();
            }
        }

        std::vector<std::unique_ptr<ColumnIterator>> field_iters;
        for (int i = 0; i < num_fields; ++i) {
            ASSIGN_OR_RETURN(auto iter, (*_sub_readers)[i]->new_iterator(child_paths[i]));
            field_iters.emplace_back(std::move(iter));
        }
        return create_struct_iter(this, std::move(null_iter), std::move(field_iters), path);
    } else {
        return Status::NotSupported("unsupported type to create iterator: " + std::to_string(_column_type));
    }
}

StatusOr<std::unique_ptr<ColumnIterator>> ColumnReader::_new_json_iterator(ColumnAccessPath* path,
                                                                           const TabletColumn* column) {
    DCHECK(_column_type == LogicalType::TYPE_JSON);
    auto json_iter = std::make_unique<ScalarColumnIterator>(this);
    // access sub columns
    std::vector<ColumnAccessPath*> target_leafs;
    std::vector<std::string> target_paths;
    std::vector<LogicalType> target_types;
    if (path != nullptr && !path->children().empty()) {
        auto field_name = path->absolute_path();
        path->get_all_leafs(&target_leafs);
        for (auto& p : target_leafs) {
            // use absolute path, not relative path
            // root path is field name, we remove it
            target_paths.emplace_back(p->absolute_path().substr(field_name.size() + 1));
            target_types.emplace_back(p->value_type().type);
        }
    }

    if (!_is_flat_json) {
        if (path == nullptr || path->children().empty()) {
            return json_iter;
        }
        // dynamic flattern
        // we must dynamic flat json, because we don't know other segment wasn't the paths
        return create_json_dynamic_flat_iterator(std::move(json_iter), target_paths, target_types,
                                                 path->is_from_compaction());
    }

    std::vector<std::string> source_paths;
    std::vector<LogicalType> source_types;
    std::unique_ptr<ColumnIterator> null_iter;
    std::vector<std::unique_ptr<ColumnIterator>> all_iters;
    size_t start = is_nullable() ? 1 : 0;
    size_t end = _has_remain ? _sub_readers->size() - 1 : _sub_readers->size();
    if (is_nullable()) {
        ASSIGN_OR_RETURN(null_iter, (*_sub_readers)[0]->new_iterator());
    }

    if (path == nullptr || path->children().empty() || path->is_from_compaction()) {
        DCHECK(_is_flat_json);
        for (size_t i = start; i < end; i++) {
            const auto& rd = (*_sub_readers)[i];
            std::string name = rd->name();
            ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
            source_paths.emplace_back(name);
            source_types.emplace_back(rd->column_type());
            all_iters.emplace_back(std::move(iter));
        }

        if (_has_remain) {
            const auto& rd = (*_sub_readers)[end];
            ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
            all_iters.emplace_back(std::move(iter));
        }

        if (path == nullptr || path->children().empty()) {
            // access whole json
            return create_json_merge_iterator(this, std::move(null_iter), std::move(all_iters), source_paths,
                                              source_types);
        } else {
            DCHECK(path->is_from_compaction());
            return create_json_flat_iterator(this, std::move(null_iter), std::move(all_iters), target_paths,
                                             target_types, source_paths, source_types, true);
        }
    }

    bool need_remain = false;
    std::set<std::string> check_paths;
    for (size_t k = 0; k < target_paths.size(); k++) {
        auto& target = target_paths[k];
        size_t i = start;
        for (; i < end; i++) {
            const auto& rd = (*_sub_readers)[i];
            std::string name = rd->name();
            if (check_paths.contains(name)) {
                continue;
            }
            // target: b.b2.b3
            // source: b.b2
            if (target == name || target.starts_with(name + ".")) {
                ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
                source_paths.emplace_back(name);
                source_types.emplace_back(rd->column_type());
                all_iters.emplace_back(std::move(iter));
                check_paths.emplace(name);
                break;
            } else if (name.starts_with(target + ".")) {
                // target: b.b2
                // source: b.b2.b3
                if (target_types[k] != TYPE_JSON && !is_string_type(target_types[k])) {
                    // don't need column and remain
                    break;
                }

                if (_remain_filter != nullptr &&
                    !_remain_filter->test_bytes(target_leafs[i]->path().data(), target_leafs[i]->path().size())) {
                    need_remain = false;
                } else {
                    need_remain = true;
                }

                ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
                source_paths.emplace_back(name);
                source_types.emplace_back(rd->column_type());
                all_iters.emplace_back(std::move(iter));
                check_paths.emplace(name);
            }
        }
        need_remain |= (i == end);
    }

    if (_has_remain && need_remain) {
        const auto& rd = (*_sub_readers)[end];
        ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
        all_iters.emplace_back(std::move(iter));
    }

    if (all_iters.empty()) {
        DCHECK(!_sub_readers->empty());
        DCHECK(source_paths.empty());
        // has none remain and can't hit any column, we read any one
        // why not return null directly, segemnt iterater need ordinal index...
        // it's canbe optimized
        size_t index = start;
        LogicalType type = (*_sub_readers)[start]->column_type();
        for (size_t i = start + 1; i < end; i++) {
            const auto& rd = (*_sub_readers)[i];
            if (type < rd->column_type()) {
                index = i;
                type = rd->column_type();
            }
        }
        const auto& rd = (*_sub_readers)[index];
        ASSIGN_OR_RETURN(auto iter, rd->new_iterator());
        all_iters.emplace_back(std::move(iter));
        source_paths.emplace_back(rd->name());
        source_types.emplace_back(rd->column_type());
    }

    return create_json_flat_iterator(this, std::move(null_iter), std::move(all_iters), target_paths, target_types,
                                     source_paths, source_types);
}

size_t ColumnReader::mem_usage() const {
    size_t size = sizeof(ColumnReader) + _meta_mem_usage.load(std::memory_order_relaxed);

    if (_sub_readers != nullptr) {
        for (auto& reader : *_sub_readers) {
            size += reader->mem_usage();
        }
    }

    return size;
}

NgramBloomFilterReaderOptions ColumnReader::_get_reader_options_for_ngram() const {
    // initialize with invalid number
    NgramBloomFilterReaderOptions reader_options;
    std::shared_ptr<TabletIndex> ngram_bf_index;

    Status status = _segment->tablet_schema().get_indexes_for_column(_column_unique_id, NGRAMBF, ngram_bf_index);
    if (!status.ok() || ngram_bf_index.get() == nullptr) {
        return reader_options;
    }

    const std::map<std::string, std::string>& index_properties = ngram_bf_index->index_properties();
    auto it = index_properties.find(GRAM_NUM_KEY);
    if (it != index_properties.end()) {
        // Found the key "ngram_size"
        const std::string& gram_num_str = it->second; // The value corresponding to the key "ngram_size"
        reader_options.index_gram_num = std::stoi(gram_num_str);
    }

    it = index_properties.find(CASE_SENSITIVE_KEY);
    if (it != index_properties.end()) {
        // Found the key "case_sensitive"
        const std::string& case_sensitive_str = it->second; // The value corresponding to the key "case_sensitive"
        reader_options.index_case_sensitive = (case_sensitive_str == "true");
    }

    return reader_options;
}

} // namespace starrocks
