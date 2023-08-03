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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/zone_map_index.cpp

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

#include "storage/rowset/zone_map_index.h"

#include <bthread/sys_futex.h>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "storage/chunk_helper.h"
#include "storage/decimal_type_info.h"
#include "storage/olap_define.h"
#include "storage/olap_type_infra.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_reader.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/unaligned_access.h"

namespace starrocks {

template <LogicalType type>
struct ZoneMapDatumBase {
    using CppType = typename TypeTraits<type>::CppType;
    CppType value;

    virtual ~ZoneMapDatumBase() = default;

    virtual void reset(TypeInfo* type_info) { type_info->set_to_min(&value); }
    virtual void resize_container_for_fit(TypeInfo* type_info, const void* v) {}
    virtual std::string to_zone_map_string(TypeInfo* type_info) const { return type_info->to_string(&value); }
};

template <LogicalType type>
struct ZoneMapDatum final : public ZoneMapDatumBase<type> {};

template <>
struct ZoneMapDatum<TYPE_DECIMAL32> : public ZoneMapDatumBase<TYPE_DECIMAL32> {
    std::string to_zone_map_string(TypeInfo* type_info) const { return get_decimal_zone_map_string(type_info, &value); }
};

template <>
struct ZoneMapDatum<TYPE_DECIMAL64> : public ZoneMapDatumBase<TYPE_DECIMAL64> {
    std::string to_zone_map_string(TypeInfo* type_info) const { return get_decimal_zone_map_string(type_info, &value); }
};

template <>
struct ZoneMapDatum<TYPE_DECIMAL128> : public ZoneMapDatumBase<TYPE_DECIMAL128> {
    std::string to_zone_map_string(TypeInfo* type_info) const { return get_decimal_zone_map_string(type_info, &value); }
};

template <>
struct ZoneMapDatum<TYPE_CHAR> : public ZoneMapDatumBase<TYPE_CHAR> {
    void resize_container_for_fit(TypeInfo* type_info, const void* v) {
        static const int INIT_SIZE = 64;
        const Slice* slice = reinterpret_cast<const Slice*>(v);
        if (slice->size > _length) {
            _length = std::max<int>(BitUtil::next_power_of_two(slice->size), INIT_SIZE);
            raw::stl_string_resize_uninitialized(&_value_container, _length);
            value.data = _value_container.data();
            value.size = 0;
        }
    }

    void reset(TypeInfo* type_info) {
        value.data = _value_container.data();
        value.size = 0;
    }

    int _length = 0;
    std::string _value_container;
};

template <>
struct ZoneMapDatum<TYPE_VARCHAR> : public ZoneMapDatum<TYPE_CHAR> {};

template <LogicalType type>
struct ZoneMap {
    ZoneMapDatum<type> min_value;
    ZoneMapDatum<type> max_value;

    // if both has_null and has_not_null is false, means no rows.
    // if has_null is true and has_not_null is false, means all rows is null.
    // if has_null is false and has_not_null is true, means all rows is not null.
    // if has_null is true and has_not_null is true, means some rows is null and others are not.
    // has_null means whether zone has null value
    bool has_null = false;
    // has_not_null means whether zone has none-null value
    bool has_not_null = false;

    void to_proto(ZoneMapPB* dst, TypeInfo* type_info) const {
        dst->set_min(min_value.to_zone_map_string(type_info));
        dst->set_max(max_value.to_zone_map_string(type_info));
        dst->set_has_null(has_null);
        dst->set_has_not_null(has_not_null);
    }
};

template <LogicalType type>
class ZoneMapIndexWriterImpl final : public ZoneMapIndexWriter {
    using CppType = typename TypeTraits<type>::CppType;

public:
    // TypeInfo is used for all kinds of types. It is used to change the content of datum of the max/min value.
    // length is only used for CHAR/VARCHAR, and used to allocate enough memory for min/max value.
    explicit ZoneMapIndexWriterImpl(TypeInfo* type_info);

    void add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override { _page_zone_map.has_null |= count > 0; }

    // mark the end of one data page so that we can finalize the corresponding zone map
    Status flush() override;

    Status finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) override;

    uint64_t size() const override { return _estimated_size; }

private:
    void _reset_zone_map(ZoneMap<type>* zone_map) {
        // we should allocate max varchar length and set to max for min value
        zone_map->min_value.reset(_type_info);
        zone_map->max_value.reset(_type_info);
        zone_map->has_null = false;
        zone_map->has_not_null = false;
    }

    TypeInfo* _type_info;
    // memory will be managed by MemPool
    ZoneMap<type> _page_zone_map;
    ZoneMap<type> _segment_zone_map;

    // serialized ZoneMapPB for each data page
    std::vector<std::string> _values;
    uint64_t _estimated_size = 0;
};

template <LogicalType type>
ZoneMapIndexWriterImpl<type>::ZoneMapIndexWriterImpl(TypeInfo* type_info) : _type_info(type_info) {
    _reset_zone_map(&_page_zone_map);
    _reset_zone_map(&_segment_zone_map);
}

template <LogicalType type>
void ZoneMapIndexWriterImpl<type>::add_values(const void* values, size_t count) {
    if (count > 0) {
        if (_page_zone_map.has_not_null) {
            const auto* vals = reinterpret_cast<const CppType*>(values);
            auto [pmin, pmax] = std::minmax_element(vals, vals + count);
            if (unaligned_load<CppType>(pmin) < _page_zone_map.min_value.value) {
                _page_zone_map.min_value.resize_container_for_fit(_type_info, pmin);
                _type_info->direct_copy(&_page_zone_map.min_value.value, pmin);
            }
            if (unaligned_load<CppType>(pmax) > _page_zone_map.max_value.value) {
                _page_zone_map.max_value.resize_container_for_fit(_type_info, pmax);
                _type_info->direct_copy(&_page_zone_map.max_value.value, pmax);
            }
        } else {
            _page_zone_map.has_not_null = true;
            const auto* vals = reinterpret_cast<const CppType*>(values);
            auto [pmin, pmax] = std::minmax_element(vals, vals + count);

            _page_zone_map.min_value.resize_container_for_fit(_type_info, pmin);
            _type_info->direct_copy(&_page_zone_map.min_value.value, pmin);

            _page_zone_map.max_value.resize_container_for_fit(_type_info, pmax);
            _type_info->direct_copy(&_page_zone_map.max_value.value, pmax);
        }
    }
}

template <LogicalType type>
Status ZoneMapIndexWriterImpl<type>::flush() {
    // Update segment zone map.
    if (_page_zone_map.has_not_null) {
        if (_segment_zone_map.has_not_null) {
            if (_page_zone_map.min_value.value < _segment_zone_map.min_value.value) {
                _segment_zone_map.min_value.resize_container_for_fit(_type_info, &_page_zone_map.min_value.value);
                _type_info->direct_copy(&_segment_zone_map.min_value.value, &_page_zone_map.min_value.value);
            }
            if (_page_zone_map.max_value.value > _segment_zone_map.max_value.value) {
                _segment_zone_map.max_value.resize_container_for_fit(_type_info, &_page_zone_map.max_value.value);
                _type_info->direct_copy(&_segment_zone_map.max_value.value, &_page_zone_map.max_value.value);
            }
        } else {
            _segment_zone_map.min_value.resize_container_for_fit(_type_info, &_page_zone_map.min_value.value);
            _type_info->direct_copy(&_segment_zone_map.min_value.value, &_page_zone_map.min_value.value);

            _segment_zone_map.max_value.resize_container_for_fit(_type_info, &_page_zone_map.max_value.value);
            _type_info->direct_copy(&_segment_zone_map.max_value.value, &_page_zone_map.max_value.value);
        }
        _segment_zone_map.has_not_null = true;
    }

    if (_page_zone_map.has_null) {
        _segment_zone_map.has_null = true;
    }

    ZoneMapPB zone_map_pb;
    _page_zone_map.to_proto(&zone_map_pb, _type_info);
    _reset_zone_map(&_page_zone_map);

    std::string serialized_zone_map;
    bool ret = zone_map_pb.SerializeToString(&serialized_zone_map);
    if (!ret) {
        return Status::InternalError("serialize zone map failed");
    }
    _estimated_size += serialized_zone_map.size() + sizeof(uint32_t);
    _values.push_back(std::move(serialized_zone_map));
    return Status::OK();
}

struct ZoneMapIndexWriterBuilder {
    template <LogicalType ftype>
    std::unique_ptr<ZoneMapIndexWriter> operator()(TypeInfo* type_info) {
        return std::make_unique<ZoneMapIndexWriterImpl<ftype>>(type_info);
    }
};

std::unique_ptr<ZoneMapIndexWriter> ZoneMapIndexWriter::create(TypeInfo* type_info) {
    return field_type_dispatch_zonemap_index(type_info->type(), ZoneMapIndexWriterBuilder(), type_info);
}

template <LogicalType type>
Status ZoneMapIndexWriterImpl<type>::finish(WritableFile* wfile, ColumnIndexMetaPB* index_meta) {
    index_meta->set_type(ZONE_MAP_INDEX);
    ZoneMapIndexPB* meta = index_meta->mutable_zone_map_index();
    // store segment zone map
    _segment_zone_map.to_proto(meta->mutable_segment_zone_map(), _type_info);

    // write out zone map for each data pages
    TypeInfoPtr typeinfo = get_type_info(TYPE_OBJECT);
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = EncodingInfo::get_default_encoding(TYPE_OBJECT, false);
    options.compression = NO_COMPRESSION; // currently not compressed

    IndexedColumnWriter writer(options, typeinfo, wfile);
    RETURN_IF_ERROR(writer.init());

    for (auto& value : _values) {
        Slice value_slice(value);
        RETURN_IF_ERROR(writer.add(&value_slice));
    }
    return writer.finish(meta->mutable_page_zone_maps());
}

ZoneMapIndexReader::ZoneMapIndexReader() {
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(), sizeof(ZoneMapIndexReader));
}

ZoneMapIndexReader::~ZoneMapIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(), _mem_usage());
}

StatusOr<bool> ZoneMapIndexReader::load(const IndexReadOptions& opts, const ZoneMapIndexPB& meta) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(opts, meta);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                     _mem_usage() - sizeof(ZoneMapIndexReader))
        } else {
            _reset();
        }
        return st;
    });
}

Status ZoneMapIndexReader::_do_load(const IndexReadOptions& opts, const ZoneMapIndexPB& meta) {
    IndexedColumnReader reader(meta.page_zone_maps());
    RETURN_IF_ERROR(reader.load(opts));
    std::unique_ptr<IndexedColumnIterator> iter;
    RETURN_IF_ERROR(reader.new_iterator(opts, &iter));

    _page_zone_maps.resize(reader.num_values());

    auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(i));
        size_t num_to_read = 1;
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter->next_batch(&num_read, column.get()));
        DCHECK(num_to_read == num_read);

        ColumnViewer<TYPE_VARCHAR> viewer(column);
        auto value = viewer.value(0);
        if (!_page_zone_maps[i].ParseFromArray(value.data, value.size)) {
            return Status::Corruption("Failed to parse zone map");
        }

        // Currently if the column type is varchar(length) and the values is all null,
        // a zonemap string of length will be written to the segment file,
        // causing the loaded metadata to occupy a large amount of memory.
        //
        // The main purpose of this code is to optimize the reading of segment files
        // generated by the old version.
        if (_page_zone_maps[i].has_has_not_null() && !_page_zone_maps[i].has_not_null()) {
            delete _page_zone_maps[i].release_min();
            delete _page_zone_maps[i].release_max();
        }
        column->resize(0);
    }
    return Status::OK();
}

size_t ZoneMapIndexReader::_mem_usage() const {
    size_t size = sizeof(ZoneMapIndexReader);
    for (const auto& zone_map : _page_zone_maps) {
        size += zone_map.SpaceUsedLong();
    }
    return size;
}

} // namespace starrocks
