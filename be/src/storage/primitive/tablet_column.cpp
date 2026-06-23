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

#include "storage/primitive/tablet_column.h"

#include <sstream>
#include <utility>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "storage/primitive/tablet_column_util.h"
#include "storage/primitive/type_utils.h"
#include "types/type_info.h"

namespace starrocks {

size_t TabletColumn::estimate_field_size(size_t variable_length) const {
    return TypeUtils::estimate_field_size(_type, variable_length);
}

TabletColumn::TabletColumn() = default;

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type) : _aggregation(agg), _type(type) {}

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type, bool is_nullable)
        : _aggregation(agg), _type(type) {
    _length = get_type_info(type)->size();
    _set_flag(kIsNullableShift, is_nullable);
}

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type, bool is_nullable, int32_t unique_id,
                           size_t length)
        : _unique_id(unique_id), _length(length), _aggregation(agg), _type(type) {
    _set_flag(kIsNullableShift, is_nullable);
}

TabletColumn::TabletColumn(const TabletColumn& rhs)
        : _col_name(rhs._col_name),
          _unique_id(rhs._unique_id),
          _length(rhs._length),
          _aggregation(rhs._aggregation),
          _type(rhs._type),
          _index_length(rhs._index_length),
          _precision(rhs._precision),
          _scale(rhs._scale),
          _extended_info(rhs._extended_info ? std::make_unique<ExtendedColumnInfo>(*rhs._extended_info) : nullptr),
          _flags(rhs._flags) {
    if (rhs._extra_fields != nullptr) {
        _extra_fields = new ExtraFields(*rhs._extra_fields);
    }
    if (rhs._agg_state_desc != nullptr) {
        _agg_state_desc = new AggStateDesc(*rhs._agg_state_desc);
    }
}

TabletColumn::TabletColumn(TabletColumn&& rhs) noexcept
        : _col_name(std::move(rhs._col_name)),
          _unique_id(rhs._unique_id),
          _length(rhs._length),
          _aggregation(rhs._aggregation),
          _type(rhs._type),
          _index_length(rhs._index_length),
          _precision(rhs._precision),
          _scale(rhs._scale),
          _extended_info(std::move(rhs._extended_info)),
          _flags(rhs._flags),
          _extra_fields(rhs._extra_fields),
          _agg_state_desc(rhs._agg_state_desc) {
    rhs._extra_fields = nullptr;
    rhs._agg_state_desc = nullptr;
}

TabletColumn::TabletColumn(const ColumnPB& column) {
    init_from_pb(column);
}

TabletColumn::TabletColumn(const TColumn& column) {
    init_from_thrift(column);
}

TabletColumn::~TabletColumn() {
    delete _extra_fields;
    delete _agg_state_desc;
}

void TabletColumn::swap(TabletColumn* rhs) {
    using std::swap;
    swap(_col_name, rhs->_col_name);
    swap(_unique_id, rhs->_unique_id);
    swap(_length, rhs->_length);
    swap(_aggregation, rhs->_aggregation);
    swap(_type, rhs->_type);
    swap(_index_length, rhs->_index_length);
    swap(_precision, rhs->_precision);
    swap(_scale, rhs->_scale);
    swap(_flags, rhs->_flags);
    swap(_extra_fields, rhs->_extra_fields);
    swap(_agg_state_desc, rhs->_agg_state_desc);
    swap(_extended_info, rhs->_extended_info);
}

TabletColumn& TabletColumn::operator=(const TabletColumn& rhs) {
    TabletColumn tmp(rhs);
    swap(&tmp);
    return *this;
}

TabletColumn& TabletColumn::operator=(TabletColumn&& rhs) noexcept {
    TabletColumn tmp(std::move(rhs));
    swap(&tmp);
    return *this;
}

void TabletColumn::init_from_pb(const ColumnPB& column) {
    _unique_id = column.unique_id();
    _col_name.assign(column.name());
    _type = string_to_logical_type(column.type());

    // NOTE(alvin): Change _type to format v2 type to have TableColumn has only storage V2 format.
    bool is_format_v1 = TypeUtils::specific_type_of_format_v1(_type);
    if (is_format_v1) {
        _type = TypeUtils::to_storage_format_v2(_type);
        auto type_info = get_type_info(_type);
        _length = type_info->size();
        if (column.has_index_length()) {
            _index_length = type_info->size();
        }
    } else {
        _length = column.length();
        if (column.has_index_length()) {
            // https://github.com/StarRocks/starrocks/issues/677
            // DCHECK_LE(column.index_length(), UINT8_MAX);
            _index_length = column.index_length();
        }
    }

    _set_flag(kIsKeyShift, column.is_key());
    _set_flag(kIsNullableShift, column.is_nullable());
    _set_flag(kIsBfColumnShift, column.is_bf_column());
    _set_flag(kHasBitmapIndexShift, column.has_bitmap_index());
    _set_flag(kHasPrecisionShift, column.has_precision());
    _set_flag(kHasScaleShift, column.has_frac());
    _set_flag(kHasAutoIncrementShift, column.is_auto_increment());

    if (column.has_precision()) {
        DCHECK_LE(column.precision(), UINT8_MAX);
        _precision = column.precision();
    }
    if (column.has_frac()) {
        DCHECK_LE(column.frac(), UINT8_MAX);
        _scale = column.frac();
    }
    if (column.has_aggregation()) {
        _aggregation = get_aggregation_type_by_string(column.aggregation());
    }
    if (column.has_default_value()) {
        ExtraFields* extra = _get_or_alloc_extra_fields();
        extra->has_default_value = true;
        extra->default_value = column.default_value();
    }
    for (size_t i = 0; i < column.children_columns_size(); ++i) {
        TabletColumn sub_column;
        sub_column.init_from_pb(column.children_columns(i));
        add_sub_column(std::move(sub_column));
    }
    // agg state type info
    if (column.has_agg_state_desc()) {
        VLOG(2) << "column contains agg state type info, add into extra fields";
        auto& agg_state_desc_pb = column.agg_state_desc();
        auto desc = AggStateDesc::from_protobuf(agg_state_desc_pb);
        _agg_state_desc = new AggStateDesc(std::move(desc));
    }
}

void TabletColumn::init_from_thrift(const TColumn& tcolumn) {
    _unique_id = tcolumn.col_unique_id;
    ColumnPB column_pb;
    auto shared_tcolumn_desc = std::make_shared<TColumn>(tcolumn);
    convert_to_new_version(shared_tcolumn_desc.get());

    WARN_IF_ERROR(t_column_to_pb_column(_unique_id, *shared_tcolumn_desc, &column_pb),
                  "failed to covert TColumn to ColumnPB");
    init_from_pb(column_pb);
}

void TabletColumn::to_schema_pb(ColumnPB* column) const {
    column->mutable_name()->assign(_col_name.data(), _col_name.size());
    column->set_unique_id(_unique_id);
    column->set_type(logical_type_to_string(_type));
    column->set_is_key(is_key());
    column->set_is_nullable(is_nullable());
    column->set_is_auto_increment(is_auto_increment());
    if (has_default_value()) {
        column->set_default_value(default_value());
    }
    if (has_precision()) {
        column->set_precision(_precision);
    }
    if (has_scale()) {
        column->set_frac(_scale);
    }
    column->set_length(_length);
    column->set_index_length(_index_length);
    column->set_is_bf_column(is_bf_column());
    column->set_aggregation(get_string_by_aggregation_type(_aggregation));
    column->set_has_bitmap_index(has_bitmap_index());
    for (int i = 0; i < subcolumn_count(); i++) {
        subcolumn(i).to_schema_pb(column->add_children_columns());
    }
    if (has_agg_state_desc()) {
        auto* agg_state_desc = get_agg_state_desc();
        auto* agg_state_pb = column->mutable_agg_state_desc();
        agg_state_desc->to_protobuf(agg_state_pb);
    }
}

void TabletColumn::add_sub_column(const TabletColumn& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.push_back(sub_column);
}

void TabletColumn::add_sub_column(TabletColumn&& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.emplace_back(std::move(sub_column));
}

bool TabletColumn::is_support_checksum() const {
    if (!is_support_checksum_type(_type)) {
        return false;
    }
    for (auto i = 0; i < subcolumn_count(); ++i) {
        const auto& sub_col = subcolumn(i);
        if (!sub_col.is_support_checksum()) {
            return false;
        }
    }
    return true;
}

bool operator==(const TabletColumn& a, const TabletColumn& b) {
    if (a._flags != b._flags) return false;
    if (a._unique_id != b._unique_id) return false;
    if (a._col_name != b._col_name) return false;
    if (a._type != b._type) return false;
    if (a._aggregation != b._aggregation) return false;
    if (a.has_default_value() != b.has_default_value()) return false;
    if (a.has_default_value()) {
        if (a.default_value() != b.default_value()) return false;
    }
    if (a.has_precision() != b.has_precision()) return false;
    if (a.has_precision()) {
        if (a._precision != b._precision) return false;
    }
    if (a.has_scale() != b.has_scale()) return false;
    if (a.has_scale()) {
        if (a._scale != b._scale) return false;
    }
    if (a._length != b._length) return false;
    if (a._index_length != b._index_length) return false;
    return true;
}

bool operator!=(const TabletColumn& a, const TabletColumn& b) {
    return !(a == b);
}

std::string TabletColumn::debug_string() const {
    std::stringstream ss;
    ss << "(unique_id=" << _unique_id << ",name=" << _col_name << ",type=" << _type << ",is_key=" << is_key()
       << ",aggregation=" << _aggregation << ",is_nullable=" << is_nullable()
       << ",default_value=" << (has_default_value() ? default_value() : "N/A")
       << ",precision=" << (has_precision() ? std::to_string(_precision) : "N/A")
       << ",frac=" << (has_scale() ? std::to_string(_scale) : "N/A") << ",length=" << _length
       << ",index_length=" << static_cast<int>(_index_length) << ",is_bf_column=" << is_bf_column()
       << ",has_bitmap_index=" << has_bitmap_index() << ")";
    return ss.str();
}

} // namespace starrocks
