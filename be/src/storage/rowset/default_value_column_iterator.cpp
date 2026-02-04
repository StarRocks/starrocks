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

#include "storage/rowset/default_value_column_iterator.h"

#include <algorithm>
#include <variant>

#include "base/utility/mem_util.hpp"
#include "column/array_column.h"
#include "column/column.h"
#include "column/column_access_path.h"
#include "column/column_builder.h"
#include "column/datum.h"
#include "runtime/decimalv3.h"
#include "storage/range.h"
#include "storage/types.h"
#include "types/array_type_info.h"
#include "types/map_type_info.h"
#include "types/struct_type_info.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

namespace starrocks {

static void _project_default_datum_by_path_if_needed(Datum* datum, const TypeInfo* type_info,
                                                     const ColumnAccessPath* path) {
    if (datum == nullptr || path == nullptr || type_info == nullptr) return;
    if (path->children().empty()) return;
    if (datum->is_null()) return;

    try {
        if (type_info->type() == TYPE_ARRAY) {
            const ColumnAccessPath* value_path = nullptr;
            if (path->children().size() == 1) {
                auto* p = path->children()[0].get();
                if (p->is_index() || p->is_all()) {
                    value_path = p;
                }
            }
            if (value_path == nullptr || value_path->children().empty()) return;

            auto element_type_info = get_item_type_info(type_info);
            if (element_type_info == nullptr) return;

            const auto& array = datum->get<DatumArray>();
            DatumArray projected;
            projected.reserve(array.size());
            for (const auto& elem : array) {
                Datum child_datum = elem;
                _project_default_datum_by_path_if_needed(&child_datum, element_type_info.get(), value_path);
                projected.emplace_back(std::move(child_datum));
            }
            datum->set_array(std::move(projected));
            return;
        }

        if (type_info->type() == TYPE_MAP) {
            const ColumnAccessPath* value_path = nullptr;
            if (path->children().size() == 1) {
                auto* p = path->children()[0].get();
                if (p->is_index() || p->is_all()) {
                    value_path = p;
                }
            }
            if (value_path == nullptr || value_path->children().empty()) return;

            auto value_type_info = get_value_type_info(type_info);
            if (value_type_info == nullptr) return;

            const auto& mp = datum->get<DatumMap>();
            DatumMap projected;
            for (const auto& it : mp) {
                Datum v = it.second;
                _project_default_datum_by_path_if_needed(&v, value_type_info.get(), value_path);
                projected.emplace(it.first, std::move(v));
            }
            datum->set<DatumMap>(std::move(projected));
            return;
        }

        if (type_info->type() != TYPE_STRUCT) {
            return;
        }

        const DatumStruct& full = datum->get_struct();
        const auto& field_types = get_struct_field_types(type_info);

        std::vector<const ColumnAccessPath*> selected(field_types.size(), nullptr);
        for (const auto& child : path->children()) {
            uint32_t idx = child->index();
            if (idx < selected.size()) {
                selected[idx] = child.get();
            } else {
                LOG(WARNING) << "Struct default projection: child index out of range. idx=" << idx
                             << ", field_size=" << selected.size() << ", path=" << path->to_string();
            }
        }

        DatumStruct projected;
        projected.reserve(path->children().size());
        for (size_t i = 0; i < selected.size(); ++i) {
            if (selected[i] == nullptr) continue;

            Datum child_datum;
            if (i < full.size()) {
                child_datum = full[i];
            } else {
                child_datum.set_null();
            }

            if (i < field_types.size() && field_types[i] != nullptr) {
                _project_default_datum_by_path_if_needed(&child_datum, field_types[i].get(), selected[i]);
            }

            projected.emplace_back(std::move(child_datum));
        }

        datum->set(projected);
    } catch (const std::bad_variant_access& e) {
        LOG(WARNING) << "Struct default projection failed (bad_variant_access). Treat as NULL. path="
                     << path->to_string() << ", err=" << e.what();
        datum->set_null();
    }
}

class VPackToDatumCaster {
public:
    VPackToDatumCaster(const vpack::Slice& json_slice, const TypeInfo* type_info, MemPool* mem_pool)
            : _json_slice(json_slice), _type_info(type_info), _mem_pool(mem_pool) {}

    StatusOr<Datum> cast() {
        LogicalType target_type = _type_info->type();

        if (target_type == TYPE_ARRAY) return cast_array();
        if (target_type == TYPE_MAP) return cast_map();
        if (target_type == TYPE_STRUCT) return cast_struct();
        if (target_type == TYPE_JSON) return cast_json();

        return cast_primitive_dispatch(target_type);
    }

private:
    StatusOr<Datum> cast_primitive_dispatch(LogicalType target_type) {
        switch (target_type) {
        case TYPE_BOOLEAN:
            return cast_primitive<TYPE_BOOLEAN>();
        case TYPE_TINYINT:
            return cast_primitive<TYPE_TINYINT>();
        case TYPE_SMALLINT:
            return cast_primitive<TYPE_SMALLINT>();
        case TYPE_INT:
            return cast_primitive<TYPE_INT>();
        case TYPE_BIGINT:
            return cast_primitive<TYPE_BIGINT>();
        case TYPE_LARGEINT:
            return cast_primitive<TYPE_LARGEINT>();
        case TYPE_FLOAT:
            return cast_primitive<TYPE_FLOAT>();
        case TYPE_DOUBLE:
            return cast_primitive<TYPE_DOUBLE>();
        case TYPE_DATE:
            return cast_primitive<TYPE_DATE>();
        case TYPE_DATETIME:
            return cast_primitive<TYPE_DATETIME>();
        case TYPE_VARCHAR:
        case TYPE_CHAR:
            return cast_string();
        case TYPE_DECIMAL32:
            return cast_decimal<int32_t>();
        case TYPE_DECIMAL64:
            return cast_decimal<int64_t>();
        case TYPE_DECIMAL128:
            return cast_decimal<int128_t>();
        case TYPE_DECIMAL256:
            return cast_decimal<int256_t>();
        default:
            return Status::NotSupported(fmt::format("Unsupported type for default value: {}", target_type));
        }
    }

    template <LogicalType TYPE>
    StatusOr<Datum> cast_primitive() {
        ColumnBuilder<TYPE> builder(1);
        Status st = cast_vpjson_to<TYPE, false>(_json_slice, builder);
        RETURN_IF_ERROR(st);
        auto col = builder.build(false);

        Datum result;
        if constexpr (TYPE == TYPE_BOOLEAN) {
            result.set<bool>(col->get(0).get_int8() != 0);
        } else if constexpr (TYPE == TYPE_TINYINT) {
            result.set_int8(col->get(0).get_int8());
        } else if constexpr (TYPE == TYPE_SMALLINT) {
            result.set_int16(col->get(0).get_int16());
        } else if constexpr (TYPE == TYPE_INT) {
            result.set_int32(col->get(0).get_int32());
        } else if constexpr (TYPE == TYPE_BIGINT) {
            result.set_int64(col->get(0).get_int64());
        } else if constexpr (TYPE == TYPE_LARGEINT) {
            result.set_int128(col->get(0).get_int128());
        } else if constexpr (TYPE == TYPE_FLOAT) {
            result.set_float(col->get(0).get_float());
        } else if constexpr (TYPE == TYPE_DOUBLE) {
            result.set_double(col->get(0).get_double());
        } else if constexpr (TYPE == TYPE_DATE) {
            result.set_date(col->get(0).get_date());
        } else if constexpr (TYPE == TYPE_DATETIME) {
            result.set_timestamp(col->get(0).get_timestamp());
        }
        return result;
    }

    StatusOr<Datum> cast_string() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        Status st = cast_vpjson_to<TYPE_VARCHAR, false>(_json_slice, builder);
        RETURN_IF_ERROR(st);
        auto col = builder.build(false);
        Slice temp_slice = col->get(0).get_slice();

        char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(temp_slice.size));
        if (UNLIKELY(string_buffer == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        memory_copy(string_buffer, temp_slice.data, temp_slice.size);

        Datum result;
        result.set_slice(Slice(string_buffer, temp_slice.size));
        return result;
    }

    template <typename DecimalType>
    StatusOr<Datum> cast_decimal() {
        std::string str_value;
        if (_json_slice.isString()) {
            vpack::ValueLength len;
            const char* str = _json_slice.getStringUnchecked(len);
            str_value.assign(str, len);
        } else if (_json_slice.isNumber()) {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            str_value = _json_slice.toJson(&options);
        } else {
            return Status::InvalidArgument("DECIMAL value must be string or number in JSON");
        }

        int precision = _type_info->precision();
        int scale = _type_info->scale();

        DecimalType decimal_value;
        bool overflow = DecimalV3Cast::from_string<DecimalType>(&decimal_value, precision, scale, str_value.c_str(),
                                                                str_value.size());

        if (overflow) {
            return Status::InvalidArgument(fmt::format("Failed to parse DECIMAL from: {}", str_value));
        }

        Datum result;
        result.set<DecimalType>(decimal_value);
        return result;
    }

    StatusOr<Datum> cast_array() {
        if (!_json_slice.isArray()) {
            Datum result;
            result.set_null();
            return result;
        }

        auto element_type_info = get_item_type_info(_type_info);

        DatumArray datum_array;
        for (const auto& element : vpack::ArrayIterator(_json_slice)) {
            VPackToDatumCaster element_caster(element, element_type_info.get(), _mem_pool);
            ASSIGN_OR_RETURN(auto element_datum, element_caster.cast());
            datum_array.push_back(std::move(element_datum));
        }

        Datum result;
        result.set_array(datum_array);
        return result;
    }

    StatusOr<Datum> cast_map() {
        if (!_json_slice.isObject()) {
            Datum result;
            result.set_null();
            return result;
        }

        if (_json_slice.length() == 0) {
            DatumMap datum_map;
            Datum result;
            result.set<DatumMap>(datum_map);
            return result;
        }

        auto key_type_info = get_key_type_info(_type_info);
        auto value_type_info = get_value_type_info(_type_info);

        DatumMap datum_map;
        // Use sequential iteration (true) to preserve field order
        for (const auto& pair : vpack::ObjectIterator(_json_slice, true)) {
            std::string key_str = pair.key.copyString();
            vpack::Builder key_builder;
            key_builder.add(vpack::Value(key_str));
            vpack::Slice key_slice = key_builder.slice();

            VPackToDatumCaster key_caster(key_slice, key_type_info.get(), _mem_pool);
            ASSIGN_OR_RETURN(auto key_datum, key_caster.cast());
            DatumKey datum_key = key_datum.convert2DatumKey();

            VPackToDatumCaster value_caster(pair.value, value_type_info.get(), _mem_pool);
            ASSIGN_OR_RETURN(auto value_datum, value_caster.cast());

            datum_map[datum_key] = std::move(value_datum);
        }

        Datum result;
        result.set<DatumMap>(datum_map);
        return result;
    }

    StatusOr<Datum> cast_struct() {
        const auto& field_types = get_struct_field_types(_type_info);

        if (_json_slice.isArray()) {
            DatumStruct datum_struct;
            size_t field_idx = 0;

            for (const auto& element : vpack::ArrayIterator(_json_slice)) {
                if (field_idx >= field_types.size()) {
                    break;
                }

                VPackToDatumCaster field_caster(element, field_types[field_idx].get(), _mem_pool);
                ASSIGN_OR_RETURN(auto field_datum, field_caster.cast());
                datum_struct.push_back(std::move(field_datum));
                field_idx++;
            }

            while (field_idx < field_types.size()) {
                Datum null_datum;
                null_datum.set_null();
                datum_struct.push_back(null_datum);
                field_idx++;
            }

            Datum result;
            result.set<DatumStruct>(datum_struct);
            return result;

        } else if (_json_slice.isObject()) {
            DatumStruct datum_struct;
            size_t field_idx = 0;

            // Use sequential iteration (true) to preserve field order
            for (const auto& pair : vpack::ObjectIterator(_json_slice, true)) {
                if (field_idx >= field_types.size()) {
                    break;
                }

                VPackToDatumCaster field_caster(pair.value, field_types[field_idx].get(), _mem_pool);
                ASSIGN_OR_RETURN(auto field_datum, field_caster.cast());
                datum_struct.push_back(std::move(field_datum));
                field_idx++;
            }

            while (field_idx < field_types.size()) {
                Datum null_datum;
                null_datum.set_null();
                datum_struct.push_back(null_datum);
                field_idx++;
            }

            Datum result;
            result.set<DatumStruct>(datum_struct);
            return result;
        } else {
            Datum result;
            result.set_null();
            return result;
        }
    }

    StatusOr<Datum> cast_json() {
        JsonValue* json_value = new JsonValue(_json_slice);
        Datum result;
        result.set_json(json_value);
        return result;
    }

private:
    const vpack::Slice& _json_slice;
    const TypeInfo* _type_info;
    MemPool* _mem_pool;
};

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;

    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            if (_type_info->type() == TYPE_ARRAY || _type_info->type() == TYPE_MAP ||
                _type_info->type() == TYPE_STRUCT) {
                _type_size = sizeof(Datum);
            } else {
                _type_size = _type_info->size();
            }
            _mem_value = reinterpret_cast<void*>(_pool.allocate(static_cast<int64_t>(_type_size)));
            if (UNLIKELY(_mem_value == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            Status status = Status::OK();
            if (_type_info->type() == TYPE_CHAR) {
                auto length = static_cast<int32_t>(_schema_length);
                char* string_buffer = reinterpret_cast<char*>(_pool.allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == TYPE_VARCHAR || _type_info->type() == TYPE_HLL ||
                       _type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_PERCENTILE) {
                auto length = static_cast<int32_t>(_default_value.length());
                char* string_buffer = reinterpret_cast<char*>(_pool.allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memory_copy(string_buffer, _default_value.c_str(), length);
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == TYPE_JSON) {
                auto json_or = JsonValue::parse_json_or_string(Slice(_default_value));
                if (!json_or.ok()) {
                    // If JSON parse fails, treat as NULL to avoid query errors
                    // This prevents returning malformed data when FE validation is bypassed
                    LOG(WARNING) << "Failed to parse JSON default value '" << _default_value
                                 << "', treating as NULL: " << json_or.status();
                    _is_default_value_null = true;
                } else {
                    Slice json_slice = json_or.value().get_slice();
                    auto length = static_cast<int32_t>(json_slice.size);
                    char* string_buffer = reinterpret_cast<char*>(_pool.allocate(length));
                    if (UNLIKELY(string_buffer == nullptr)) {
                        return Status::InternalError("Mem usage has exceed the limit of BE");
                    }
                    memory_copy(string_buffer, json_slice.data, length);
                    (static_cast<Slice*>(_mem_value))->size = length;
                    (static_cast<Slice*>(_mem_value))->data = string_buffer;
                }
            } else if (_type_info->type() == TYPE_ARRAY || _type_info->type() == TYPE_MAP ||
                       _type_info->type() == TYPE_STRUCT) {
                auto json_or = JsonValue::parse_json_or_string(Slice(_default_value));
                if (!json_or.ok()) {
                    LOG(ERROR) << "Failed to parse complex type default value as JSON: '" << _default_value
                               << "', type: " << _type_info->type() << ", error: " << json_or.status()
                               << ", treating as NULL";
                    _is_default_value_null = true;
                } else {
                    vpack::Slice json_slice = json_or.value().to_vslice();

                    VPackToDatumCaster caster(json_slice, _type_info.get(), &_pool);
                    auto datum_or = caster.cast();
                    if (!datum_or.ok()) {
                        LOG(ERROR) << "Failed to convert complex type default value to Datum: '" << _default_value
                                   << "', type: " << _type_info->type() << ", error: " << datum_or.status()
                                   << ", treating as NULL";
                        _is_default_value_null = true;
                    } else {
                        new (_mem_value) Datum(std::move(datum_or.value()));
                        if (_path != nullptr) {
                            _project_default_datum_by_path_if_needed(reinterpret_cast<Datum*>(_mem_value),
                                                                     _type_info.get(), _path);
                        }
                    }
                }
            } else {
                RETURN_IF_ERROR(_type_info->from_string(_mem_value, _default_value));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError("invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, Column* dst) {
    if (_is_default_value_null) {
        [[maybe_unused]] bool ok = dst->append_nulls(*n);
        _current_rowid += *n;
        DCHECK(ok) << "cannot append null to non-nullable column";
    } else {
        if (_type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_HLL ||
            _type_info->type() == TYPE_PERCENTILE || _type_info->type() == TYPE_JSON) {
            std::vector<Slice> slices;
            slices.reserve(*n);
            for (size_t i = 0; i < *n; i++) {
                slices.emplace_back(*reinterpret_cast<const Slice*>(_mem_value));
            }
            (void)dst->append_strings(slices);
        } else {
            dst->append_value_multiple_times(_mem_value, *n);
        }
        _current_rowid += *n;
    }
    if (_may_contain_deleted_row) {
        dst->set_delete_state(DEL_PARTIAL_SATISFIED);
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    size_t to_read = range.span_size();
    // if array column is nullable, range maybe empty. Before we support add/drop field for struct,
    // the element column iterator of array can not be default value column iterator.
    // However, if the element type of array is struct and we add a new field into the struct element,
    // we may access `DefaultValueColumnIterator` right now.
    // And when the range is empty, the `range.end()` will meet nullptr.
    // So if the range is empty, return ok is enough.
    if (to_read == 0) {
        return Status::OK();
    }
    if (_is_default_value_null) {
        [[maybe_unused]] bool ok = dst->append_nulls(to_read);
        DCHECK(ok) << "cannot append null to non-nullable column";
    } else {
        if (_type_info->type() == TYPE_OBJECT || _type_info->type() == TYPE_HLL ||
            _type_info->type() == TYPE_PERCENTILE || _type_info->type() == TYPE_JSON) {
            std::vector<Slice> slices;
            slices.reserve(to_read);
            for (size_t i = 0; i < to_read; i++) {
                slices.emplace_back(*reinterpret_cast<const Slice*>(_mem_value));
            }
            [[maybe_unused]] auto ret = dst->append_strings(slices);
        } else {
            dst->append_value_multiple_times(_mem_value, to_read);
        }
    }
    _current_rowid = range.end();
    if (_may_contain_deleted_row) {
        dst->set_delete_state(DEL_PARTIAL_SATISFIED);
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    return next_batch(&size, values);
}

Status DefaultValueColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                              const ColumnPredicate* del_predicate,
                                                              SparseRange<>* row_ranges, CompoundNodeType pred_relation,
                                                              const Range<>* src_range) {
    DCHECK(row_ranges->empty());
    // TODO
    if (src_range == nullptr) {
        row_ranges->add({0, static_cast<rowid_t>(_num_rows)});
    } else {
        row_ranges->add(*src_range);
    }
    // TODO: Setting `_may_contained_deleted_row` to true is a temporary fix,
    // which will affect performance in some scenarios.
    // It is best to filter according to DefaultValue,
    // but the current Expr framework does not support filter for a single line, which will be added later.
    _may_contain_deleted_row = true;
    return Status::OK();
}

} // namespace starrocks