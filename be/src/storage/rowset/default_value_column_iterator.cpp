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

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_builder.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "storage/range.h"
#include "storage/types.h"
#include "types/array_type_info.h"
#include "types/map_type_info.h"
#include "types/struct_type_info.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "util/mem_util.hpp"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

namespace starrocks {

// Helper: Recursively convert JSON vpack::Slice to Datum based on TypeInfo
// This mirrors the logic in CastJsonToArray/Map/Struct but produces Datum directly
// Reference: be/src/exprs/cast_expr_array.cpp, cast_expr_map.cpp
static StatusOr<Datum> cast_json_to_datum_recursive(const vpack::Slice& json_slice, const TypeInfo* type_info,
                                                     MemPool* mem_pool) {
    LogicalType target_type = type_info->type();
    
    if (target_type == TYPE_ARRAY) {
        // Reference: CastJsonToArray::evaluate_checked in cast_expr_array.cpp
        if (!json_slice.isArray()) {
            // Not an array, return null datum
            Datum result;
            result.set_null();
            return result;
        }
        
        // Get element type from ArrayTypeInfo
        auto element_type_info = get_item_type_info(type_info);
        
        // Build DatumArray by recursively converting each element
        DatumArray datum_array;
        for (const auto& element : vpack::ArrayIterator(json_slice)) {
            ASSIGN_OR_RETURN(auto element_datum, cast_json_to_datum_recursive(element, element_type_info.get(), mem_pool));
            datum_array.push_back(std::move(element_datum));
        }
        
        Datum result;
        result.set_array(datum_array);
        return result;
        
    } else if (target_type == TYPE_MAP) {
        // Reference: CastJsonToMap::evaluate_checked in cast_expr_map.cpp
        if (!json_slice.isObject()) {
            // Not an object, return null datum
            Datum result;
            result.set_null();
            return result;
        }
        
        // Get key and value types from MapTypeInfo
        auto key_type_info = get_key_type_info(type_info);
        auto value_type_info = get_value_type_info(type_info);
        
        // Build DatumMap by recursively converting each key-value pair
        // Note: JSON object keys are always strings, need to cast to target key type
        DatumMap datum_map;
        for (const auto& pair : vpack::ObjectIterator(json_slice)) {
            // Key: JSON object key is a string, convert to target key type
            std::string key_str = pair.key.copyString();
            vpack::Builder key_builder;
            key_builder.add(vpack::Value(key_str));
            vpack::Slice key_slice = key_builder.slice();
            
            ASSIGN_OR_RETURN(auto key_datum, cast_json_to_datum_recursive(key_slice, key_type_info.get(), mem_pool));
            DatumKey datum_key = key_datum.convert2DatumKey();
            
            // Value: recursively convert
            ASSIGN_OR_RETURN(auto value_datum, cast_json_to_datum_recursive(pair.value, value_type_info.get(), mem_pool));
            
            datum_map[datum_key] = std::move(value_datum);
        }
        
        Datum result;
        result.set<DatumMap>(datum_map);
        return result;
        
    } else if (target_type == TYPE_STRUCT) {
        // Reference: CastJsonToStruct::evaluate_checked in cast_expr_struct.cpp
        // STRUCT can be represented as JSON array (positional) or JSON object (by field name)
        
        // Get field types from StructTypeInfo
        const auto& field_types = get_struct_field_types(type_info);
        
        if (json_slice.isArray()) {
            // JSON array: positional mapping [field0_value, field1_value, ...]
            DatumStruct datum_struct;
            size_t field_idx = 0;
            
            for (const auto& element : vpack::ArrayIterator(json_slice)) {
                if (field_idx >= field_types.size()) {
                    // More elements in JSON than fields in STRUCT, ignore extras
                    break;
                }
                
                ASSIGN_OR_RETURN(auto field_datum, 
                               cast_json_to_datum_recursive(element, field_types[field_idx].get(), mem_pool));
                datum_struct.push_back(std::move(field_datum));
                field_idx++;
            }
            
            // If JSON has fewer elements than STRUCT fields, fill remaining with NULL
            while (field_idx < field_types.size()) {
                Datum null_datum;
                null_datum.set_null();
                datum_struct.push_back(null_datum);
                field_idx++;
            }
            
            Datum result;
            result.set<DatumStruct>(datum_struct);
            return result;
            
        } else if (json_slice.isObject()) {
            // JSON object: field name mapping {"field1": value1, "field2": value2}
            // Extract values in iteration order (keys are sorted in vpack::Object)
            // Note: We don't validate field names against schema, just take values in order
            DatumStruct datum_struct;
            size_t field_idx = 0;
            
            for (const auto& pair : vpack::ObjectIterator(json_slice)) {
                if (field_idx >= field_types.size()) {
                    // More fields in JSON than in STRUCT schema, ignore extras
                    break;
                }
                
                ASSIGN_OR_RETURN(auto field_datum,
                               cast_json_to_datum_recursive(pair.value, field_types[field_idx].get(), mem_pool));
                datum_struct.push_back(std::move(field_datum));
                field_idx++;
            }
            
            // If JSON has fewer fields than STRUCT, fill remaining with NULL
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
            // Neither array nor object, return null
            Datum result;
            result.set_null();
            return result;
        }
        
    } else if (target_type == TYPE_JSON) {
        // JSON type: need to allocate JsonValue on heap
        JsonValue* json_value = new JsonValue(json_slice);
        Datum result;
        result.set_json(json_value);
        return result;
        
    } else {
        // Primitive types: use cast_vpjson_to to convert, then extract Datum
        Datum result;
        
        if (target_type == TYPE_BOOLEAN) {
            ColumnBuilder<TYPE_BOOLEAN> builder(1);
            Status st = cast_vpjson_to<TYPE_BOOLEAN, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set<bool>(col->get(0).get_int8() != 0);
        } else if (target_type == TYPE_TINYINT) {
            ColumnBuilder<TYPE_TINYINT> builder(1);
            Status st = cast_vpjson_to<TYPE_TINYINT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_int8(col->get(0).get_int8());
        } else if (target_type == TYPE_SMALLINT) {
            ColumnBuilder<TYPE_SMALLINT> builder(1);
            Status st = cast_vpjson_to<TYPE_SMALLINT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_int16(col->get(0).get_int16());
        } else if (target_type == TYPE_INT) {
            ColumnBuilder<TYPE_INT> builder(1);
            Status st = cast_vpjson_to<TYPE_INT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_int32(col->get(0).get_int32());
        } else if (target_type == TYPE_BIGINT) {
            ColumnBuilder<TYPE_BIGINT> builder(1);
            Status st = cast_vpjson_to<TYPE_BIGINT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_int64(col->get(0).get_int64());
        } else if (target_type == TYPE_LARGEINT) {
            ColumnBuilder<TYPE_LARGEINT> builder(1);
            Status st = cast_vpjson_to<TYPE_LARGEINT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_int128(col->get(0).get_int128());
        } else if (target_type == TYPE_FLOAT) {
            ColumnBuilder<TYPE_FLOAT> builder(1);
            Status st = cast_vpjson_to<TYPE_FLOAT, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_float(col->get(0).get_float());
        } else if (target_type == TYPE_DOUBLE) {
            ColumnBuilder<TYPE_DOUBLE> builder(1);
            Status st = cast_vpjson_to<TYPE_DOUBLE, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_double(col->get(0).get_double());
        } else if (target_type == TYPE_VARCHAR || target_type == TYPE_CHAR) {
            ColumnBuilder<TYPE_VARCHAR> builder(1);
            Status st = cast_vpjson_to<TYPE_VARCHAR, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            Slice temp_slice = col->get(0).get_slice();
            
            // Copy string data to mem_pool for persistent storage
            char* string_buffer = reinterpret_cast<char*>(mem_pool->allocate(temp_slice.size));
            if (UNLIKELY(string_buffer == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            memory_copy(string_buffer, temp_slice.data, temp_slice.size);
            result.set_slice(Slice(string_buffer, temp_slice.size));
        } else if (target_type == TYPE_DATE) {
            ColumnBuilder<TYPE_DATE> builder(1);
            Status st = cast_vpjson_to<TYPE_DATE, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_date(col->get(0).get_date());
        } else if (target_type == TYPE_DATETIME) {
            ColumnBuilder<TYPE_DATETIME> builder(1);
            Status st = cast_vpjson_to<TYPE_DATETIME, false>(json_slice, builder);
            RETURN_IF_ERROR(st);
            auto col = builder.build(false);
            result.set_timestamp(col->get(0).get_timestamp());
        } else {
            return Status::NotSupported(fmt::format("Unsupported type for default value: {}", target_type));
        }
        
        return result;
    }
}


Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    
    // [DEFAULT_VALUE_DEBUG] Log when default value iterator is used
    LOG(ERROR) << "[DEFAULT_VALUE_DEBUG] DefaultValueColumnIterator::init called, "
               << "has_default=" << _has_default_value
               << ", default_value='" << _default_value << "'"
               << ", type=" << _type_info->type()
               << ", is_nullable=" << _is_nullable;
    
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            // For complex types (ARRAY, MAP, STRUCT), we store a Datum object in _mem_value
            // For other types, we store the raw value according to _type_info->size()
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
                    LOG(ERROR) << "Failed to parse JSON default value '" << _default_value
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
                // For complex types, default_value is a JSON string (e.g., "[1,2,3]", "{\"k\":\"v\"}", "{\"f1\":1}")
                // Parse JSON and convert to Datum, then store the Datum in _mem_value
                auto json_or = JsonValue::parse_json_or_string(Slice(_default_value));
                if (!json_or.ok()) {
                    LOG(ERROR) << "Failed to parse complex type default value as JSON: '" << _default_value
                               << "', type: " << _type_info->type() << ", error: " << json_or.status()
                               << ", treating as NULL";
                    _is_default_value_null = true;
                } else {
                    // Convert JSON to Datum based on TypeInfo
                    vpack::Slice json_slice = json_or.value().to_vslice();
                    auto datum_or = cast_json_to_datum_recursive(json_slice, _type_info.get(), &_pool);
                    if (!datum_or.ok()) {
                        LOG(ERROR) << "Failed to convert complex type default value to Datum: '" << _default_value
                                   << "', type: " << _type_info->type() << ", error: " << datum_or.status()
                                   << ", treating as NULL";
                        _is_default_value_null = true;
                    } else {
                        // Store Datum in _mem_value
                        // Note: _mem_value points to a Datum object
                        new (_mem_value) Datum(std::move(datum_or.value()));
                        
                        LOG(ERROR) << "[DEFAULT_VALUE_DEBUG] Complex type default value converted to Datum: "
                                   << "type=" << _type_info->type();
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
            _current_rowid += *n;
        } else {
            // For all types including complex types (ARRAY, MAP, STRUCT),
            // _mem_value stores a Datum that can be directly appended
            dst->append_value_multiple_times(_mem_value, *n);
            _current_rowid += *n;
        }
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
        _current_rowid = range.end();
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
            _current_rowid = range.end();
        } else {
            // For all types including complex types (ARRAY, MAP, STRUCT),
            // _mem_value stores a Datum that can be directly appended
            dst->append_value_multiple_times(_mem_value, to_read);
            _current_rowid = range.end();
        }
    }
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
