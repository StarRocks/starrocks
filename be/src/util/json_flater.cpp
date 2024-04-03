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

#include "util/json_flater.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/json_converter.h"

namespace starrocks {

void append_to_bool(const vpack::Slice* json, NullableColumn* result) {
    try {
        if (json->isNone() || json->isNull()) {
            result->append_nulls(1);
        } else if (json->isBool()) {
            result->null_column()->append(0);
            auto res = json->getBool();
            down_cast<RunTimeColumnType<TYPE_BOOLEAN>*>(result->data_column().get())->append(res);
        } else if (json->isString()) {
            vpack::ValueLength len;
            const char* str = json->getStringUnchecked(len);
            StringParser::ParseResult parseResult;
            auto r = StringParser::string_to_int<int32_t>(str, len, &parseResult);
            if (parseResult != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                bool b = StringParser::string_to_bool(str, len, &parseResult);
                if (parseResult != StringParser::PARSE_SUCCESS) {
                    result->append_nulls(1);
                } else {
                    down_cast<RunTimeColumnType<TYPE_BOOLEAN>*>(result->data_column().get())->append(b);
                }
            } else {
                down_cast<RunTimeColumnType<TYPE_BOOLEAN>*>(result->data_column().get())->append(r != 0);
            }
        } else if (json->isNumber()) {
            result->null_column()->append(0);
            auto res = json->getNumber<double>();
            down_cast<RunTimeColumnType<TYPE_BOOLEAN>*>(result->data_column().get())->append(res != 0);
        } else {
            result->append_nulls(1);
        }
    } catch (const vpack::Exception& e) {
        result->append_nulls(1);
    }
}

template <LogicalType TYPE>
void append_to_number(const vpack::Slice* json, NullableColumn* result) {
    try {
        if (LIKELY(json->isNumber() || json->isString())) {
            auto st = get_number_from_vpjson<TYPE>(*json);
            if (st.ok()) {
                result->null_column()->append(0);
                down_cast<RunTimeColumnType<TYPE>*>(result->data_column().get())->append(st.value());
            } else {
                result->append_nulls(1);
            }
        } else if (json->isNone() || json->isNull()) {
            result->append_nulls(1);
        } else if (json->isBool()) {
            result->null_column()->append(0);
            down_cast<RunTimeColumnType<TYPE>*>(result->data_column().get())->append(json->getBool());
        } else {
            result->append_nulls(1);
        }
    } catch (const vpack::Exception& e) {
        result->append_nulls(1);
    }
}

void append_to_string(const vpack::Slice* json, NullableColumn* result) {
    try {
        if (json->isNone() || json->isNull()) {
            result->append_nulls(1);
        } else if (json->isString()) {
            result->null_column()->append(0);
            vpack::ValueLength len;
            const char* str = json->getStringUnchecked(len);
            down_cast<BinaryColumn*>(result->data_column().get())->append(Slice(str, len));
        } else {
            result->null_column()->append(0);
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            std::string str = json->toJson(&options);
            down_cast<BinaryColumn*>(result->data_column().get())->append(Slice(str));
        }
    } catch (const vpack::Exception& e) {
        result->append_nulls(1);
    }
}

void append_to_json(const vpack::Slice* json, NullableColumn* result) {
    if (json->isNone()) {
        result->append_nulls(1);
    } else {
        result->null_column()->append(0);
        down_cast<JsonColumn*>(result->data_column().get())->append(JsonValue(*json));
    }
}

using JsonFlatAppendFunc = void (*)(const vpack::Slice* json, NullableColumn* result);
static const uint8_t JSON_BASE_TYPE_BITS = 0; // least flat to JSON type

// clang-format off
// bool will flatting as string, because it's need save string-literal(true/false)
// int & string compatible type is json, because int cast to string will add double quote, it's different with json
static const std::unordered_map<vpack::ValueType, uint8_t> JSON_TYPE_BITS{
        {vpack::ValueType::None, 255},      // 111111 11, 255
        {vpack::ValueType::SmallInt, 113},  // 011100 01, 113
        {vpack::ValueType::UInt, 96},       // 011000 00, 96
        {vpack::ValueType::Int, 97},        // 011000 01, 97
        {vpack::ValueType::Double, 64},     // 010000 00, 64
        {vpack::ValueType::String, 128},    // 100000 00, 128
};

static const std::unordered_map<uint8_t, LogicalType> JSON_BITS_TO_LOGICAL_TYPE {
    {JSON_TYPE_BITS.at(vpack::ValueType::None),        LogicalType::TYPE_BOOLEAN},
    {JSON_TYPE_BITS.at(vpack::ValueType::SmallInt),    LogicalType::TYPE_SMALLINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Int),         LogicalType::TYPE_BIGINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::UInt),        LogicalType::TYPE_LARGEINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Double),      LogicalType::TYPE_DOUBLE},
    {JSON_TYPE_BITS.at(vpack::ValueType::String),      LogicalType::TYPE_VARCHAR},
    {JSON_BASE_TYPE_BITS,                                LogicalType::TYPE_JSON},
};

static const std::unordered_map<uint8_t, JsonFlatAppendFunc> JSON_BITS_FUNC {
    {JSON_TYPE_BITS.at(vpack::ValueType::None),        &append_to_bool},
    {JSON_TYPE_BITS.at(vpack::ValueType::SmallInt),    &append_to_number<LogicalType::TYPE_SMALLINT>},
    {JSON_TYPE_BITS.at(vpack::ValueType::Int),         &append_to_number<LogicalType::TYPE_BIGINT>},
    {JSON_TYPE_BITS.at(vpack::ValueType::UInt),        &append_to_number<LogicalType::TYPE_LARGEINT>},
    {JSON_TYPE_BITS.at(vpack::ValueType::Double),      &append_to_number<LogicalType::TYPE_DOUBLE>},
    {JSON_TYPE_BITS.at(vpack::ValueType::String),      &append_to_string},
    {JSON_BASE_TYPE_BITS,                                &append_to_json},
};
// clang-format on

uint8_t JsonFlater::get_compatibility_type(vpack::ValueType type1, uint8_t type2) {
    if (JSON_TYPE_BITS.contains(type1)) {
        return JSON_TYPE_BITS.at(type1) & type2;
    }
    return JSON_BASE_TYPE_BITS;
}

JsonFlater::JsonFlater(std::vector<std::string>& paths) : _flat_paths(paths) {
    _flat_types.resize(paths.size(), JSON_BASE_TYPE_BITS);
    for (int i = 0; i < _flat_paths.size(); i++) {
        _flat_index[_flat_paths[i]] = i;
    }
};

JsonFlater::JsonFlater(std::vector<std::string>& paths, const std::vector<LogicalType>& types) : _flat_paths(paths) {
    for (const auto& t : types) {
        for (const auto& [k, v] : JSON_BITS_TO_LOGICAL_TYPE) {
            if (t == v) {
                _flat_types.emplace_back(k);
                break;
            }
        }
    }
    DCHECK_EQ(_flat_types.size(), types.size());
    for (int i = 0; i < _flat_paths.size(); i++) {
        _flat_index[_flat_paths[i]] = i;
    }
};

std::vector<LogicalType> JsonFlater::get_flat_types() {
    std::vector<LogicalType> types;
    for (const auto& t : _flat_types) {
        types.emplace_back(JSON_BITS_TO_LOGICAL_TYPE.at(t));
    }
    return types;
}

struct FlatColumnDesc {
public:
    uint8_t type = JsonFlater::JSON_NULL_TYPE_BITS;
    uint64_t hits = 0;
    uint16_t casts = 0;
};

void JsonFlater::derived_paths(std::vector<ColumnPtr>& json_datas) {
    _flat_paths.clear();
    _flat_types.clear();

    if (json_datas.empty()) {
        return;
    }

    size_t total_rows = 0;
    size_t null_count = 0;

    for (auto& column : json_datas) {
        total_rows += column->size();
        if (column->only_null() || column->empty()) {
            null_count += column->size();
            continue;
        } else if (column->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(column.get());
            null_count += nullable_column->null_count();
        }
    }

    // more than half of null
    if (null_count > total_rows * config::json_flat_null_factor) {
        VLOG(8) << "flat json, null_count[" << null_count << "], row[" << total_rows
                << "], null_factor: " << config::json_flat_null_factor;
        return;
    }

    // extract common keys, type
    std::unordered_map<std::string_view, FlatColumnDesc> derived_maps;
    for (size_t k = 0; k < json_datas.size(); k++) {
        size_t row_count = json_datas[k]->size();

        ColumnViewer<TYPE_JSON> viewer(json_datas[k]);
        for (size_t i = 0; i < row_count; ++i) {
            if (viewer.is_null(i)) {
                continue;
            }

            JsonValue* json = viewer.value(i);
            auto vslice = json->to_vslice();

            if (vslice.isNull() || vslice.isNone() || vslice.isEmptyObject() || !vslice.isObject()) {
                continue;
            }

            vpack::ObjectIterator iter(vslice);
            for (const auto& it : iter) {
                std::string_view name = it.key.stringView();
                derived_maps[name].hits++;
                uint8_t type = derived_maps[name].type;
                uint8_t compatibility_type =
                        JsonFlater::get_compatibility_type(it.value.type(), derived_maps[name].type);
                derived_maps[name].type = compatibility_type;
                derived_maps[name].casts += (type != compatibility_type);
            }
        }
    }

    if (derived_maps.size() <= config::json_flat_internal_column_min_limit) {
        VLOG(8) << "flat json, internal column too less: " << derived_maps.size()
                << ", at least: " << config::json_flat_internal_column_min_limit;
        return;
    }

    // sort by hit, casts
    std::vector<pair<std::string_view, FlatColumnDesc>> top_hits(derived_maps.begin(), derived_maps.end());
    std::sort(top_hits.begin(), top_hits.end(),
              [](const pair<std::string_view, FlatColumnDesc>& a, const pair<std::string_view, FlatColumnDesc>& b) {
                  // check hits, the higher the hit rate, the higher the priority.
                  if (a.second.hits != b.second.hits) {
                      return a.second.hits > b.second.hits;
                  }
                  // check type, the scalar type has the highest priority.
                  if (a.second.type != b.second.type) {
                      return a.second.type > b.second.type;
                  }
                  // check casts, the fewer the types of inference cast, the higher the priority.
                  if (a.second.casts != b.second.casts) {
                      return a.second.casts < b.second.casts;
                  }

                  // sort by name, just for stable order
                  return a.first < b.first;
              });

    for (int i = 0; i < top_hits.size() && i < config::json_flat_column_max; i++) {
        const auto& [name, desc] = top_hits[i];
        // check sparsity
        if (desc.hits >= total_rows * config::json_flat_sparsity_factor) {
            _flat_paths.emplace_back(name);
            _flat_types.emplace_back(desc.type);
        }
        VLOG(8) << "flat json[" << name << "], hit[" << desc.hits << "], row[" << total_rows << "]";
    }

    // init index map
    for (int i = 0; i < _flat_paths.size(); i++) {
        _flat_index[_flat_paths[i]] = i;
    }
}

void JsonFlater::flatten(const Column* json_column, std::vector<ColumnPtr>* result) {
    DCHECK(result->size() == _flat_paths.size());

    // input
    const JsonColumn* json_data = nullptr;
    if (json_column->is_nullable()) {
        // append null column
        auto* nullable_column = down_cast<const NullableColumn*>(json_column);
        json_data = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    } else {
        json_data = down_cast<const JsonColumn*>(json_column);
    }

    std::vector<NullableColumn*> flat_jsons;
    for (size_t i = 0; i < _flat_paths.size(); i++) {
        flat_jsons.emplace_back(down_cast<NullableColumn*>((*result)[i].get()));
    }

    // output
    std::vector<int> flat_hit(_flat_paths.size(), -1);
    for (size_t row = 0; row < json_column->size(); row++) {
        if (json_column->is_null(row)) {
            for (size_t k = 0; k < result->size(); k++) {
                (*result)[k]->append_nulls(1);
            }
            continue;
        }

        auto* obj = json_data->get_object(row);
        auto vslice = obj->to_vslice();
        if (vslice.isNone() || vslice.isNull() || vslice.isEmptyObject() || !vslice.isObject()) {
            for (size_t k = 0; k < result->size(); k++) {
                (*result)[k]->append_nulls(1);
            }
            continue;
        }

        size_t hit = _flat_paths.size();
        vpack::ObjectIterator iter(vslice);

        for (const auto& it : iter) {
            std::string_view path = it.key.stringView();
            auto iter = _flat_index.find(std::string(path));
            if (iter != _flat_index.end()) {
                int index = iter->second;
                uint8_t type = _flat_types[index];
                auto func = JSON_BITS_FUNC.at(type);
                func(&it.value, flat_jsons[index]);
                flat_hit[index]++;
                hit--;
            }

            if (hit == 0) {
                break;
            }
        }

        if (UNLIKELY(hit > 0)) {
            for (size_t k = 0; k < flat_hit.size() && hit > 0; k++) {
                if (flat_hit[k] != row) {
                    flat_jsons[k]->append_nulls(1);
                    flat_hit[k]++;
                    hit--;
                    DCHECK_EQ(flat_hit[k], row);
                }
            }
        }
    }
    for (auto& col : *result) {
        down_cast<NullableColumn*>(col.get())->update_has_null();
    }
}
} // namespace starrocks
