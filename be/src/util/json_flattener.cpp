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

#include "util/json_flattener.h"

#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/types.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/column_reader.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/json_converter.h"
#include "util/runtime_profile.h"

namespace starrocks {

namespace flat_json {
template <LogicalType TYPE>
void extract_number(const vpack::Slice* json, NullableColumn* result) {
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

void extract_string(const vpack::Slice* json, NullableColumn* result) {
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
            options.dumpAttributesInIndexOrder = false;
            std::string str = json->toJson(&options);
            down_cast<BinaryColumn*>(result->data_column().get())->append(Slice(str));
        }
    } catch (const vpack::Exception& e) {
        result->append_nulls(1);
    }
}

void extract_json(const vpack::Slice* json, NullableColumn* result) {
    if (json->isNone()) {
        result->append_nulls(1);
    } else {
        result->null_column()->append(0);
        down_cast<JsonColumn*>(result->data_column().get())->append(JsonValue(*json));
    }
}

template <LogicalType TYPE>
void merge_number(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const RunTimeColumnType<TYPE>*>(nullable_column->data_column().get());

    if constexpr (TYPE == LogicalType::TYPE_LARGEINT) {
        // the value is from json, must be uint64_t
        builder->addUnchecked(name.data(), name.size(), vpack::Value((uint64_t)col->get_data()[idx]));
    } else {
        builder->addUnchecked(name.data(), name.size(), vpack::Value(col->get_data()[idx]));
    }
}

void merge_string(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
    builder->addUnchecked(name.data(), name.size(), vpack::Value(col->get_slice(idx).to_string()));
}

void merge_json(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    builder->addUnchecked(name.data(), name.size(), col->get_object(idx)->to_vslice());
}

// clang-format off
using JsonFlatExtractFunc = void (*)(const vpack::Slice* json, NullableColumn* result);
using JsonFlatMergeFunc = void (*)(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx);
static const uint8_t JSON_BASE_TYPE_BITS = 0;   // least flat to JSON type
static const uint8_t JSON_BIGINT_TYPE_BITS = 7; // bigint compatible type

// bool will flatting as string, because it's need save string-literal(true/false)
// int & string compatible type is json, because int cast to string will add double quote, it's different with json
static const FlatJsonHashMap<vpack::ValueType, uint8_t> JSON_TYPE_BITS {
        {vpack::ValueType::None, 31},      //  00011111, 31
        {vpack::ValueType::SmallInt, 15},  //  00001111, 15
        {vpack::ValueType::Int, 7},        //  00000111, 7
        {vpack::ValueType::UInt, 3},       //  00000011, 3
        {vpack::ValueType::Double, 1},     //  00000001, 1
        {vpack::ValueType::String, 16},    //  00010000, 16
};

// starrocks json fucntio only support read as bigint/string/bool/double, smallint will cast to bigint, so we save as bigint directly
static const FlatJsonHashMap<uint8_t, LogicalType> JSON_BITS_TO_LOGICAL_TYPE {
    {JSON_TYPE_BITS.at(vpack::ValueType::None),        LogicalType::TYPE_TINYINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::SmallInt),    LogicalType::TYPE_BIGINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Int),         LogicalType::TYPE_BIGINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::UInt),        LogicalType::TYPE_LARGEINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Double),      LogicalType::TYPE_DOUBLE},
    {JSON_TYPE_BITS.at(vpack::ValueType::String),      LogicalType::TYPE_VARCHAR},
    {JSON_BASE_TYPE_BITS,                                LogicalType::TYPE_JSON},
};

static FlatJsonHashMap<LogicalType, uint8_t> LOGICAL_TYPE_TO_JSON_BITS {
    {LogicalType::TYPE_TINYINT,         JSON_TYPE_BITS.at(vpack::ValueType::None)},
    {LogicalType::TYPE_BIGINT,          JSON_TYPE_BITS.at(vpack::ValueType::Int)},
    {LogicalType::TYPE_LARGEINT,        JSON_TYPE_BITS.at(vpack::ValueType::UInt)},
    {LogicalType::TYPE_DOUBLE,          JSON_TYPE_BITS.at(vpack::ValueType::Double)},
    {LogicalType::TYPE_VARCHAR,         JSON_TYPE_BITS.at(vpack::ValueType::String)},
    {LogicalType::TYPE_JSON,            JSON_BASE_TYPE_BITS},
};

static const FlatJsonHashMap<LogicalType, JsonFlatExtractFunc> JSON_EXTRACT_FUNC {
    {LogicalType::TYPE_TINYINT,         &extract_number<LogicalType::TYPE_TINYINT>},
    {LogicalType::TYPE_BIGINT,          &extract_number<LogicalType::TYPE_BIGINT>},
    {LogicalType::TYPE_LARGEINT,        &extract_number<LogicalType::TYPE_LARGEINT>},
    {LogicalType::TYPE_DOUBLE,          &extract_number<LogicalType::TYPE_DOUBLE>},
    {LogicalType::TYPE_VARCHAR,         &extract_string},
    {LogicalType::TYPE_CHAR,            &extract_string},
    {LogicalType::TYPE_JSON,            &extract_json},
};

// should match with extract function
static const FlatJsonHashMap<LogicalType, JsonFlatMergeFunc> JSON_MERGE_FUNC {
    {LogicalType::TYPE_TINYINT,       &merge_number<LogicalType::TYPE_TINYINT>},
    {LogicalType::TYPE_BIGINT,        &merge_number<LogicalType::TYPE_BIGINT>},
    {LogicalType::TYPE_LARGEINT,      &merge_number<LogicalType::TYPE_LARGEINT>},
    {LogicalType::TYPE_DOUBLE,        &merge_number<LogicalType::TYPE_DOUBLE>},
    {LogicalType::TYPE_VARCHAR,       &merge_string},
    {LogicalType::TYPE_JSON,          &merge_json},
};
// clang-format on

inline uint8_t get_compatibility_type(vpack::ValueType type1, uint8_t type2) {
    auto iter = JSON_TYPE_BITS.find(type1);
    return iter != JSON_TYPE_BITS.end() ? type2 & iter->second : JSON_BASE_TYPE_BITS;
}

} // namespace flat_json

static const double FILTER_TEST_FPP[]{0.05, 0.1, 0.15, 0.2, 0.25, 0.3};

double estimate_filter_fpp(uint64_t element_nums) {
    uint32_t bytes[6];
    int min_idx = 7;
    double min = config::json_flat_remain_filter_max_bytes + 1;
    for (int i = 0; i < 6; i++) {
        bytes[i] = BloomFilter::estimate_bytes(element_nums, FILTER_TEST_FPP[i]);
        double d = bytes[i] + FILTER_TEST_FPP[i];
        if (d < min) {
            min = d;
            min_idx = i;
        }
    }
    return min_idx < 6 ? FILTER_TEST_FPP[min_idx] : -1;
}

std::pair<std::string_view, std::string_view> JsonFlatPath::split_path(const std::string_view& path) {
    size_t pos = 0;
    pos = path.find('.', pos);
    std::string_view key;
    std::string_view next;
    if (pos == std::string::npos) {
        key = path;
    } else {
        key = path.substr(0, pos);
        next = path.substr(pos + 1);
    }

    return {key, next};
}

JsonFlatPath* JsonFlatPath::normalize_from_path(const std::string_view& path, JsonFlatPath* root) {
    if (path.empty()) {
        return root;
    }
    auto [key, next] = split_path(path);
    auto iter = root->children.find(key);
    JsonFlatPath* child_path = nullptr;

    if (iter == root->children.end()) {
        root->children.emplace(key, std::make_unique<JsonFlatPath>());
        child_path = root->children[key].get();
    } else {
        child_path = iter->second.get();
    }
    return normalize_from_path(next, child_path);
}

/*
* to mark new root
*              root(Ig)
*           /       |       \
*        a(Ex)     b(Ig)    c(Ex)
*        /         /    \      \   
*      any     b1(Ex) b2(N)     any
*                    /    \
*                b3(IN)   b4(IN)
*/
void JsonFlatPath::set_root(const std::string_view& new_root_path, JsonFlatPath* node) {
    node->op = OP_IGNORE;
    if (new_root_path.empty()) {
        node->op = OP_ROOT;
        return;
    }
    auto [key, next] = split_path(new_root_path);

    auto iter = node->children.begin();
    for (; iter != node->children.end(); iter++) {
        iter->second->op = OP_EXCLUDE;
        if (iter->first == key) {
            set_root(next, iter->second.get());
        }
    }
}

StatusOr<size_t> check_null_factor(const std::vector<const Column*>& json_datas) {
    size_t total_rows = 0;
    size_t null_count = 0;

    for (auto& column : json_datas) {
        total_rows += column->size();
        if (column->only_null() || column->empty()) {
            null_count += column->size();
            continue;
        } else if (column->is_nullable()) {
            auto* nullable_column = down_cast<const NullableColumn*>(column);
            null_count += nullable_column->null_count();
        }
    }

    // more than half of null
    if (null_count > total_rows * config::json_flat_null_factor) {
        VLOG(8) << "flat json, null_count[" << null_count << "], row[" << total_rows
                << "], null_factor: " << config::json_flat_null_factor;
        return Status::InternalError("json flat null factor too high");
    }

    return total_rows - null_count;
}

JsonPathDeriver::JsonPathDeriver(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                 bool has_remain)
        : _has_remain(has_remain), _paths(std::move(paths)), _types(types) {
    for (size_t i = 0; i < _paths.size(); i++) {
        auto* leaf = JsonFlatPath::normalize_from_path(_paths[i], _path_root.get());
        leaf->type = types[i];
        leaf->index = i;
    }
}

void JsonPathDeriver::derived(const std::vector<const Column*>& json_datas) {
    DCHECK(_paths.empty());
    DCHECK(_types.empty());
    DCHECK(_derived_maps.empty());
    DCHECK(_path_root == nullptr);

    if (json_datas.empty()) {
        return;
    }

    auto res = check_null_factor(json_datas);
    if (!res.ok()) {
        return;
    }
    _total_rows = res.value();

    _path_root = std::make_shared<JsonFlatPath>();
    // init path by flat json
    _derived_on_flat_json(json_datas);

    // extract common keys, type
    size_t mark_row = 0;
    for (size_t k = 0; k < json_datas.size(); k++) {
        _derived(json_datas[k], mark_row);
        mark_row += json_datas[k]->size();
    }

    _finalize();
}

JsonFlatPath* JsonPathDeriver::_normalize_exists_path(const std::string_view& path, JsonFlatPath* root, uint64_t hits) {
    if (path.empty()) {
        return root;
    }

    _derived_maps[root].hits += hits;
    _derived_maps[root].type = flat_json::JSON_BASE_TYPE_BITS;

    auto [key, next] = JsonFlatPath::split_path(path);
    auto iter = root->children.find(key);
    JsonFlatPath* child_path = nullptr;

    if (iter == root->children.end()) {
        root->children.emplace(key, std::make_unique<JsonFlatPath>());
        child_path = root->children[key].get();
    } else {
        child_path = iter->second.get();
    }

    return _normalize_exists_path(next, child_path, hits);
}

void JsonPathDeriver::derived(const std::vector<const ColumnReader*>& json_readers) {
    DCHECK(_paths.empty());
    DCHECK(_types.empty());
    DCHECK(_derived_maps.empty());
    DCHECK(_path_root == nullptr);

    if (json_readers.empty()) {
        return;
    }

    _path_root = std::make_shared<JsonFlatPath>();
    _total_rows = 0;

    // extract flat paths
    for (const auto& reader : json_readers) {
        DCHECK_EQ(LogicalType::TYPE_JSON, reader->column_type());
        DCHECK(!reader->sub_readers()->empty());
        _total_rows += reader->num_rows();

        int start = reader->is_nullable() ? 1 : 0;
        int end = reader->has_remain_json() ? reader->sub_readers()->size() - 1 : reader->sub_readers()->size();
        _has_remain |= reader->has_remain_json();
        for (size_t i = start; i < end; i++) {
            const auto& sub = (*reader->sub_readers())[i];
            // compaction only extract common leaf, extract parent node need more compute on remain, it's bad performance
            auto leaf = _normalize_exists_path(sub->name(), _path_root.get(), 0);
            _derived_maps[leaf].type &= flat_json::LOGICAL_TYPE_TO_JSON_BITS.at(sub->column_type());
            _derived_maps[leaf].hits += reader->num_rows();
        }
    }
    _derived_maps.erase(_path_root.get());

    _min_json_sparsity_factory = 1; // only extract common schema
    _finalize();
}

void JsonPathDeriver::_derived_on_flat_json(const std::vector<const Column*>& json_datas) {
    // extract flat paths
    for (size_t k = 0; k < json_datas.size(); k++) {
        auto col = json_datas[k];
        const JsonColumn* json_col;
        size_t hits = 0;
        if (col->is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(col);
            hits = col->size() - nullable->null_count();
            json_col = down_cast<const JsonColumn*>(nullable->data_column().get());
        } else {
            hits = col->size();
            json_col = down_cast<const JsonColumn*>(col);
        }

        if (!json_col->is_flat_json()) {
            continue;
        }

        const auto& paths = json_col->flat_column_paths();
        const auto& types = json_col->flat_column_types();

        for (size_t i = 0; i < paths.size(); i++) {
            auto leaf = _normalize_exists_path(paths[i], _path_root.get(), hits);
            _derived_maps[leaf].type &= flat_json::LOGICAL_TYPE_TO_JSON_BITS.at(types[i]);
            _derived_maps[leaf].hits += hits;
        }
    }
    _derived_maps.erase(_path_root.get());
}

void JsonPathDeriver::_derived(const Column* col, size_t mark_row) {
    size_t row_count = col->size();
    const JsonColumn* json_col;

    if (col->is_nullable()) {
        auto nullable = down_cast<const NullableColumn*>(col);
        json_col = down_cast<const JsonColumn*>(nullable->data_column().get());
    } else {
        json_col = down_cast<const JsonColumn*>(col);
    }

    if (json_col->is_flat_json()) {
        if (json_col->has_remain()) {
            json_col = down_cast<const JsonColumn*>(json_col->get_remain().get());
        } else {
            return;
        }
    }

    for (size_t i = 0; i < row_count; ++i) {
        if (col->is_null(i)) {
            continue;
        }

        JsonValue* json = json_col->get_object(i);
        auto vslice = json->to_vslice();

        if (vslice.isNull() || vslice.isNone()) {
            continue;
        }

        if (vslice.isEmptyObject() || !vslice.isObject()) {
            _has_remain = true;
            continue;
        }

        _visit_json_paths(vslice, _path_root.get(), mark_row + i);
    }
}

void JsonPathDeriver::_visit_json_paths(const vpack::Slice& value, JsonFlatPath* root, size_t mark_row) {
    vpack::ObjectIterator it(value, false);

    for (; it.valid(); it.next()) {
        auto current = (*it);
        // sub-object?
        auto v = current.value;
        auto k = current.key.stringView();

        if (!root->children.contains(k)) {
            root->children.emplace(k, std::make_unique<JsonFlatPath>());
        }
        auto child = root->children[k].get();
        auto desc = &_derived_maps[child];
        desc->hits++;
        desc->multi_times += (desc->last_row == mark_row);
        desc->last_row = mark_row;

        if (v.isObject()) {
            child->remain = v.isEmptyObject();
            desc->type = flat_json::JSON_BASE_TYPE_BITS;
            _visit_json_paths(v, child, mark_row);
        } else {
            auto desc = &_derived_maps[child];
            vpack::ValueType json_type = v.type();
            desc->type = flat_json::get_compatibility_type(json_type, desc->type);
            if (json_type == vpack::ValueType::UInt) {
                desc->max = std::max(desc->max, v.getUIntUnchecked());
            }
        }
    }
}

// why dfs? because need compute parent isn't extract base on bottom-up, stack is not suitable
uint32_t JsonPathDeriver::_dfs_finalize(JsonFlatPath* node, const std::string& absolute_path,
                                        std::vector<std::pair<JsonFlatPath*, std::string>>* hit_leaf) {
    uint32_t flat_count = 0;
    for (auto& [key, child] : node->children) {
        if (!key.empty() && key.find('.') == std::string::npos) {
            // ignore empty key/quote key, it's can't handle in SQL
            // why not support `.` in key?
            // FE will add `"`. e.g: `a.b` -> `"a.b"`, in binary `\"a.b\"`
            // but BE is hard to handle `"`, because vpackjson don't add escape for `"` and `\`
            // input string `a\"b` -> in binary `a\\\"b` -> vpack json binary `a\"b`
            // it's take us can't identify `"` and `\` corrently
            std::string abs_path = fmt::format("{}.{}", absolute_path, key);
            flat_count += _dfs_finalize(child.get(), abs_path, hit_leaf);
        } else {
            child->remain = true;
        }
    }
    if (flat_count == 0 && !absolute_path.empty()) {
        // leaf node or all children is remain
        // check sparsity, same key may appear many times in json, so we need avoid duplicate compute hits
        auto desc = _derived_maps[node];
        if (desc.multi_times <= 0 && desc.hits >= _total_rows * _min_json_sparsity_factory) {
            hit_leaf->emplace_back(node, absolute_path);
            node->type = flat_json::JSON_BITS_TO_LOGICAL_TYPE.at(desc.type);
            node->remain = false;
            return 1;
        } else {
            node->remain = true;
            return 0;
        }
    } else {
        node->remain |= (flat_count != node->children.size());
        return 1;
    }
}

void dfs_add_remain_keys(JsonFlatPath* node, std::set<std::string_view>* remain_keys) {
    auto iter = node->children.begin();
    while (iter != node->children.end()) {
        auto child = iter->second.get();
        dfs_add_remain_keys(child, remain_keys);
        if (child->remain) {
            node->remain |= true;
            remain_keys->insert(iter->first);
        }
        iter++;
    }
}

void JsonPathDeriver::_finalize() {
    // try downgrade json-uint to bigint
    int128_t max = RunTimeTypeLimits<TYPE_BIGINT>::max_value();
    for (auto& [name, desc] : _derived_maps) {
        if (desc.type == flat_json::JSON_TYPE_BITS.at(vpack::ValueType::UInt) && desc.max <= max) {
            desc.type = flat_json::JSON_BIGINT_TYPE_BITS;
        }
    }

    std::vector<std::pair<JsonFlatPath*, std::string>> hit_leaf;
    _dfs_finalize(_path_root.get(), "", &hit_leaf);

    // sort by name, just for stable order
    std::sort(hit_leaf.begin(), hit_leaf.end(), [&](const auto& a, const auto& b) {
        auto desc_a = _derived_maps[a.first];
        auto desc_b = _derived_maps[b.first];
        return desc_a.hits > desc_b.hits;
    });
    size_t limit = config::json_flat_column_max > 0 ? config::json_flat_column_max : std::numeric_limits<size_t>::max();
    for (size_t i = limit; i < hit_leaf.size(); i++) {
        if (!hit_leaf[i].first->remain && _derived_maps[hit_leaf[i].first].hits >= _total_rows) {
            limit++;
            continue;
        }
        hit_leaf[i].first->remain = true;
    }
    if (hit_leaf.size() > limit) {
        _has_remain |= true;
        hit_leaf.resize(limit);
    }
    std::sort(hit_leaf.begin(), hit_leaf.end(), [](const auto& a, const auto& b) { return a.second < b.second; });
    for (auto& [node, path] : hit_leaf) {
        node->index = _paths.size();
        _paths.emplace_back(path.substr(1));
        _types.emplace_back(node->type);
    }

    std::set<std::string_view> remain_keys;
    dfs_add_remain_keys(_path_root.get(), &remain_keys);
    _has_remain |= _path_root->remain;
    if (_has_remain && _generate_filter) {
        double fpp = estimate_filter_fpp(remain_keys.size());
        if (fpp < 0) {
            return;
        }
        std::unique_ptr<BloomFilter> bf;
        Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
        DCHECK(st.ok());
        st = bf->init(remain_keys.size(), fpp, HASH_MURMUR3_X64_64);
        DCHECK(st.ok());
        for (const auto& key : remain_keys) {
            bf->add_bytes(key.data(), key.size());
        }
        _remain_filter = std::move(bf);
    }
}

JsonFlattener::JsonFlattener(JsonPathDeriver& deriver) {
    DCHECK(deriver.flat_path_root() != nullptr);
    _dst_paths = deriver.flat_paths();
    _has_remain = deriver.has_remain_json();
    auto types = deriver.flat_types();

    _dst_root = std::make_shared<JsonFlatPath>();
    for (size_t i = 0; i < _dst_paths.size(); i++) {
        auto* leaf = JsonFlatPath::normalize_from_path(_dst_paths[i], _dst_root.get());
        leaf->type = types[i];
        leaf->index = i;

        _flat_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(types[i]), true));
    }

    if (_has_remain) {
        _flat_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_JSON), false));
        _remain = down_cast<JsonColumn*>(_flat_columns.back().get());
    }
}

JsonFlattener::JsonFlattener(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                             bool has_remain)
        : _has_remain(has_remain), _dst_paths(std::move(paths)) {
    _dst_root = std::make_shared<JsonFlatPath>();

    for (size_t i = 0; i < _dst_paths.size(); i++) {
        auto* leaf = JsonFlatPath::normalize_from_path(_dst_paths[i], _dst_root.get());
        leaf->type = types[i];
        leaf->index = i;

        _flat_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(types[i]), true));
    }

    if (_has_remain) {
        _flat_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(LogicalType::TYPE_JSON), false));
        _remain = down_cast<JsonColumn*>(_flat_columns.back().get());
    }
}
void JsonFlattener::flatten(const Column* json_column) {
    for (auto& col : _flat_columns) {
        DCHECK_EQ(col->size(), 0);
        col->reserve(json_column->size());
    }
    // input
    const JsonColumn* json_data = nullptr;
    if (json_column->is_nullable()) {
        // append null column
        auto* nullable_column = down_cast<const NullableColumn*>(json_column);
        json_data = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    } else {
        json_data = down_cast<const JsonColumn*>(json_column);
    }

    // output
    if (_has_remain) {
        _flatten<true>(json_column, json_data);
        for (size_t i = 0; i < _flat_columns.size() - 1; i++) {
            down_cast<NullableColumn*>(_flat_columns[i].get())->update_has_null();
        }
    } else {
        _flatten<false>(json_column, json_data);
        for (size_t i = 0; i < _flat_columns.size(); i++) {
            down_cast<NullableColumn*>(_flat_columns[i].get())->update_has_null();
        }
    }

    for (auto& col : _flat_columns) {
        DCHECK_EQ(col->size(), json_column->size());
    }
}

template <bool REMAIN>
bool JsonFlattener::_flatten_json(const vpack::Slice& value, const JsonFlatPath* root, vpack::Builder* builder,
                                  uint32_t* hit_count) {
    vpack::ObjectIterator it(value, false);
    for (; it.valid(); it.next()) {
        auto current = (*it);
        // sub-object
        auto v = current.value;
        auto k = current.key.stringView();

        auto child = root->children.find(k);
        if constexpr (REMAIN) {
            if (child == root->children.end()) {
                builder->addUnchecked(k.data(), k.size(), v);
                continue;
            }
        } else {
            if (*hit_count == _dst_paths.size()) {
                return false;
            }
            if (child == root->children.end()) {
                continue;
            }
        }

        if (child->second->children.empty()) {
            // leaf node
            auto index = child->second->index;
            DCHECK(_flat_columns.size() > index);
            DCHECK(_flat_columns[index]->is_nullable());
            auto* c = down_cast<NullableColumn*>(_flat_columns[index].get());
            auto func = flat_json::JSON_EXTRACT_FUNC.at(child->second->type);
            func(&v, c);
            *hit_count += 1;
            // not leaf node, should goto deep
        } else if (v.isObject()) {
            if constexpr (REMAIN) {
                builder->addUnchecked(k.data(), k.size(), vpack::Value(vpack::ValueType::Object));
                _flatten_json<REMAIN>(v, child->second.get(), builder, hit_count);
                builder->close();
            } else {
                if (!_flatten_json<REMAIN>(v, child->second.get(), builder, hit_count)) {
                    return false;
                }
            }
        } else {
            if constexpr (REMAIN) {
                builder->addUnchecked(k.data(), k.size(), v);
            }
        }
    }
    return true;
}

template <bool HAS_REMAIN>
void JsonFlattener::_flatten(const Column* json_column, const JsonColumn* json_data) {
    DCHECK(!_dst_paths.empty());
    // output
    DCHECK_LE(_dst_paths.size(), std::numeric_limits<int>::max());
    for (size_t row = 0; row < json_column->size(); row++) {
        if (json_column->is_null(row)) {
            for (size_t k = 0; k < _flat_columns.size(); k++) { // all is null
                _flat_columns[k]->append_default(1);
            }
            continue;
        }

        auto* obj = json_data->get_object(row);
        auto vslice = obj->to_vslice();
        if (vslice.isNone() || vslice.isNull()) {
            for (size_t k = 0; k < _flat_columns.size(); k++) { // all is null
                _flat_columns[k]->append_default(1);
            }
            continue;
        }

        if (vslice.isEmptyObject() || !vslice.isObject()) {
            for (size_t k = 0; k < _dst_paths.size(); k++) { // remain push object
                _flat_columns[k]->append_default(1);
            }
            if constexpr (HAS_REMAIN) {
                _remain->append(obj);
            }
            continue;
        }
        // to count how many columns hit in json, for append default value
        uint32_t hit_count = 0;
        if constexpr (HAS_REMAIN) {
            vpack::Builder builder;
            builder.add(vpack::Value(vpack::ValueType::Object));
            _flatten_json<HAS_REMAIN>(vslice, _dst_root.get(), &builder, &hit_count);
            builder.close();
            _remain->append(JsonValue(builder.slice()));
        } else {
            _flatten_json<HAS_REMAIN>(vslice, _dst_root.get(), nullptr, &hit_count);
        }

        if (UNLIKELY(hit_count < _dst_paths.size())) {
            for (auto& col : _flat_columns) {
                if (col->size() != row + 1) {
                    DCHECK_EQ(col->size(), row);
                    col->append_default(1);
                }
            }
        }

        for (auto& col : _flat_columns) {
            DCHECK_EQ(col->size(), row + 1);
        }
        if constexpr (HAS_REMAIN) {
            DCHECK_EQ(row + 1, _remain->size());
        }
    }
}

Columns JsonFlattener::mutable_result() {
    Columns res;
    for (size_t i = 0; i < _flat_columns.size(); i++) {
        auto cloned = _flat_columns[i]->clone_empty();
        res.emplace_back(std::move(_flat_columns[i]));
        _flat_columns[i] = std::move(cloned);
    }
    if (_has_remain) {
        _remain = down_cast<JsonColumn*>(_flat_columns.back().get());
    }
    return res;
}

JsonMerger::JsonMerger(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain)
        : _src_paths(std::move(paths)), _has_remain(has_remain) {
    _src_root = std::make_shared<JsonFlatPath>();

    for (size_t i = 0; i < _src_paths.size(); i++) {
        auto* leaf = JsonFlatPath::normalize_from_path(_src_paths[i], _src_root.get());
        leaf->type = types[i];
        leaf->index = i;
    }
}

void JsonMerger::set_exclude_paths(const std::vector<std::string>& exclude_paths) {
    this->_exclude_paths = exclude_paths;
    for (auto& path : _exclude_paths) {
        auto* leaf = JsonFlatPath::normalize_from_path(path, _src_root.get());
        leaf->op = JsonFlatPath::OP_EXCLUDE;
    }
}

// add all level paths, e.g: a.b.c, level path: a.b, leaf path: c
void JsonMerger::_add_level_paths_impl(const std::string_view& path, JsonFlatPath* root) {
    if (path.empty()) {
        return;
    }

    auto [key, next] = JsonFlatPath::split_path(path);
    if (next.empty()) {
        // don't add leaf node
        return;
    }

    auto iter = root->children.find(key);
    JsonFlatPath* child_path = nullptr;

    if (iter == root->children.end()) {
        root->children.emplace(key, std::make_unique<JsonFlatPath>());
        child_path = root->children[key].get();
        child_path->op = JsonFlatPath::OP_NEW_LEVEL;
    } else {
        child_path = iter->second.get();
    }
    _add_level_paths_impl(next, child_path);
}

void JsonMerger::add_level_paths(const std::vector<std::string>& level_paths) {
    this->_level_paths = level_paths;
    for (auto& path : _level_paths) {
        _add_level_paths_impl(path, _src_root.get());
    }
}

void JsonMerger::set_root_path(const std::string& base_path) {
    JsonFlatPath::set_root(base_path, _src_root.get());
}

ColumnPtr JsonMerger::merge(const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    DCHECK(_src_columns.empty());

    _result = NullableColumn::create(JsonColumn::create(), NullColumn::create());
    _json_result = down_cast<JsonColumn*>(down_cast<NullableColumn*>(_result.get())->data_column().get());
    _null_result = down_cast<NullColumn*>(down_cast<NullableColumn*>(_result.get())->null_column().get());
    size_t rows = columns[0]->size();
    _result->reserve(rows);

    for (auto& col : columns) {
        _src_columns.emplace_back(col.get());
    }

    if (_src_root->op == JsonFlatPath::OP_INCLUDE) {
        _merge_impl<true>(rows);
    } else {
        _merge_impl<false>(rows);
    }

    _src_columns.clear();
    if (_output_nullable) {
        down_cast<NullableColumn*>(_result.get())->update_has_null();
        return _result;
    } else {
        return down_cast<NullableColumn*>(_result.get())->data_column();
    }
}

template <bool IN_TREE>
void JsonMerger::_merge_impl(size_t rows) {
    if (_has_remain) {
        auto remain = down_cast<const JsonColumn*>(_src_columns.back());
        for (size_t i = 0; i < rows; i++) {
            auto obj = remain->get_object(i);
            auto vs = obj->to_vslice();
            if (obj->is_invalid()) {
                vpack::Builder builder;
                builder.add(vpack::Value(vpack::ValueType::Object));
                _merge_json(_src_root.get(), &builder, i);
                builder.close();
                auto slice = builder.slice();
                _json_result->append(JsonValue(slice));
                _null_result->append(slice.isEmptyObject());
            } else if (!vs.isObject()) {
                for (int k = 0; k < _src_paths.size(); k++) {
                    // check child column should be null
                    DCHECK(_src_columns[k]->is_null(i));
                }
                _json_result->append(JsonValue(vs));
                _null_result->append(vs.isEmptyObject());
            } else {
                vpack::Builder builder;
                builder.add(vpack::Value(vpack::ValueType::Object));
                _merge_json_with_remain<IN_TREE>(_src_root.get(), &vs, &builder, i);
                builder.close();
                auto slice = builder.slice();
                _json_result->append(JsonValue(slice));
                _null_result->append(slice.isEmptyObject());
            }
        }
    } else {
        for (size_t i = 0; i < rows; i++) {
            vpack::Builder builder;
            builder.add(vpack::Value(vpack::ValueType::Object));
            _merge_json(_src_root.get(), &builder, i);
            builder.close();
            _json_result->append(JsonValue(builder.slice()));
        }
        int zero = 0;
        _null_result->append_value_multiple_times(&zero, rows);
    }
}

template <bool IN_TREE>
void JsonMerger::_merge_json_with_remain(const JsonFlatPath* root, const vpack::Slice* remain, vpack::Builder* builder,
                                         size_t index) {
    // #ifndef NDEBUG
    //     std::string json = remain->toJson();
    // #endif
    vpack::ObjectIterator it(*remain, false);
    for (; it.valid(); it.next()) {
        auto k = it.key().stringView();
        auto v = it.value();

        auto iter = root->children.find(k);
        if (iter == root->children.end()) {
            if constexpr (IN_TREE) {
                // only remain contains
                builder->addUnchecked(k.data(), k.size(), v);
            }
            continue;
        }
        auto* child = iter->second.get();
        if (child->op == JsonFlatPath::OP_EXCLUDE) {
            continue;
        }
        if (v.isObject()) {
            if (child->op == JsonFlatPath::OP_IGNORE) {
                _merge_json_with_remain<false>(child, &v, builder, index);
            } else if (child->op == JsonFlatPath::OP_ROOT) {
                _merge_json_with_remain<true>(child, &v, builder, index);
            } else {
                DCHECK(child->op == JsonFlatPath::OP_INCLUDE || child->op == JsonFlatPath::OP_NEW_LEVEL);
                builder->addUnchecked(k.data(), k.size(), vpack::Value(vpack::ValueType::Object));
                _merge_json_with_remain<true>(child, &v, builder, index);
                builder->close();
            }
            continue;
        }
        // leaf node
        DCHECK(child->op == JsonFlatPath::OP_INCLUDE || child->op == JsonFlatPath::OP_ROOT ||
               child->op == JsonFlatPath::OP_NEW_LEVEL);
        builder->addUnchecked(k.data(), k.size(), v);
    }
    for (auto& [child_name, child] : root->children) {
        if (child->op == JsonFlatPath::OP_EXCLUDE) {
            continue;
        }
        // e.g. flat path: b.b2.b3}
        // json: {"b": {}}
        // we can't output: {"b": {}} to {"b": {"b2": {}}}
        // must direct relation when has REMAIN
        if (child->children.empty() && child->op != JsonFlatPath::OP_NEW_LEVEL) {
            DCHECK(child->op == JsonFlatPath::OP_INCLUDE);
            auto col = _src_columns[child->index];
            if (!col->is_null(index)) {
                DCHECK(flat_json::JSON_MERGE_FUNC.contains(child->type));
                auto func = flat_json::JSON_MERGE_FUNC.at(child->type);
                func(builder, child_name, col, index);
            }
            continue;
        }
    }
}

void JsonMerger::_merge_json(const JsonFlatPath* root, vpack::Builder* builder, size_t index) {
    for (auto& [child_name, child] : root->children) {
        if (child->op == JsonFlatPath::OP_EXCLUDE) {
            continue;
        }

        if (child->children.empty() && child->op != JsonFlatPath::OP_NEW_LEVEL) {
            DCHECK(child->op == JsonFlatPath::OP_INCLUDE || child->op == JsonFlatPath::OP_ROOT);
            auto col = _src_columns[child->index];
            if (!col->is_null(index)) {
                DCHECK(flat_json::JSON_MERGE_FUNC.contains(child->type));
                auto func = flat_json::JSON_MERGE_FUNC.at(child->type);
                func(builder, child_name, col, index);
            }
            continue;
        }

        if (child->op == JsonFlatPath::OP_IGNORE) {
            // don't add level
            _merge_json(child.get(), builder, index);
        } else if (child->op == JsonFlatPath::OP_ROOT) {
            _merge_json(child.get(), builder, index);
        } else {
            builder->addUnchecked(child_name.data(), child_name.size(), vpack::Value(vpack::ValueType::Object));
            _merge_json(child.get(), builder, index);
            builder->close();
        }
    }
}

HyperJsonTransformer::HyperJsonTransformer(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                           bool has_remain)
        : _dst_remain(has_remain), _dst_paths(std::move(paths)), _dst_types(types) {
    for (size_t i = 0; i < _dst_paths.size(); i++) {
        _dst_columns.emplace_back(ColumnHelper::create_column(TypeDescriptor(types[i]), true));
    }

    if (_dst_remain) {
        _dst_columns.emplace_back(JsonColumn::create());
    }
}

void HyperJsonTransformer::init_read_task(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                          bool has_remain) {
    _src_paths = paths;
    _src_types = types;

    _merge_tasks.clear();
    _flat_tasks.clear();
    _pool.clear();

    std::vector<int> equals;
    std::vector<int> merges;
    std::unordered_set<int> check_dst;
    DCHECK(!_dst_remain);
    // equals & merge
    for (size_t i = 0; i < _dst_paths.size(); i++) {
        equals.clear();
        merges.clear();
        for (size_t j = 0; j < _src_paths.size(); j++) {
            if (_dst_paths[i] == _src_paths[j]) {
                equals.emplace_back(j); // equals
#ifdef NDEBUG
                break; // must only one
#endif
            } else if (_src_paths[j].starts_with(_dst_paths[i] + ".")) {
                merges.emplace_back(j);
            }
        }

        DCHECK(equals.empty() || merges.empty());
        if (!equals.empty()) {
            DCHECK_EQ(equals.size(), 1);
            check_dst.emplace(i);
            auto& mk = _merge_tasks.emplace_back();
            mk.is_merge = false;
            mk.src_index.emplace_back(equals[0]);
            mk.dst_index = i;
            if (_dst_types[i] != _src_types[equals[0]]) {
                mk.need_cast = true;
                SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(_src_types[equals[0]]));
                ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(_src_types[equals[0]]),
                                                                    TypeDescriptor(_dst_types[i]), col_ref, &_pool);
            }
        } else if (!merges.empty()) {
            check_dst.emplace(i);
            if (_dst_types[i] != TYPE_JSON && !is_string_type(_dst_types[i])) {
                continue;
            }
            auto& mk = _merge_tasks.emplace_back();
            mk.is_merge = true;
            mk.src_index = merges;
            mk.dst_index = i;
            if (_dst_types[i] != TYPE_JSON) {
                // must be to string, merge result must be string
                mk.need_cast = true;
                SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(TYPE_JSON));
                ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(TYPE_JSON),
                                                                    TypeDescriptor(_dst_types[i]), col_ref, &_pool);
            }
        }
    }

    std::vector<int> flats;
    for (size_t j = 0; j < _src_paths.size(); j++) {
        flats.clear();
        for (size_t i = 0; i < _dst_paths.size(); i++) {
#ifdef NDEBUG
            if (!check_dst.contains(i) && _dst_paths[i].starts_with(_src_paths[j] + ".")) {
#else
            if (_dst_paths[i].starts_with(_src_paths[j] + ".")) {
#endif
                flats.emplace_back(i);
                DCHECK(!check_dst.contains(i));
                check_dst.emplace(i);
            }
        }
        if (!flats.empty()) {
            auto& fk = _flat_tasks.emplace_back();
            fk.src_index = j;
            fk.dst_index = flats;
        }
    }

    if (has_remain && _dst_paths.size() != check_dst.size()) {
        // must from remain
        flats.clear();
        for (size_t i = 0; i < _dst_paths.size(); i++) {
            if (!check_dst.contains(i)) {
                flats.emplace_back(i);
                DCHECK(!check_dst.contains(i));
                check_dst.emplace(i);
            }
        }

        auto& fk = _flat_tasks.emplace_back();
        fk.src_index = _src_paths.size();
        fk.dst_index = flats;
    }

    for (auto& fk : _flat_tasks) {
        std::vector<std::string> p;
        std::vector<LogicalType> t;
        if (fk.src_index == _src_paths.size()) {
            DCHECK(has_remain);
            for (auto& index : fk.dst_index) {
                p.emplace_back(_dst_paths[index]);
                t.emplace_back(_dst_types[index]);
            }
        } else {
            for (auto& index : fk.dst_index) {
                // compute flatten path
                auto dp = _dst_paths[index].substr(_src_paths[fk.src_index].size() + 1);
                p.emplace_back(dp);
                t.emplace_back(_dst_types[index]);
            }
        }
        fk.flattener = std::make_unique<JsonFlattener>(p, t, false);
    }

    for (auto& mk : _merge_tasks) {
        if (mk.is_merge) {
            std::vector<std::string> p;
            std::vector<LogicalType> t;
            for (auto& index : mk.src_index) {
                p.emplace_back(_src_paths[index]);
                t.emplace_back(_src_types[index]);
            }
            if (has_remain) {
                mk.src_index.emplace_back(paths.size());
            }
            mk.merger = std::make_unique<JsonMerger>(p, t, has_remain);
            mk.merger->set_root_path(_dst_paths[mk.dst_index]);
            mk.merger->set_output_nullable(true);
        }
    }
}

void HyperJsonTransformer::init_compaction_task(const std::vector<std::string>& paths,
                                                const std::vector<LogicalType>& types, bool has_remain) {
    _src_paths = paths;
    _src_types = types;

    _merge_tasks.clear();
    _flat_tasks.clear();
    _pool.clear();

    std::unordered_set<std::string> _src_set(_src_paths.begin(), _src_paths.end());

    // output remain, must put merge task at first
    if (_dst_remain) {
        _merge_tasks.emplace_back();
        _merge_tasks.back().dst_index = _dst_paths.size();
        _merge_tasks.back().is_merge = true;
    }

    for (size_t j = 0; j < _src_paths.size(); j++) {
        size_t i = 0;
        for (; i < _dst_paths.size(); i++) {
            DCHECK(!_src_paths[j].starts_with(_dst_paths[i] + "."));
            DCHECK(!_dst_paths[i].starts_with(_src_paths[j] + "."));
            if (_dst_paths[i] == _src_paths[j]) {
                auto& mk = _merge_tasks.emplace_back();
                mk.is_merge = false;
                mk.is_merge = false;
                mk.src_index.emplace_back(j);
                mk.dst_index = i;
                if (_dst_types[i] != _src_types[j]) {
                    mk.need_cast = true;
                    SlotDescriptor source_slot(i, "mock_solt", TypeDescriptor(_src_types[j]));
                    ColumnRef* col_ref = _pool.add(new ColumnRef(&source_slot));
                    mk.cast_expr = VectorizedCastExprFactory::from_type(TypeDescriptor(_src_types[j]),
                                                                        TypeDescriptor(_dst_types[i]), col_ref, &_pool);
                }
                break;
            }
        }

        if (i >= _dst_paths.size() && _dst_remain) {
            _merge_tasks[0].src_index.emplace_back(j);
        }
    }

    std::vector<std::string> all_flat_paths; // for remove from remain
    if (has_remain) {
        for (size_t i = 0; i < _dst_paths.size(); i++) {
            if (_src_set.find(_dst_paths[i]) == _src_set.end()) {
                // merge to remain
                if (_flat_tasks.empty()) {
                    _flat_tasks.emplace_back();
                    _flat_tasks.back().src_index = _src_paths.size();
                }
                _flat_tasks.back().dst_index.emplace_back(i);
            }
        }

        for (size_t i = 0; i < _flat_tasks.size(); i++) {
            auto& fk = _flat_tasks[i];
            std::vector<std::string> p;
            std::vector<LogicalType> t;
            for (auto& index : fk.dst_index) {
                all_flat_paths.emplace_back(_dst_paths[index]);
                p.emplace_back(_dst_paths[index]);
                t.emplace_back(_dst_types[index]);
            }
            fk.flattener = std::make_unique<JsonFlattener>(p, t, false);
        }
    }

    if (_dst_remain) {
        auto& mk = _merge_tasks[0];
        DCHECK(mk.is_merge);
        std::vector<std::string> p;
        std::vector<LogicalType> t;

        for (auto& index : mk.src_index) {
            p.emplace_back(_src_paths[index]);
            t.emplace_back(_src_types[index]);
        }
        mk.merger = std::make_unique<JsonMerger>(p, t, has_remain);
        mk.merger->add_level_paths(_dst_paths);
        mk.merger->set_exclude_paths(all_flat_paths);
        if (has_remain) {
            _merge_tasks[0].src_index.emplace_back(_src_paths.size());
        }
    }

    for (size_t i = 1; i < _merge_tasks.size(); i++) {
        DCHECK(!_merge_tasks[i].is_merge);
    }
}

Status HyperJsonTransformer::trans(const Columns& columns) {
    DCHECK(_dst_remain ? _dst_columns.size() == _dst_paths.size() + 1 : _dst_columns.size() == _dst_paths.size());
    for (auto& col : _dst_columns) {
        DCHECK_EQ(col->size(), 0);
    }
    {
        SCOPED_RAW_TIMER(&_cast_ms);
        for (auto& task : _merge_tasks) {
            if (!task.is_merge) {
                RETURN_IF_ERROR(_equals(task, columns));
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_merge_ms);
        for (auto& task : _merge_tasks) {
            if (task.is_merge) {
                RETURN_IF_ERROR(_merge(task, columns));
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_flat_ms);
        for (auto& task : _flat_tasks) {
            _flat(task, columns);
        }
    }

    size_t rows = columns[0]->size();
    for (size_t i = 0; i < _dst_columns.size(); i++) {
        if (_dst_columns[i]->size() == 0) {
            _dst_columns[i]->append_default(rows);
        } else {
            DCHECK_EQ(rows, _dst_columns[i]->size());
        }
    }
    return Status::OK();
}

Status HyperJsonTransformer::_equals(const MergeTask& task, const Columns& columns) {
    DCHECK(task.src_index.size() == 1);
    if (task.need_cast) {
        auto& col = columns[task.src_index[0]];
        return _cast(task, col);
    }
    _dst_columns[task.dst_index] = columns[task.src_index[0]];
    return Status::OK();
}

Status HyperJsonTransformer::_cast(const MergeTask& task, const ColumnPtr& col) {
    DCHECK(task.need_cast);
    Chunk chunk;
    chunk.append_column(col, task.dst_index);
    ASSIGN_OR_RETURN(auto res, task.cast_expr->evaluate_checked(nullptr, &chunk));
    res->set_delete_state(col->delete_state());

    if (res->only_null()) {
        auto check = _dst_columns[task.dst_index]->append_nulls(col->size());
        DCHECK(check);
    } else if (res->is_constant()) {
        auto data = down_cast<ConstColumn*>(res.get())->data_column();
        _dst_columns[task.dst_index]->append_value_multiple_times(*data, 0, col->size());
    } else if (_dst_columns[task.dst_index]->is_nullable() && !res->is_nullable()) {
        auto nl = NullColumn::create(col->size(), 0);
        _dst_columns[task.dst_index] = NullableColumn::create(res, std::move(nl));
    } else {
        DCHECK_EQ(_dst_columns[task.dst_index]->is_nullable(), res->is_nullable());
        _dst_columns[task.dst_index].swap(res);
    }
    return Status::OK();
}

Status HyperJsonTransformer::_merge(const MergeTask& task, const Columns& columns) {
    if (task.dst_index == _dst_paths.size()) {
        DCHECK(_dst_remain);
        // output to remain
        if (task.src_index.size() == 1 && task.src_index[0] == _src_paths.size() && !task.merger->has_exclude_paths()) {
            // only use remain
            _dst_columns[task.dst_index] = columns[task.src_index[0]];
            return Status::OK();
        }
    }

    if (task.src_index.empty()) {
        // input has no remain and hit columns, but need output remain...
        DCHECK(_dst_remain);
        _dst_columns[task.dst_index]->append_default(columns[0]->size());
        return Status::OK();
    }

    Columns cols;
    for (auto& index : task.src_index) {
        cols.emplace_back(columns[index]);
    }
    auto result = task.merger->merge(cols);
    if (task.need_cast) {
        return _cast(task, result);
    } else {
        _dst_columns[task.dst_index] = result;
    }
    return Status::OK();
}

void HyperJsonTransformer::_flat(const FlatTask& task, const Columns& columns) {
    if (task.dst_index.empty()) {
        return;
    }

    if (task.src_index != _src_types.size() && _src_types[task.src_index] != LogicalType::TYPE_JSON) {
        // not json column, don't need to flatten
        for (size_t i = 0; i < task.dst_index.size(); i++) {
            auto check = _dst_columns[task.dst_index[i]]->append_nulls(columns[0]->size());
            DCHECK(check);
        }
        return;
    }

    task.flattener->flatten(columns[task.src_index].get());
    auto result = task.flattener->mutable_result();

    for (size_t i = 0; i < task.dst_index.size(); i++) {
        _dst_columns[task.dst_index[i]] = result[i];
    }
}

Columns HyperJsonTransformer::mutable_result() {
    Columns res;
    for (size_t i = 0; i < _dst_columns.size(); i++) {
        auto cloned = _dst_columns[i]->clone_empty();
        res.emplace_back(std::move(_dst_columns[i]));
        _dst_columns[i] = std::move(cloned);
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::cast_paths() const {
    std::vector<std::string> res;
    for (auto& task : _merge_tasks) {
        if (task.is_merge) {
            continue;
        }
        for (auto& index : task.src_index) {
            if (index == _src_paths.size()) {
                res.emplace_back("remain");
            } else {
                res.emplace_back(_src_paths[index]);
            }
        }
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::merge_paths() const {
    std::vector<std::string> res;
    for (auto& task : _merge_tasks) {
        if (!task.is_merge) {
            continue;
        }
        for (auto& index : task.src_index) {
            if (index == _src_paths.size()) {
                res.emplace_back("remain");
            } else {
                res.emplace_back(_src_paths[index]);
            }
        }
    }
    return res;
}

std::vector<std::string> HyperJsonTransformer::flat_paths() const {
    std::vector<std::string> res;
    for (auto& task : _flat_tasks) {
        if (task.src_index == _src_paths.size()) {
            res.emplace_back("remain");
        } else {
            res.emplace_back(_src_paths[task.src_index]);
        }
    }
    return res;
}

} // namespace starrocks
