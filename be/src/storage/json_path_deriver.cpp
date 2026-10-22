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

#include "storage/json_path_deriver.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/string/string_parser.hpp"
#include "column/flat_json/flat_json_internal.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/block_split_bloom_filter.h"
#include "common/bloom_filter.h"
#include "common/config_exec_fwd.h"
#include "common/config_json_flat_fwd.h"
#include "common/status.h"
#include "fmt/format.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "storage/rowset/column_reader.h"
#include "storage_primitive/flat_json_config.h"
#include "types/json_value.h"
#include "types/logical_type.h"

namespace starrocks {

static const double FILTER_TEST_FPP[]{0.05, 0.1, 0.15, 0.2, 0.25, 0.3};
static const uint64_t FILTER_MAX_ELEMNT_NUMS = 1024;
// The trie keeps at most kNodesPerFlatColumn nodes per column json_flat_column_max allows to extract
// (and never budgets for fewer than kMinColumnBudget columns): plenty for nested documents whose
// keys repeat, while bounding documents whose key names are data.
static constexpr int kMinColumnBudget = 100;
static constexpr size_t kNodesPerFlatColumn = 64;

// Hits of an already-flattened path that the complex-type check may count as base-typed values
// (JsonFlatPath::flat_base_type_hits). Values only reach a non-JSON sub-column after being flattened
// as a base type; a JSON sub-column holds complex values, so it stays subject to
// enable_json_flat_complex_type exactly as when deriving from data.
static uint64_t base_type_hits(LogicalType sub_column_type, uint64_t hits) {
    return sub_column_type == LogicalType::TYPE_JSON ? 0 : hits;
}

// Value range of a BIGINT sub-column; nullopt when unknown.
using BigintRange = std::optional<std::pair<int64_t, int64_t>>;

static BigintRange bigint_range_of(const ColumnReader* sub) {
    const ZoneMapPB* zm = sub->segment_zone_map();
    if (zm == nullptr) {
        return std::nullopt;
    }
    if (!zm->has_not_null()) {
        return std::make_pair(int64_t{0}, int64_t{0});
    }
    if (!zm->has_min() || !zm->has_max()) {
        return std::nullopt;
    }
    StringParser::ParseResult min_result;
    StringParser::ParseResult max_result;
    auto min = StringParser::string_to_int<int64_t>(zm->min().data(), zm->min().size(), &min_result);
    auto max = StringParser::string_to_int<int64_t>(zm->max().data(), zm->max().size(), &max_result);
    if (min_result != StringParser::PARSE_SUCCESS || max_result != StringParser::PARSE_SUCCESS) {
        return std::nullopt;
    }
    return std::make_pair(min, max);
}

static BigintRange bigint_range_of(const Column& sub) {
    if (sub.is_nullable()) {
        const auto& nullable = down_cast<const NullableColumn&>(sub);
        const auto* data = down_cast<const Int64Column*>(nullable.data_column().get());
        int64_t min = 0;
        int64_t max = 0;
        for (size_t i = 0; i < sub.size(); i++) {
            if (!nullable.is_null(i)) {
                min = std::min(min, data->get_data()[i]);
                max = std::max(max, data->get_data()[i]);
            }
        }
        return std::make_pair(min, max);
    }
    return std::nullopt;
}

// Accounts an already-flattened sub-column of type `type` holding `hits` values to `leaf`.
//
// The layout derived from sub-columns is read by casting each source sub-column to the derived type, so the
// derived type must hold every source value exactly; dfs_keep_numbers_exact() checks that from the value range
// of the path. A LARGEINT sub-column only exists for values past INT64_MAX; a BIGINT sub-column contributes its
// value range, or the whole BIGINT range when it is unknown.
static void add_flat_sub_column(JsonFlatPath* leaf, LogicalType type, uint64_t hits, const BigintRange& range) {
    leaf->json_type &= flat_json::LOGICAL_TYPE_TO_JSON_BITS.at(type);
    if (type == LogicalType::TYPE_LARGEINT) {
        leaf->max_uint = std::numeric_limits<uint64_t>::max();
    } else if (type == LogicalType::TYPE_BIGINT) {
        const auto [min, max] = range.value_or(
                std::make_pair(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()));
        leaf->min_int = std::min(leaf->min_int, min);
        if (max > 0) {
            leaf->max_uint = std::max(leaf->max_uint, static_cast<uint64_t>(max));
        }
    }
    leaf->hits += hits;
    leaf->flat_base_type_hits += base_type_hits(type, hits);
}

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
StatusOr<size_t> JsonPathDeriver::check_null_factor(const std::vector<const Column*>& json_datas) {
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
    if (null_count > total_rows * _max_json_null_factor) {
        VLOG(8) << "flat json, null_count[" << null_count << "], row[" << total_rows
                << "], null_factor: " << _max_json_null_factor;
        return Status::InternalError("json flat null factor too high");
    }

    return total_rows - null_count;
}

JsonPathDeriver::JsonPathDeriver()
        : _min_json_sparsity_factory(config::json_flat_sparsity_factor),
          _max_json_null_factor(config::json_flat_null_factor),
          _max_column(config::json_flat_column_max) {}

JsonPathDeriver::JsonPathDeriver(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                 bool has_remain)
        : JsonPathDeriver() {
    _has_remain = has_remain;
    _paths = paths;
    _types = types;
    for (size_t i = 0; i < _paths.size(); i++) {
        auto* leaf = JsonFlatPath::normalize_from_path(_paths[i], _path_root.get());
        leaf->type = types[i];
        leaf->index = i;
    }
}

void JsonPathDeriver::init_flat_json_config(const FlatJsonConfig* flat_json_config) {
    if (flat_json_config != nullptr) {
        _max_json_null_factor = flat_json_config->get_flat_json_null_factor();
        _min_json_sparsity_factory = flat_json_config->get_flat_json_sparsity_factor();
        _max_column = flat_json_config->get_flat_json_max_column_max();
    } else {
        _max_json_null_factor = config::json_flat_null_factor;
        _min_json_sparsity_factory = config::json_flat_sparsity_factor;
        _max_column = config::json_flat_column_max;
    }
}

void JsonPathDeriver::derived(const std::vector<const Column*>& json_datas) {
    DCHECK(_paths.empty());
    DCHECK(_types.empty());
    // DCHECK(_derived_maps.empty());
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
    // init path by flat JSON
    _derived_on_flat_json(json_datas);
    _max_nodes = static_cast<size_t>(std::max(_max_column, kMinColumnBudget)) * kNodesPerFlatColumn;
    _num_nodes = _count_nodes(_path_root.get());

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

    root->hits += hits;
    root->json_type = flat_json::JSON_BASE_TYPE_BITS;

    auto [key, next] = JsonFlatPath::split_path(path);
    auto [iter, inserted] = root->children.try_emplace(key);
    if (inserted) {
        iter->second = std::make_unique<JsonFlatPath>();
    }

    return _normalize_exists_path(next, iter->second.get(), hits);
}

void JsonPathDeriver::derived(const std::vector<const ColumnReader*>& json_readers) {
    DCHECK(_paths.empty());
    DCHECK(_types.empty());
    // DCHECK(_derived_maps.empty());
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
            add_flat_sub_column(
                    leaf, sub->column_type(), reader->num_rows(),
                    sub->column_type() == LogicalType::TYPE_BIGINT ? bigint_range_of(sub.get()) : std::nullopt);
        }
    }

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
            add_flat_sub_column(leaf, types[i], hits,
                                types[i] == LogicalType::TYPE_BIGINT ? bigint_range_of(*json_col->get_flat_field(i))
                                                                     : std::nullopt);
        }
    }
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

    size_t ignore_max = _total_rows * _min_json_sparsity_factory;
    ignore_max = _total_rows > ignore_max ? _total_rows - ignore_max : 0;
    const size_t check_batch = std::max<size_t>(1, config::vector_chunk_size);
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

        // we required the hit-rate of the path >= _min_json_sparsity_factor, so the max number of missed rows is (1 - _min_json_sparsity_factor) * total_rows.
        // if the number of missed rows exceeds (1 - _min_json_sparsity_factor) * total_rows, then the path must not be extracted.
        // Nothing can be ruled out that way before ignore_max rows, so start checking there.
        const size_t row = mark_row + i;
        if (row > ignore_max && (row % check_batch) == 0) {
            _clean_sparsity_path("", _path_root.get(), row - ignore_max);
            _num_nodes = _count_nodes(_path_root.get());
        }
        if (!_path_tree_full && _max_nodes > 0 && _num_nodes >= _max_nodes) {
            _shrink_path_tree(row);
        }
    }
}

void JsonPathDeriver::_clean_sparsity_path(const std::string_view& name, JsonFlatPath* node, size_t check_hits_min) {
    for (auto& [key, child] : node->children) {
        _clean_sparsity_path(key, child.get(), check_hits_min);
    }
    auto iter = node->children.begin();
    while (iter != node->children.end()) {
        auto child = iter->second.get();
        if (child->hits < check_hits_min) {
            if (_generate_filter) {
                _remain_keys.insert(iter->first);
                _add_remain_subtree(child);
            }
            node->remain = true;
            iter = node->children.erase(iter);
        } else {
            iter++;
        }
    }
    if (_generate_filter && node->remain) {
        _remain_keys.insert(name);
    }
    if (_remain_keys.size() > FILTER_MAX_ELEMNT_NUMS) {
        _generate_filter = false;
        _remain_keys.clear();
    }
}

size_t JsonPathDeriver::_count_nodes(const JsonFlatPath* node) {
    size_t n = node->children.size();
    for (const auto& [_, child] : node->children) {
        n += _count_nodes(child.get());
    }
    return n;
}

void JsonPathDeriver::_add_remain_key(const std::string_view& key) {
    if (!_generate_filter) {
        return;
    }
    _remain_keys.insert(key);
    if (_remain_keys.size() > FILTER_MAX_ELEMNT_NUMS) {
        _generate_filter = false;
        _remain_keys.clear();
    }
}

void JsonPathDeriver::_add_remain_keys_in(const vpack::Slice& value) {
    if (!value.isObject()) {
        return;
    }
    for (vpack::ObjectIterator it(value, true); _generate_filter && it.valid(); it.next()) {
        auto current = (*it);
        _add_remain_key(current.key.stringView());
        _add_remain_keys_in(current.value);
    }
}

void JsonPathDeriver::_add_remain_subtree(const JsonFlatPath* node) {
    for (auto iter = node->children.begin(); _generate_filter && iter != node->children.end(); ++iter) {
        _add_remain_key(iter->first);
        _add_remain_subtree(iter->second.get());
    }
}

// Drops paths seen in a single row so far (other than the current one), like _clean_sparsity_path
// drops the ones that can no longer reach the sparsity threshold.
size_t JsonPathDeriver::_drop_singleton_paths(JsonFlatPath* node, size_t current_row) {
    size_t dropped = 0;
    for (auto& [_, child] : node->children) {
        dropped += _drop_singleton_paths(child.get(), current_row);
    }
    auto iter = node->children.begin();
    while (iter != node->children.end()) {
        const auto* child = iter->second.get();
        if (child->hits < 2 && child->last_row != current_row) {
            _add_remain_key(iter->first);
            _add_remain_subtree(child);
            node->remain = true;
            iter = node->children.erase(iter);
            dropped++;
        } else {
            ++iter;
        }
    }
    return dropped;
}

void JsonPathDeriver::_shrink_path_tree(size_t current_row) {
    _drop_singleton_paths(_path_root.get(), current_row);
    _num_nodes = _count_nodes(_path_root.get());
    if (_num_nodes >= _max_nodes) {
        _path_tree_full = true;
        VLOG(2) << "flat json path tree full at row " << current_row << ", nodes: " << _num_nodes;
    }
}

void JsonPathDeriver::_visit_json_paths(const vpack::Slice& value, JsonFlatPath* root, size_t mark_row) {
    vpack::ObjectIterator it(value, true);

    for (; it.valid(); it.next()) {
        auto current = (*it);
        // sub-object?
        auto v = current.value;
        auto k = current.key.stringView();

        auto iter = root->children.find(k);
        if (iter == root->children.end()) {
            // The budget also holds within a row: a single row can carry more paths than the whole
            // trie may have, so stop adding as soon as it is reached, not only between rows.
            if (_path_tree_full || (_max_nodes > 0 && _num_nodes >= _max_nodes)) {
                // Never flattened, so it stays in this node's remain.
                root->remain = true;
                _add_remain_key(k);
                _add_remain_keys_in(v);
                continue;
            }
            iter = root->children.emplace(k, std::make_unique<JsonFlatPath>()).first;
            _num_nodes++;
        }
        auto child = iter->second.get();
        child->hits++;
        child->multi_times += (child->last_row == mark_row);
        child->last_row = mark_row;

        if (v.isObject()) {
            // If we have seen any non-object value on the same key before, keep parent as remain.
            // This covers array<->object and primitive<->object conflicts and preserves the original structure.
            if (child->hits > child->object_count + 1) {
                root->remain = true;
            }
            child->object_count++;
            // Accumulate remain status: if node is ever empty in any row, mark as remain
            child->remain |= v.isEmptyObject();
            child->json_type = flat_json::JSON_BASE_TYPE_BITS;
            _visit_json_paths(v, child, mark_row);
        } else { // NOTE that array is also treated as primitive here.
            // If this node was previously visited as object, but now we see a primitive,
            // this indicates a type mismatch: path tree expects object but actual data has primitive.
            // Mark the parent node as remain to preserve the actual data structure.
            if (!child->children.empty()) {
                root->remain = true;
            }
            vpack::ValueType json_type = v.type();
            child->json_type = flat_json::get_compatibility_type(json_type, child->json_type);
            child->base_type_count += flat_json::JSON_BASE_TYPE.count(json_type);
            if (json_type == vpack::ValueType::UInt) {
                child->max_uint = std::max(child->max_uint, v.getUIntUnchecked());
            } else if (json_type == vpack::ValueType::Int || json_type == vpack::ValueType::SmallInt) {
                const int64_t value = v.getInt();
                if (value < 0) {
                    child->min_int = std::min(child->min_int, value);
                } else {
                    child->max_uint = std::max(child->max_uint, static_cast<uint64_t>(value));
                }
            }
        }
    }
}

// A numeric type the bits meet at must hold every value of the path exactly, or the value read back differs from
// the one loaded. Integers and doubles meet at DOUBLE, which rounds integers past 2^53; negative and unsigned
// integers meet at LARGEINT, which is rebuilt as uint64 (JsonMerger). Such a path is flattened as JSON instead,
// which keeps each value as it was.
void dfs_keep_numbers_exact(JsonFlatPath* node) {
    constexpr int64_t kMaxExactDoubleInt = int64_t{1} << 53;
    const bool past_double =
            node->max_uint > static_cast<uint64_t>(kMaxExactDoubleInt) || node->min_int < -kMaxExactDoubleInt;
    const bool past_bigint = node->max_uint > static_cast<uint64_t>(RunTimeTypeLimits<TYPE_BIGINT>::max_value());
    if ((node->json_type == flat_json::JSON_TYPE_BITS.at(vpack::ValueType::Double) && past_double) ||
        (node->json_type == flat_json::JSON_TYPE_BITS.at(vpack::ValueType::UInt) && node->min_int < 0 && past_bigint)) {
        node->json_type = flat_json::JSON_BASE_TYPE_BITS;
    }
    for (auto& [_, child] : node->children) {
        dfs_keep_numbers_exact(child.get());
    }
}

// Helper to check and update uint to bigint recursively
void dfs_downgrade_uint(JsonFlatPath* node) {
    int128_t max = RunTimeTypeLimits<TYPE_BIGINT>::max_value();
    if (node->json_type == flat_json::JSON_TYPE_BITS.at(vpack::ValueType::UInt) && node->max_uint <= max) {
        node->json_type = flat_json::JSON_BIGINT_TYPE_BITS;
    }
    for (auto& [_, child] : node->children) {
        dfs_downgrade_uint(child.get());
    }
}

// why dfs? because need compute parent isn't extract base on bottom-up, stack is not suitable
uint32_t JsonPathDeriver::_dfs_finalize(JsonFlatPath* node, const std::string& absolute_path,
                                        std::vector<std::pair<JsonFlatPath*, std::string>>* hit_leaf) {
    // Type conflict: node has both object and primitive values, flatten as TYPE_JSON
    if (!absolute_path.empty() && !node->children.empty()) {
        if (node->base_type_count > 0) {
            for (auto& [key, child] : node->children) {
                child->remain = true;
            }
            hit_leaf->emplace_back(node, absolute_path);
            node->type = LogicalType::TYPE_JSON;
            node->remain = false;
            return 1;
        }
    }

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

        bool is_base_type = node->base_type_count + node->flat_base_type_hits >=
                            node->hits - (node->hits * config::json_flat_complex_type_factor);
        bool type_check = config::enable_json_flat_complex_type || is_base_type;
        if (type_check && node->multi_times <= 0 && node->hits >= _total_rows * _min_json_sparsity_factory) {
            hit_leaf->emplace_back(node, absolute_path);
            node->type = flat_json::JSON_BITS_TO_LOGICAL_TYPE.at(node->json_type);
            node->remain = false;
            return 1;
        } else {
            node->remain = true;
            return 0;
        }
    } else {
        // For intermediate nodes: mark as remain if not all children are flattened,
        // or if the node itself was ever empty in any row (to preserve structures like {"100": {}})
        if (flat_count != node->children.size()) {
            node->remain = true;
        } else if (!absolute_path.empty() && node->remain) {
            // Node was marked as remain because it was empty in some row(s)
            // Keep it as remain even if all children are flattened
        }
        return 1;
    }
}
void dfs_add_remain_keys(JsonFlatPath* node, std::unordered_set<std::string_view>* remain_keys) {
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
    dfs_keep_numbers_exact(_path_root.get());
    // try downgrade json-uint to bigint
    dfs_downgrade_uint(_path_root.get());

    std::vector<std::pair<JsonFlatPath*, std::string>> hit_leaf;
    _dfs_finalize(_path_root.get(), "", &hit_leaf);

    // sort by name, just for stable order
    std::sort(hit_leaf.begin(), hit_leaf.end(),
              [&](const auto& a, const auto& b) { return a.first->hits > b.first->hits; });
    size_t limit = _max_column > 0 ? _max_column : std::numeric_limits<size_t>::max();
    for (size_t i = limit; i < hit_leaf.size(); i++) {
        if (!hit_leaf[i].first->remain && hit_leaf[i].first->hits >= _total_rows) {
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

    dfs_add_remain_keys(_path_root.get(), &_remain_keys);
    _has_remain |= _path_root->remain;
    if (_has_remain && _generate_filter) {
        if (_remain_keys.size() > FILTER_MAX_ELEMNT_NUMS) {
            _generate_filter = false;
            _remain_keys.clear();
            return;
        }
        double fpp = estimate_filter_fpp(_remain_keys.size());
        if (fpp < 0) {
            _remain_keys.clear();
            return;
        }
        std::unique_ptr<BloomFilter> bf;
        Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
        DCHECK(st.ok());
        st = bf->init(_remain_keys.size(), fpp, HASH_MURMUR3_X64_64);
        DCHECK(st.ok());
        for (const auto& key : _remain_keys) {
            bf->add_bytes(key.data(), key.size());
        }
        _remain_filter = std::move(bf);
        _remain_keys.clear();
    }
}

} // namespace starrocks
