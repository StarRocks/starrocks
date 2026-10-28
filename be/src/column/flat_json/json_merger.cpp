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

#include "column/flat_json/json_merger.h"

#include <velocypack/StringRef.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/flat_json/flat_json_internal.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "types/json_value.h"

namespace starrocks {

namespace {
// The keys that occur more than once in the object `obj`; stored JSON keeps repeated keys. Most objects have none,
// and a sorted object keeps its index in key order, where a repeated key is adjacent: that case costs one pass over
// the index and allocates nothing.
phmap::flat_hash_set<std::string_view> repeated_keys(const vpack::Slice& obj) {
    phmap::flat_hash_set<std::string_view> repeated;
    if (obj.isSorted()) {
        const vpack::ValueLength n = obj.length();
        for (vpack::ValueLength i = 1; i < n; i++) {
            auto key = obj.keyAt(i).stringView();
            if (key == obj.keyAt(i - 1).stringView()) {
                repeated.insert(key);
            }
        }
        return repeated;
    }
    phmap::flat_hash_set<std::string_view> seen;
    for (vpack::ObjectIterator it(obj, true); it.valid(); it.next()) {
        auto key = it.key().stringView();
        if (!seen.insert(key).second) {
            repeated.insert(key);
        }
    }
    return repeated;
}
} // namespace

JsonMerger::JsonMerger(const std::vector<std::string>& paths, const std::vector<LogicalType>& types, bool has_remain)
        : _src_paths(paths), _has_remain(has_remain) {
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
        const JsonFlatPath* node = _src_root.get();
        for (std::string_view rest = path; !rest.empty();) {
            auto [key, next] = JsonFlatPath::split_path(rest);
            node = node->children.at(key).get();
            if (!next.empty()) {
                _above_excluded.insert(node);
            }
            rest = next;
        }
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

    auto [iter, inserted] = root->children.try_emplace(key);
    if (inserted) {
        iter->second = std::make_unique<JsonFlatPath>();
        iter->second->op = JsonFlatPath::OP_NEW_LEVEL;
    }
    _add_level_paths_impl(next, iter->second.get());
}

void JsonMerger::add_level_paths(const std::vector<std::string>& level_paths) {
    this->_level_paths = level_paths;
    for (auto& path : _level_paths) {
        _add_level_paths_impl(path, _src_root.get());
    }
}

void JsonMerger::set_root_path(const std::string& base_path) {
    _merge_root = JsonFlatPath::normalize_from_path(base_path, _src_root.get());
    _root_path.clear();
    for (std::string_view rest = base_path; !rest.empty();) {
        auto [key, next] = JsonFlatPath::split_path(rest);
        _root_path.emplace_back(key);
        rest = next;
    }
    JsonFlatPath::set_root(base_path, _src_root.get());
}

ColumnPtr JsonMerger::merge(const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    DCHECK(_src_columns.empty());

    auto result = NullableColumn::create(JsonColumn::create(), NullColumn::create());
    auto* nullable_result = down_cast<NullableColumn*>(result.get());
    auto* json_result = down_cast<JsonColumn*>(nullable_result->data_column_raw_ptr());
    auto* null_result = down_cast<NullColumn*>(nullable_result->null_column_raw_ptr());
    size_t rows = columns[0]->size();
    result->reserve(rows);

    for (auto& col : columns) {
        _src_columns.emplace_back(col.get());
    }

    _merge_impl(rows, *json_result, *null_result);

    _src_columns.clear();
    if (_output_nullable) {
        nullable_result->update_has_null();
        // IMPORTANT: Check column integrity to prevent NullableColumn inconsistency
        result->check_or_die();
        return result;
    } else {
        auto data_column = nullable_result->data_column();
        // IMPORTANT: Check column integrity to prevent NullableColumn inconsistency
        data_column->check_or_die();
        return data_column;
    }
}

void JsonMerger::_merge_impl(size_t rows, JsonColumn& json_result, NullColumn& null_result) {
    const auto* root = _merge_root == nullptr ? _src_root.get() : _merge_root;
    if (_has_remain) {
        auto remain = down_cast<const JsonColumn*>(_src_columns.back());
        for (size_t i = 0; i < rows; i++) {
            auto obj = remain->get_object(i);
            auto vs = obj->to_vslice();
            // Start at the requested value: it can be an array/scalar, and {} is distinct from a missing path.
            for (const auto& key : _root_path) {
                if (!vs.isObject()) {
                    vs = vpack::Slice::noneSlice();
                    break;
                }
                vs = vs.get(vpack::StringRef(key.data(), key.size()));
            }
            if (obj->is_invalid() || (!_root_path.empty() && vs.isNone())) {
                vpack::Builder builder;
                builder.add(vpack::Value(vpack::ValueType::Object));
                _merge_json(root, &builder, i);
                builder.close();
                auto slice = builder.slice();
                json_result.append(JsonValue(slice));
                null_result.append(slice.isEmptyObject());
            } else if (!vs.isObject()) {
                if (_root_path.empty()) {
                    for (int k = 0; k < _src_paths.size(); k++) {
                        DCHECK(_src_columns[k]->is_null(i));
                    }
                }
                json_result.append(JsonValue(vs));
                null_result.append(0);
            } else {
                vpack::Builder builder;
                builder.add(vpack::Value(vpack::ValueType::Object));
                _merge_json_with_remain<true>(root, &vs, &builder, i);
                builder.close();
                auto slice = builder.slice();
                json_result.append(JsonValue(slice));
                null_result.append(slice.isEmptyObject() && _root_path.empty());
            }
        }
    } else {
        for (size_t i = 0; i < rows; i++) {
            vpack::Builder builder;
            builder.add(vpack::Value(vpack::ValueType::Object));
            _merge_json(root, &builder, i);
            builder.close();
            auto json = builder.slice();
            json_result.append(JsonValue(json));
            null_result.append(json.isEmptyObject());
        }
    }
}

template <bool IN_TREE>
void JsonMerger::_merge_json_with_remain(const JsonFlatPath* root, const vpack::Slice* remain, vpack::Builder* builder,
                                         size_t index, bool keep_excluded) {
    // The keys this object repeats, collected once, on its first key at or above an excluded path.
    std::optional<phmap::flat_hash_set<std::string_view>> repeated;
    auto is_repeated = [&](std::string_view key) {
        if (!repeated.has_value()) {
            repeated = repeated_keys(*remain);
        }
        return repeated->contains(key);
    };
    vpack::ObjectIterator it(*remain, true);
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
            // An excluded path moves to a sub-column, which holds a single value (JsonFlattener takes the one a
            // read of the whole JSON finds). Keep every copy of a repeated key here instead, and the path in every
            // copy of a repeated object above it, so rebuilding the JSON, which prefers the remain, still gives
            // back each of them.
            if (keep_excluded || is_repeated(k)) {
                builder->addUnchecked(k.data(), k.size(), v);
            }
            continue;
        }
        if (v.isObject()) {
            const bool keep = keep_excluded || (_above_excluded.contains(child) && is_repeated(k));
            if (child->op == JsonFlatPath::OP_IGNORE) {
                _merge_json_with_remain<false>(child, &v, builder, index, keep);
            } else if (child->op == JsonFlatPath::OP_ROOT) {
                _merge_json_with_remain<true>(child, &v, builder, index, keep);
            } else {
                DCHECK(child->op == JsonFlatPath::OP_INCLUDE || child->op == JsonFlatPath::OP_NEW_LEVEL);
                bool has_value = false;
                _check_has_non_null_values(child, index, &has_value);
                // When IN_TREE=false, skip empty remain objects that have no flat column values
                if constexpr (!IN_TREE) {
                    if (v.isEmptyObject() && !has_value) {
                        continue;
                    }
                }
                vpack::Builder temp_builder;
                temp_builder.add(vpack::Value(vpack::ValueType::Object));
                // Use IN_TREE=false for empty remain to build from flat columns only,
                // IN_TREE=true otherwise to merge remain and flat columns
                if (v.isEmptyObject()) {
                    _merge_json_with_remain<false>(child, &v, &temp_builder, index, keep);
                } else {
                    _merge_json_with_remain<true>(child, &v, &temp_builder, index, keep);
                }
                temp_builder.close();
                auto temp_slice = temp_builder.slice();
                // When IN_TREE=true, preserve remain keys even if empty to maintain original structure
                if constexpr (IN_TREE) {
                    builder->addUnchecked(k.data(), k.size(), temp_slice);
                } else if (!temp_slice.isEmptyObject()) {
                    builder->addUnchecked(k.data(), k.size(), temp_slice);
                }
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

        // Skip keys already processed from remain in the first loop when IN_TREE=true
        bool key_processed_from_remain = remain->hasKey(vpack::StringRef(child_name.data(), child_name.size()));
        if (key_processed_from_remain) {
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

        // For intermediate nodes not in remain, only create if we have flat values for descendants
        bool has_value = false;
        _check_has_non_null_values(child.get(), index, &has_value);
        if (has_value) {
            builder->addUnchecked(child_name.data(), child_name.size(), vpack::Value(vpack::ValueType::Object));
            _merge_json(child.get(), builder, index);
            builder->close();
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
            // Check if any leaf descendant has value in this row
            // If yes, create the object structure; if no, skip to avoid creating empty objects
            bool has_value = false;
            _check_has_non_null_values(child.get(), index, &has_value);

            if (has_value) {
                builder->addUnchecked(child_name.data(), child_name.size(), vpack::Value(vpack::ValueType::Object));
                _merge_json(child.get(), builder, index);
                builder->close();
            }
        }
    }
}

void JsonMerger::_check_has_non_null_values(const JsonFlatPath* root, size_t index, bool* has_non_null_values) {
    for (auto& [child_name, child] : root->children) {
        if (child->op == JsonFlatPath::OP_EXCLUDE) {
            continue;
        }

        if (child->children.empty() && child->op != JsonFlatPath::OP_NEW_LEVEL) {
            // Leaf node - check if the value is not null
            auto col = _src_columns[child->index];
            if (!col->is_null(index)) {
                *has_non_null_values = true;
                return;
            }
        } else {
            // Non-leaf node - recursively check children
            _check_has_non_null_values(child.get(), index, has_non_null_values);
            if (*has_non_null_values) {
                return;
            }
        }
    }
}

} // namespace starrocks
