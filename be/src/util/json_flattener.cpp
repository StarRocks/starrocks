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

#include "util/json_flattener.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "gutil/casts.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"
#include "util/flat_json_internal.h"
#include "util/json_path_deriver.h"

namespace starrocks {

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
        : _has_remain(has_remain), _dst_paths(paths) {
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

    // IMPORTANT: Check column integrity to prevent NullableColumn inconsistency
    for (auto& col : _flat_columns) {
        col->check_or_die();
    }
}

template <bool REMAIN>
bool JsonFlattener::_flatten_json(const vpack::Slice& value, const JsonFlatPath* root, vpack::Builder* builder,
                                  uint32_t* hit_count) {
    vpack::ObjectIterator it(value, true);
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
            DCHECK(flat_json::JSON_EXTRACT_FUNC.contains(child->second->type))
                    << "unsupported json type: " << child->second->type;
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

MutableColumns JsonFlattener::mutable_result() {
    MutableColumns res;
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

} // namespace starrocks
