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

#include "storage/rowset/json_column_compactor.h"

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "column/column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "storage/rowset/column_writer.h"
#include "types/constexpr.h"
#include "util/json_flattener.h"

namespace starrocks {
Status FlatJsonColumnCompactor::append(const Column& column) {
    // compaction will reuse column, must copy in there.
    _json_datas.emplace_back(column.clone());

    _estimate_size += column.byte_size();
    return Status::OK();
}

Status FlatJsonColumnCompactor::_compact_columns(Columns& json_datas) {
    // all json datas must full json
    JsonPathDeriver deriver;
    std::vector<const Column*> vc;
    for (const auto& js : json_datas) {
        vc.emplace_back(js.get());
    }
    deriver.set_generate_filter(true);
    deriver.derived(vc);

    _flat_paths = deriver.flat_paths();
    _flat_types = deriver.flat_types();
    _has_remain = deriver.has_remain_json();
    _remain_filter = deriver.remain_fitler();

    VLOG(2) << "FlatJsonColumnCompactor compact_columns, json_datas size: " << json_datas.size()
            << ", flat json: " << JsonFlatPath::debug_flat_json(_flat_paths, _flat_types, _has_remain);

    if (_flat_paths.empty()) {
        // write json directly
        return _merge_columns(json_datas);
    }
    return _flatten_columns(json_datas);
}

bool check_is_same_schema(const JsonColumn* one, const JsonColumn* two) {
    if (one == nullptr || two == nullptr) {
        return false;
    }

    if (one->is_flat_json() && two->is_flat_json()) {
        return one->flat_column_paths() == two->flat_column_paths() &&
               one->flat_column_types() == two->flat_column_types() && one->has_remain() == two->has_remain();
    }
    return false;
}

Status FlatJsonColumnCompactor::_merge_columns(Columns& json_datas) {
    VLOG(2) << "FlatJsonColumnCompactor merge_columns, json_datas: " << json_datas.size();
    _is_flat = false;
    _json_meta->mutable_json_meta()->set_has_remain(false);
    _json_meta->mutable_json_meta()->set_is_flat(false);

    const JsonColumn* pre_col = nullptr;
    std::unique_ptr<JsonMerger> merger = nullptr;
    for (auto& col : json_datas) {
        const JsonColumn* json_col;
        NullColumnPtr null_col;
        if (col->is_nullable()) {
            auto nullable_column = down_cast<const NullableColumn*>(col.get());
            json_col = down_cast<const JsonColumn*>(nullable_column->data_column().get());
            null_col = nullable_column->null_column();
        } else {
            json_col = down_cast<const JsonColumn*>(col.get());
        }

        if (!json_col->is_flat_json()) {
            VLOG(2) << "FlatJsonColumnCompactor merge_columns direct write";
            RETURN_IF_ERROR(_json_writer->append(*col));
        } else {
            VLOG(2) << "FlatJsonColumnCompactor merge_columns merge: " << json_col->debug_flat_paths();
            if (!check_is_same_schema(pre_col, json_col)) {
                merger = std::make_unique<JsonMerger>(json_col->flat_column_paths(), json_col->flat_column_types(),
                                                      json_col->has_remain());
                pre_col = json_col;
            }
            auto j = merger->merge(json_col->get_flat_fields_ptrs());

            if (col->is_nullable()) {
                auto n = NullableColumn::create(j, null_col);
                n->set_has_null(col->has_null());
                RETURN_IF_ERROR(_json_writer->append(*n));
            } else {
                RETURN_IF_ERROR(_json_writer->append(*j));
            }
        }
        col->resize_uninitialized(0);
    }
    return Status::OK();
}

Status FlatJsonColumnCompactor::_flatten_columns(Columns& json_datas) {
    VLOG(2) << "FlatJsonColumnCompactor flatten_columns, json_datas: " << json_datas.size();
    _is_flat = true;

    // init flattener first, the flat_paths/types will change in _init_flat_writers
    JsonFlattener flattener(_flat_paths, _flat_types, _has_remain);
    HyperJsonTransformer transformer(_flat_paths, _flat_types, _has_remain);

    RETURN_IF_ERROR(_init_flat_writers());
    JsonColumn* pre_col = nullptr;
    for (auto& col : json_datas) {
        JsonColumn* json_col;
        if (col->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(col.get());
            json_col = down_cast<JsonColumn*>(nullable_column->data_column().get());
        } else {
            json_col = down_cast<JsonColumn*>(col.get());
        }

        if (!json_col->is_flat_json()) {
            VLOG(2) << "FlatJsonColumnCompactor flatten_columns flat json.";
            flattener.flatten(json_col);
            _flat_columns = flattener.mutable_result();
        } else {
            if (!check_is_same_schema(pre_col, json_col)) {
                transformer.init_compaction_task(json_col->flat_column_paths(), json_col->flat_column_types(),
                                                 json_col->has_remain());
                pre_col = json_col;
            }
            VLOG(2) << "FlatJsonColumnCompactor flatten_columns hyper-transformer: " << json_col->debug_flat_paths();
            RETURN_IF_ERROR(transformer.trans(json_col->get_flat_fields_ptrs()));
            _flat_columns = transformer.mutable_result();
        }

        // recode null column in 1st
        if (_json_meta->is_nullable()) {
            auto nulls = NullColumn::create();
            uint8_t IS_NULL = 1;
            uint8_t NOT_NULL = 0;
            if (col->only_null()) {
                nulls->append_value_multiple_times(&IS_NULL, col->size());
            } else if (col->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(col.get());
                auto* nl = down_cast<NullColumn*>(nullable_column->null_column().get());
                nulls->append(*nl, 0, nl->size());
            } else {
                nulls->append_value_multiple_times(&NOT_NULL, col->size());
            }

            _flat_columns.insert(_flat_columns.begin(), std::move(nulls));
        }

        RETURN_IF_ERROR(_write_flat_column());
        _flat_columns.clear();
        col->resize_uninitialized(0); // release after write
    }

    return Status::OK();
}

Status FlatJsonColumnCompactor::finish() {
    RETURN_IF_ERROR(_compact_columns(_json_datas));
    _json_datas.clear(); // release after write
    for (auto& iter : _flat_writers) {
        RETURN_IF_ERROR(iter->finish());
    }
    return _json_writer->finish();
}

Status JsonColumnCompactor::append(const Column& column) {
    const JsonColumn* json_col;
    NullColumnPtr nulls = nullptr;
    if (column.is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(&column);
        nulls = nullable_column->null_column();
        json_col = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    } else {
        json_col = down_cast<const JsonColumn*>(&column);
    }

    if (!json_col->is_flat_json()) {
        return _json_writer->append(column);
    }

    JsonMerger merger(json_col->flat_column_paths(), json_col->flat_column_types(), json_col->has_remain());
    auto p = merger.merge(json_col->get_flat_fields_ptrs());

    if (column.is_nullable()) {
        auto n = NullableColumn::create(p, nulls);
        return _json_writer->append(*n);
    } else {
        return _json_writer->append(*p);
    }
}

Status JsonColumnCompactor::finish() {
    _json_meta->mutable_json_meta()->set_format_version(kJsonMetaDefaultFormatVersion);
    _json_meta->mutable_json_meta()->set_has_remain(false);
    _json_meta->mutable_json_meta()->set_is_flat(false);
    return _json_writer->finish();
}

} // namespace starrocks
