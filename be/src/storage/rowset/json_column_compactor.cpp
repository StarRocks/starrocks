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
#include "column/flat_json/json_flat_path.h"
#include "column/flat_json/json_flattener.h"
#include "column/flat_json/json_merger.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "exprs/hyper_json_transformer.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "storage/flat_json_metrics.h"
#include "storage/json_path_deriver.h"
#include "storage/rowset/column_writer.h"
#include "types/constexpr.h"

namespace starrocks {
Status FlatJsonColumnCompactor::append(const Column& column) {
    // compaction will reuse the column, must copy in there.
    _json_datas.emplace_back(column.clone());

    _estimate_size += column.byte_size();
    return Status::OK();
}

Status FlatJsonColumnCompactor::_compact_columns(MutableColumns& json_datas) {
    // all json datas must full json
    JsonPathDeriver deriver;
    std::vector<const Column*> vc;
    for (const auto& js : json_datas) {
        vc.emplace_back(js.get());
    }
    deriver.set_generate_filter(true);
    deriver.init_flat_json_config(_flat_json_config);

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

    auto st = _flatten_columns(json_datas);
    if (st.ok()) {
        return Status::OK();
    }

    // The load path already survives this: FlatJsonColumnWriter::finish() falls back to plain JSON
    // when flattening fails, most often because a subfield exceeds config::olap_string_max_length.
    // Compaction had no such fallback, so a row that was accepted at write time made every later
    // compaction of that tablet fail forever.
    //
    // This fallback deliberately sits here rather than in finish(): the _flat_paths.empty() branch
    // above has already gone through _merge_columns(), and retrying it after a failure there would
    // append every row a second time.

    FlatJsonMetrics::instance()->flat_json_compaction_fallback_total.increment(1);

    // json_datas holds one entry per append(), i.e. per chunk, not per row -- and the sources are
    // all still intact here precisely because the release was moved out of the flatten loop.
    size_t num_rows = 0;
    for (const auto& col : json_datas) {
        num_rows += col->size();
    }
    // Lead with the unique id: _column_name comes from ColumnWriterOptions::field_name, which
    // SegmentWriter only fills in for the load path, so it is empty during compaction.
    //
    // Only the status code, never its message. The failure this fallback exists for is
    // StringColumnWriter's length check, whose message embeds the entire offending value
    // ("string length({}) > limit({}), string: {}") -- 1.05MB of base64 per occurrence in the
    // incident that prompted this fix. That value is a JSON subfield of a customer document, so
    // truncating it to a few hundred bytes bounds the log without changing what it is: their
    // payload, written into be.WARNING.
    LOG(WARNING) << "FlatJsonColumnCompactor falls back to plain json, column unique_id=" << _json_meta->unique_id()
                 << (_column_name.empty() ? "" : " (" + _column_name + ")") << ", chunks: " << json_datas.size()
                 << ", rows: " << num_rows << ", reason: " << st.code_as_string();

    // _flatten_columns() may have run partway: _init_flat_writers() can have created sub-writers and
    // stamped is_flat=true plus per-sub-column children onto _json_meta before _write_flat_column()
    // failed. Drop that half-built state before handing the data to _json_writer. The invariant
    // checked downstream by get_next_rowid()/write_data()/write_*_index() is
    // _flat_writers.empty() iff !_is_flat, and a stale is_flat=true would also mislead the reader
    // into looking for flat sub-columns that do not exist on disk.
    _flat_writers.clear();
    _flat_paths.clear();
    _flat_types.clear();
    _flat_columns.clear();
    _subcolumn_dict_valid.clear();
    _has_remain = false;
    _remain_filter.reset();
    _json_meta->clear_children_columns();
    _json_meta->mutable_json_meta()->clear_remain_filter();
    // _merge_columns() itself resets is_flat and has_remain on the meta.
    return _merge_columns(json_datas);
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

Status FlatJsonColumnCompactor::_merge_columns(MutableColumns& json_datas) {
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
            json_col = down_cast<const JsonColumn*>(nullable_column->data_column_raw_ptr());
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
            auto j = merger->merge(json_col->get_flat_fields());

            if (col->is_nullable()) {
                auto n_ptr = NullableColumn::create(j, null_col)->as_mutable_raw_ptr();
                auto* n = down_cast<NullableColumn*>(n_ptr);
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

Status FlatJsonColumnCompactor::_flatten_columns(MutableColumns& json_datas) {
    FlatJsonMetrics::instance()->flat_json_compaction_total.increment(1);
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
            json_col = down_cast<JsonColumn*>(nullable_column->data_column_raw_ptr());
        } else {
            json_col = down_cast<JsonColumn*>(col.get());
        }

        if (!json_col->is_flat_json()) {
            VLOG(2) << "FlatJsonColumnCompactor flatten_columns flat json.";
            flattener.flatten(json_col);
            _flat_columns = flattener.mutable_result();
        } else {
            if (!check_is_same_schema(pre_col, json_col)) {
                // Only a difference from a schema we have already seen is a change. pre_col is null
                // until the first flat input, and check_is_same_schema() answers false for null, so
                // counting unconditionally here made every compaction of a flat column report at least
                // one "schema change" -- the metric measured "had a flat input", not form drift.
                if (pre_col != nullptr) {
                    FlatJsonMetrics::instance()->flat_json_compaction_schema_change_total.increment(1);
                }
                transformer.init_compaction_task(json_col->flat_column_paths(), json_col->flat_column_types(),
                                                 json_col->has_remain());
                pre_col = json_col;
            }
            VLOG(2) << "FlatJsonColumnCompactor flatten_columns hyper-transformer: " << json_col->debug_flat_paths();
            RETURN_IF_ERROR(transformer.trans(json_col->get_flat_fields()));
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
                const auto* nl = down_cast<const NullColumn*>(nullable_column->null_column_raw_ptr());
                nulls->append(*nl, 0, nl->size());
            } else {
                nulls->append_value_multiple_times(&NOT_NULL, col->size());
            }

            _flat_columns.insert(_flat_columns.begin(), std::move(nulls));
        }

        RETURN_IF_ERROR(_write_flat_column());
        _flat_columns.clear();
    }

    // Release only once every column has been written. Releasing inside the loop leaves the caller's
    // fallback with nothing to fall back to: a failure at column k has already destroyed columns
    // 0..k-1, so re-writing what is left would silently drop their rows. FlatJsonColumnWriter's
    // _flat_column() is structured the same way, for the same reason.
    for (auto& col : json_datas) {
        col->resize_uninitialized(0);
    }
    return Status::OK();
}

Status FlatJsonColumnCompactor::finish() {
    RETURN_IF_ERROR(_compact_columns(_json_datas));
    _json_datas.clear(); // release after write

    RETURN_IF_ERROR(_json_writer->finish());

    // Check global dict validity for flat writers
    _subcolumn_dict_valid.clear();

    for (size_t i = 0; i < _flat_writers.size(); i++) {
        RETURN_IF_ERROR(_flat_writers[i]->finish());

        // Record dict validity for each sub-column
        bool sub_dict_valid = _flat_writers[i]->is_global_dict_valid();
        std::string sub_column_key = _column_name + "." + _flat_paths[i];
        _subcolumn_dict_valid[sub_column_key] = sub_dict_valid;
    }

    _json_meta->set_total_mem_footprint(total_mem_footprint());
    return Status::OK();
}

Status JsonColumnCompactor::append(const Column& column) {
    const JsonColumn* json_col;
    NullColumnPtr nulls = nullptr;
    if (column.is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(&column);
        nulls = nullable_column->null_column();
        json_col = down_cast<const JsonColumn*>(nullable_column->data_column_raw_ptr());
    } else {
        json_col = down_cast<const JsonColumn*>(&column);
    }

    if (!json_col->is_flat_json()) {
        return _json_writer->append(column);
    }

    JsonMerger merger(json_col->flat_column_paths(), json_col->flat_column_types(), json_col->has_remain());
    auto p = merger.merge(json_col->get_flat_fields());

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

    // Check global dict validity
    RETURN_IF_ERROR(_json_writer->finish());
    _is_global_dict_valid = _json_writer->is_global_dict_valid();

    return Status::OK();
}

} // namespace starrocks
