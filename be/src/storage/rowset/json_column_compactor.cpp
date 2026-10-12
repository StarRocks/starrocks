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
#include "common/config_compaction_fwd.h"
#include "exprs/hyper_json_transformer.h"
#include "fmt/format.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "storage/flat_json_metrics.h"
#include "storage/json_path_deriver.h"
#include "storage/rowset/column_writer.h"
#include "types/constexpr.h"

namespace starrocks {

bool check_is_same_schema(const JsonColumn* one, const JsonColumn* two);

namespace {
// The JSON data part of a compaction input column, or nullptr when it has none (a const-null column).
const JsonColumn* json_data_of(const Column& column) {
    if (column.only_null()) {
        return nullptr;
    }
    if (column.is_nullable()) {
        return down_cast<const JsonColumn*>(down_cast<const NullableColumn&>(column).data_column_raw_ptr());
    }
    return down_cast<const JsonColumn*>(&column);
}

// Buffered input past which a uniformly laid out flat segment is written as it arrives. Small segments
// keep the full re-derivation; large ones must not hold their whole input in memory.
int64_t streaming_threshold_bytes() {
    const int64_t mem_limit = config::compaction_memory_limit_per_worker;
    return mem_limit > 0 ? mem_limit / 8 : -1;
}
} // namespace

Status FlatJsonColumnCompactor::append(const Column& column) {
    if (_streaming) {
        const JsonColumn* json_col = json_data_of(column);
        if (json_col == nullptr) {
            return Status::InternalError("flat json compaction input changed layout mid-segment: const-null column");
        }
        return _stream_write(column, *json_col);
    }

    // compaction will reuse the column, must copy in there.
    _json_datas.emplace_back(column.clone());
    _estimate_size += column.byte_size();

    const JsonColumn* json_col = json_data_of(column);
    // A flat layout with no named path (only a remain column) is never streamed: the buffered path writes
    // such a segment as plain JSON, and a flat segment without sub-columns is not a layout readers expect.
    _uniform_flat_layout = _uniform_flat_layout && json_col != nullptr && json_col->is_flat_json() &&
                           !json_col->flat_column_paths().empty() &&
                           check_is_same_schema(json_data_of(*_json_datas.front()), json_col);
    const int64_t threshold = streaming_threshold_bytes();
    if (_uniform_flat_layout && threshold > 0 && _estimate_size > static_cast<size_t>(threshold)) {
        return _start_streaming();
    }
    return Status::OK();
}

Status FlatJsonColumnCompactor::_start_streaming() {
    // Every input segment being flat is what makes the compaction reader transform all of them into
    // the one layout derived from their metadata (see TabletReader's compaction access paths), so a
    // segment that is still uniform this far in will stay so: adopt its layout and stop buffering.
    const JsonColumn* first = json_data_of(*_json_datas.front());
    FlatJsonMetrics::instance()->flat_json_compaction_total.increment(1);
    FlatJsonMetrics::instance()->flat_json_compaction_streaming_total.increment(1);
    VLOG(2) << "FlatJsonColumnCompactor streams flat json after " << _json_datas.size()
            << " chunks: " << first->debug_flat_paths();
    _streaming = true;
    _is_flat = true;
    _flat_paths = first->flat_column_paths();
    _flat_types = first->flat_column_types();
    _has_remain = first->has_remain();
    // No remain filter: it can only be built from every remain value of the segment. Without one a
    // reader falls back to reading the remain column, which is always correct.
    _remain_filter.reset();
    RETURN_IF_ERROR(_init_flat_writers());

    MutableColumns buffered = std::move(_json_datas);
    _json_datas.clear();
    for (auto& col : buffered) {
        RETURN_IF_ERROR(_stream_write(*col, *json_data_of(*col)));
        col.reset(); // release as soon as written
    }
    return Status::OK();
}

Status FlatJsonColumnCompactor::_stream_write(const Column& column, const JsonColumn& json_col) {
    // _init_flat_writers() put a "nulls" writer first for a nullable column and a "remain" writer last.
    const size_t first_field = _json_meta->is_nullable() ? 1 : 0;
    const size_t num_named_writers = _flat_writers.size() - first_field - (_has_remain ? 1 : 0);
    // Check the remain column and the named paths separately: a chunk trading a named path for a remain
    // column has as many columns as there are writers, and would put its remain JSON into a path's writer.
    if (!json_col.is_flat_json() || json_col.has_remain() != _has_remain ||
        json_col.flat_column_paths().size() != num_named_writers) {
        // The compaction reader gives every chunk of a segment the same layout; anything else would be
        // silently mis-written, so stop the compaction instead.
        return Status::InternalError(fmt::format("flat json compaction input changed layout mid-segment: {} -> {}",
                                                 JsonFlatPath::debug_flat_json(_flat_paths, _flat_types, _has_remain),
                                                 json_col.debug_flat_paths()));
    }
    for (size_t i = 0; i < json_col.flat_column_paths().size(); i++) {
        if (json_col.flat_column_paths()[i] != _flat_paths[first_field + i] ||
            json_col.flat_column_types()[i] != _flat_types[first_field + i]) {
            return Status::InternalError(fmt::format("flat json compaction input changed layout mid-segment at {}",
                                                     json_col.flat_column_paths()[i]));
        }
    }

    if (_json_meta->is_nullable()) {
        auto nulls = NullColumn::create();
        if (column.is_nullable()) {
            const auto* nl = down_cast<const NullableColumn&>(column).null_column_raw_ptr();
            nulls->append(*nl, 0, nl->size());
        } else {
            constexpr uint8_t kNotNull = 0;
            nulls->append_value_multiple_times(&kNotNull, column.size());
        }
        RETURN_IF_ERROR(_flat_writers[0]->append(*nulls));
    }
    for (size_t i = first_field; i < _flat_writers.size(); i++) {
        RETURN_IF_ERROR(_flat_writers[i]->append(*json_col.get_flat_field(static_cast<int>(i - first_field))));
    }
    return Status::OK();
}

uint64_t FlatJsonColumnCompactor::estimate_buffer_size() {
    if (!_streaming) {
        return FlatJsonColumnWriter::estimate_buffer_size();
    }
    // What is actually held: the sub-column writers' encoded pages.
    uint64_t size = 0;
    for (auto& w : _flat_writers) {
        size += w->estimate_buffer_size();
    }
    return size;
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
                FlatJsonMetrics::instance()->flat_json_compaction_schema_change_total.increment(1);
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
        col->resize_uninitialized(0); // release after write
    }

    return Status::OK();
}

Status FlatJsonColumnCompactor::finish() {
    if (!_streaming) {
        RETURN_IF_ERROR(_compact_columns(_json_datas));
        _json_datas.clear(); // release after write
    }

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
