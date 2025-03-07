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

#include "storage/rowset/json_column_writer.h"

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "runtime/types.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/common.h"
#include "storage/rowset/json_column_compactor.h"
#include "types/constexpr.h"
#include "types/logical_type.h"
#include "util/json_flattener.h"
#include "velocypack/vpack.h"

namespace starrocks {

FlatJsonColumnWriter::FlatJsonColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* wfile,
                                           std::unique_ptr<ScalarColumnWriter> json_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _json_meta(opts.meta),
          _wfile(wfile),
          _json_writer(std::move(json_writer)) {}

Status FlatJsonColumnWriter::init() {
    _json_meta->mutable_json_meta()->set_format_version(kJsonMetaDefaultFormatVersion);
    _json_meta->mutable_json_meta()->set_has_remain(false);
    _json_meta->mutable_json_meta()->set_is_flat(false);

    return _json_writer->init();
}

Status FlatJsonColumnWriter::append(const Column& column) {
    DCHECK(_flat_paths.empty());
    DCHECK(_flat_types.empty());
    DCHECK(_flat_writers.empty());

    if (column.is_nullable()) {
        auto nullable = down_cast<const NullableColumn*>(&column);
        DCHECK(!down_cast<const JsonColumn*>(nullable->data_column().get())->is_flat_json());
    } else {
        DCHECK(!down_cast<const JsonColumn*>(&column)->is_flat_json());
    }

    // schema change will reuse column, must copy in there.
    _json_datas.emplace_back(column.clone());
    _estimate_size += column.byte_size();
    return Status::OK();
}

Status FlatJsonColumnWriter::_flat_column(Columns& json_datas) {
    // all json datas must full json
    JsonPathDeriver deriver;
    std::vector<const Column*> vc;
    for (const auto& js : json_datas) {
        vc.emplace_back(js.get());
    }
    deriver.derived(vc);

    _flat_paths = deriver.flat_paths();
    _flat_types = deriver.flat_types();
    _has_remain = deriver.has_remain_json();

    VLOG(2) << "FlatJsonColumnWriter flat_column flat json: "
            << JsonFlatPath::debug_flat_json(_flat_paths, _flat_types, _has_remain);
    if (_flat_paths.empty()) {
        return Status::InternalError("doesn't have flat column.");
    }

    JsonFlattener flattener(deriver);

    RETURN_IF_ERROR(_init_flat_writers());
    for (auto& col : json_datas) {
        auto* json_data = col.get();
        flattener.flatten(json_data);
        _flat_columns = flattener.mutable_result();

        // recode null column in 1st
        if (_json_meta->is_nullable()) {
            auto nulls = NullColumn::create();
            uint8_t IS_NULL = 1;
            uint8_t NOT_NULL = 0;
            if (json_data->only_null()) {
                nulls->append_value_multiple_times(&IS_NULL, json_data->size());
            } else if (json_data->is_nullable()) {
                auto* nullable_column = down_cast<const NullableColumn*>(json_data);
                auto* nl = down_cast<const NullColumn*>(nullable_column->null_column().get());
                nulls->append(*nl, 0, nl->size());
            } else {
                nulls->append_value_multiple_times(&NOT_NULL, json_data->size());
            }

            _flat_columns.insert(_flat_columns.begin(), std::move(nulls));
        }
        RETURN_IF_ERROR(_write_flat_column());
        _flat_columns.clear();
        col->resize_uninitialized(0); // release after write
    }
    return Status::OK();
}

Status FlatJsonColumnWriter::_init_flat_writers() {
    // update json meta
    _json_meta->mutable_json_meta()->set_has_remain(_has_remain);
    _json_meta->mutable_json_meta()->set_is_flat(true);

    if (_remain_filter != nullptr) {
        _json_meta->mutable_json_meta()->set_remain_filter(_remain_filter->data(), _remain_filter->size());
    }

    // recode null column in 1st
    if (_json_meta->is_nullable()) {
        _flat_paths.insert(_flat_paths.begin(), "nulls");
        _flat_types.insert(_flat_types.begin(), LogicalType::TYPE_TINYINT);
    }

    if (_has_remain) {
        _flat_paths.emplace_back("remain");
        _flat_types.emplace_back(LogicalType::TYPE_JSON);
    }

    for (size_t i = 0; i < _flat_paths.size(); i++) {
        ColumnWriterOptions opts;
        opts.meta = _json_meta->add_children_columns();
        opts.meta->set_column_id(i);
        opts.meta->set_unique_id(i);
        opts.meta->set_type(_flat_types[i]);
        if (_flat_types[i] == TYPE_VARCHAR) {
            opts.meta->set_length(config::olap_string_max_length);
        } else {
            DCHECK_NE(_flat_types[i], TYPE_CHAR);
            // set length for non-string type (e.g. int, double, date, etc.
            opts.meta->set_length(get_type_info(_flat_types[i])->size());
        }
        if ((_json_meta->is_nullable() && i == 0) || (i == _flat_paths.size() - 1 && _has_remain)) {
            opts.meta->set_is_nullable(false);
        } else {
            opts.meta->set_is_nullable(true);
        }
        opts.meta->set_encoding(DEFAULT_ENCODING);
        opts.meta->set_compression(_json_meta->compression());

        if (_flat_types[i] == LogicalType::TYPE_JSON) {
            opts.meta->mutable_json_meta()->set_format_version(kJsonMetaDefaultFormatVersion);
            opts.meta->mutable_json_meta()->set_is_flat(false);
        }

        opts.meta->set_name(_flat_paths[i]);
        opts.need_flat = false;

        TabletColumn col(StorageAggregateType::STORAGE_AGGREGATE_NONE, _flat_types[i], true);
        ASSIGN_OR_RETURN(auto fw, ColumnWriter::create(opts, &col, _wfile));
        _flat_writers.emplace_back(std::move(fw));

        RETURN_IF_ERROR(_flat_writers[i]->init());
    }
    return Status::OK();
}

Status FlatJsonColumnWriter::_write_flat_column() {
    DCHECK(!_flat_columns.empty());
    DCHECK_EQ(_flat_columns.size(), _flat_writers.size());
    // flat datas
    for (size_t i = 0; i < _flat_columns.size(); i++) {
        RETURN_IF_ERROR(_flat_writers[i]->append(*_flat_columns[i]));
    }

    return Status::OK();
}

Status FlatJsonColumnWriter::finish() {
    auto st = _flat_column(_json_datas);
    _is_flat = st.ok();
    if (!st.ok()) {
        for (auto& col : _json_datas) {
            RETURN_IF_ERROR(_json_writer->append(*col));
        }
    }
    _json_datas.clear(); // release after write
    // flat datas
    for (size_t i = 0; i < _flat_writers.size(); i++) {
        RETURN_IF_ERROR(_flat_writers[i]->finish());
    }
    return _json_writer->finish();
}

ordinal_t FlatJsonColumnWriter::get_next_rowid() const {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    if (!_is_flat) {
        return _json_writer->get_next_rowid();
    }
    return _flat_writers[0]->get_next_rowid();
}

uint64_t FlatJsonColumnWriter::estimate_buffer_size() {
    return _estimate_size;
}

uint64_t FlatJsonColumnWriter::total_mem_footprint() const {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    uint64_t size = 0;
    for (auto& w : _flat_writers) {
        size += w->total_mem_footprint();
    }
    size += _json_writer->total_mem_footprint();
    return size;
}

Status FlatJsonColumnWriter::write_data() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_data());
    }
    return _json_writer->write_data();
}

Status FlatJsonColumnWriter::write_ordinal_index() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_ordinal_index());
    }
    return _json_writer->write_ordinal_index();
}
Status FlatJsonColumnWriter::write_zone_map() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_zone_map());
    }
    return _json_writer->write_zone_map();
}

Status FlatJsonColumnWriter::write_bitmap_index() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_bitmap_index());
    }
    return _json_writer->write_bitmap_index();
}

Status FlatJsonColumnWriter::write_bloom_filter_index() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_bloom_filter_index());
    }
    return _json_writer->write_bloom_filter_index();
}

Status FlatJsonColumnWriter::finish_current_page() {
    DCHECK(_is_flat ? !_flat_writers.empty() : _flat_writers.empty());
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->finish_current_page());
    }
    return _json_writer->finish_current_page();
}

StatusOr<std::unique_ptr<ColumnWriter>> create_json_column_writer(const ColumnWriterOptions& opts,
                                                                  TypeInfoPtr type_info, WritableFile* wfile,
                                                                  std::unique_ptr<ScalarColumnWriter> json_writer) {
    VLOG(2) << "Create Json Column Writer is_compaction: " << opts.is_compaction << ", need_flat : " << opts.need_flat;
    // compaction
    if (opts.is_compaction) {
        if (opts.need_flat) {
            return std::make_unique<FlatJsonColumnCompactor>(opts, std::move(type_info), wfile, std::move(json_writer));
        } else {
            return std::make_unique<JsonColumnCompactor>(opts, std::move(type_info), wfile, std::move(json_writer));
        }
    }

    // load
    if (!opts.need_flat) {
        return std::move(json_writer);
    } else {
        return std::make_unique<FlatJsonColumnWriter>(opts, std::move(type_info), wfile, std::move(json_writer));
    }
}
} // namespace starrocks
