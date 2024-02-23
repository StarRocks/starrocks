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
#include <utility>

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
#include "storage/rowset/column_writer.h"
#include "types/logical_type.h"
#include "util/json_util.h"
#include "velocypack/vpack.h"

namespace starrocks {

class FlatJsonColumnWriter final : public ColumnWriter {
public:
    FlatJsonColumnWriter(const ColumnWriterOptions& opts, const TypeInfoPtr& type_info, WritableFile* wfile,
                         std::unique_ptr<ScalarColumnWriter> json_writer);

    ~FlatJsonColumnWriter() override = default;

    Status init() override { return _json_column_writer->init(); };

    Status append(const Column& column) override;

    Status finish_current_page() override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;

    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_bitmap_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _json_column_writer->get_next_rowid(); }

    bool is_global_dict_valid() override { return _json_column_writer->is_global_dict_valid(); }

    uint64_t total_mem_footprint() const override { return _json_column_writer->total_mem_footprint(); }

private:
    void _flat_column(std::vector<ColumnPtr>& json_datas);

private:
    std::unique_ptr<ScalarColumnWriter> _json_column_writer;
    ColumnMetaPB* _json_meta;
    WritableFile* _wfile;

    std::vector<ColumnPtr> _json_datas;

    std::vector<std::unique_ptr<ColumnWriter>> _flat_writers;
    std::vector<std::string> _flat_paths;
    std::vector<ColumnPtr> _flat_columns;
};

FlatJsonColumnWriter::FlatJsonColumnWriter(const ColumnWriterOptions& opts, const TypeInfoPtr& type_info,
                                           WritableFile* wfile, std::unique_ptr<ScalarColumnWriter> json_writer)
        : ColumnWriter(std::move(type_info), opts.meta->length(), opts.meta->is_nullable()),
          _json_column_writer(std::move(json_writer)),
          _json_meta(opts.meta),
          _wfile(wfile) {}

Status FlatJsonColumnWriter::append(const Column& column) {
    RETURN_IF_ERROR(_json_column_writer->append(column));
    // write process/compection will reuse column, must copy in there.
    // @Todo: avoid memory copy
    auto clone = column.clone_empty();
    clone->append(column);
    _json_datas.emplace_back(std::move(clone));
    return Status::OK();
}

void FlatJsonColumnWriter::_flat_column(std::vector<ColumnPtr>& json_datas) {
    DCHECK(_flat_paths.empty());
    DCHECK(!json_datas.empty());

    size_t total_rows = 0;
    size_t null_count = 0;

    for (auto& column : json_datas) {
        DCHECK(!column->is_constant());

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

    // extract common keys
    std::unordered_map<std::string, uint64_t> hit_maps;
    for (size_t k = 0; k < json_datas.size(); k++) {
        size_t row_count = json_datas[k]->size();

        ColumnViewer<TYPE_JSON> viewer(json_datas[k]);
        for (size_t i = 0; i < row_count; ++i) {
            if (viewer.is_null(i)) {
                continue;
            }

            JsonValue* json = viewer.value(i);
            auto vslice = json->to_vslice();

            if (vslice.isNull()) {
                continue;
            }

            if (vslice.isNone() || !vslice.isObject() || vslice.isEmptyObject()) {
                VLOG(8) << "flat json, row isn't object, can't be flatten";
                return;
            }

            std::vector<std::string> keys = arangodb::velocypack::Collection::keys(json->to_vslice());
            for (auto& sr : keys) {
                hit_maps[sr]++;
            }
        }
    }

    if (hit_maps.size() <= config::json_flat_internal_column_min_limit) {
        VLOG(8) << "flat json, internal column too less: " << hit_maps.size()
                << ", at least: " << config::json_flat_internal_column_min_limit;
        return;
    }

    // sort by hit
    std::vector<pair<std::string, std::uint64_t>> top_hits(hit_maps.begin(), hit_maps.end());
    std::sort(top_hits.begin(), top_hits.end(),
              [](const pair<std::string, std::uint64_t>& a, const pair<std::string, std::uint64_t>& b) {
                  return a.second > b.second;
              });

    for (int i = 0; i < top_hits.size() && i < config::json_flat_column_max; i++) {
        const auto& [name, hit] = top_hits[i];
        // check sparsity
        if (hit >= total_rows * config::json_flat_sparsity_factor) {
            if (name.find('.') != std::string::npos) {
                // add escape
                _flat_paths.emplace_back(fmt::format("\"{}\"", name));
            } else {
                _flat_paths.emplace_back(name);
            }
        }
        VLOG(8) << "flat json[" << name << "], hit[" << hit << "], row[" << total_rows << "]";
    }

    if (_flat_paths.empty()) {
        return;
    }

    // extract flat column
    for (size_t i = 0; i < _flat_paths.size(); i++) {
        _flat_columns.emplace_back(NullableColumn::create(JsonColumn::create(), NullColumn::create()));
    }

    JsonFlater flater(_flat_paths);

    for (auto& col : json_datas) {
        flater.flatten(col.get(), &_flat_columns);
    }

    // recode null column in 1st
    if (_json_meta->is_nullable()) {
        auto nulls = NullColumn::create();
        uint8_t IS_NULL = 1;
        uint8_t NOT_NULL = 0;
        for (auto& col : json_datas) {
            if (col->only_null()) {
                nulls->append_value_multiple_times(&IS_NULL, col->size());
            } else if (col->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(col.get());
                auto* nl = down_cast<NullColumn*>(nullable_column->null_column().get());
                nulls->append(*nl, 0, nl->size());
            } else {
                nulls->append_value_multiple_times(&NOT_NULL, col->size());
            }
        }

        _flat_columns.insert(_flat_columns.begin(), nulls);
        _flat_paths.insert(_flat_paths.begin(), "nulls");
    }
}

Status FlatJsonColumnWriter::finish() {
    for (const auto& js : _json_datas) {
        DCHECK_GT(js->size(), 0);
    }
    _flat_column(_json_datas);
    _json_datas.clear(); // release column data

    if (!_flat_columns.empty()) {
        // nulls
        if (_json_meta->is_nullable()) {
            ColumnWriterOptions opts;
            opts.meta = _json_meta->add_children_columns();
            opts.meta->set_column_id(0);
            opts.meta->set_unique_id(0);
            opts.meta->set_type(LogicalType::TYPE_TINYINT);
            opts.meta->set_length(get_type_info(LogicalType::TYPE_TINYINT)->size());
            opts.meta->set_is_nullable(false);
            opts.meta->set_name("nulls");
            opts.meta->set_encoding(DEFAULT_ENCODING);
            opts.meta->set_compression(_json_meta->compression());

            TabletColumn col(StorageAggregateType::STORAGE_AGGREGATE_NONE, LogicalType::TYPE_TINYINT, true);
            ASSIGN_OR_RETURN(auto fw, ColumnWriter::create(opts, &col, _wfile));
            _flat_writers.emplace_back(std::move(fw));

            RETURN_IF_ERROR(_flat_writers[0]->init());
            RETURN_IF_ERROR(_flat_writers[0]->append(*_flat_columns[0]));
            RETURN_IF_ERROR(_flat_writers[0]->finish());

            VLOG(8) << "flush flat json nulls";
        }

        int start = _json_meta->is_nullable() ? 1 : 0;
        // flat datas
        for (size_t i = start; i < _flat_columns.size(); i++) {
            ColumnWriterOptions opts;
            opts.meta = _json_meta->add_children_columns();
            opts.meta->set_column_id(i);
            opts.meta->set_unique_id(i);
            opts.meta->set_type(LogicalType::TYPE_JSON);
            opts.meta->set_length(get_type_info(LogicalType::TYPE_JSON)->size());
            opts.meta->set_is_nullable(true);
            opts.meta->set_encoding(DEFAULT_ENCODING);
            opts.meta->set_compression(_json_meta->compression());
            opts.meta->mutable_json_meta()->set_format_version(kJsonMetaDefaultFormatVersion);
            opts.meta->set_name(_flat_paths[i]);
            opts.need_flat = false;

            TabletColumn col(StorageAggregateType::STORAGE_AGGREGATE_NONE, LogicalType::TYPE_JSON, true);
            ASSIGN_OR_RETURN(auto fw, ColumnWriter::create(opts, &col, _wfile));
            _flat_writers.emplace_back(std::move(fw));

            RETURN_IF_ERROR(_flat_writers[i]->init());
            RETURN_IF_ERROR(_flat_writers[i]->append(*_flat_columns[i]));
            RETURN_IF_ERROR(_flat_writers[i]->finish());

            VLOG(8) << "flush flat json: " << _flat_paths[i];
        }
    }

    return _json_column_writer->finish();
}

uint64_t FlatJsonColumnWriter::estimate_buffer_size() {
    uint64_t size = _json_column_writer->estimate_buffer_size();
    for (auto& w : _flat_writers) {
        size += w->estimate_buffer_size();
    }
    return size;
}

Status FlatJsonColumnWriter::write_data() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_data());
    }
    return _json_column_writer->write_data();
}

Status FlatJsonColumnWriter::write_ordinal_index() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_ordinal_index());
    }
    return _json_column_writer->write_ordinal_index();
}
Status FlatJsonColumnWriter::write_zone_map() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_zone_map());
    }
    return _json_column_writer->write_zone_map();
}

Status FlatJsonColumnWriter::write_bitmap_index() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_bitmap_index());
    }
    return _json_column_writer->write_bitmap_index();
}

Status FlatJsonColumnWriter::write_bloom_filter_index() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->write_bloom_filter_index());
    }
    return _json_column_writer->write_bloom_filter_index();
}

Status FlatJsonColumnWriter::finish_current_page() {
    for (auto& w : _flat_writers) {
        RETURN_IF_ERROR(w->finish_current_page());
    }
    return _json_column_writer->finish_current_page();
}

StatusOr<std::unique_ptr<ColumnWriter>> create_json_column_writer(const ColumnWriterOptions& opts,
                                                                  const TypeInfoPtr& type_info, WritableFile* wfile,
                                                                  std::unique_ptr<ScalarColumnWriter> json_writer) {
    if (!opts.need_flat) {
        return std::move(json_writer);
    }
    return std::make_unique<FlatJsonColumnWriter>(opts, type_info, wfile, std::move(json_writer));
}
} // namespace starrocks
