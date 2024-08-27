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

#include "storage/meta_reader.h"

#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/datum_convert.h"
#include "common/status.h"
#include "runtime/global_dict/config.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {

std::vector<std::string> SegmentMetaCollecter::support_collect_fields = {"flat_json_meta", "dict_merge", "max", "min",
                                                                         "count"};

Status SegmentMetaCollecter::parse_field_and_colname(const std::string& item, std::string* field,
                                                     std::string* col_name) {
    for (auto& support_collect_field : support_collect_fields) {
        if (item.size() <= support_collect_field.size()) {
            continue;
        }

        if (item.find(support_collect_field) != std::string::npos &&
            item.substr(0, support_collect_field.size()) == support_collect_field) {
            *field = support_collect_field;
            *col_name = item.substr(support_collect_field.size() + 1);
            return Status::OK();
        }
    }
    return Status::InvalidArgument("cannot find column: " + item);
}

MetaReader::MetaReader() : _is_init(false), _has_more(false) {}

Status MetaReader::open() {
    return Status::OK();
}

Status MetaReader::_read(Chunk* chunk, size_t n) {
    if (_collect_context.seg_collecters.size() == 0) {
        // no segment, fill chunk with an empty result
        if (_has_count_agg) {
            _fill_empty_result(chunk);
        }
        _has_more = false;
        return Status::OK();
    }

    std::vector<Column*> columns;
    for (size_t i = 0; i < _collect_context.seg_collecter_params.fields.size(); ++i) {
        const ColumnPtr& col = chunk->get_column_by_index(i);
        columns.emplace_back(col.get());
    }

    size_t remaining = n;
    while (remaining > 0) {
        if (_collect_context.cursor_idx >= _collect_context.seg_collecters.size()) {
            _has_more = false;
            return Status::OK();
        }
        RETURN_IF_ERROR(_collect_context.seg_collecters[_collect_context.cursor_idx]->open());
        RETURN_IF_ERROR(_collect_context.seg_collecters[_collect_context.cursor_idx]->collect(&columns));
        _collect_context.seg_collecters[_collect_context.cursor_idx].reset();
        remaining--;
        _collect_context.cursor_idx++;
    }

    return Status::OK();
}

void MetaReader::_fill_empty_result(Chunk* chunk) {
    DCHECK(chunk != nullptr);
    for (size_t i = 0; i < _collect_context.result_slot_ids.size(); i++) {
        auto s_id = _collect_context.result_slot_ids[i];
        auto slot = _params.desc_tbl->get_slot_descriptor(s_id);
        const auto& field = _collect_context.seg_collecter_params.fields[i];
        ColumnPtr column = chunk->get_column_by_slot_id(slot->id());
        if (field == "count") {
            column->append_datum(int64_t(0));
        } else {
            column->append_nulls(1);
        }
    }
}

bool MetaReader::has_more() {
    return _has_more;
}

Status MetaReader::_fill_result_chunk(Chunk* chunk) {
    for (size_t i = 0; i < _collect_context.result_slot_ids.size(); i++) {
        auto s_id = _collect_context.result_slot_ids[i];
        auto slot = _params.desc_tbl->get_slot_descriptor(s_id);
        const auto& field = _collect_context.seg_collecter_params.fields[i];
        if (field == "dict_merge") {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_VARCHAR;
            TypeDescriptor desc;
            desc.type = TYPE_ARRAY;
            desc.children.emplace_back(item_desc);
            ColumnPtr column = ColumnHelper::create_column(desc, _has_count_agg);
            chunk->append_column(std::move(column), slot->id());
        } else if (field == "count") {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_BIGINT;
            TypeDescriptor desc;
            desc.type = TYPE_BIGINT;
            desc.children.emplace_back(item_desc);
            ColumnPtr column = ColumnHelper::create_column(desc, false);
            chunk->append_column(std::move(column), slot->id());
        } else if (field == "flat_json_meta") {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_VARCHAR;
            TypeDescriptor desc;
            desc.type = TYPE_ARRAY;
            desc.children.emplace_back(item_desc);
            ColumnPtr column = ColumnHelper::create_column(desc, false);
            chunk->append_column(std::move(column), slot->id());
        } else {
            ColumnPtr column = ColumnHelper::create_column(slot->type(), _has_count_agg);
            chunk->append_column(std::move(column), slot->id());
        }
    }
    return Status::OK();
}

SegmentMetaCollecter::SegmentMetaCollecter(SegmentSharedPtr segment) : _segment(std::move(segment)) {}

SegmentMetaCollecter::~SegmentMetaCollecter() = default;

Status SegmentMetaCollecter::init(const SegmentMetaCollecterParams* params) {
    if (UNLIKELY(params == nullptr)) {
        return Status::InvalidArgument("params is nullptr");
    }
    if (UNLIKELY(params->fields.size() != params->field_type.size())) {
        return Status::InvalidArgument(fmt::format("unmatched field name count({}) and field type count({})",
                                                   params->fields.size(), params->field_type.size()));
    }
    if (UNLIKELY(params->fields.size() != params->cids.size())) {
        return Status::InvalidArgument(fmt::format("unmatched field name count({}) and column id count({})",
                                                   params->fields.size(), params->cids.size()));
    }
    if (UNLIKELY(params->fields.size() != params->read_page.size())) {
        return Status::InvalidArgument(fmt::format("unmatched field name count({}) and read page flags count({})",
                                                   params->fields.size(), params->read_page.size()));
    }
    if (UNLIKELY(params->tablet_schema == nullptr)) {
        return Status::InvalidArgument("tablet schema is nullptr");
    }
    _params = params;
    return Status::OK();
}

Status SegmentMetaCollecter::open() {
    if (UNLIKELY(_params == nullptr)) {
        return Status::InternalError("SegmentMetaCollecter::init() has not been called");
    }
    RETURN_IF_ERROR(_init_return_column_iterators());
    return Status::OK();
}

Status SegmentMetaCollecter::_init_return_column_iterators() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_segment->file_name()));
    RandomAccessFileOptions ropts;
    if (_segment->encryption_info()) {
        ropts.encryption_info = *_segment->encryption_info();
    }
    ASSIGN_OR_RETURN(_read_file, fs->new_random_access_file(ropts, _segment->file_name()));

    auto max_cid = _params->cids.empty() ? 0 : *std::max_element(_params->cids.begin(), _params->cids.end());
    _column_iterators.resize(max_cid + 1);
    for (int i = 0; i < _params->fields.size(); i++) {
        if (_params->read_page[i]) {
            auto cid = _params->cids[i];
            if (_column_iterators[cid] == nullptr) {
                const TabletColumn& col = _params->tablet_schema->column(cid);
                ASSIGN_OR_RETURN(_column_iterators[cid], _segment->new_column_iterator_or_default(col, nullptr));

                ColumnIteratorOptions iter_opts;
                iter_opts.check_dict_encoding = true;
                iter_opts.read_file = _read_file.get();
                iter_opts.stats = &_stats;
                RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
            }
        }
    }
    return Status::OK();
}

Status SegmentMetaCollecter::collect(std::vector<Column*>* dsts) {
    if (UNLIKELY(dsts->size() != _params->fields.size())) {
        return Status::InvalidArgument(
                fmt::format("invalid column count. expect: {} real: {}", _params->fields.size(), dsts->size()));
    }

    for (size_t i = 0; i < _params->fields.size(); i++) {
        RETURN_IF_ERROR(_collect(_params->fields[i], _params->cids[i], (*dsts)[i], _params->field_type[i]));
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect(const std::string& name, ColumnId cid, Column* column, LogicalType type) {
    if (name == "dict_merge") {
        return _collect_dict(cid, column, type);
    } else if (name == "max") {
        return _collect_max(cid, column, type);
    } else if (name == "min") {
        return _collect_min(cid, column, type);
    } else if (name == "count") {
        return _collect_count(column, type);
    } else if (name == "flat_json_meta") {
        return _collect_flat_json(cid, column);
    }
    return Status::NotSupported("Not Support Collect Meta: " + name);
}

Status SegmentMetaCollecter::_collect_flat_json(ColumnId cid, Column* column) {
    const ColumnReader* col_reader = _segment->column(cid);
    if (col_reader == nullptr) {
        return Status::NotFound("don't found column");
    }
    if (col_reader->column_type() != TYPE_JSON) {
        return Status::InternalError("column type mismatch");
    }

    if (col_reader->sub_readers() == nullptr || col_reader->sub_readers()->size() < 1) {
        column->append_datum(DatumArray());
        return Status::OK();
    }

    ArrayColumn* array_column = down_cast<ArrayColumn*>(column);
    size_t size = array_column->offsets_column()->get_data().back();
    for (const auto& sub_reader : *col_reader->sub_readers()) {
        std::string str = fmt::format("{}({})", sub_reader->name(), type_to_string(sub_reader->column_type()));
        array_column->elements_column()->append_datum(Slice(str));
    }
    array_column->offsets_column()->append(size + col_reader->sub_readers()->size());
    return Status::OK();
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, Column* column, LogicalType type) {
    if (!_column_iterators[cid]) {
        return Status::InvalidArgument("Invalid Collect Params.");
    }

    std::vector<Slice> words;
    if (!_column_iterators[cid]->all_page_dict_encoded()) {
        return Status::GlobalDictError("no global dict");
    } else {
        RETURN_IF_ERROR(_column_iterators[cid]->fetch_all_dict_words(&words));
    }

    if (words.size() > DICT_DECODE_MAX_SIZE) {
        return Status::GlobalDictError("global dict greater than DICT_DECODE_MAX_SIZE");
    }

    // array<string> has none dict, return directly
    if (words.size() < 1) {
        return Status::OK();
    }

    [[maybe_unused]] NullableColumn* nullable_column = nullptr;
    ArrayColumn* array_column = nullptr;

    if (column->is_nullable()) {
        nullable_column = down_cast<NullableColumn*>(column);
        array_column = down_cast<ArrayColumn*>(nullable_column->mutable_data_column());
    } else {
        array_column = down_cast<ArrayColumn*>(column);
    }

    auto* offsets = array_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();
    end_offset += words.size();
    offsets->append(end_offset);

    // add elements
    auto dst = array_column->elements_column().get();
    CHECK(dst->append_strings(words));

    if (column->is_nullable()) {
        nullable_column->null_column_data().emplace_back(0);
    }

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_max(ColumnId cid, Column* column, LogicalType type) {
    return __collect_max_or_min<true>(cid, column, type);
}

Status SegmentMetaCollecter::_collect_min(ColumnId cid, Column* column, LogicalType type) {
    return __collect_max_or_min<false>(cid, column, type);
}

template <bool is_max>
Status SegmentMetaCollecter::__collect_max_or_min(ColumnId cid, Column* column, LogicalType type) {
    if (cid >= _segment->num_columns()) {
        return Status::NotFound("");
    }
    const ColumnReader* col_reader = _segment->column(cid);
    if (col_reader == nullptr || col_reader->segment_zone_map() == nullptr) {
        return Status::NotFound("");
    }
    if (col_reader->column_type() != type) {
        return Status::InternalError("column type mismatch");
    }
    const ZoneMapPB* segment_zone_map_pb = col_reader->segment_zone_map();
    TypeInfoPtr type_info = get_type_info(delegate_type(type));
    if constexpr (!is_max) {
        Datum min;
        if (!segment_zone_map_pb->has_null()) {
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &min, segment_zone_map_pb->min(), nullptr));
            column->append_datum(min);
        }
    } else if constexpr (is_max) {
        Datum max;
        if (segment_zone_map_pb->has_not_null()) {
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &max, segment_zone_map_pb->max(), nullptr));
            column->append_datum(max);
        }
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect_count(Column* column, LogicalType type) {
    uint32_t num_rows = _segment->num_rows();
    column->append_datum(int64_t(num_rows));
    return Status::OK();
}

} // namespace starrocks
