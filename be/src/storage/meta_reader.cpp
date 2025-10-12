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

#include <sstream>
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

std::vector<std::string> SegmentMetaCollecter::support_collect_fields = {
        META_FLAT_JSON_META, META_DICT_MERGE, META_MAX,         META_MIN,
        META_COUNT_ROWS,     META_COUNT_COL,  META_COLUMN_SIZE, META_COLUMN_COMPRESSED_SIZE};

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
        auto col = chunk->get_mutable_column_by_index(i);
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
        auto column = chunk->get_mutable_column_by_slot_id(slot->id());
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
        if (field == META_DICT_MERGE) {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_VARCHAR;
            TypeDescriptor desc;
            desc.type = TYPE_ARRAY;
            desc.children.emplace_back(item_desc);
            MutableColumnPtr column = ColumnHelper::create_column(desc, _has_count_agg);
            chunk->append_column(std::move(column), slot->id());
        } else if (field == META_COUNT_COL || field == META_COUNT_ROWS) {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_BIGINT;
            TypeDescriptor desc;
            desc.type = TYPE_BIGINT;
            desc.children.emplace_back(item_desc);
            MutableColumnPtr column = ColumnHelper::create_column(desc, true);
            chunk->append_column(std::move(column), slot->id());
        } else if (field == META_FLAT_JSON_META) {
            TypeDescriptor item_desc;
            item_desc.type = TYPE_VARCHAR;
            TypeDescriptor desc;
            desc.type = TYPE_ARRAY;
            desc.children.emplace_back(item_desc);
            MutableColumnPtr column = ColumnHelper::create_column(desc, false);
            chunk->append_column(std::move(column), slot->id());
        } else if (field == META_COLUMN_SIZE || field == META_COLUMN_COMPRESSED_SIZE) {
            TypeDescriptor desc;
            desc.type = TYPE_BIGINT;
            MutableColumnPtr column = ColumnHelper::create_column(desc, true);
            chunk->append_column(std::move(column), slot->id());
        } else {
            MutableColumnPtr column = ColumnHelper::create_column(slot->type(), true);
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
    ASSIGN_OR_RETURN(_read_file, fs->new_random_access_file_with_bundling(ropts, _segment->file_info()));

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
    if (name == META_DICT_MERGE) {
        return _collect_dict(cid, column, type);
    } else if (name == META_MAX) {
        return _collect_max(cid, column, type);
    } else if (name == META_MIN) {
        return _collect_min(cid, column, type);
    } else if (name == META_COUNT_ROWS) {
        return _collect_rows(column, type);
    } else if (name == META_FLAT_JSON_META) {
        return _collect_flat_json(cid, column);
    } else if (name == META_COUNT_COL) {
        return _collect_count(cid, column, type);
    } else if (name == META_COLUMN_SIZE) {
        return _collect_column_size(cid, column, type);
    } else if (name == META_COLUMN_COMPRESSED_SIZE) {
        return _collect_column_compressed_size(cid, column, type);
    }
    return Status::NotSupported("Not Support Collect Meta: " + name);
}

std::string append_read_name(const ColumnReader* col_reader) {
    std::stringstream stream;
    if (col_reader->column_type() == LogicalType::TYPE_JSON) {
        for (const auto& sub_reader : *col_reader->sub_readers()) {
            stream << fmt::format("{}({}), ", sub_reader->name(), type_to_string(sub_reader->column_type()));
        }
        auto str = stream.str();
        return str.substr(0, str.size() - 2);
    }
    if (col_reader->column_type() == LogicalType::TYPE_ARRAY) {
        auto child = append_read_name((*col_reader->sub_readers())[0].get());
        if (!child.empty()) {
            stream << "[" << child << "]";
        }
    } else if (col_reader->column_type() == LogicalType::TYPE_MAP) {
        auto child = append_read_name((*col_reader->sub_readers())[1].get());
        if (!child.empty()) {
            stream << "{" << child << "}";
        }
    } else if (col_reader->column_type() == LogicalType::TYPE_STRUCT) {
        for (const auto& sub_reader : *col_reader->sub_readers()) {
            auto child = append_read_name(sub_reader.get());
            if (!child.empty()) {
                stream << sub_reader->name() << "(" << child << "), ";
            }
        }
        auto str = stream.str();
        return str.substr(0, str.size() - 2);
    }
    return stream.str();
}

Status SegmentMetaCollecter::_collect_flat_json(ColumnId cid, Column* column) {
    const ColumnReader* col_reader = _segment->column(cid);
    if (col_reader == nullptr) {
        return Status::NotFound("don't found column");
    }

    if (!is_semi_type(col_reader->column_type())) {
        return Status::InternalError("column type mismatch");
    }

    if (col_reader->sub_readers() == nullptr || col_reader->sub_readers()->size() < 1) {
        column->append_datum(DatumArray());
        return Status::OK();
    }

    ArrayColumn* array_column = down_cast<ArrayColumn*>(column);
    auto offsets_col = array_column->offsets_column_mutable_ptr();
    auto elements_col = array_column->elements_column_mutable_ptr();
    size_t size = offsets_col->immutable_data().back();
    auto res = append_read_name(col_reader);
    if (!res.empty()) {
        elements_col->append_datum(Slice(res));
        offsets_col->append(size + 1);
    }
    return Status::OK();
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, Column* column, LogicalType type) {
    if (!_column_iterators[cid]) {
        return Status::InvalidArgument("Invalid Collect Params.");
    }
    auto& schema = _params->tablet_schema;
    RETURN_IF(cid < 0 || cid >= schema->num_columns(), Status::InvalidArgument("Invalid cid: " + std::to_string(cid)));
    auto& tablet_column = schema->column(cid);
    if (tablet_column.type() == TYPE_VARCHAR || tablet_column.type() == TYPE_ARRAY) {
        RETURN_IF_ERROR(_collect_dict_for_column(_column_iterators[cid].get(), cid, column));
    } else if (tablet_column.type() == TYPE_JSON) {
        RETURN_IF_ERROR(_collect_dict_for_flatjson(cid, column));
    } else {
        return Status::InvalidArgument("unsupported column type: " + type_to_string(tablet_column.type()));
    }
    return {};
}

Status SegmentMetaCollecter::_collect_dict_for_column(ColumnIterator* column_iter, ColumnId cid, Column* column) {
    std::vector<Slice> words;
    if (!column_iter->all_page_dict_encoded()) {
        auto& tablet_column = _params->tablet_schema->column(cid);
        // For JSON data, the schema may be heterogeneous, meaning that some segments might not contain the dictionary column,
        // but a global dictionary could still be present and usable.
        if (!tablet_column.is_extended()) {
            return Status::GlobalDictError("no global dict");
        } else {
            return Status::OK();
        }
    } else {
        RETURN_IF_ERROR(column_iter->fetch_all_dict_words(&words));
    }

    if (words.size() > _params->low_cardinality_threshold) {
        return Status::GlobalDictError(fmt::format("global dict size:{} greater than low_cardinality_threshold:{}",
                                                   words.size(), _params->low_cardinality_threshold));
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

    auto offsets = array_column->offsets_column_mutable_ptr();
    auto& data = offsets->immutable_data();
    size_t end_offset = data.back();
    end_offset += words.size();
    offsets->append(end_offset);

    // add elements
    auto dst = array_column->elements_column_mutable_ptr();
    CHECK(dst->append_strings(words));

    if (column->is_nullable()) {
        nullable_column->null_column_data().emplace_back(0);
    }

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_dict_for_flatjson(ColumnId cid, Column* column) {
    auto& tablet_column = _segment->tablet_schema().column(cid);
    if (tablet_column.type() != TYPE_JSON) {
        return Status::InvalidArgument("not a flat json column");
    }
    auto column_reader = _segment->column(cid);
    RETURN_IF(column_reader == nullptr, Status::NotFound(fmt::format("column not found: {}", tablet_column.name())));

    auto& sub_readers = *column_reader->sub_readers();
    for (auto& sub_reader : sub_readers) {
        if (sub_reader->column_type() == TYPE_VARCHAR) {
            ASSIGN_OR_RETURN(auto source_iter, sub_reader->new_iterator());
            ColumnIteratorOptions iter_opts;
            iter_opts.check_dict_encoding = true;
            iter_opts.read_file = _read_file.get();
            iter_opts.stats = &_stats;
            RETURN_IF_ERROR(source_iter->init(iter_opts));

            Status st = (_collect_dict_for_column(source_iter.get(), cid, column));
            if (st.is_global_dict_error()) {
                // ignore
            } else if (!st.ok()) {
                return st;
            }
            VLOG(2) << "collect_dict_for_flatjson: " << sub_reader->name();
        }
    }

    return {};
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
    ColumnReader* col_reader = const_cast<ColumnReader*>(_segment->column(cid));
    if (col_reader == nullptr || col_reader->segment_zone_map() == nullptr) {
        return Status::NotFound("");
    }
    if (col_reader->column_type() != type) {
        return Status::InternalError("column type mismatch");
    }
    const ZoneMapPB* segment_zone_map_pb = col_reader->segment_zone_map();
    TypeInfoPtr type_info = get_type_info(delegate_type(type));
    if constexpr (!is_max) { // min
        Datum min;
        if (segment_zone_map_pb->has_not_null()) {
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &min, segment_zone_map_pb->min(), nullptr));
            column->append_datum(min);
        } else {
            column->append_nulls(1);
        }
    } else if constexpr (is_max) {
        Datum max;
        if (segment_zone_map_pb->has_not_null()) {
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &max, segment_zone_map_pb->max(), nullptr));
            column->append_datum(max);
        } else {
            column->append_nulls(1);
        }
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect_rows(Column* column, LogicalType type) {
    uint32_t num_rows = _segment->num_rows();
    column->append_datum(int64_t(num_rows));
    return Status::OK();
}

Status SegmentMetaCollecter::_collect_count(ColumnId cid, Column* column, LogicalType type) {
    if (!_column_iterators[cid]) {
        return Status::InvalidArgument("Invalid Collect Params.");
    }

    uint32_t num_rows = _segment->num_rows();
    size_t nulls = 0;
    RETURN_IF_ERROR(_column_iterators[cid]->seek_to_first());
    RETURN_IF_ERROR(_column_iterators[cid]->null_count(&nulls));
    column->append_datum(int64_t(num_rows - nulls));

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_column_size(ColumnId cid, Column* column, LogicalType type) {
    ColumnReader* col_reader = const_cast<ColumnReader*>(_segment->column(cid));
    RETURN_IF(col_reader == nullptr, Status::NotFound("column not found: " + std::to_string(cid)));

    size_t total_mem_footprint = _collect_column_size_recursive(col_reader);
    column->append_datum(int64_t(total_mem_footprint));
    return Status::OK();
}

Status SegmentMetaCollecter::_collect_column_compressed_size(ColumnId cid, Column* column, LogicalType type) {
    // Compressed size estimation: sum of data page sizes via ordinal index ranges
    ColumnReader* col_reader = const_cast<ColumnReader*>(_segment->column(cid));
    RETURN_IF(col_reader == nullptr, Status::NotFound("column not found: " + std::to_string(cid)));

    int64_t total = _collect_column_compressed_size_recursive(col_reader);
    column->append_datum(total);
    return Status::OK();
}

size_t SegmentMetaCollecter::_collect_column_size_recursive(ColumnReader* col_reader) {
    size_t total_mem_footprint = col_reader->total_mem_footprint();

    if (col_reader->sub_readers() != nullptr) {
        for (const auto& sub_reader : *col_reader->sub_readers()) {
            total_mem_footprint += _collect_column_size_recursive(sub_reader.get());
        }
    }

    return total_mem_footprint;
}

int64_t SegmentMetaCollecter::_collect_column_compressed_size_recursive(ColumnReader* col_reader) {
    OlapReaderStatistics stats;
    IndexReadOptions opts;
    opts.use_page_cache = false;
    opts.read_file = _read_file.get();
    opts.stats = &stats;

    Status status = col_reader->load_ordinal_index(opts);
    if (!status.ok()) {
        return 0; // Return 0 on error, caller should handle the error
    }

    int64_t total = col_reader->data_page_footprint();

    if (col_reader->sub_readers() != nullptr) {
        for (const auto& sub_reader : *col_reader->sub_readers()) {
            total += _collect_column_compressed_size_recursive(sub_reader.get());
        }
    }

    return total;
}

} // namespace starrocks
