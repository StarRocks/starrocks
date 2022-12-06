// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "storage/meta_reader.h"

#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/status.h"
#include "runtime/global_dict/config.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset.h"
namespace starrocks::vectorized {

std::vector<std::string> SegmentMetaCollecter::support_collect_fields = {"dict_merge", "max", "min"};

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
    return Status::InvalidArgument(item);
}

MetaReader::MetaReader() : _is_init(false), _has_more(false) {}

Status MetaReader::open() {
    return Status::OK();
}

Status MetaReader::_read(Chunk* chunk, size_t n) {
    std::vector<vectorized::Column*> columns;
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

bool MetaReader::has_more() {
    return _has_more;
}

SegmentMetaCollecter::SegmentMetaCollecter(SegmentSharedPtr segment) : _segment(std::move(segment)) {}

SegmentMetaCollecter::~SegmentMetaCollecter() = default;

Status SegmentMetaCollecter::init(const SegmentMetaCollecterParams* params) {
    _params = params;
    return Status::OK();
}

Status SegmentMetaCollecter::open() {
    RETURN_IF_ERROR(_init_return_column_iterators());
    return Status::OK();
}

Status SegmentMetaCollecter::_init_return_column_iterators() {
    DCHECK_EQ(_params->fields.size(), _params->cids.size());
    DCHECK_EQ(_params->fields.size(), _params->read_page.size());

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_segment->file_name()));
    ASSIGN_OR_RETURN(_read_file, fs->new_random_access_file(_segment->file_name()));

    _column_iterators.resize(_params->max_cid + 1, nullptr);
    for (int i = 0; i < _params->fields.size(); i++) {
        if (_params->read_page[i]) {
            auto cid = _params->cids[i];
            if (_column_iterators[cid] == nullptr) {
                RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
                _obj_pool.add(_column_iterators[cid]);

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

Status SegmentMetaCollecter::collect(std::vector<vectorized::Column*>* dsts) {
    DCHECK_EQ(dsts->size(), _params->fields.size());

    for (size_t i = 0; i < _params->fields.size(); i++) {
        RETURN_IF_ERROR(_collect(_params->fields[i], _params->cids[i], (*dsts)[i], _params->field_type[i]));
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect(const std::string& name, ColumnId cid, vectorized::Column* column,
                                      LogicalType type) {
    if (name == "dict_merge") {
        return _collect_dict(cid, column, type);
    } else if (name == "max") {
        return _collect_max(cid, column, type);
    } else if (name == "min") {
        return _collect_min(cid, column, type);
    }
    return Status::NotSupported("Not Support Collect Meta: " + name);
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, vectorized::Column* column, LogicalType type) {
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

    vectorized::ArrayColumn* array_column = nullptr;
    array_column = down_cast<vectorized::ArrayColumn*>(column);

    auto* offsets = array_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();
    end_offset += words.size();
    offsets->append(end_offset);

    // add elements
    auto dst = array_column->elements_column().get();
    CHECK(dst->append_strings(words));

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_max(ColumnId cid, vectorized::Column* column, LogicalType type) {
    return __collect_max_or_min<true>(cid, column, type);
}

Status SegmentMetaCollecter::_collect_min(ColumnId cid, vectorized::Column* column, LogicalType type) {
    return __collect_max_or_min<false>(cid, column, type);
}

template <bool is_max>
Status SegmentMetaCollecter::__collect_max_or_min(ColumnId cid, vectorized::Column* column, LogicalType type) {
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
        vectorized::Datum min;
        if (!segment_zone_map_pb->has_null()) {
            RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &min, segment_zone_map_pb->min(), nullptr));
            column->append_datum(min);
        }
    } else if constexpr (is_max) {
        vectorized::Datum max;
        if (segment_zone_map_pb->has_not_null()) {
            RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &max, segment_zone_map_pb->max(), nullptr));
            column->append_datum(max);
        }
    }
    return Status::OK();
}

} // namespace starrocks::vectorized
