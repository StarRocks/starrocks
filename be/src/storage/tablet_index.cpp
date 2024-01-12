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

#include "storage/tablet_index.h"

#include "storage/utils.h"
#include "util/json_util.h"

namespace starrocks {

/******************************************************************
 * TabletIndex
 ******************************************************************/

Status TabletIndex::init_from_thrift(const TOlapTableIndex& index, const TabletSchema& tablet_schema) {
    _index_name = index.index_name;
    _index_id = index.index_id;
    // init col_unique_id in index at be side, since col_unique_id may be -1 at fe side
    // get column unique id by name
    std::vector<int32_t> col_unique_ids(index.columns.size());
    for (size_t i = 0; i < index.columns.size(); i++) {
        auto column_idx = tablet_schema.field_index(index.columns[i]);
        col_unique_ids[i] = tablet_schema.column(column_idx).unique_id();
    }
    _col_unique_ids = std::move(col_unique_ids);

    ASSIGN_OR_RETURN(_index_type, _convert_index_type_from_thrift(index.index_type));
    FILL_INDEX_PARAM(common_properties)
    FILL_INDEX_PARAM(index_properties)
    FILL_INDEX_PARAM(search_properties)
    FILL_INDEX_PARAM(extra_properties)
    return Status::OK();
}

Status TabletIndex::init_from_pb(const TabletIndexPB& index) {
    _index_id = index.index_id();
    _index_name = index.index_name();
    _index_type = index.index_type();
    _col_unique_ids.clear();
    for (const auto& col_unique_id : index.col_unique_id()) {
        _col_unique_ids.push_back(col_unique_id);
    }
    if (index.has_index_properties()) {
        const auto& serialized_prop_stream = index.index_properties();
        std::map<std::string, std::map<std::string, std::string>> properties;
        auto status = from_json(serialized_prop_stream, &properties);
        if (status.ok()) {
            FILL_INDEX_INTERNAL(common_properties)
            FILL_INDEX_INTERNAL(index_properties)
            FILL_INDEX_INTERNAL(search_properties)
            FILL_INDEX_INTERNAL(extra_properties)
        } else {
            LOG(WARNING) << "parse json from serialized_prop_stream error, content is '" << serialized_prop_stream
                         << "'";
            return status;
        }
    }
    return Status::OK();
}

void TabletIndex::to_schema_pb(TabletIndexPB* index) const {
    index->set_index_id(_index_id);
    index->set_index_name(_index_name);
    index->clear_col_unique_id();
    for (const auto& col_unique_id : _col_unique_ids) {
        index->add_col_unique_id(col_unique_id);
    }
    index->set_index_type(_index_type);
    std::map<std::string, std::map<std::string, std::string>> map;
    FILL_INDEX_TO_MAP(common_properties, map)
    FILL_INDEX_TO_MAP(index_properties, map)
    FILL_INDEX_TO_MAP(search_properties, map)
    FILL_INDEX_TO_MAP(extra_properties, map)

    std::string meta_string = to_json(map);
    index->set_index_properties(meta_string);
}

const std::string TabletIndex::properties_to_json() const {
    std::map<std::string, std::map<std::string, std::string>> map;
    FILL_INDEX_TO_MAP(common_properties, map)
    FILL_INDEX_TO_MAP(index_properties, map)
    FILL_INDEX_TO_MAP(search_properties, map)
    FILL_INDEX_TO_MAP(extra_properties, map)

    return to_json(map);
}

StatusOr<IndexType> TabletIndex::_convert_index_type_from_thrift(TIndexType::type index_type) {
    switch (index_type) {
    case TIndexType::BITMAP:
        return IndexType::BITMAP;
    case TIndexType::GIN:
        return IndexType::GIN;
    default:
        // Handle other potential TIndexTypes or set a default value and/or log an error
        std::string type_str;
        EnumToString(TIndexType, index_type, type_str);
        LOG(ERROR) << "Unhandled index type: " << type_str;
        return Status::InternalError("Unexpect index type: " + type_str);
    }
}

} // namespace starrocks