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

#pragma once

#include "gen_cpp/Descriptors_types.h"
#include "storage/tablet_schema.h"

namespace starrocks {

#define FILL_INDEX_PARAM(index_properties_name)              \
    if (index.__isset.index_properties_name) {               \
        for (const auto& kv : index.index_properties_name) { \
            _##index_properties_name[kv.first] = kv.second;  \
        }                                                    \
    }

#define FILL_INDEX_INTERNAL(properties_name)                                   \
    if (auto it = properties.find(#properties_name); it != properties.end()) { \
        _##properties_name = std::move(it->second);                            \
        properties.erase(it);                                                  \
    }

#define FILL_INDEX_TO_MAP(properties_name, map_name) map_name.emplace(#properties_name, _##properties_name);

class TabletSchema;

// Tablet-level index metadata, can be utilized to build, query and compact indexes.
class TabletIndex {
public:
    TabletIndex() = default;

    Status init_from_thrift(const starrocks::TOlapTableIndex& index, const TabletSchema& tablet_schema);
    Status init_from_pb(const TabletIndexPB& index);
    void to_schema_pb(TabletIndexPB* index) const;
    const std::string properties_to_json() const;

    const int64_t index_id() const { return _index_id; }
    const std::string& index_name() const { return _index_name; }
    const IndexType index_type() const { return _index_type; }
    const std::vector<int32_t>& col_unique_ids() const { return _col_unique_ids; }
    const bool contains_column(int32_t column_uid) const {
        for (const int32_t& uid : _col_unique_ids) {
            if (uid == column_uid) return true;
        }
        return false;
    }

    const std::map<std::string, std::string>& common_properties() const { return _common_properties; }
    const std::map<std::string, std::string>& index_properties() const { return _index_properties; }
    const std::map<std::string, std::string>& search_properties() const { return _search_properties; }
    const std::map<std::string, std::string>& extra_properties() const { return _extra_properties; }

    // ================ for ut ================
    void add_common_properties(const std::string& key, const std::string& value) {
        _common_properties.insert(std::make_pair(key, value));
    }
    void add_index_properties(const std::string& key, const std::string& value) {
        _index_properties.insert(std::make_pair(key, value));
    }
    void add_search_properties(const std::string& key, const std::string& value) {
        _search_properties.insert(std::make_pair(key, value));
    }
    void add_extra_properties(const std::string& key, const std::string& value) {
        _extra_properties.insert(std::make_pair(key, value));
    }

    int64_t mem_usage() const {
        int64_t mem_usage = sizeof(TabletIndex);
        for (const auto& p : _common_properties) {
            mem_usage += sizeof(char) * (p.first.size() + p.second.size());
        }
        for (const auto& p : _index_properties) {
            mem_usage += sizeof(char) * (p.first.size() + p.second.size());
        }
        for (const auto& p : _search_properties) {
            mem_usage += sizeof(char) * (p.first.size() + p.second.size());
        }
        for (const auto& p : _extra_properties) {
            mem_usage += sizeof(char) * (p.first.size() + p.second.size());
        }
        mem_usage += sizeof(int32_t) * _col_unique_ids.size();
        mem_usage += sizeof(char) * _index_name.size();

        return mem_usage;
    }

private:
    int32_t _index_id = -1;
    std::string _index_name;
    IndexType _index_type = INDEX_UNKNOWN;
    std::vector<int32_t> _col_unique_ids;
    std::map<std::string, std::string> _common_properties;
    std::map<std::string, std::string> _index_properties;
    std::map<std::string, std::string> _search_properties;
    std::map<std::string, std::string> _extra_properties;

    StatusOr<IndexType> _convert_index_type_from_thrift(TIndexType::type index_type);
};

} // namespace starrocks