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

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>
#include <unordered_map>

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks {

class OrcMapping;
using OrcMappingPtr = std::shared_ptr<OrcMapping>;

struct OrcMappingOrOrcColumnId {
    const OrcMappingPtr orc_mapping = nullptr;
    // Large invalid number
    size_t orc_column_id = SIZE_MAX;
};

// OrcMapping is used to create a global mapping for orc
// Why we need this?
// One reason is that in orc, field name is not unique, different nested level may have the same field name.
// But luckily, orc provide a global unique column id, so we can do
// subfield materialized, subfield lazy materialized according global unique column id.
// Another reason is that in orc, struct subfield position may different from position in create table sql.
// So we need to find specific column by subfield name rather than column position.
// And considering that we will frequently access this mapping relationship, we need to maintain an OrcMapping globally.
//
// ============NOTICE============
// OrcMapping's structure liking struct, it has a nested relationship (Only complex types will have nested mapping).
//
// First level: std::vector(SlotDescriptor) => Top level of orc type.
// Second level(Only SlotDescriptor's type is complex type will contain this level): subfield position => second level position of orc.
// If current level is Array Type, it will has only one child mapping.
// If current level is Map Type, it will has only two child mapping(Key & Value).
// ==============Example============
// Define a table:
// {
//      col1: INT,
//      col2: MAP<INT, STRUCT<col2_1: INT, col2_2: INT>>
//      col3: ARRAY<STRUCT<col3_1: INT, col3_2: INT>>
// }
// Orc file content, orc is a flatten structure:
// root: 0
// ├── col1: 1
// ├── col2: 2
// │   ├── key: 3
// │   └── value: 4
// │       ├── col2_1: 5
// │       └── col2_2: 6
// └── col3: 7
//     └── col3.element: 8
//         ├── col3_1: 9
//         └── col3_2: 10
//
// The result of OrcMapping, left is src_pos, right is orc column id.
// 0->1
// 1->2
// ├── 0->3
// └── 1->4
//     ├── 0->5
//     └── 1->6
// 2->7
// └── 0->8
//     ├── 0->9
//     └── 1->10
class OrcMapping {
public:
    // Only Array, Map, Struct contains child mapping, other logical types will return nullptr directly.
    // src_pos is origin column position in table definition.
    const OrcMappingOrOrcColumnId& get_column_id_or_child_mapping(size_t original_pos_in_table_definition);

    void add_mapping(size_t pos_in_src, size_t orc_column_id, const OrcMappingPtr& child_mapping);

    void clear();

    // Only include leaf node
    Status set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                 std::list<uint64_t>* column_id_list);

    // Include in slot_descriptor level, complex type's lazy load is not support now.
    Status set_lazyload_column_id(const uint64_t slot_pos, std::list<uint64_t>* column_id_list);

private:
    std::unordered_map<size_t, OrcMappingOrOrcColumnId> _mapping;

    Status set_include_column_id_by_type(const OrcMappingPtr& mapping, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list);
};

class OrcMappingFactory {
public:
    // NOTICE: orc_use_column_names will only control first level behavior, but struct subfield will still use
    // column name rather than position in table definition.
    static StatusOr<std::unique_ptr<OrcMapping>> build_mapping(const std::vector<SlotDescriptor*>& slot_descs,
                                                               const orc::Type& root_orc_type,
                                                               const bool case_sensitive,
                                                               const bool orc_use_column_names,
                                                               const std::vector<std::string>* hive_column_names);

private:
    static Status _check_orc_type_can_converte_2_logical_type(const orc::Type& orc_source_type,
                                                              const TypeDescriptor& slot_target_type);

    static Status _init_orc_mapping_with_orc_column_names(std::unique_ptr<OrcMapping>& mapping,
                                                          const std::vector<SlotDescriptor*>& slot_descs,
                                                          const orc::Type& orc_root_type, const bool case_sensitive);

    static Status _init_orc_mapping_with_hive_column_names(std::unique_ptr<OrcMapping>& mapping,
                                                           const std::vector<SlotDescriptor*>& slot_descs,
                                                           const orc::Type& orc_root_type, const bool case_sensitive,
                                                           const std::vector<std::string>* hive_column_names);

    static Status _set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
                                     const orc::Type& orc_type, const bool case_sensitive);
};

} // namespace starrocks
