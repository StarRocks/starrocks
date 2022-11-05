// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <memory>
#include <orc/OrcFile.hh>
#include <unordered_map>

#include "common/status.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"

namespace starrocks::vectorized {

class OrcMapping;
using OrcMappingPtr = std::shared_ptr<OrcMapping>;

// OrcMapping is used to create a global mapping for orc
// Why we need this?
// One reason is that in orc, field name is not unique, different nested level may have the same field name.
// But luckily, orc provide a global unique column id, so we can do
// subfield materlizied, subfield lazy materlizied according global unique column id.
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
// 3->7
// └── 0->8
//     ├── 0->9
//     └── 1->10
class OrcMapping {
public:
    size_t get_column_id(size_t src_pos);

    // Only Array, Map, Struct contains child mapping, other primitive types will return nullptr directly.
    // src_pos is origin column position in table defination.
    const OrcMappingPtr get_child_mapping(size_t src_pos);

    void clear();

    static std::unique_ptr<OrcMapping> build_mapping(const std::vector<SlotDescriptor*>& slot_descs,
                                                     const orc::Type& root_orc_type, const bool case_sensitve);

    Status set_include_column_id(const uint64_t slot_pos, const TypeDescriptor& desc,
                                 std::list<uint64_t>* column_id_list);

private:
    // index in src_slot_descriptor -> column id in orc types
    std::unordered_map<size_t, size_t> _mapping;
    // Used for complex data types.
    std::unordered_map<size_t, OrcMappingPtr> _children;

    void add_mapping(size_t pos_in_src, size_t orc_column_id);
    void add_child(size_t pos_in_src, const OrcMappingPtr& child_mapping);

    static Status _init_orc_mapping(std::unique_ptr<OrcMapping>& mapping,
                                    const std::vector<SlotDescriptor*>& slot_descs, const orc::Type& orc_root_type,
                                    const bool case_sensitve);

    static Status _set_child_mapping(const OrcMappingPtr& mapping, const TypeDescriptor& origin_type,
                                     const orc::Type& orc_type, const bool case_sensitive);

    Status set_include_column_id_by_type(const OrcMappingPtr mapping, const TypeDescriptor& desc,
                                         std::list<uint64_t>* column_id_list);

    static std::string format_column_name(const std::string& col_name, bool case_sensitive);
};
} // namespace starrocks::vectorized