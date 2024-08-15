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

#include <memory>
#include <string>
#include <vector>

#include "column/column.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

class Field;
class ObjectPool;
class RuntimeState;

/*
 * Used to describe the access path of the subfield, it's like a file path.
 * e.g. 
 *  column: structA STRUCT<a STRCUT<a1 INT, b STRUCT<c INT>>>
 *  path: /structA/a/b/c
 *  type: /ROOT/FIELD/FIELD/FIELD
 *  index: /7/0/1/0, offset in storage
 */
class ColumnAccessPath {
public:
    static StatusOr<std::unique_ptr<ColumnAccessPath>> create(const TColumnAccessPath& column_path, RuntimeState* state,
                                                              ObjectPool* pool);

    Status init(const std::string& parent_path, const TColumnAccessPath& column_path, RuntimeState* state,
                ObjectPool* pool);

    static StatusOr<std::unique_ptr<ColumnAccessPath>> create(const TAccessPathType::type& type,
                                                              const std::string& path, uint32_t index,
                                                              const std::string& prefix = "");
    // the path doesn't contains root
    static void insert_json_path(ColumnAccessPath* root, LogicalType type, const std::string& path);

    const std::string& path() const { return _path; }

    uint32_t index() const { return _column_index; }

    const std::vector<std::unique_ptr<ColumnAccessPath>>& children() const { return _children; }

    std::vector<std::unique_ptr<ColumnAccessPath>>& children() { return _children; }

    void set_from_compaction(bool from_compaction) { _from_compaction = from_compaction; }

    bool is_from_compaction() const { return _from_compaction; }

    bool is_key() const { return _type == TAccessPathType::type::KEY; }

    bool is_offset() const { return _type == TAccessPathType::type::OFFSET; }

    bool is_field() const { return _type == TAccessPathType::type::FIELD; }

    bool is_all() const { return _type == TAccessPathType::type::ALL; }

    bool is_index() const { return _type == TAccessPathType::type::INDEX; }

    bool is_from_predicate() const { return _from_predicate; }

    const std::string& absolute_path() const { return _absolute_path; }

    // flat json use this to get the type of the path
    const TypeDescriptor& value_type() const { return _value_type; }

    // segement may have different column schema(because schema change),
    // we need copy one and set the offset of schema, to help column reader find column access path
    StatusOr<std::unique_ptr<ColumnAccessPath>> convert_by_index(const Field* field, uint32_t index);

    ColumnAccessPath* get_child(const std::string& path);

    const std::string to_string() const;

    size_t leaf_size() const;

    void get_all_leafs(std::vector<ColumnAccessPath*>* result);

private:
    // path type, to mark the path is KEY/OFFSET/FIELD/ALL/INDEX
    TAccessPathType::type _type;

    std::string _path;

    std::string _absolute_path;

    // column index in storage
    // the root index is the offset of table schema
    // the FIELD index is the offset of struct schema
    // it's unused for MAP/JSON/ARRAY now
    uint32_t _column_index;

    bool _from_predicate;

    bool _from_compaction = false;

    // the data type of the subfield
    TypeDescriptor _value_type;

    std::vector<std::unique_ptr<ColumnAccessPath>> _children;
};

using ColumnAccessPathPtr = std::unique_ptr<ColumnAccessPath>;

inline std::ostream& operator<<(std::ostream& out, const ColumnAccessPath& val) {
    out << val.to_string();
    return out;
}

} // namespace starrocks
