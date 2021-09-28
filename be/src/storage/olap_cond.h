// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/olap_cond.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/column_data_file.pb.h"
#include "storage/field.h"
#include "storage/row_cursor.h"
#include "storage/rowset/segment_v2/bloom_filter.h"
#include "storage/wrapper_field.h"

namespace starrocks {

class WrapperField;
class RowCursorCell;

enum CondOp {
    OP_NULL = -1, // invalid op
    OP_EQ = 0,    // equal
    OP_NE = 1,    // not equal
    OP_LT = 2,    // less than
    OP_LE = 3,    // less or equal
    OP_GT = 4,    // greater than
    OP_GE = 5,    // greater or equal
    OP_IN = 6,    // in
    OP_IS = 7,    // is null or not null
    OP_NOT_IN = 8 // not in
};

// Hash functor for IN set
struct FieldHash {
    size_t operator()(const WrapperField* field) const { return field->hash_code(); }
};

// Equal function for IN set
struct FieldEqual {
    bool operator()(const WrapperField* left, const WrapperField* right) const { return left->cmp(right) == 0; }
};

struct Cond {
public:
    Cond();
    ~Cond();

    OLAPStatus init(const TCondition& tcond, const TabletColumn& column);

    bool eval(const RowCursorCell& cell) const;

    bool eval(const KeyRange& statistic) const;
    int del_eval(const KeyRange& stat) const;

    bool eval(const segment_v2::BloomFilter* bf) const;

    bool can_do_bloom_filter() const { return op == OP_EQ || op == OP_IN || op == OP_IS; }

    Status convert_to(FieldType new_type, std::unique_ptr<Cond>* output) const;

    CondOp op{OP_NULL};
    // valid when op is not OP_IN or OP_NOT_IN
    WrapperField* operand_field{nullptr};
    // valid when op is OP_IN or OP_NOT_IN
    using FieldSet = std::unordered_set<const WrapperField*, FieldHash, FieldEqual>;
    FieldSet operand_set;
    // valid when op is OP_IN or OP_NOT_IN, represents the minimum value of in elements
    WrapperField* min_value_filed{nullptr};
    // valid when op is OP_IN or OP_NOT_IN, represents the maximum value of in elements
    WrapperField* max_value_filed{nullptr};
};

class CondColumn {
public:
    CondColumn(const TabletSchema& tablet_schema, int32_t index) : _col_index(index) {
        _conds.clear();
        _is_key = tablet_schema.column(_col_index).is_key();
    }
    ~CondColumn();

    OLAPStatus add_cond(const TCondition& tcond, const TabletColumn& column);

    bool eval(const RowCursor& row) const;

    bool eval(const std::pair<WrapperField*, WrapperField*>& statistic) const;
    int del_eval(const std::pair<WrapperField*, WrapperField*>& statistic) const;

    bool eval(const segment_v2::BloomFilter* bf) const;

    bool can_do_bloom_filter() const {
        for (auto& cond : _conds) {
            if (cond->can_do_bloom_filter()) {
                // if any cond can do bloom filter
                return true;
            }
        }
        return false;
    }

    inline bool is_key() const { return _is_key; }

    const std::vector<Cond*>& conds() const { return _conds; }

    Status convert_to(FieldType new_type, std::unique_ptr<CondColumn>* output) const;

private:
    CondColumn() {}
    bool _is_key{true};
    int32_t _col_index{-1};
    std::vector<Cond*> _conds;
};

class Conditions {
public:
    // Key: field index of condition's column
    // Value: CondColumn object
    typedef std::map<int32_t, CondColumn*> CondColumns;

    Conditions() = default;
    ~Conditions() { finalize(); }

    void finalize() {
        for (auto& it : _columns) {
            delete it.second;
        }
        _columns.clear();
    }

    void set_tablet_schema(const TabletSchema* schema) { _schema = schema; }

    OLAPStatus append_condition(const TCondition& condition);

    bool delete_conditions_eval(const RowCursor& row) const;

    const CondColumns& columns() const { return _columns; }

    CondColumn* get_column(int32_t cid) const;

    Status convert_to(const Conditions** output, const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const;

private:
    int32_t _get_field_index(const std::string& field_name) const {
        for (int i = 0; i < _schema->num_columns(); i++) {
            if (_schema->column(i).name() == field_name) {
                return i;
            }
        }
        LOG(WARNING) << "invalid field name. [name='" << field_name << "']";
        return -1;
    }

    bool _cond_column_is_key_or_duplicate(const CondColumn* cc) const {
        return cc->is_key() || _schema->keys_type() == KeysType::DUP_KEYS;
    }

private:
    const TabletSchema* _schema;
    CondColumns _columns; // list of condition column
};

} // namespace starrocks
