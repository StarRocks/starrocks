// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_info.cpp

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

#include "exec/tablet_info.h"

#include <memory>

#include "runtime/mem_pool.h"

namespace starrocks {

static const std::string LOAD_OP_COLUMN = "__op";

void OlapTableIndexSchema::to_protobuf(POlapTableIndexSchema* pindex) const {
    pindex->set_id(index_id);
    pindex->set_schema_hash(schema_hash);
    for (auto slot : slots) {
        pindex->add_columns(slot->col_name());
    }
}

Status OlapTableSchemaParam::init(const POlapTableSchemaParam& pschema) {
    _db_id = pschema.db_id();
    _table_id = pschema.table_id();
    _version = pschema.version();
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(pschema.tuple_desc()));
    for (auto& p_slot_desc : pschema.slot_descs()) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(p_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& p_index : pschema.indexes()) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = p_index.id();
        index->schema_hash = p_index.schema_hash();
        for (auto& col : p_index.columns()) {
            auto it = slots_map.find(col);
            if (it != std::end(slots_map)) {
                index->slots.emplace_back(it->second);
            }
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(), [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
        return lhs->index_id < rhs->index_id;
    });
    return Status::OK();
}

Status OlapTableSchemaParam::init(const TOlapTableSchemaParam& tschema) {
    _db_id = tschema.db_id;
    _table_id = tschema.table_id;
    _version = tschema.version;
    std::map<std::string, SlotDescriptor*> slots_map;
    _tuple_desc = _obj_pool.add(new TupleDescriptor(tschema.tuple_desc));
    for (auto& t_slot_desc : tschema.slot_descs) {
        auto slot_desc = _obj_pool.add(new SlotDescriptor(t_slot_desc));
        _tuple_desc->add_slot(slot_desc);
        slots_map.emplace(slot_desc->col_name(), slot_desc);
    }
    for (auto& t_index : tschema.indexes) {
        auto index = _obj_pool.add(new OlapTableIndexSchema());
        index->index_id = t_index.id;
        index->schema_hash = t_index.schema_hash;
        for (auto& col : t_index.columns) {
            auto it = slots_map.find(col);
            if (it != std::end(slots_map)) {
                index->slots.emplace_back(it->second);
            }
        }
        _indexes.emplace_back(index);
    }

    std::sort(_indexes.begin(), _indexes.end(), [](const OlapTableIndexSchema* lhs, const OlapTableIndexSchema* rhs) {
        return lhs->index_id < rhs->index_id;
    });
    return Status::OK();
}

void OlapTableSchemaParam::to_protobuf(POlapTableSchemaParam* pschema) const {
    pschema->set_db_id(_db_id);
    pschema->set_table_id(_table_id);
    pschema->set_version(_version);
    _tuple_desc->to_protobuf(pschema->mutable_tuple_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(pschema->add_slot_descs());
    }
    for (auto index : _indexes) {
        index->to_protobuf(pschema->add_indexes());
    }
}

std::string OlapTableSchemaParam::debug_string() const {
    std::stringstream ss;
    ss << "tuple_desc=" << _tuple_desc->debug_string();
    return ss.str();
}

} // namespace starrocks
