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

#include "runtime/descriptors.h"

#include <protocol/TDebugProtocol.h>

#include <ios>
#include <sstream>

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/descriptors.pb.h"

namespace starrocks {
const int RowDescriptor::INVALID_IDX = -1;
SlotDescriptor::SlotDescriptor(SlotId id, std::string name, TypeDescriptor type)
        : _id(id),
          _type(std::move(type)),
          _parent(0),
          _col_name(std::move(name)),
          _col_unique_id(-1),
          _slot_idx(0),
          _slot_size(_type.get_slot_size()),
          _is_materialized(false),
          _is_output_column(false),
          _is_nullable(true),
          _is_virtual(false) {}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
        : _id(tdesc.id),
          _type(TypeDescriptor::from_thrift(tdesc.slotType)),
          _parent(tdesc.parent),
          _col_name(tdesc.colName),
          _col_unique_id(tdesc.col_unique_id),
          _col_physical_name(tdesc.col_physical_name),
          _slot_idx(tdesc.slotIdx),
          _slot_size(_type.get_slot_size()),
          _is_materialized(tdesc.isMaterialized),
          _is_output_column(tdesc.__isset.isOutputColumn ? tdesc.isOutputColumn : true),
          _is_nullable(tdesc.__isset.isNullable
                               ? tdesc.isNullable
                               : (tdesc.__isset.nullIndicatorBit ? tdesc.nullIndicatorBit != -1 : true)),
          _is_virtual(tdesc.__isset.is_virtual_column ? tdesc.is_virtual_column : false) {}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
          _type(TypeDescriptor::from_protobuf(pdesc.slot_type())),
          _parent(pdesc.parent()),
          _col_name(pdesc.col_name()),
          _col_unique_id(-1),
          _slot_idx(pdesc.slot_idx()),
          _slot_size(_type.get_slot_size()),
          _is_materialized(pdesc.is_materialized()),
          _is_output_column(true),
          _is_nullable(pdesc.has_is_nullable() ? pdesc.is_nullable() : pdesc.null_indicator_bit() != -1),
          _is_virtual(pdesc.is_virtual()) {}

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    *pslot->mutable_slot_type() = _type.to_protobuf();
    // NOTE: column_pos is not used anymore, use default value 0
    pslot->set_column_pos(0);
    // NOTE: _tuple_offset is not used anymore, use default value 0.
    pslot->set_byte_offset(0);
    pslot->set_null_indicator_byte(0);
    pslot->set_null_indicator_bit(_is_nullable ? 0 : -1);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_is_materialized(_is_materialized);
    pslot->set_is_nullable(_is_nullable);
    pslot->set_is_virtual(_is_virtual);
}

std::string SlotDescriptor::debug_string() const {
    std::stringstream out;
    out << "Slot(id=" << _id << " type=" << _type << " name=" << _col_name << " col_unique_id=" << _col_unique_id
        << " col_physical_name=" << _col_physical_name << " nullable=" << _is_nullable << ")";
    return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
        : _name(tdesc.tableName), _database(tdesc.dbName), _id(tdesc.id) {}

std::string TableDescriptor::debug_string() const {
    std::stringstream out;
    out << "#name=" << _name;
    return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
        : _id(tdesc.id), _table_desc(nullptr), _byte_size(tdesc.byteSize) {}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc)
        : _id(pdesc.id()), _table_desc(nullptr), _byte_size(pdesc.byte_size()) {}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);
    _decoded_slots.push_back(slot);
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    ptuple->set_byte_size(_byte_size);
    // NOTE: _num_null_bytes is not used, set a default value 1
    ptuple->set_num_null_bytes(1);
    ptuple->set_table_id(-1);
    // NOTE: _num_null_slots is not used, set a default value 1
    ptuple->set_num_null_slots(1);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id << " size=" << _byte_size;
    if (_table_desc != nullptr) {
        //out << " " << _table_desc->debug_string();
    }

    out << " slots=[";
    for (size_t i = 0; i < _slots.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << _slots[i]->debug_string();
    }

    out << "]";
    out << ")";
    return out.str();
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples) {
    DCHECK_GT(row_tuples.size(), 0);

    for (int row_tuple : row_tuples) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuple);
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != nullptr);
    }

    init_tuple_idx_map();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc) : _tuple_desc_map(1, tuple_desc) {
    init_tuple_idx_map();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (auto& i : _tuple_desc_map) {
        max_id = std::max(i->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    return _tuple_idx_map[id];
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (auto& i : _tuple_desc_map) {
        row_tuple_ids->push_back(i->id());
    }
}

void RowDescriptor::to_protobuf(google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) {
    row_tuple_ids->Clear();
    for (auto desc : _tuple_desc_map) {
        row_tuple_ids->Add(desc->id());
    }
}

bool RowDescriptor::is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

std::string RowDescriptor::debug_string() const {
    std::stringstream ss;

    ss << "tuple_desc_map: [";
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        ss << _tuple_desc_map[i]->debug_string();
        if (i != _tuple_desc_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_id_map: [";
    for (int i = 0; i < _tuple_idx_map.size(); ++i) {
        ss << _tuple_idx_map[i];
        if (i != _tuple_idx_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    return ss.str();
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    auto i = _tbl_desc_map.find(id);
    if (i == _tbl_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    auto i = _tuple_desc_map.find(id);
    if (i == _tuple_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor_with_column(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _slot_with_column_name_map.find(id);

    if (i == _slot_with_column_name_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

// return all registered tuple descriptors
void DescriptorTbl::get_tuple_descs(std::vector<TupleDescriptor*>* descs) const {
    descs->clear();

    for (auto i : _tuple_desc_map) {
        descs->push_back(i.second);
    }
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (auto i : _tuple_desc_map) {
        out << i.second->debug_string() << '\n';
    }

    return out.str();
}

std::string RowPositionDescriptor::debug_string() const {
    std::stringstream out;
    out << "RowPositionDescriptor(type=" << _type << " row_source_slot_id=" << _row_source_slot_id
        << " fetch_ref_slot_ids=[";
    for (const auto& slot_id : _fetch_ref_slot_ids) {
        out << slot_id << ", ";
    }
    out << "], lookup_ref_slot_ids=[";
    for (const auto& slot_id : _lookup_ref_slot_ids) {
        out << slot_id << ", ";
    }
    out << "])";
    return out.str();
}

RowPositionDescriptor* RowPositionDescriptor::from_thrift(const TRowPositionDescriptor& t_desc, ObjectPool* pool) {
    RowPositionDescriptor* desc = nullptr;
    switch (t_desc.row_position_type) {
    case TRowPositionType::ICEBERG_V3_ROW_POSITION: {
        std::vector<SlotId> fetch_ref_slot_ids;
        for (const auto& slot_id : t_desc.fetch_ref_slots) {
            fetch_ref_slot_ids.emplace_back(slot_id);
        }
        std::vector<SlotId> lookup_ref_slot_ids;
        for (const auto& slot_id : t_desc.lookup_ref_slots) {
            lookup_ref_slot_ids.emplace_back(slot_id);
        }
        desc = pool->add(
                new IcebergV3RowPositionDescriptor(t_desc.row_source_slot, fetch_ref_slot_ids, lookup_ref_slot_ids));
        break;
    }
    default: {
        DCHECK(false) << "Unknown row position type: " << t_desc.row_position_type;
        break;
    }
    }
    return desc;
}

} // namespace starrocks
