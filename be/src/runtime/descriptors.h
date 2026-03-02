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

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/common.h>

#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"     // for TTupleId
#include "gen_cpp/FrontendService_types.h" // for TTupleId
#include "gen_cpp/Types_types.h"
#include "types/type_descriptor.h"

namespace starrocks {

class ObjectPool;
class TDescriptorTable;
class TSlotDescriptor;
class TTupleDescriptor;
class Expr;
class ExprContext;
class RuntimeState;
class SchemaScanner;
class IcebergDeleteFileMeta;
class OlapTableSchemaParam;
class PTupleDescriptor;
class PSlotDescriptor;
class RowPositionDescriptor;

class SlotDescriptor {
public:
    SlotDescriptor(SlotId id, std::string name, TypeDescriptor type);

    SlotId id() const { return _id; }
    const TypeDescriptor& type() const { return _type; }
    TypeDescriptor& type() { return _type; }
    TupleId parent() const { return _parent; }
    bool is_materialized() const { return _is_materialized; }
    bool is_output_column() const { return _is_output_column; }
    bool is_nullable() const { return _is_nullable; }
    bool is_virtual() const { return _is_virtual; }

    int slot_size() const { return _slot_size; }

    const std::string& col_name() const { return _col_name; }

    void to_protobuf(PSlotDescriptor* pslot) const;

    std::string debug_string() const;

    int32_t col_unique_id() const { return _col_unique_id; }
    const std::string& col_physical_name() const { return _col_physical_name; }

    SlotDescriptor(const TSlotDescriptor& tdesc);

private:
    friend class DescriptorTbl;
    friend class TupleDescriptor;
    friend class SchemaScanner;
    friend class OlapTableSchemaParam;
    friend class IcebergDeleteFileMeta;

    const SlotId _id;
    TypeDescriptor _type;
    const TupleId _parent;
    const std::string _col_name;
    const int32_t _col_unique_id;
    const std::string _col_physical_name;

    // the idx of the slot in the tuple descriptor (0-based).
    // this is provided by the FE
    const int _slot_idx;

    // the byte size of this slot.
    const int _slot_size;

    const bool _is_materialized;
    const bool _is_output_column;

    const bool _is_nullable;

    const bool _is_virtual;

    SlotDescriptor(const PSlotDescriptor& pdesc);
};

// Base class for table descriptors.
class TableDescriptor {
public:
    TableDescriptor(const TTableDescriptor& tdesc);
    virtual ~TableDescriptor() = default;
    TableId table_id() const { return _id; }
    virtual std::string debug_string() const;

    const std::string& name() const { return _name; }
    const std::string& database() const { return _database; }

private:
    std::string _name;
    std::string _database;
    TableId _id;
};

class TupleDescriptor {
public:
    int byte_size() const { return _byte_size; }
    const std::vector<SlotDescriptor*>& slots() const { return _slots; }
    std::vector<SlotDescriptor*>& slots() { return _slots; }
    const std::vector<SlotDescriptor*>& decoded_slots() const { return _decoded_slots; }
    std::vector<SlotDescriptor*>& decoded_slots() { return _decoded_slots; }
    const TableDescriptor* table_desc() const { return _table_desc; }
    void set_table_desc(TableDescriptor* table_desc) { _table_desc = table_desc; }

    SlotDescriptor* get_slot_by_id(SlotId id) const {
        for (auto s : _slots) {
            if (s->id() == id) {
                return s;
            }
        }
        return nullptr;
    }

    TupleId id() const { return _id; }

    std::string debug_string() const;

    void to_protobuf(PTupleDescriptor* ptuple) const;

private:
    friend class DescriptorTbl;
    friend class OlapTableSchemaParam;

    const TupleId _id;
    TableDescriptor* _table_desc;
    int _byte_size;
    std::vector<SlotDescriptor*> _slots; // contains all slots
    // For a low cardinality string column with global dict,
    // The type in _slots is int, in _decode_slots is varchar
    std::vector<SlotDescriptor*> _decoded_slots;

    TupleDescriptor(const TTupleDescriptor& tdesc);
    TupleDescriptor(const PTupleDescriptor& tdesc);
    void add_slot(SlotDescriptor* slot);
};

class DescriptorTbl {
public:
    // Creates a descriptor tbl within 'pool' from thrift_tbl and returns it via 'tbl'.
    // Returns OK on success, otherwise error (in which case 'tbl' will be unset).
    static Status create(RuntimeState* state, ObjectPool* pool, const TDescriptorTable& thrift_tbl, DescriptorTbl** tbl,
                         int32_t chunk_size);

    TableDescriptor* get_table_descriptor(TableId id) const;
    TupleDescriptor* get_tuple_descriptor(TupleId id) const;
    SlotDescriptor* get_slot_descriptor(SlotId id) const;
    SlotDescriptor* get_slot_descriptor_with_column(SlotId id) const;

    // return all registered tuple descriptors
    void get_tuple_descs(std::vector<TupleDescriptor*>* descs) const;

    std::string debug_string() const;

private:
    typedef std::unordered_map<TableId, TableDescriptor*> TableDescriptorMap;
    typedef std::unordered_map<TupleId, TupleDescriptor*> TupleDescriptorMap;
    typedef std::unordered_map<SlotId, SlotDescriptor*> SlotDescriptorMap;

    TableDescriptorMap _tbl_desc_map;
    TupleDescriptorMap _tuple_desc_map;
    SlotDescriptorMap _slot_desc_map;
    SlotDescriptorMap _slot_with_column_name_map;

    DescriptorTbl() = default;
};

// Records positions of tuples within row produced by ExecNode.
// TODO: this needs to differentiate between tuples contained in row
// and tuples produced by ExecNode (parallel to PlanNode.rowTupleIds and
// PlanNode.tupleIds); right now, we conflate the two (and distinguish based on
// context; for instance, HdfsScanNode uses these tids to create row batches, ie, the
// first case, whereas TopNNode uses these tids to copy output rows, ie, the second
// case)
class RowDescriptor {
public:
    RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples);

    // standard copy c'tor, made explicit here
    RowDescriptor(const RowDescriptor& desc) = default;

    RowDescriptor(TupleDescriptor* tuple_desc);

    // dummy descriptor, needed for the JNI EvalPredicate() function
    RowDescriptor() = default;

    static const int INVALID_IDX;

    // Returns INVALID_IDX if id not part of this row.
    int get_tuple_idx(TupleId id) const;

    // Return descriptors for all tuples in this row, in order of appearance.
    const std::vector<TupleDescriptor*>& tuple_descriptors() const { return _tuple_desc_map; }

    // Populate row_tuple_ids with our ids.
    void to_thrift(std::vector<TTupleId>* row_tuple_ids);
    void to_protobuf(google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids);

    // Return true if the tuple ids of this descriptor are a prefix
    // of the tuple ids of other_desc.
    bool is_prefix_of(const RowDescriptor& other_desc) const;

    // Return true if the tuple ids of this descriptor match tuple ids of other desc.
    bool equals(const RowDescriptor& other_desc) const;

    std::string debug_string() const;

private:
    // Initializes tupleIdxMap during c'tor using the _tuple_desc_map.
    void init_tuple_idx_map();

    // map from position of tuple w/in row to its descriptor
    std::vector<TupleDescriptor*> _tuple_desc_map;

    // map from TupleId to position of tuple w/in row
    std::vector<int> _tuple_idx_map;
};

// used to describe row position, only used in global late materialization
class RowPositionDescriptor {
public:
    enum Type : uint8_t {
        ICEBERG_V3 = 0,
    };
    RowPositionDescriptor(Type type, SlotId row_source_slot_id, std::vector<SlotId> fetch_ref_slot_ids,
                          std::vector<SlotId> lookup_ref_slot_ids)
            : _type(type),
              _row_source_slot_id(row_source_slot_id),
              _fetch_ref_slot_ids(std::move(fetch_ref_slot_ids)),
              _lookup_ref_slot_ids(std::move(lookup_ref_slot_ids)) {}

    virtual ~RowPositionDescriptor() = default;

    Type type() const { return _type; }

    SlotId get_row_source_slot_id() const { return _row_source_slot_id; }

    const std::vector<SlotId>& get_fetch_ref_slot_ids() const { return _fetch_ref_slot_ids; }
    const std::vector<SlotId>& get_lookup_ref_slot_ids() const { return _lookup_ref_slot_ids; }
    std::string debug_string() const;

    static RowPositionDescriptor* from_thrift(const TRowPositionDescriptor& t_desc, ObjectPool* pool);

protected:
    Type _type;
    SlotId _row_source_slot_id;
    std::vector<SlotId> _fetch_ref_slot_ids;
    std::vector<SlotId> _lookup_ref_slot_ids;
};

class IcebergV3RowPositionDescriptor : public RowPositionDescriptor {
public:
    IcebergV3RowPositionDescriptor(SlotId row_source_slot_id, std::vector<SlotId> fetch_ref_slot_ids,
                                   std::vector<SlotId> lookup_ref_slot_ids)
            : RowPositionDescriptor(ICEBERG_V3, row_source_slot_id, std::move(fetch_ref_slot_ids),
                                    std::move(lookup_ref_slot_ids)) {}
    ~IcebergV3RowPositionDescriptor() override = default;
};

} // namespace starrocks
