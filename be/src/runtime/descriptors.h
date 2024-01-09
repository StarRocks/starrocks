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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/descriptors.h

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

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/stubs/common.h>

#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "gen_cpp/Descriptors_types.h"     // for TTupleId
#include "gen_cpp/FrontendService_types.h" // for TTupleId
#include "gen_cpp/Types_types.h"
#include "runtime/types.h"

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

// Location information for null indicator bit for particular slot.
// For non-nullable slots, the byte_offset will be 0 and the bit_mask will be 0.
// This allows us to do the NullIndicatorOffset operations (tuple + byte_offset &/|
// bit_mask) regardless of whether the slot is nullable or not.
// This is more efficient than branching to check if the slot is non-nullable.
struct NullIndicatorOffset {
    int byte_offset;
    uint8_t bit_mask;   // to extract null indicator
    uint8_t bit_offset; // only used to serialize, from 1 to 8

    NullIndicatorOffset(int byte_offset, int bit_offset_)
            : byte_offset(byte_offset),
              bit_mask(bit_offset_ == -1 ? 0 : 1 << (7 - bit_offset_)),
              bit_offset(bit_offset_) {}

    bool equals(const NullIndicatorOffset& o) const {
        return this->byte_offset == o.byte_offset && this->bit_mask == o.bit_mask;
    }

    std::string debug_string() const;
};

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator);

class SlotDescriptor {
public:
    SlotDescriptor(SlotId id, std::string name, TypeDescriptor type);

    SlotId id() const { return _id; }
    const TypeDescriptor& type() const { return _type; }
    TypeDescriptor& type() { return _type; }
    TupleId parent() const { return _parent; }
    bool is_materialized() const { return _is_materialized; }
    bool is_output_column() const { return _is_output_column; }
    bool is_nullable() const { return _null_indicator_offset.bit_mask != 0; }

    int slot_size() const { return _slot_size; }

    const std::string& col_name() const { return _col_name; }

    void to_protobuf(PSlotDescriptor* pslot) const;

    std::string debug_string() const;

    int32_t col_unique_id() const { return _col_unique_id; }

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
    const NullIndicatorOffset _null_indicator_offset;
    const std::string _col_name;
    const int32_t _col_unique_id;

    // the idx of the slot in the tuple descriptor (0-based).
    // this is provided by the FE
    const int _slot_idx;

    // the byte size of this slot.
    const int _slot_size;

    const bool _is_materialized;
    const bool _is_output_column;

    // @todo: replace _null_indicator_offset when remove _null_indicator_offset
    const bool _is_nullable;

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

// ============== HDFS Table Descriptor ============

class HdfsPartitionDescriptor {
public:
    HdfsPartitionDescriptor(const THdfsTable& thrift_table, const THdfsPartition& thrift_partition);
    HdfsPartitionDescriptor(const THudiTable& thrift_table, const THdfsPartition& thrift_partition);
    HdfsPartitionDescriptor(const TDeltaLakeTable& thrift_table, const THdfsPartition& thrift_partition);
    HdfsPartitionDescriptor(const TIcebergTable& thrift_table, const THdfsPartition& thrift_partition);

    int64_t id() const { return _id; }
    THdfsFileFormat::type file_format() { return _file_format; }
    std::string& location() { return _location; }
    // ExprContext is constant/literal for sure
    // such as hdfs://path/x=1/y=2/zzz, then
    // partition slots would be [x, y]
    // partition key values wold be [1, 2]
    std::vector<ExprContext*>& partition_key_value_evals() { return _partition_key_value_evals; }
    Status create_part_key_exprs(RuntimeState* state, ObjectPool* pool, int32_t chunk_size);

private:
    int64_t _id = 0;
    THdfsFileFormat::type _file_format;
    std::string _location;

    const std::vector<TExpr>& _thrift_partition_key_exprs;
    std::vector<ExprContext*> _partition_key_value_evals;
};

class HiveTableDescriptor : public TableDescriptor {
public:
    HiveTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    virtual bool has_partition() const = 0;
    virtual bool is_partition_col(const SlotDescriptor* slot) const;
    virtual int get_partition_col_index(const SlotDescriptor* slot) const;
    virtual HdfsPartitionDescriptor* get_partition(int64_t partition_id) const;
    virtual bool has_base_path() const { return false; }
    virtual const std::string& get_base_path() const { return _table_location; }

    Status create_key_exprs(RuntimeState* state, ObjectPool* pool, int32_t chunk_size) {
        for (auto& part : _partition_id_to_desc_map) {
            RETURN_IF_ERROR(part.second->create_part_key_exprs(state, pool, chunk_size));
        }
        return Status::OK();
    }

    StatusOr<TPartitionMap*> deserialize_partition_map(const TCompressedPartitionMap& compressed_partition_map,
                                                       ObjectPool* pool);

protected:
    std::string _hdfs_base_path;
    std::vector<TColumn> _columns;
    std::vector<TColumn> _partition_columns;
    std::map<int64_t, HdfsPartitionDescriptor*> _partition_id_to_desc_map;
    std::string _table_location;
};

class HdfsTableDescriptor : public HiveTableDescriptor {
public:
    HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~HdfsTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::map<std::string, std::string> get_serde_properties() const;
    const std::string& get_time_zone() const;

private:
    std::string _serde_lib;
    std::string _input_format;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::map<std::string, std::string> _serde_properties;
    std::string _time_zone;
};

class IcebergTableDescriptor : public HiveTableDescriptor {
public:
    IcebergTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~IcebergTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const TIcebergSchema* get_iceberg_schema() const { return &_t_iceberg_schema; }
    bool is_unpartitioned_table() { return _partition_column_names.empty(); }
    const std::vector<std::string>& partition_column_names() { return _partition_column_names; }
    const std::vector<std::string> full_column_names();
    std::vector<int32_t> partition_index_in_schema();
    bool has_base_path() const override { return true; }

    Status set_partition_desc_map(const TIcebergTable& thrift_table, ObjectPool* pool);

private:
    TIcebergSchema _t_iceberg_schema;
    std::vector<std::string> _partition_column_names;
};

class FileTableDescriptor : public HiveTableDescriptor {
public:
    FileTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~FileTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_table_locations() const;
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::string& get_time_zone() const;

private:
    std::string _serde_lib;
    std::string _input_format;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::string _time_zone;
};

class DeltaLakeTableDescriptor : public HiveTableDescriptor {
public:
    DeltaLakeTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~DeltaLakeTableDescriptor() override = default;
    bool has_partition() const override { return true; }
};

class HudiTableDescriptor : public HiveTableDescriptor {
public:
    HudiTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~HudiTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    const std::string& get_instant_time() const;
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::string& get_time_zone() const;

private:
    std::string _hudi_instant_time;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::string _input_format;
    std::string _serde_lib;
    std::string _time_zone;
};

class PaimonTableDescriptor : public HiveTableDescriptor {
public:
    PaimonTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~PaimonTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_paimon_native_table() const;
    const std::string& get_time_zone() const;

private:
    std::string _paimon_native_table;
    std::string _time_zone;
};

class OdpsTableDescriptor : public HiveTableDescriptor {
public:
    OdpsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~OdpsTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_database_name() const;
    const std::string& get_table_name() const;
    const std::string& get_time_zone() const;

private:
    std::string _database_name;
    std::string _table_name;
    std::string _time_zone;
};

// ===========================================

class OlapTableDescriptor : public TableDescriptor {
public:
    OlapTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
};

class SchemaTableDescriptor : public TableDescriptor {
public:
    SchemaTableDescriptor(const TTableDescriptor& tdesc);
    ~SchemaTableDescriptor() override;
    std::string debug_string() const override;
    TSchemaTableType::type schema_table_type() const { return _schema_table_type; }

private:
    TSchemaTableType::type _schema_table_type;
};

class BrokerTableDescriptor : public TableDescriptor {
public:
    BrokerTableDescriptor(const TTableDescriptor& tdesc);
    ~BrokerTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class EsTableDescriptor : public TableDescriptor {
public:
    EsTableDescriptor(const TTableDescriptor& tdesc);
    ~EsTableDescriptor() override;
    std::string debug_string() const override;

private:
};

class MySQLTableDescriptor : public TableDescriptor {
public:
    MySQLTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    const std::string mysql_db() const { return _mysql_db; }
    const std::string mysql_table() const { return _mysql_table; }
    const std::string host() const { return _host; }
    const std::string port() const { return _port; }
    const std::string user() const { return _user; }
    const std::string passwd() const { return _passwd; }

private:
    std::string _mysql_db;
    std::string _mysql_table;
    std::string _host;
    std::string _port;
    std::string _user;
    std::string _passwd;
};

class JDBCTableDescriptor : public TableDescriptor {
public:
    JDBCTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    const std::string jdbc_driver_name() const { return _jdbc_driver_name; }
    const std::string jdbc_driver_url() const { return _jdbc_driver_url; }
    const std::string jdbc_driver_checksum() const { return _jdbc_driver_checksum; }
    const std::string jdbc_driver_class() const { return _jdbc_driver_class; }
    const std::string jdbc_url() const { return _jdbc_url; }
    const std::string jdbc_table() const { return _jdbc_table; }
    const std::string jdbc_user() const { return _jdbc_user; }
    const std::string jdbc_passwd() const { return _jdbc_passwd; }

private:
    std::string _jdbc_driver_name;
    std::string _jdbc_driver_url;
    std::string _jdbc_driver_checksum;
    std::string _jdbc_driver_class;

    std::string _jdbc_url;
    std::string _jdbc_table;
    std::string _jdbc_user;
    std::string _jdbc_passwd;
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
    RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
                  const std::vector<bool>& nullable_tuples);

    // standard copy c'tor, made explicit here
    RowDescriptor(const RowDescriptor& desc)

            = default;

    RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable);

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

    // _tuple_idx_nullable_map[i] is true if tuple i can be null
    std::vector<bool> _tuple_idx_nullable_map;

    // map from TupleId to position of tuple w/in row
    std::vector<int> _tuple_idx_map;
};

} // namespace starrocks
